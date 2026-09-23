"""Immutable Clean Parquet build, validation, and atomic current publish.

Slice 9 implements D-004.  A classified rental month is materialized into a
staging directory, validated by reading the Parquet back through DuckDB, then
promoted into an immutable ``clean_version_id`` directory.  The only mutable
published state is the tiny month current-pointer JSON, replaced atomically
after every hard gate passes.
"""

from __future__ import annotations

import csv
import hashlib
import json
import os
import re
import shutil
import tempfile
from collections.abc import Mapping
from contextlib import contextmanager
from dataclasses import asdict, dataclass, fields
from datetime import datetime
from decimal import Decimal
from pathlib import Path
from typing import Callable

import duckdb

from .canonical_dq import SEVERITY_QUARANTINE, SEVERITY_WARNING
from .errors import CleanPublishError
from .group_dq import ClassifiedCanonicalRow, GroupDqSummary, classify_registered_source_month
from .manifest import SourceRecord, load_manifest

CLEAN_VERSION_PREFIX = "cv1_"
CONFIGURATION_FINGERPRINT_PREFIX = "cfg1_"
CURRENT_POINTER_SCHEMA_VERSION = 1
METADATA_SCHEMA_VERSION = 1

STATUS_PUBLISHED_CURRENT = "PUBLISHED_CURRENT"
STATUS_REUSED_CURRENT = "REUSED_CURRENT"
STATUS_BUILT_HISTORICAL = "BUILT_HISTORICAL"
STATUS_REUSED_HISTORICAL = "REUSED_HISTORICAL"

_MONTH_PATTERN = re.compile(r"20\d{2}-(?:0[1-9]|1[0-2])")
_CLEAN_VERSION_PATTERN = re.compile(r"cv1_[0-9a-f]{64}")
_CONFIGURATION_FINGERPRINT_PATTERN = re.compile(r"cfg1_[0-9a-f]{64}")
_ISSUE_JSON_SCHEMA = (
    '[{"rule_id":"VARCHAR","severity":"VARCHAR","field":"VARCHAR","reason":"VARCHAR"}]'
)

_CLEAN_SCHEMA = (
    ("source_row_id", "VARCHAR"),
    ("source_version_id", "VARCHAR"),
    ("member_name", "VARCHAR"),
    ("source_row_number", "BIGINT"),
    ("row_content_sha256", "VARCHAR"),
    ("source_month", "VARCHAR"),
    ("schema_version", "VARCHAR"),
    ("bike_id", "VARCHAR"),
    ("rent_at", "TIMESTAMP"),
    ("rent_station_no_raw", "VARCHAR"),
    ("rent_station_no", "VARCHAR"),
    ("rent_station_name", "VARCHAR"),
    ("rent_dock_no", "BIGINT"),
    ("return_at", "TIMESTAMP"),
    ("return_station_no_raw", "VARCHAR"),
    ("return_station_no", "VARCHAR"),
    ("return_station_name", "VARCHAR"),
    ("return_dock_no", "BIGINT"),
    ("usage_min", "BIGINT"),
    ("distance_m", "DECIMAL(38,18)"),
    ("birth_year", "SMALLINT"),
    ("sex_raw", "VARCHAR"),
    ("sex_code", "VARCHAR"),
    ("user_type", "VARCHAR"),
    ("rent_station_internal_id", "VARCHAR"),
    ("return_station_internal_id", "VARCHAR"),
    ("bike_type", "VARCHAR"),
    ("logical_trip_key", "VARCHAR"),
    ("logical_trip_conflict", "BOOLEAN"),
    ("exact_duplicate_group", "BOOLEAN"),
    ("is_completed_trip", "BOOLEAN"),
    ("distance_metric_eligible", "BOOLEAN"),
    ("station_history_match", "BOOLEAN"),
    ("dq_issues", "STRUCT(rule_id VARCHAR, severity VARCHAR, field VARCHAR, reason VARCHAR)[]"),
    ("dq_warning_count", "INTEGER"),
    ("row_quarantined", "BOOLEAN"),
    ("trusted_fact_eligible", "BOOLEAN"),
)

_QUARANTINE_SCHEMA = (
    ("source_row_id", "VARCHAR"),
    ("source_version_id", "VARCHAR"),
    ("source_month", "VARCHAR"),
    ("logical_trip_key", "VARCHAR"),
    ("rule_id", "VARCHAR"),
    ("severity", "VARCHAR"),
    ("field", "VARCHAR"),
    ("reason", "VARCHAR"),
)

_METADATA_FIELDS = {
    "schema_version",
    "source_month",
    "source_version_id",
    "source_artifact_sha256",
    "source_artifact_size_bytes",
    "clean_version_id",
    "transformation_version",
    "configuration",
    "configuration_fingerprint",
    "source_row_count",
    "clean_row_count",
    "trusted_fact_eligible_count",
    "quarantined_row_count",
    "quarantine_issue_count",
    "logical_conflict_group_count",
    "exact_duplicate_group_count",
    "logical_conflict_row_count",
    "exact_duplicate_row_count",
    "schema_versions",
    "clean_parquet",
    "quarantine_parquet",
}


@dataclass(frozen=True)
class CleanPublishResult:
    status: str
    source_month: str
    source_version_id: str
    clean_version_id: str
    clean_version_path: Path
    current_pointer_path: Path
    clean_row_count: int
    trusted_fact_eligible_count: int
    quarantined_row_count: int
    quarantine_issue_count: int


def compute_configuration_fingerprint(configuration: Mapping[str, object]) -> str:
    canonical = _canonical_configuration(configuration)
    return CONFIGURATION_FINGERPRINT_PREFIX + hashlib.sha256(canonical).hexdigest()


def compute_clean_version_id(
    *,
    source_month: str,
    source_version_id: str,
    transformation_version: str,
    configuration_fingerprint: str,
) -> str:
    source_month = _require_month(source_month)
    _require_nonempty_string(source_version_id, "source_version_id")
    _require_nonempty_string(transformation_version, "transformation_version")
    if _CONFIGURATION_FINGERPRINT_PATTERN.fullmatch(configuration_fingerprint) is None:
        raise CleanPublishError(
            "configuration_fingerprint must use the cfg1_ deterministic identity prefix"
        )
    payload = _canonical_json_bytes(
        [source_month, source_version_id, transformation_version, configuration_fingerprint]
    )
    return CLEAN_VERSION_PREFIX + hashlib.sha256(payload).hexdigest()


def publish_clean_month(
    *,
    manifest: str | Path,
    raw_root: str | Path,
    clean_root: str | Path,
    source_version_id: str,
    source_month: str,
    transformation_version: str,
    configuration: Mapping[str, object],
    before_current_publish: Callable[[Path], None] | None = None,
) -> CleanPublishResult:
    """Build, validate, immutably preserve, then atomically publish one Clean month."""
    source_month = _require_month(source_month)
    _require_nonempty_string(transformation_version, "transformation_version")
    canonical_configuration = _canonical_configuration_object(configuration)
    configuration_fingerprint = compute_configuration_fingerprint(canonical_configuration)
    clean_version_id = compute_clean_version_id(
        source_month=source_month,
        source_version_id=source_version_id,
        transformation_version=transformation_version,
        configuration_fingerprint=configuration_fingerprint,
    )
    records = load_manifest(manifest)
    source_index, source_record = _find_source_record(records, source_version_id)

    clean_root_path = Path(clean_root)
    version_path = clean_version_path(clean_root_path, source_month, clean_version_id)
    current_path = current_pointer_path(clean_root_path, source_month)
    initial_pointer = _load_current_pointer_if_present(current_path, source_month=source_month)
    initial_current_metadata: dict[str, object] | None = None
    initial_current_source_index: int | None = None
    if initial_pointer is not None:
        initial_current_path = clean_version_path(
            clean_root_path, source_month, initial_pointer["clean_version_id"]
        )
        initial_current_metadata = validate_clean_version(initial_current_path)
        _require_bundle_binding(
            initial_current_metadata,
            expected_source_month=source_month,
            expected_clean_version_id=initial_pointer["clean_version_id"],
        )
        initial_current_source_index, initial_current_record = _find_source_record(
            records, initial_current_metadata["source_version_id"]
        )
        _require_metadata_matches_source_record(initial_current_metadata, initial_current_record)
        if initial_current_record.dataset_id != source_record.dataset_id:
            raise CleanPublishError(
                "current Clean pointer and candidate source versions belong to different datasets"
            )
    staging_parent = clean_root_path / "staging" / f"source_month={source_month}"
    staging_parent.mkdir(parents=True, exist_ok=True)
    stage_path = Path(
        tempfile.mkdtemp(prefix=f"clean_version_id={clean_version_id}-", dir=staging_parent)
    )

    try:
        group_summary = _build_staging_parquet(
            stage_path=stage_path,
            manifest=manifest,
            raw_root=raw_root,
            source_version_id=source_version_id,
            source_month=source_month,
        )
        metadata = _build_metadata(
            stage_path=stage_path,
            source_record=source_record,
            source_month=source_month,
            clean_version_id=clean_version_id,
            transformation_version=transformation_version,
            configuration=canonical_configuration,
            configuration_fingerprint=configuration_fingerprint,
            group_summary=group_summary,
        )
        _write_json_file(stage_path / "metadata.json", metadata)
        validated = validate_clean_version(stage_path)
        _require_bundle_binding(
            validated,
            expected_source_month=source_month,
            expected_clean_version_id=clean_version_id,
        )
        _require_metadata_matches_source_record(validated, source_record)

        version_reused = False
        if version_path.exists():
            existing = validate_clean_version(version_path)
            _require_bundle_binding(
                existing,
                expected_source_month=source_month,
                expected_clean_version_id=clean_version_id,
            )
            _require_same_immutable_version(existing=existing, candidate=validated)
            version_reused = True
            shutil.rmtree(stage_path)
        else:
            version_path.parent.mkdir(parents=True, exist_ok=True)
            try:
                os.replace(stage_path, version_path)
            except OSError:
                # A concurrent same-input publisher may have won promotion after
                # our existence check. Only converge on a fully valid, identical
                # immutable winner; unrelated promotion failures still propagate.
                if not version_path.exists():
                    raise
                existing = validate_clean_version(version_path)
                _require_bundle_binding(
                    existing,
                    expected_source_month=source_month,
                    expected_clean_version_id=clean_version_id,
                )
                _require_same_immutable_version(existing=existing, candidate=validated)
                version_reused = True
                shutil.rmtree(stage_path)

        pointer = _load_current_pointer_if_present(current_path, source_month=source_month)
        if pointer != initial_pointer:
            raise CleanPublishError(
                "current Clean pointer changed during build; refusing to overwrite concurrent publish"
            )
        if pointer is not None and pointer["clean_version_id"] == clean_version_id:
            return _result_from_metadata(
                status=STATUS_REUSED_CURRENT,
                metadata=validated,
                version_path=version_path,
                current_path=current_path,
            )

        if pointer is not None:
            assert initial_current_metadata is not None
            assert initial_current_source_index is not None
            if initial_current_source_index > source_index:
                return _result_from_metadata(
                    status=(
                        STATUS_REUSED_HISTORICAL if version_reused else STATUS_BUILT_HISTORICAL
                    ),
                    metadata=validated,
                    version_path=version_path,
                    current_path=current_path,
                )

        if before_current_publish is not None:
            before_current_publish(version_path)
        with _current_publish_lock(clean_root_path, source_month, clean_version_id):
            latest_pointer = _load_current_pointer_if_present(
                current_path, source_month=source_month
            )
            if latest_pointer != initial_pointer:
                raise CleanPublishError(
                    "current Clean pointer changed before atomic publish; refusing concurrent rollback"
                )
            _write_current_pointer_atomic(
                current_path,
                source_month=source_month,
                clean_version_id=clean_version_id,
            )
        return _result_from_metadata(
            status=STATUS_PUBLISHED_CURRENT,
            metadata=validated,
            version_path=version_path,
            current_path=current_path,
        )
    except Exception:
        # Failed staging is intentionally retained as diagnostic evidence.  Once
        # promotion succeeded, stage_path no longer exists and the immutable
        # version remains safe even if current-pointer publication fails.
        raise


def clean_version_path(clean_root: str | Path, source_month: str, clean_version_id: str) -> Path:
    source_month = _require_month(source_month)
    if (
        not isinstance(clean_version_id, str)
        or _CLEAN_VERSION_PATTERN.fullmatch(clean_version_id) is None
    ):
        raise CleanPublishError("clean_version_id must use the cv1_ deterministic identity prefix")
    return (
        Path(clean_root)
        / "versions"
        / f"source_month={source_month}"
        / f"clean_version_id={clean_version_id}"
    )


def current_pointer_path(clean_root: str | Path, source_month: str) -> Path:
    source_month = _require_month(source_month)
    return Path(clean_root) / "current" / f"source_month={source_month}.json"


def validate_clean_version(version_path: str | Path) -> dict[str, object]:
    """Hard-validate one immutable Clean version by reading its durable artifacts."""
    path = Path(version_path)
    metadata_path = path / "metadata.json"
    clean_path = path / "clean.parquet"
    quarantine_path = path / "quarantine.parquet"
    metadata = _load_metadata(metadata_path)
    _validate_metadata_identity(metadata)
    _require_file_fingerprint(clean_path, metadata["clean_parquet"], label="clean.parquet")
    _require_file_fingerprint(
        quarantine_path, metadata["quarantine_parquet"], label="quarantine.parquet"
    )

    connection = duckdb.connect(config={"memory_limit": "2GB"})
    try:
        clean_sql = _sql_path(clean_path)
        quarantine_sql = _sql_path(quarantine_path)
        _require_schema(connection, clean_sql, _CLEAN_SCHEMA, label="clean.parquet")
        _require_schema(connection, quarantine_sql, _QUARANTINE_SCHEMA, label="quarantine.parquet")

        source_month = metadata["source_month"]
        source_version_id = metadata["source_version_id"]
        invalid_identity = connection.execute(
            f"""
            SELECT COUNT(*)
            FROM read_parquet('{clean_sql}', hive_partitioning=false)
            WHERE source_month IS DISTINCT FROM ?
               OR source_version_id IS DISTINCT FROM ?
            """,
            [source_month, source_version_id],
        ).fetchone()[0]
        if invalid_identity:
            raise CleanPublishError(
                f"clean.parquet contains {invalid_identity} rows outside requested month/source identity"
            )

        (
            clean_count,
            trusted_count,
            quarantined_count,
            unique_source_rows,
            invalid_eligibility,
            enriched_station_count,
            required_null_count,
            logical_conflict_row_count,
            exact_duplicate_row_count,
            logical_conflict_group_count,
            exact_duplicate_group_count,
        ) = connection.execute(
            f"""
            SELECT
                COUNT(*),
                COUNT(*) FILTER (WHERE trusted_fact_eligible),
                COUNT(*) FILTER (WHERE row_quarantined),
                COUNT(DISTINCT source_row_id),
                COUNT(*) FILTER (
                    WHERE trusted_fact_eligible IS NULL
                       OR row_quarantined IS NULL
                       OR trusted_fact_eligible IS DISTINCT FROM (NOT row_quarantined)
                ),
                COUNT(*) FILTER (WHERE station_history_match IS NOT NULL),
                COUNT(*) FILTER (
                    WHERE source_row_id IS NULL
                       OR source_version_id IS NULL
                       OR member_name IS NULL
                       OR source_row_number IS NULL
                       OR row_content_sha256 IS NULL
                       OR source_month IS NULL
                       OR schema_version IS NULL
                       OR logical_trip_conflict IS NULL
                       OR exact_duplicate_group IS NULL
                       OR is_completed_trip IS NULL
                       OR distance_metric_eligible IS NULL
                       OR dq_issues IS NULL
                       OR dq_warning_count IS NULL
                       OR row_quarantined IS NULL
                       OR trusted_fact_eligible IS NULL
                ),
                COUNT(*) FILTER (WHERE logical_trip_conflict),
                COUNT(*) FILTER (WHERE exact_duplicate_group),
                COUNT(DISTINCT logical_trip_key) FILTER (WHERE logical_trip_conflict),
                COUNT(DISTINCT logical_trip_key) FILTER (WHERE exact_duplicate_group)
            FROM read_parquet('{clean_sql}', hive_partitioning=false)
            """
        ).fetchone()
        if clean_count == 0:
            raise CleanPublishError("clean.parquet must contain at least one canonical row")
        if unique_source_rows != clean_count:
            raise CleanPublishError("clean.parquet source_row_id values are not unique")
        if invalid_eligibility:
            raise CleanPublishError(
                "clean.parquet violates trusted_fact_eligible == NOT row_quarantined"
            )
        if required_null_count:
            raise CleanPublishError(
                "clean.parquet contains NULL in a required canonical/DQ identity field"
            )
        if enriched_station_count:
            raise CleanPublishError(
                "Slice 9 Clean publish requires station_history_match to remain NULL before enrichment"
            )
        if clean_count != trusted_count + quarantined_count:
            raise CleanPublishError(
                "Clean reconciliation failed: clean rows != trusted rows + quarantined rows"
            )

        (
            invalid_issue_field_count,
            invalid_issue_severity_count,
            issue_state_mismatch_count,
        ) = connection.execute(
            f"""
            WITH clean AS (
                SELECT *
                FROM read_parquet('{clean_sql}', hive_partitioning=false)
            ), exploded AS (
                SELECT
                    c.source_row_id,
                    issue.rule_id AS rule_id,
                    issue.severity AS severity,
                    issue.field AS field,
                    issue.reason AS reason
                FROM clean AS c,
                     UNNEST(c.dq_issues) AS issues(issue)
            ), issue_stats AS (
                SELECT
                    source_row_id,
                    COUNT(*) FILTER (
                        WHERE rule_id IS NULL OR trim(rule_id) = ''
                           OR severity IS NULL OR trim(severity) = ''
                           OR field IS NULL OR trim(field) = ''
                           OR reason IS NULL OR trim(reason) = ''
                    ) AS invalid_issue_fields,
                    COUNT(*) FILTER (
                        WHERE severity IS NOT NULL
                          AND severity NOT IN (?, ?)
                    ) AS invalid_issue_severity,
                    COUNT(*) FILTER (WHERE severity = ?) > 0 AS expected_quarantined,
                    COUNT(DISTINCT rule_id) FILTER (WHERE severity = ?)
                        AS expected_warning_count
                FROM exploded
                GROUP BY source_row_id
            )
            SELECT
                COALESCE(SUM(s.invalid_issue_fields), 0),
                COALESCE(SUM(s.invalid_issue_severity), 0),
                COUNT(*) FILTER (
                    WHERE c.row_quarantined IS DISTINCT FROM COALESCE(s.expected_quarantined, FALSE)
                       OR c.dq_warning_count IS DISTINCT FROM COALESCE(s.expected_warning_count, 0)
                )
            FROM clean AS c
            LEFT JOIN issue_stats AS s USING (source_row_id)
            """,
            [
                SEVERITY_WARNING,
                SEVERITY_QUARANTINE,
                SEVERITY_QUARANTINE,
                SEVERITY_WARNING,
            ],
        ).fetchone()
        if invalid_issue_field_count:
            raise CleanPublishError(
                "clean.parquet contains DQ issue with missing/empty rule_id, severity, field, or reason"
            )
        if invalid_issue_severity_count:
            raise CleanPublishError(
                "clean.parquet contains DQ issue severity outside WARNING/QUARANTINE contract"
            )
        if issue_state_mismatch_count:
            raise CleanPublishError(
                "clean.parquet DQ issue semantics do not reconcile with row_quarantined/dq_warning_count"
            )

        quarantine_issue_count = connection.execute(
            f"SELECT COUNT(*) FROM read_parquet('{quarantine_sql}', hive_partitioning=false)"
        ).fetchone()[0]
        quarantine_distinct_rows = connection.execute(
            f"SELECT COUNT(DISTINCT source_row_id) FROM read_parquet('{quarantine_sql}', hive_partitioning=false)"
        ).fetchone()[0]
        if quarantine_distinct_rows != quarantined_count:
            raise CleanPublishError(
                "quarantine distinct source_row_id set does not reconcile with quarantined Clean rows"
            )

        set_difference = connection.execute(
            f"""
            WITH clean_q AS (
                SELECT DISTINCT source_row_id
                FROM read_parquet('{clean_sql}', hive_partitioning=false)
                WHERE row_quarantined
            ), side_q AS (
                SELECT DISTINCT source_row_id
                FROM read_parquet('{quarantine_sql}', hive_partitioning=false)
            )
            SELECT
                (SELECT COUNT(*) FROM (SELECT * FROM clean_q EXCEPT SELECT * FROM side_q)),
                (SELECT COUNT(*) FROM (SELECT * FROM side_q EXCEPT SELECT * FROM clean_q))
            """
        ).fetchone()
        if set_difference != (0, 0):
            raise CleanPublishError(
                "quarantine source_row_id set is not exactly the filtered quarantined Clean set"
            )

        issue_difference = connection.execute(
            f"""
            WITH exploded AS (
                SELECT
                    c.source_row_id,
                    c.source_version_id,
                    c.source_month,
                    c.logical_trip_key,
                    issue.rule_id AS rule_id,
                    issue.severity AS severity,
                    issue.field AS field,
                    issue.reason AS reason
                FROM read_parquet('{clean_sql}', hive_partitioning=false) AS c,
                     UNNEST(c.dq_issues) AS issues(issue)
                WHERE c.row_quarantined
            ), side AS (
                SELECT * FROM read_parquet('{quarantine_sql}', hive_partitioning=false)
            )
            SELECT
                (SELECT COUNT(*) FROM (SELECT * FROM exploded EXCEPT ALL SELECT * FROM side)),
                (SELECT COUNT(*) FROM (SELECT * FROM side EXCEPT ALL SELECT * FROM exploded))
            """
        ).fetchone()
        if issue_difference != (0, 0):
            raise CleanPublishError(
                "quarantine.parquet is not the exact issue-grain explosion of quarantined Clean rows"
            )

        schema_versions = [
            row[0]
            for row in connection.execute(
                f"SELECT DISTINCT schema_version FROM read_parquet('{clean_sql}', hive_partitioning=false) ORDER BY schema_version"
            ).fetchall()
        ]
    except duckdb.Error as exc:
        raise CleanPublishError(f"Clean Parquet validation failed: {exc}") from exc
    finally:
        connection.close()

    expected_counts = {
        "source_row_count": clean_count,
        "clean_row_count": clean_count,
        "trusted_fact_eligible_count": trusted_count,
        "quarantined_row_count": quarantined_count,
        "quarantine_issue_count": quarantine_issue_count,
        "logical_conflict_group_count": logical_conflict_group_count,
        "exact_duplicate_group_count": exact_duplicate_group_count,
        "logical_conflict_row_count": logical_conflict_row_count,
        "exact_duplicate_row_count": exact_duplicate_row_count,
        "schema_versions": schema_versions,
    }
    for key, actual in expected_counts.items():
        if metadata[key] != actual:
            raise CleanPublishError(
                f"metadata {key}={metadata[key]!r} does not match Parquet value {actual!r}"
            )
    return metadata


def _build_staging_parquet(
    *,
    stage_path: Path,
    manifest: str | Path,
    raw_root: str | Path,
    source_version_id: str,
    source_month: str,
) -> GroupDqSummary:
    stage_csv = stage_path / "classified-rows.csv"
    with stage_csv.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle, lineterminator="\n")

        def write_row(row: ClassifiedCanonicalRow) -> None:
            _validate_clean_scalar_ranges(row)
            writer.writerow((_serialize_classified_row(row),))

        summary = classify_registered_source_month(
            manifest=manifest,
            raw_root=raw_root,
            source_version_id=source_version_id,
            source_month=source_month,
            on_row=write_row,
        )

    database_path = stage_path / "clean-build.duckdb"
    connection = duckdb.connect(
        str(database_path),
        config={
            "memory_limit": "5GB",
            "threads": "2",
            "preserve_insertion_order": "false",
        },
    )
    try:
        stage_sql = _sql_path(stage_csv)
        connection.execute("CREATE TABLE classified_json(row_payload VARCHAR NOT NULL)")
        connection.execute(
            f"COPY classified_json FROM '{stage_sql}' (FORMAT CSV, HEADER FALSE)"
        )
        issue_schema = _ISSUE_JSON_SCHEMA.replace("'", "''")
        connection.execute(
            f"""
            CREATE TABLE clean_rows AS
            SELECT
                json_extract_string(row_payload, '$.source_row_id')::VARCHAR AS source_row_id,
                json_extract_string(row_payload, '$.source_version_id')::VARCHAR AS source_version_id,
                json_extract_string(row_payload, '$.member_name')::VARCHAR AS member_name,
                json_extract_string(row_payload, '$.source_row_number')::BIGINT AS source_row_number,
                json_extract_string(row_payload, '$.row_content_sha256')::VARCHAR AS row_content_sha256,
                json_extract_string(row_payload, '$.source_month')::VARCHAR AS source_month,
                json_extract_string(row_payload, '$.schema_version')::VARCHAR AS schema_version,
                json_extract_string(row_payload, '$.bike_id')::VARCHAR AS bike_id,
                json_extract_string(row_payload, '$.rent_at')::TIMESTAMP AS rent_at,
                json_extract_string(row_payload, '$.rent_station_no_raw')::VARCHAR AS rent_station_no_raw,
                json_extract_string(row_payload, '$.rent_station_no')::VARCHAR AS rent_station_no,
                json_extract_string(row_payload, '$.rent_station_name')::VARCHAR AS rent_station_name,
                json_extract_string(row_payload, '$.rent_dock_no')::BIGINT AS rent_dock_no,
                json_extract_string(row_payload, '$.return_at')::TIMESTAMP AS return_at,
                json_extract_string(row_payload, '$.return_station_no_raw')::VARCHAR AS return_station_no_raw,
                json_extract_string(row_payload, '$.return_station_no')::VARCHAR AS return_station_no,
                json_extract_string(row_payload, '$.return_station_name')::VARCHAR AS return_station_name,
                json_extract_string(row_payload, '$.return_dock_no')::BIGINT AS return_dock_no,
                json_extract_string(row_payload, '$.usage_min')::BIGINT AS usage_min,
                json_extract_string(row_payload, '$.distance_m')::DECIMAL(38,18) AS distance_m,
                json_extract_string(row_payload, '$.birth_year')::SMALLINT AS birth_year,
                json_extract_string(row_payload, '$.sex_raw')::VARCHAR AS sex_raw,
                json_extract_string(row_payload, '$.sex_code')::VARCHAR AS sex_code,
                json_extract_string(row_payload, '$.user_type')::VARCHAR AS user_type,
                json_extract_string(row_payload, '$.rent_station_internal_id')::VARCHAR AS rent_station_internal_id,
                json_extract_string(row_payload, '$.return_station_internal_id')::VARCHAR AS return_station_internal_id,
                json_extract_string(row_payload, '$.bike_type')::VARCHAR AS bike_type,
                json_extract_string(row_payload, '$.logical_trip_key')::VARCHAR AS logical_trip_key,
                json_extract_string(row_payload, '$.logical_trip_conflict')::BOOLEAN AS logical_trip_conflict,
                json_extract_string(row_payload, '$.exact_duplicate_group')::BOOLEAN AS exact_duplicate_group,
                json_extract_string(row_payload, '$.is_completed_trip')::BOOLEAN AS is_completed_trip,
                json_extract_string(row_payload, '$.distance_metric_eligible')::BOOLEAN AS distance_metric_eligible,
                CAST(NULL AS BOOLEAN) AS station_history_match,
                from_json(json_extract(row_payload, '$.dq_issues'), '{issue_schema}') AS dq_issues,
                json_extract_string(row_payload, '$.dq_warning_count')::INTEGER AS dq_warning_count,
                json_extract_string(row_payload, '$.row_quarantined')::BOOLEAN AS row_quarantined,
                json_extract_string(row_payload, '$.trusted_fact_eligible')::BOOLEAN AS trusted_fact_eligible
            FROM classified_json
            """
        )
        clean_sql = _sql_path(stage_path / "clean.parquet")
        quarantine_sql = _sql_path(stage_path / "quarantine.parquet")
        connection.execute(
            f"""
            COPY (
                SELECT * FROM clean_rows ORDER BY source_row_id
            ) TO '{clean_sql}' (FORMAT PARQUET, COMPRESSION ZSTD)
            """
        )
        connection.execute(
            f"""
            COPY (
                SELECT
                    c.source_row_id,
                    c.source_version_id,
                    c.source_month,
                    c.logical_trip_key,
                    issue.rule_id AS rule_id,
                    issue.severity AS severity,
                    issue.field AS field,
                    issue.reason AS reason
                FROM clean_rows AS c,
                     UNNEST(c.dq_issues) AS issues(issue)
                WHERE c.row_quarantined
                ORDER BY
                    c.source_row_id,
                    issue.rule_id,
                    issue.severity,
                    issue.field,
                    issue.reason
            ) TO '{quarantine_sql}' (FORMAT PARQUET, COMPRESSION ZSTD)
            """
        )
    except duckdb.Error as exc:
        raise CleanPublishError(f"Clean staging Parquet build failed: {exc}") from exc
    finally:
        connection.close()
    stage_csv.unlink(missing_ok=True)
    database_path.unlink(missing_ok=True)
    return summary


def _build_metadata(
    *,
    stage_path: Path,
    source_record: SourceRecord,
    source_month: str,
    clean_version_id: str,
    transformation_version: str,
    configuration: dict[str, object],
    configuration_fingerprint: str,
    group_summary: GroupDqSummary,
) -> dict[str, object]:
    connection = duckdb.connect(config={"memory_limit": "2GB"})
    try:
        clean_sql = _sql_path(stage_path / "clean.parquet")
        quarantine_sql = _sql_path(stage_path / "quarantine.parquet")
        schema_versions = [
            row[0]
            for row in connection.execute(
                f"SELECT DISTINCT schema_version FROM read_parquet('{clean_sql}', hive_partitioning=false) ORDER BY schema_version"
            ).fetchall()
        ]
        quarantine_issue_count = connection.execute(
            f"SELECT COUNT(*) FROM read_parquet('{quarantine_sql}', hive_partitioning=false)"
        ).fetchone()[0]
    except duckdb.Error as exc:
        raise CleanPublishError(f"cannot summarize staged Clean Parquet: {exc}") from exc
    finally:
        connection.close()

    clean_size, clean_sha = _fingerprint_file(stage_path / "clean.parquet")
    quarantine_size, quarantine_sha = _fingerprint_file(stage_path / "quarantine.parquet")
    return {
        "schema_version": METADATA_SCHEMA_VERSION,
        "source_month": source_month,
        "source_version_id": source_record.source_version_id,
        "source_artifact_sha256": source_record.sha256,
        "source_artifact_size_bytes": source_record.size_bytes,
        "clean_version_id": clean_version_id,
        "transformation_version": transformation_version,
        "configuration": configuration,
        "configuration_fingerprint": configuration_fingerprint,
        "source_row_count": group_summary.clean_row_count,
        "clean_row_count": group_summary.clean_row_count,
        "trusted_fact_eligible_count": group_summary.trusted_fact_eligible_count,
        "quarantined_row_count": group_summary.quarantined_row_count,
        "quarantine_issue_count": int(quarantine_issue_count),
        "logical_conflict_group_count": group_summary.logical_conflict_group_count,
        "exact_duplicate_group_count": group_summary.exact_duplicate_group_count,
        "logical_conflict_row_count": group_summary.logical_conflict_row_count,
        "exact_duplicate_row_count": group_summary.exact_duplicate_row_count,
        "schema_versions": schema_versions,
        "clean_parquet": {"size_bytes": clean_size, "sha256": clean_sha},
        "quarantine_parquet": {"size_bytes": quarantine_size, "sha256": quarantine_sha},
    }


def _serialize_classified_row(row: ClassifiedCanonicalRow) -> str:
    payload: dict[str, object] = {}
    for field in fields(ClassifiedCanonicalRow):
        value = getattr(row, field.name)
        if isinstance(value, datetime):
            payload[field.name] = value.isoformat(sep=" ")
        elif isinstance(value, Decimal):
            payload[field.name] = str(value)
        elif field.name == "dq_issues":
            payload[field.name] = [asdict(issue) for issue in value]
        else:
            payload[field.name] = value
    return _canonical_json_text(payload)


def _validate_clean_scalar_ranges(row: ClassifiedCanonicalRow) -> None:
    if row.birth_year is not None and not -(2**15) <= row.birth_year <= 2**15 - 1:
        raise CleanPublishError(
            f"birth_year {row.birth_year} cannot be represented by Clean SMALLINT contract"
        )
    if row.distance_m is not None:
        value = row.distance_m
        scale = max(-value.as_tuple().exponent, 0)
        integer_digits = max(value.adjusted() + 1, 0) if value else 1
        if scale > 18 or integer_digits > 20:
            raise CleanPublishError(
                f"distance_m {value} cannot be represented exactly by Clean DECIMAL(38,18) contract"
            )


def _load_metadata(path: Path) -> dict[str, object]:
    try:
        with path.open("r", encoding="utf-8") as handle:
            payload = json.load(handle, object_pairs_hook=_reject_duplicate_pairs)
    except FileNotFoundError as exc:
        raise CleanPublishError(f"missing immutable Clean metadata: {path}") from exc
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise CleanPublishError(f"cannot read immutable Clean metadata {path}: {exc}") from exc
    if not isinstance(payload, dict) or set(payload) != _METADATA_FIELDS:
        raise CleanPublishError(f"{path}: metadata fields do not match D-004 contract")
    return payload


def _validate_metadata_identity(metadata: dict[str, object]) -> None:
    if metadata["schema_version"] != METADATA_SCHEMA_VERSION:
        raise CleanPublishError("unsupported Clean metadata schema_version")
    source_month = _require_month(metadata["source_month"])
    source_version_id = _require_nonempty_string(metadata["source_version_id"], "source_version_id")
    transformation_version = _require_nonempty_string(
        metadata["transformation_version"], "transformation_version"
    )
    configuration = metadata["configuration"]
    if not isinstance(configuration, dict):
        raise CleanPublishError("metadata configuration must be a JSON object")
    configuration_fingerprint = compute_configuration_fingerprint(configuration)
    if metadata["configuration_fingerprint"] != configuration_fingerprint:
        raise CleanPublishError("metadata configuration_fingerprint does not match configuration")
    expected_clean_version_id = compute_clean_version_id(
        source_month=source_month,
        source_version_id=source_version_id,
        transformation_version=transformation_version,
        configuration_fingerprint=configuration_fingerprint,
    )
    if metadata["clean_version_id"] != expected_clean_version_id:
        raise CleanPublishError("metadata clean_version_id does not match its deterministic inputs")
    if not isinstance(metadata["source_artifact_sha256"], str) or re.fullmatch(
        r"[0-9a-f]{64}", metadata["source_artifact_sha256"]
    ) is None:
        raise CleanPublishError("metadata source_artifact_sha256 is invalid")
    if not isinstance(metadata["source_artifact_size_bytes"], int) or isinstance(
        metadata["source_artifact_size_bytes"], bool
    ):
        raise CleanPublishError("metadata source_artifact_size_bytes must be an integer")
    for key in (
        "source_row_count",
        "clean_row_count",
        "trusted_fact_eligible_count",
        "quarantined_row_count",
        "quarantine_issue_count",
        "logical_conflict_group_count",
        "exact_duplicate_group_count",
        "logical_conflict_row_count",
        "exact_duplicate_row_count",
    ):
        if not isinstance(metadata[key], int) or isinstance(metadata[key], bool) or metadata[key] < 0:
            raise CleanPublishError(f"metadata {key} must be a non-negative integer")
    if not isinstance(metadata["schema_versions"], list) or not all(
        isinstance(item, str) and item for item in metadata["schema_versions"]
    ):
        raise CleanPublishError("metadata schema_versions must be a list of non-empty strings")


def _require_schema(
    connection: duckdb.DuckDBPyConnection,
    parquet_sql_path: str,
    expected: tuple[tuple[str, str], ...],
    *,
    label: str,
) -> None:
    actual = tuple(
        (row[0], row[1])
        for row in connection.execute(
            f"DESCRIBE SELECT * FROM read_parquet('{parquet_sql_path}', hive_partitioning=false)"
        ).fetchall()
    )
    if actual != expected:
        raise CleanPublishError(f"{label} schema mismatch: expected {expected!r}, got {actual!r}")


def _require_file_fingerprint(path: Path, metadata_value: object, *, label: str) -> None:
    if not isinstance(metadata_value, dict) or set(metadata_value) != {"size_bytes", "sha256"}:
        raise CleanPublishError(f"metadata {label} fingerprint is malformed")
    size, sha256 = _fingerprint_file(path)
    if metadata_value != {"size_bytes": size, "sha256": sha256}:
        raise CleanPublishError(f"{label} fingerprint does not match immutable metadata")


def _require_same_immutable_version(
    *, existing: dict[str, object], candidate: dict[str, object]
) -> None:
    if existing != candidate:
        raise CleanPublishError(
            "clean_version_id collision: existing immutable version metadata differs from rebuilt output"
        )


def _require_metadata_matches_source_record(
    metadata: dict[str, object], source_record: SourceRecord
) -> None:
    if metadata["source_version_id"] != source_record.source_version_id:
        raise CleanPublishError("Clean metadata source_version_id does not match source manifest")
    if metadata["source_artifact_sha256"] != source_record.sha256:
        raise CleanPublishError("Clean metadata source artifact hash does not match source manifest")
    if metadata["source_artifact_size_bytes"] != source_record.size_bytes:
        raise CleanPublishError("Clean metadata source artifact size does not match source manifest")


def _require_bundle_binding(
    metadata: dict[str, object],
    *,
    expected_source_month: str,
    expected_clean_version_id: str,
) -> None:
    if metadata["source_month"] != expected_source_month:
        raise CleanPublishError(
            "Clean bundle metadata source_month does not match its requested/current binding"
        )
    if metadata["clean_version_id"] != expected_clean_version_id:
        raise CleanPublishError(
            "Clean bundle metadata clean_version_id does not match its directory/current binding"
        )


def _load_current_pointer_if_present(path: Path, *, source_month: str) -> dict[str, object] | None:
    if not path.exists():
        return None
    try:
        with path.open("r", encoding="utf-8") as handle:
            payload = json.load(handle, object_pairs_hook=_reject_duplicate_pairs)
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise CleanPublishError(f"cannot read current Clean pointer {path}: {exc}") from exc
    expected_fields = {"schema_version", "source_month", "clean_version_id"}
    if not isinstance(payload, dict) or set(payload) != expected_fields:
        raise CleanPublishError(f"current Clean pointer {path} has invalid fields")
    if payload["schema_version"] != CURRENT_POINTER_SCHEMA_VERSION:
        raise CleanPublishError(f"current Clean pointer {path} has unsupported schema_version")
    if payload["source_month"] != source_month:
        raise CleanPublishError(f"current Clean pointer {path} names the wrong source_month")
    clean_version_id = payload["clean_version_id"]
    if (
        not isinstance(clean_version_id, str)
        or _CLEAN_VERSION_PATTERN.fullmatch(clean_version_id) is None
    ):
        raise CleanPublishError(f"current Clean pointer {path} has invalid clean_version_id")
    return payload


def _write_current_pointer_atomic(path: Path, *, source_month: str, clean_version_id: str) -> None:
    payload = {
        "schema_version": CURRENT_POINTER_SCHEMA_VERSION,
        "source_month": source_month,
        "clean_version_id": clean_version_id,
    }
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, temp_name = tempfile.mkstemp(prefix=f".{path.name}.", suffix=".tmp", dir=path.parent)
    temp_path = Path(temp_name)
    try:
        with os.fdopen(fd, "w", encoding="utf-8", newline="\n") as handle:
            handle.write(_canonical_json_text(payload))
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temp_path, path)
    finally:
        temp_path.unlink(missing_ok=True)


@contextmanager
def _current_publish_lock(
    clean_root: Path, source_month: str, clean_version_id: str
):
    lock_path = clean_root / "locks" / f"source_month={source_month}.lock"
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    handle = lock_path.open("a+b")
    try:
        handle.seek(0, os.SEEK_END)
        if handle.tell() == 0:
            handle.write(b"0")
            handle.flush()
            os.fsync(handle.fileno())
        try:
            _lock_file_nonblocking(handle)
        except (BlockingIOError, OSError) as exc:
            raise CleanPublishError(
                f"another same-month Clean publish holds lock {lock_path}; current pointer unchanged"
            ) from exc
        handle.seek(1)
        handle.truncate()
        handle.write((clean_version_id + "\n").encode("utf-8"))
        handle.flush()
        os.fsync(handle.fileno())
        yield
    finally:
        try:
            _unlock_file(handle)
        finally:
            handle.close()


def _lock_file_nonblocking(handle) -> None:
    if os.name == "nt":
        import msvcrt

        handle.seek(0)
        try:
            msvcrt.locking(handle.fileno(), msvcrt.LK_NBLCK, 1)
        except OSError as exc:
            raise BlockingIOError from exc
        return
    import fcntl

    try:
        fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
    except OSError as exc:
        raise BlockingIOError from exc


def _unlock_file(handle) -> None:
    if handle.closed:
        return
    if os.name == "nt":
        import msvcrt

        handle.seek(0)
        try:
            msvcrt.locking(handle.fileno(), msvcrt.LK_UNLCK, 1)
        except OSError:
            pass
        return
    import fcntl

    try:
        fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
    except OSError:
        pass


def _write_json_file(path: Path, payload: dict[str, object]) -> None:
    with path.open("w", encoding="utf-8", newline="\n") as handle:
        handle.write(_canonical_json_text(payload))
        handle.write("\n")
        handle.flush()
        os.fsync(handle.fileno())


def _result_from_metadata(
    *, status: str, metadata: dict[str, object], version_path: Path, current_path: Path
) -> CleanPublishResult:
    return CleanPublishResult(
        status=status,
        source_month=metadata["source_month"],
        source_version_id=metadata["source_version_id"],
        clean_version_id=metadata["clean_version_id"],
        clean_version_path=version_path,
        current_pointer_path=current_path,
        clean_row_count=metadata["clean_row_count"],
        trusted_fact_eligible_count=metadata["trusted_fact_eligible_count"],
        quarantined_row_count=metadata["quarantined_row_count"],
        quarantine_issue_count=metadata["quarantine_issue_count"],
    )


def _find_source_record(
    records: list[SourceRecord], source_version_id: str
) -> tuple[int, SourceRecord]:
    for index, record in enumerate(records):
        if record.source_version_id == source_version_id:
            return index, record
    raise CleanPublishError(f"source_version_id {source_version_id!r} is not registered")


def _fingerprint_file(path: Path) -> tuple[int, str]:
    digest = hashlib.sha256()
    size = 0
    try:
        with path.open("rb") as handle:
            for chunk in iter(lambda: handle.read(1024 * 1024), b""):
                size += len(chunk)
                digest.update(chunk)
    except OSError as exc:
        raise CleanPublishError(f"cannot fingerprint {path}: {exc}") from exc
    return size, digest.hexdigest()


def _canonical_configuration(configuration: Mapping[str, object]) -> bytes:
    return _canonical_json_bytes(_canonical_configuration_object(configuration))


def _canonical_configuration_object(configuration: Mapping[str, object]) -> dict[str, object]:
    if not isinstance(configuration, Mapping):
        raise CleanPublishError("configuration must be a JSON-compatible mapping")
    try:
        encoded = _canonical_json_text(dict(configuration))
        decoded = json.loads(encoded)
    except (TypeError, ValueError) as exc:
        raise CleanPublishError(f"configuration must be JSON-compatible: {exc}") from exc
    if not isinstance(decoded, dict):
        raise CleanPublishError("configuration must serialize to a JSON object")
    return decoded


def _canonical_json_bytes(value: object) -> bytes:
    return _canonical_json_text(value).encode("utf-8")


def _canonical_json_text(value: object) -> str:
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    )


def _require_month(value: object) -> str:
    if not isinstance(value, str) or _MONTH_PATTERN.fullmatch(value) is None:
        raise CleanPublishError(f"source_month must be YYYY-MM, got {value!r}")
    return value


def _require_nonempty_string(value: object, name: str) -> str:
    if not isinstance(value, str) or not value:
        raise CleanPublishError(f"{name} must be a non-empty string")
    return value


def _reject_duplicate_pairs(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise CleanPublishError(f"duplicate JSON key {key!r} is not allowed")
        result[key] = value
    return result


def _sql_path(path: Path) -> str:
    return path.resolve().as_posix().replace("'", "''")
