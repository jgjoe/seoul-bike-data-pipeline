"""Build, validate, and atomically publish source-month quality marts."""

from __future__ import annotations

import hashlib
import json
import math
import os
import re
import shutil
import tempfile
from collections.abc import Mapping
from contextlib import ExitStack, contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Callable

import duckdb

from .clean_publish import (
    clean_version_path,
    current_pointer_path as clean_current_pointer_path,
    validate_clean_version,
)
from .errors import SourceMonthQualityMartError
from .fact_publish import (
    fact_current_pointer_path,
    fact_version_path,
    validate_fact_trip_version,
)
from .manifest import SourceRecord, load_manifest
from .raw import verify_raw_artifact
from .zip_months import STATUS_UNCHANGED as ZIP_STATUS_UNCHANGED, plan_zip_months

CORE_START_MONTH = "2020-01"
CORE_END_MONTH = "2026-06"

MART_VERSION_PREFIX = "msq1_"
CONFIGURATION_FINGERPRINT_PREFIX = "cfg1_"
CURRENT_POINTER_SCHEMA_VERSION = 1
METADATA_SCHEMA_VERSION = 1
STATUS_PUBLISHED_CURRENT = "PUBLISHED_CURRENT"
STATUS_REUSED_CURRENT = "REUSED_CURRENT"

_MART_VERSION = re.compile(r"msq1_[0-9a-f]{64}")
_SOURCE_VERSION = re.compile(r"sv1_[0-9a-f]{64}")
_CLEAN_VERSION = re.compile(r"cv1_[0-9a-f]{64}")
_FACT_VERSION = re.compile(r"fv1_[0-9a-f]{64}")
_CONFIGURATION_FINGERPRINT = re.compile(r"cfg1_[0-9a-f]{64}")
_SHA256 = re.compile(r"[0-9a-f]{64}")
_MONTH = re.compile(r"20[0-9]{2}-(?:0[1-9]|1[0-2])")
_DEFAULT_SQL_PATH = Path(__file__).with_name("sql") / "mart_source_month_quality.sql"

_MART_SCHEMA = (
    ("source_month", "VARCHAR"),
    ("source_version_id", "VARCHAR"),
    ("source_row_count", "BIGINT"),
    ("clean_row_count", "BIGINT"),
    ("trusted_fact_count", "BIGINT"),
    ("quarantine_count", "BIGINT"),
    ("logical_conflict_group_count", "BIGINT"),
    ("exact_duplicate_group_count", "BIGINT"),
    ("incomplete_trip_count", "BIGINT"),
    ("station_unmatched_count", "BIGINT"),
    ("distance_zero_rate", "DOUBLE"),
    ("extreme_distance_count", "BIGINT"),
    ("invalid_birth_year_rate", "DOUBLE"),
    ("sex_missing_rate", "DOUBLE"),
    ("schema_version", "VARCHAR"),
    ("artifact_hash", "VARCHAR"),
    ("pipeline_version", "VARCHAR"),
    ("clean_version_id", "VARCHAR"),
    ("fact_version_id", "VARCHAR"),
)

_METADATA_FIELDS = {
    "schema_version",
    "source_month",
    "source_version_id",
    "clean_version_id",
    "fact_version_id",
    "transformation_version",
    "configuration",
    "configuration_fingerprint",
    "sql_sha256",
    "mart_version_id",
    "mart_row_count",
    "source_row_count",
    "clean_row_count",
    "trusted_fact_count",
    "quarantine_count",
    "warehouse_database",
}


@dataclass(frozen=True)
class SourceMonthQualityMartPublishResult:
    status: str
    source_month: str
    source_version_id: str
    mart_version_id: str
    clean_version_id: str
    fact_version_id: str
    version_path: Path
    current_pointer_path: Path
    source_row_count: int
    clean_row_count: int
    trusted_fact_count: int
    quarantine_count: int


@dataclass(frozen=True)
class SourceMonthQualityRevisionImpact:
    source_month: str
    old_source_version_id: str
    new_source_version_id: str
    affected_source_months: tuple[str, ...]


def compute_configuration_fingerprint(configuration: Mapping[str, object]) -> str:
    canonical = _canonical_configuration_object(configuration)
    return CONFIGURATION_FINGERPRINT_PREFIX + hashlib.sha256(
        _canonical_json_bytes(canonical)
    ).hexdigest()


def compute_source_month_quality_mart_version_id(
    *,
    source_month: str,
    source_version_id: str,
    clean_version_id: str,
    fact_version_id: str,
    transformation_version: str,
    configuration_fingerprint: str,
    sql_sha256: str,
) -> str:
    source_month = _require_source_month(source_month)
    _require_version(source_version_id, _SOURCE_VERSION, "source_version_id")
    _require_version(clean_version_id, _CLEAN_VERSION, "clean_version_id")
    _require_version(fact_version_id, _FACT_VERSION, "fact_version_id")
    _require_text(transformation_version, "transformation_version")
    if _CONFIGURATION_FINGERPRINT.fullmatch(configuration_fingerprint) is None:
        raise SourceMonthQualityMartError(
            "configuration_fingerprint must use cfg1_<sha256>"
        )
    if _SHA256.fullmatch(sql_sha256) is None:
        raise SourceMonthQualityMartError(
            "sql_sha256 must be a lowercase SHA-256 digest"
        )
    payload = [
        source_month,
        source_version_id,
        clean_version_id,
        fact_version_id,
        transformation_version,
        configuration_fingerprint,
        sql_sha256,
    ]
    return MART_VERSION_PREFIX + hashlib.sha256(
        _canonical_json_bytes(payload)
    ).hexdigest()


def source_month_quality_mart_root(warehouse_root: str | Path) -> Path:
    return Path(warehouse_root) / "mart_source_month_quality"


def source_month_quality_mart_version_path(
    warehouse_root: str | Path,
    source_month: str,
    mart_version_id: str,
) -> Path:
    source_month = _require_source_month(source_month)
    _require_version(mart_version_id, _MART_VERSION, "mart_version_id")
    return (
        source_month_quality_mart_root(warehouse_root)
        / "versions"
        / f"source_month={source_month}"
        / f"mart_version_id={mart_version_id}"
    )


def source_month_quality_mart_current_pointer_path(
    warehouse_root: str | Path, source_month: str
) -> Path:
    source_month = _require_source_month(source_month)
    return (
        source_month_quality_mart_root(warehouse_root)
        / "current"
        / f"source_month={source_month}.json"
    )


def plan_source_month_quality_revision_impact(
    *,
    manifest: str | Path,
    raw_root: str | Path,
    source_month: str,
    old_source_version_id: str,
    new_source_version_id: str,
) -> SourceMonthQualityRevisionImpact:
    """A rental source revision affects only its rental source month quality row."""
    source_month = _require_source_month(source_month)
    _require_version(old_source_version_id, _SOURCE_VERSION, "old_source_version_id")
    _require_version(new_source_version_id, _SOURCE_VERSION, "new_source_version_id")
    records = load_manifest(manifest)
    old_index, old_record = _find_source_record_with_index(
        records, old_source_version_id
    )
    new_index, new_record = _find_source_record_with_index(
        records, new_source_version_id
    )
    if old_record.source_version_id == new_record.source_version_id:
        raise SourceMonthQualityMartError(
            "old and new source versions must be different"
        )
    if old_index >= new_index:
        raise SourceMonthQualityMartError(
            "old source version must appear before new source version in append-only manifest"
        )
    if (
        old_record.dataset_id != new_record.dataset_id
        or old_record.logical_period != new_record.logical_period
    ):
        raise SourceMonthQualityMartError(
            "quality revision source versions must share dataset_id and logical_period"
        )
    for record in (old_record, new_record):
        verify_raw_artifact(
            raw_root=raw_root,
            source_version_id=record.source_version_id,
            expected_size=record.size_bytes,
            expected_sha256=record.sha256,
        )
    logical_period = old_record.logical_period
    if _MONTH.fullmatch(logical_period):
        if logical_period != source_month:
            raise SourceMonthQualityMartError(
                "monthly source revision does not cover requested source_month"
            )
    elif re.fullmatch(r"20[0-9]{2}", logical_period) is not None:
        plan = plan_zip_months(
            manifest=manifest,
            raw_root=raw_root,
            current_source_version_id=new_source_version_id,
            previous_source_version_id=old_source_version_id,
        )
        matching = [
            impact for impact in plan.impacts if impact.logical_month == source_month
        ]
        if len(matching) != 1:
            raise SourceMonthQualityMartError(
                "yearly source revision does not cover requested source_month"
            )
        if matching[0].status == ZIP_STATUS_UNCHANGED:
            raise SourceMonthQualityMartError(
                "requested source_month is unchanged between yearly source revisions"
            )
    else:
        raise SourceMonthQualityMartError(
            "source revision logical_period must be YYYY or YYYY-MM"
        )
    return SourceMonthQualityRevisionImpact(
        source_month=source_month,
        old_source_version_id=old_source_version_id,
        new_source_version_id=new_source_version_id,
        affected_source_months=(source_month,),
    )


def publish_current_source_month_quality_mart(
    *,
    manifest: str | Path,
    raw_root: str | Path,
    station_root: str | Path,
    clean_root: str | Path,
    warehouse_root: str | Path,
    source_month: str,
    transformation_version: str,
    configuration: Mapping[str, object],
    sql_path: str | Path | None = None,
    before_current_publish: Callable[[Path], None] | None = None,
) -> SourceMonthQualityMartPublishResult:
    source_month = _require_source_month(source_month)
    _require_text(transformation_version, "transformation_version")
    canonical_configuration = _canonical_configuration_object(configuration)
    configuration_fingerprint = compute_configuration_fingerprint(
        canonical_configuration
    )
    sql_text, sql_sha256 = _load_sql(sql_path)
    clean_root = Path(clean_root)
    warehouse_root = Path(warehouse_root)

    clean_pointer = _load_clean_pointer(
        clean_current_pointer_path(clean_root, source_month), source_month
    )
    clean_version_id = clean_pointer["clean_version_id"]
    clean_path = clean_version_path(clean_root, source_month, clean_version_id)
    captured_clean_metadata = validate_clean_version(clean_path)
    _require_clean_binding(
        captured_clean_metadata,
        source_month=source_month,
        clean_version_id=clean_version_id,
    )

    fact_pointer = _load_fact_pointer(
        fact_current_pointer_path(warehouse_root, source_month), source_month
    )
    fact_version_id = fact_pointer["fact_version_id"]
    fact_path = fact_version_path(warehouse_root, source_month, fact_version_id)
    captured_fact_metadata = validate_fact_trip_version(
        fact_path,
        manifest=manifest,
        raw_root=raw_root,
        station_root=station_root,
        clean_root=clean_root,
        warehouse_root=warehouse_root,
    )
    _require_input_alignment(
        source_month=source_month,
        clean_version_id=clean_version_id,
        clean_metadata=captured_clean_metadata,
        fact_version_id=fact_version_id,
        fact_metadata=captured_fact_metadata,
    )

    source_version_id = captured_clean_metadata["source_version_id"]
    records = load_manifest(manifest)
    captured_source_record = _find_source_record(records, source_version_id)
    _require_source_lineage(
        source_record=captured_source_record,
        clean_metadata=captured_clean_metadata,
        raw_root=raw_root,
    )
    schema_version = _schema_version_scalar(captured_clean_metadata["schema_versions"])

    mart_version_id = compute_source_month_quality_mart_version_id(
        source_month=source_month,
        source_version_id=source_version_id,
        clean_version_id=clean_version_id,
        fact_version_id=fact_version_id,
        transformation_version=transformation_version,
        configuration_fingerprint=configuration_fingerprint,
        sql_sha256=sql_sha256,
    )
    version_path = source_month_quality_mart_version_path(
        warehouse_root, source_month, mart_version_id
    )
    current_path = source_month_quality_mart_current_pointer_path(
        warehouse_root, source_month
    )
    initial_pointer = _load_mart_pointer_if_present(current_path, source_month)
    if initial_pointer is not None:
        validate_source_month_quality_mart_version(
            source_month_quality_mart_version_path(
                warehouse_root, source_month, initial_pointer["mart_version_id"]
            ),
            manifest=manifest,
            raw_root=raw_root,
            station_root=station_root,
            clean_root=clean_root,
            warehouse_root=warehouse_root,
        )

    mart_root = source_month_quality_mart_root(warehouse_root)
    if version_path.exists():
        candidate_metadata = validate_source_month_quality_mart_version(
            version_path,
            manifest=manifest,
            raw_root=raw_root,
            station_root=station_root,
            clean_root=clean_root,
            warehouse_root=warehouse_root,
        )
        _require_requested_lineage(
            candidate_metadata,
            source_version_id=source_version_id,
            clean_version_id=clean_version_id,
            fact_version_id=fact_version_id,
            transformation_version=transformation_version,
            configuration=canonical_configuration,
            configuration_fingerprint=configuration_fingerprint,
            sql_sha256=sql_sha256,
        )
    else:
        staging_root = mart_root / "staging"
        staging_root.mkdir(parents=True, exist_ok=True)
        stage_container = Path(tempfile.mkdtemp(prefix="build-", dir=staging_root))
        stage_path = (
            stage_container
            / f"source_month={source_month}"
            / f"mart_version_id={mart_version_id}"
        )
        stage_path.mkdir(parents=True)
        try:
            _build_stage_database(
                stage_path=stage_path,
                clean_path=clean_path,
                fact_path=fact_path,
                source_month=source_month,
                source_version_id=source_version_id,
                source_row_count=captured_clean_metadata["source_row_count"],
                schema_version=schema_version,
                artifact_hash=captured_source_record.sha256,
                pipeline_version=transformation_version,
                clean_version_id=clean_version_id,
                fact_version_id=fact_version_id,
                sql_text=sql_text,
            )
            candidate_metadata = _build_metadata(
                stage_path=stage_path,
                source_month=source_month,
                source_version_id=source_version_id,
                clean_version_id=clean_version_id,
                fact_version_id=fact_version_id,
                transformation_version=transformation_version,
                configuration=canonical_configuration,
                configuration_fingerprint=configuration_fingerprint,
                sql_sha256=sql_sha256,
                mart_version_id=mart_version_id,
            )
            _write_json(stage_path / "metadata.json", candidate_metadata)
            validated_stage = validate_source_month_quality_mart_version(
                stage_path,
                manifest=manifest,
                raw_root=raw_root,
                station_root=station_root,
                clean_root=clean_root,
                warehouse_root=warehouse_root,
            )
            if validated_stage != candidate_metadata:
                raise SourceMonthQualityMartError(
                    "validated source-month quality stage metadata changed unexpectedly"
                )
            version_path.parent.mkdir(parents=True, exist_ok=True)
            try:
                os.replace(stage_path, version_path)
            except OSError:
                if not version_path.exists():
                    raise
                existing = validate_source_month_quality_mart_version(
                    version_path,
                    manifest=manifest,
                    raw_root=raw_root,
                    station_root=station_root,
                    clean_root=clean_root,
                    warehouse_root=warehouse_root,
                )
                _require_same_logical_version(existing, candidate_metadata)
                candidate_metadata = existing
        finally:
            if stage_container.exists():
                shutil.rmtree(stage_container)

    with _publication_locks(
        clean_root=clean_root,
        warehouse_root=warehouse_root,
        source_month=source_month,
        mart_version_id=mart_version_id,
    ):
        _require_current_inputs_unchanged(
            clean_root=clean_root,
            warehouse_root=warehouse_root,
            source_month=source_month,
            captured_clean_pointer=clean_pointer,
            captured_fact_pointer=fact_pointer,
        )
        _validate_captured_inputs(
            manifest=manifest,
            raw_root=raw_root,
            station_root=station_root,
            clean_root=clean_root,
            warehouse_root=warehouse_root,
            clean_path=clean_path,
            fact_path=fact_path,
            captured_clean_metadata=captured_clean_metadata,
            captured_fact_metadata=captured_fact_metadata,
            captured_source_record=captured_source_record,
        )
        latest_pointer = _load_mart_pointer_if_present(current_path, source_month)
        final_metadata = validate_source_month_quality_mart_version(
            version_path,
            manifest=manifest,
            raw_root=raw_root,
            station_root=station_root,
            clean_root=clean_root,
            warehouse_root=warehouse_root,
        )
        if final_metadata != candidate_metadata:
            raise SourceMonthQualityMartError(
                "source-month quality immutable bundle changed before current publish"
            )
        if latest_pointer is not None and latest_pointer["mart_version_id"] == mart_version_id:
            return _result(
                STATUS_REUSED_CURRENT, final_metadata, version_path, current_path
            )
        if latest_pointer != initial_pointer:
            raise SourceMonthQualityMartError(
                "source-month quality current pointer changed during build; refusing rollback"
            )
        if before_current_publish is not None:
            before_current_publish(version_path)
            _require_current_inputs_unchanged(
                clean_root=clean_root,
                warehouse_root=warehouse_root,
                source_month=source_month,
                captured_clean_pointer=clean_pointer,
                captured_fact_pointer=fact_pointer,
            )
            _validate_captured_inputs(
                manifest=manifest,
                raw_root=raw_root,
                station_root=station_root,
                clean_root=clean_root,
                warehouse_root=warehouse_root,
                clean_path=clean_path,
                fact_path=fact_path,
                captured_clean_metadata=captured_clean_metadata,
                captured_fact_metadata=captured_fact_metadata,
                captured_source_record=captured_source_record,
            )
            callback_metadata = validate_source_month_quality_mart_version(
                version_path,
                manifest=manifest,
                raw_root=raw_root,
                station_root=station_root,
                clean_root=clean_root,
                warehouse_root=warehouse_root,
            )
            if callback_metadata != final_metadata:
                raise SourceMonthQualityMartError(
                    "source-month quality immutable bundle changed before atomic publish"
                )
            if _load_mart_pointer_if_present(current_path, source_month) != initial_pointer:
                raise SourceMonthQualityMartError(
                    "source-month quality current pointer changed before atomic publish"
                )
        _write_current_pointer(current_path, source_month, mart_version_id)
    return _result(STATUS_PUBLISHED_CURRENT, final_metadata, version_path, current_path)


def validate_source_month_quality_mart_version(
    version_path: str | Path,
    *,
    manifest: str | Path,
    raw_root: str | Path,
    station_root: str | Path,
    clean_root: str | Path,
    warehouse_root: str | Path,
) -> dict[str, object]:
    path = Path(version_path)
    metadata = _load_json(path / "metadata.json", "source-month quality mart metadata")
    _validate_metadata_identity(metadata)
    _require_bundle_binding(metadata, path)

    source_month = metadata["source_month"]
    clean_path = clean_version_path(
        clean_root, source_month, metadata["clean_version_id"]
    )
    clean_metadata = validate_clean_version(clean_path)
    _require_clean_binding(
        clean_metadata,
        source_month=source_month,
        clean_version_id=metadata["clean_version_id"],
    )
    fact_path = fact_version_path(
        warehouse_root, source_month, metadata["fact_version_id"]
    )
    fact_metadata = validate_fact_trip_version(
        fact_path,
        manifest=manifest,
        raw_root=raw_root,
        station_root=station_root,
        clean_root=clean_root,
        warehouse_root=warehouse_root,
    )
    _require_input_alignment(
        source_month=source_month,
        clean_version_id=metadata["clean_version_id"],
        clean_metadata=clean_metadata,
        fact_version_id=metadata["fact_version_id"],
        fact_metadata=fact_metadata,
    )
    if metadata["source_version_id"] != clean_metadata["source_version_id"]:
        raise SourceMonthQualityMartError(
            "quality metadata source_version_id does not match Clean lineage"
        )
    source_record = _find_source_record(
        load_manifest(manifest), metadata["source_version_id"]
    )
    _require_source_lineage(
        source_record=source_record,
        clean_metadata=clean_metadata,
        raw_root=raw_root,
    )

    database_path = path / "warehouse.duckdb"
    if Path(str(database_path) + ".wal").exists():
        raise SourceMonthQualityMartError(
            "source-month quality warehouse has residual WAL and is not finalized"
        )
    _require_file_fingerprint(
        database_path, metadata["warehouse_database"], label="warehouse.duckdb"
    )
    connection = duckdb.connect(str(database_path), read_only=True)
    try:
        _require_mart_schema(connection)
        actual_rows = connection.execute(
            "SELECT * FROM mart_source_month_quality"
        ).fetchall()
    except duckdb.Error as exc:
        raise SourceMonthQualityMartError(
            f"cannot validate source-month quality warehouse database: {exc}"
        ) from exc
    finally:
        connection.close()
    if len(actual_rows) != 1:
        raise SourceMonthQualityMartError(
            "mart_source_month_quality must contain exactly one row per immutable month bundle"
        )

    expected_row = _expected_row(
        source_month=source_month,
        source_record=source_record,
        clean_path=clean_path,
        clean_metadata=clean_metadata,
        fact_path=fact_path,
        fact_metadata=fact_metadata,
        pipeline_version=metadata["transformation_version"],
    )
    _require_row_equal(actual_rows[0], expected_row)
    summary = _summarize_row(actual_rows[0])
    for key, value in summary.items():
        if metadata[key] != value:
            raise SourceMonthQualityMartError(
                f"quality metadata {key}={metadata[key]!r} does not match durable value {value!r}"
            )
    return metadata


def _build_stage_database(
    *,
    stage_path: Path,
    clean_path: Path,
    fact_path: Path,
    source_month: str,
    source_version_id: str,
    source_row_count: int,
    schema_version: str,
    artifact_hash: str,
    pipeline_version: str,
    clean_version_id: str,
    fact_version_id: str,
    sql_text: str,
) -> None:
    database_path = stage_path / "warehouse.duckdb"
    connection = duckdb.connect(str(database_path))
    try:
        clean_sql = _sql_path(clean_path / "clean.parquet")
        fact_sql = _sql_path(fact_path / "warehouse.duckdb")
        connection.execute(
            f"CREATE TEMP VIEW quality_clean_input AS "
            f"SELECT * FROM read_parquet('{clean_sql}', hive_partitioning=false)"
        )
        connection.execute(
            """
            CREATE TEMP VIEW quality_issue_input AS
            SELECT
                c.source_row_id,
                issue.rule_id AS rule_id,
                issue.severity AS severity,
                issue.field AS field,
                issue.reason AS reason
            FROM quality_clean_input AS c,
                 UNNEST(c.dq_issues) AS issues(issue)
            """
        )
        connection.execute(f"ATTACH '{fact_sql}' AS quality_fact_bundle (READ_ONLY)")
        connection.execute(
            "CREATE TEMP VIEW quality_fact_input AS "
            "SELECT * FROM quality_fact_bundle.fact_trip_trusted"
        )
        connection.execute(
            """
            CREATE TEMP TABLE quality_context (
                source_month VARCHAR NOT NULL,
                source_version_id VARCHAR NOT NULL,
                source_row_count BIGINT NOT NULL,
                schema_version VARCHAR NOT NULL,
                artifact_hash VARCHAR NOT NULL,
                pipeline_version VARCHAR NOT NULL,
                clean_version_id VARCHAR NOT NULL,
                fact_version_id VARCHAR NOT NULL
            )
            """
        )
        connection.execute(
            "INSERT INTO quality_context VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            [
                source_month,
                source_version_id,
                source_row_count,
                schema_version,
                artifact_hash,
                pipeline_version,
                clean_version_id,
                fact_version_id,
            ],
        )
        connection.execute(sql_text)
        _require_mart_schema(connection)
        row_count = connection.execute(
            "SELECT COUNT(*) FROM mart_source_month_quality"
        ).fetchone()[0]
        if row_count != 1:
            raise SourceMonthQualityMartError(
                "source-month quality SQL must produce exactly one row"
            )
        connection.execute("CHECKPOINT")
    except duckdb.Error as exc:
        raise SourceMonthQualityMartError(
            f"source-month quality mart SQL build failed: {exc}"
        ) from exc
    finally:
        connection.close()
    if Path(str(database_path) + ".wal").exists():
        raise SourceMonthQualityMartError(
            "source-month quality mart build left a residual DuckDB WAL"
        )


def _build_metadata(
    *,
    stage_path: Path,
    source_month: str,
    source_version_id: str,
    clean_version_id: str,
    fact_version_id: str,
    transformation_version: str,
    configuration: dict[str, object],
    configuration_fingerprint: str,
    sql_sha256: str,
    mart_version_id: str,
) -> dict[str, object]:
    database_path = stage_path / "warehouse.duckdb"
    connection = duckdb.connect(str(database_path), read_only=True)
    try:
        rows = connection.execute(
            "SELECT * FROM mart_source_month_quality"
        ).fetchall()
    finally:
        connection.close()
    if len(rows) != 1:
        raise SourceMonthQualityMartError(
            "source-month quality metadata requires exactly one mart row"
        )
    size_bytes, sha256 = _fingerprint_file(database_path)
    return {
        "schema_version": METADATA_SCHEMA_VERSION,
        "source_month": source_month,
        "source_version_id": source_version_id,
        "clean_version_id": clean_version_id,
        "fact_version_id": fact_version_id,
        "transformation_version": transformation_version,
        "configuration": configuration,
        "configuration_fingerprint": configuration_fingerprint,
        "sql_sha256": sql_sha256,
        "mart_version_id": mart_version_id,
        **_summarize_row(rows[0]),
        "warehouse_database": {"size_bytes": size_bytes, "sha256": sha256},
    }


def _expected_row(
    *,
    source_month: str,
    source_record: SourceRecord,
    clean_path: Path,
    clean_metadata: dict[str, object],
    fact_path: Path,
    fact_metadata: dict[str, object],
    pipeline_version: str,
) -> tuple[object, ...]:
    connection = duckdb.connect()
    try:
        clean_cursor = connection.execute(
            f"""
            SELECT
                source_row_id,
                trusted_fact_eligible,
                row_quarantined,
                logical_trip_key,
                logical_trip_conflict,
                exact_duplicate_group,
                is_completed_trip,
                schema_version,
                dq_issues
            FROM read_parquet('{_sql_path(clean_path / "clean.parquet")}', hive_partitioning=false)
            ORDER BY source_row_id
            """
        )
        clean_count = 0
        trusted_count = 0
        quarantine_count = 0
        incomplete_count = 0
        conflict_groups: set[str] = set()
        duplicate_groups: set[str] = set()
        schema_versions: set[str] = set()
        distance_zero_rows = 0
        extreme_distance_rows = 0
        invalid_birth_rows = 0
        sex_missing_rows = 0
        while True:
            batch = clean_cursor.fetchmany(10_000)
            if not batch:
                break
            for (
                _source_row_id,
                trusted,
                quarantined,
                logical_trip_key,
                logical_conflict,
                exact_duplicate,
                completed,
                schema_version,
                issues,
            ) in batch:
                clean_count += 1
                trusted_count += int(trusted)
                quarantine_count += int(quarantined)
                incomplete_count += int(not completed)
                if logical_conflict:
                    if logical_trip_key is None:
                        raise SourceMonthQualityMartError(
                            "logical conflict row lacks logical_trip_key"
                        )
                    conflict_groups.add(logical_trip_key)
                if exact_duplicate:
                    if logical_trip_key is None:
                        raise SourceMonthQualityMartError(
                            "exact duplicate row lacks logical_trip_key"
                        )
                    duplicate_groups.add(logical_trip_key)
                schema_versions.add(schema_version)
                rule_ids = {_issue_rule_id(issue) for issue in issues}
                distance_zero_rows += "distance_m_zero" in rule_ids
                extreme_distance_rows += "distance_m_extreme_positive" in rule_ids
                invalid_birth_rows += bool(
                    {"birth_year_invalid", "birth_year_impossible"} & rule_ids
                )
                sex_missing_rows += "sex_missing" in rule_ids
    except duckdb.Error as exc:
        raise SourceMonthQualityMartError(
            f"cannot independently read Clean quality input: {exc}"
        ) from exc
    finally:
        connection.close()
    if clean_count == 0:
        raise SourceMonthQualityMartError(
            "quality mart cannot be computed from an empty Clean month"
        )

    if clean_count != trusted_count + quarantine_count:
        raise SourceMonthQualityMartError(
            "quality Clean reconciliation failed: clean != trusted + quarantine"
        )
    if clean_metadata["source_row_count"] != clean_count:
        raise SourceMonthQualityMartError(
            "Clean source_row_count does not match independently read Clean rows"
        )
    if clean_metadata["clean_row_count"] != clean_count:
        raise SourceMonthQualityMartError(
            "Clean metadata clean_row_count does not match independently read Clean rows"
        )
    if clean_metadata["trusted_fact_eligible_count"] != trusted_count:
        raise SourceMonthQualityMartError(
            "Clean trusted count does not match independently read Clean rows"
        )
    if clean_metadata["quarantined_row_count"] != quarantine_count:
        raise SourceMonthQualityMartError(
            "Clean quarantine count does not match independently read Clean rows"
        )

    fact_connection = duckdb.connect()
    try:
        fact_connection.execute(
            f"ATTACH '{_sql_path(fact_path / 'warehouse.duckdb')}' AS fact_bundle (READ_ONLY)"
        )
        fact_connection.execute(
            f"CREATE VIEW clean_input AS SELECT * FROM "
            f"read_parquet('{_sql_path(clean_path / 'clean.parquet')}', hive_partitioning=false)"
        )
        missing_from_fact = fact_connection.execute(
            """
            SELECT COUNT(*) FROM (
                SELECT source_row_id
                FROM clean_input
                WHERE trusted_fact_eligible
                EXCEPT
                SELECT source_row_id
                FROM fact_bundle.fact_trip_trusted
            )
            """
        ).fetchone()[0]
        extra_in_fact = fact_connection.execute(
            """
            SELECT COUNT(*) FROM (
                SELECT source_row_id
                FROM fact_bundle.fact_trip_trusted
                EXCEPT
                SELECT source_row_id
                FROM clean_input
                WHERE trusted_fact_eligible
            )
            """
        ).fetchone()[0]
        fact_count, station_unmatched_count = fact_connection.execute(
            """
            SELECT
                COUNT(*),
                COUNT(*) FILTER (WHERE NOT station_history_match)
            FROM fact_bundle.fact_trip_trusted
            """
        ).fetchone()
    except duckdb.Error as exc:
        raise SourceMonthQualityMartError(
            f"cannot independently validate trusted fact quality input: {exc}"
        ) from exc
    finally:
        fact_connection.close()
    if missing_from_fact or extra_in_fact:
        raise SourceMonthQualityMartError(
            "trusted fact source_row_id set does not match trusted Clean set"
        )
    fact_count = int(fact_count)
    station_unmatched_count = int(station_unmatched_count)
    if fact_count != trusted_count:
        raise SourceMonthQualityMartError(
            "trusted fact count does not reconcile with trusted Clean count"
        )
    if fact_metadata["fact_row_count"] != fact_count:
        raise SourceMonthQualityMartError(
            "fact metadata row count does not match independently read fact rows"
        )
    if fact_metadata["station_history_unmatched_count"] != station_unmatched_count:
        raise SourceMonthQualityMartError(
            "fact station unmatched count does not match independently read fact rows"
        )

    expected_schema_version = _schema_version_scalar(sorted(schema_versions))
    metadata_schema_version = _schema_version_scalar(clean_metadata["schema_versions"])
    if expected_schema_version != metadata_schema_version:
        raise SourceMonthQualityMartError(
            "Clean schema_versions metadata does not match durable Clean rows"
        )
    denominator = float(clean_count)
    return (
        source_month,
        source_record.source_version_id,
        clean_count,
        clean_count,
        fact_count,
        quarantine_count,
        len(conflict_groups),
        len(duplicate_groups),
        incomplete_count,
        station_unmatched_count,
        distance_zero_rows / denominator,
        extreme_distance_rows,
        invalid_birth_rows / denominator,
        sex_missing_rows / denominator,
        expected_schema_version,
        source_record.sha256,
        pipeline_version,
        clean_metadata["clean_version_id"],
        fact_metadata["fact_version_id"],
    )


def _issue_rule_id(issue: object) -> str:
    if isinstance(issue, dict):
        value = issue.get("rule_id")
    elif isinstance(issue, (tuple, list)) and issue:
        value = issue[0]
    else:
        value = getattr(issue, "rule_id", None)
    if not isinstance(value, str) or not value:
        raise SourceMonthQualityMartError(
            "Clean dq_issues contains an unreadable rule_id"
        )
    return value


def _require_row_equal(
    actual: tuple[object, ...], expected: tuple[object, ...]
) -> None:
    if len(actual) != len(expected):
        raise SourceMonthQualityMartError(
            "source-month quality row width differs from independent recomputation"
        )
    for index, (actual_value, expected_value) in enumerate(zip(actual, expected)):
        if index in {10, 12, 13}:
            actual_float = float(actual_value)
            expected_float = float(expected_value)
            tolerance = math.ulp(expected_float) * 2
            if math.isclose(
                actual_float,
                expected_float,
                rel_tol=0.0,
                abs_tol=tolerance,
            ):
                continue
        elif actual_value == expected_value:
            continue
        raise SourceMonthQualityMartError(
            "source-month quality row differs from independent recomputation "
            f"at column {index}: actual={actual_value!r}, expected={expected_value!r}"
        )


def _summarize_row(row: tuple[object, ...]) -> dict[str, int]:
    return {
        "mart_row_count": 1,
        "source_row_count": int(row[2]),
        "clean_row_count": int(row[3]),
        "trusted_fact_count": int(row[4]),
        "quarantine_count": int(row[5]),
    }


def _require_mart_schema(connection: duckdb.DuckDBPyConnection) -> None:
    table_rows = connection.execute(
        """
        SELECT table_type FROM information_schema.tables
        WHERE table_schema = 'main' AND table_name = 'mart_source_month_quality'
        """
    ).fetchall()
    if table_rows != [("BASE TABLE",)]:
        raise SourceMonthQualityMartError(
            "mart_source_month_quality must be a materialized base table"
        )
    actual = tuple(
        (row[0], row[1])
        for row in connection.execute(
            "DESCRIBE SELECT * FROM mart_source_month_quality"
        ).fetchall()
    )
    if actual != _MART_SCHEMA:
        raise SourceMonthQualityMartError(
            f"mart_source_month_quality schema mismatch: expected {_MART_SCHEMA!r}, got {actual!r}"
        )


def _validate_metadata_identity(metadata: dict[str, object]) -> None:
    if set(metadata) != _METADATA_FIELDS:
        raise SourceMonthQualityMartError(
            "source-month quality metadata fields do not match contract"
        )
    if metadata["schema_version"] != METADATA_SCHEMA_VERSION:
        raise SourceMonthQualityMartError(
            "unsupported source-month quality metadata schema_version"
        )
    source_month = _require_source_month(metadata["source_month"])
    _require_version(metadata["source_version_id"], _SOURCE_VERSION, "source_version_id")
    _require_version(metadata["clean_version_id"], _CLEAN_VERSION, "clean_version_id")
    _require_version(metadata["fact_version_id"], _FACT_VERSION, "fact_version_id")
    transformation_version = _require_text(
        metadata["transformation_version"], "transformation_version"
    )
    configuration = metadata["configuration"]
    if not isinstance(configuration, dict):
        raise SourceMonthQualityMartError(
            "quality metadata configuration must be a JSON object"
        )
    configuration_fingerprint = compute_configuration_fingerprint(configuration)
    if metadata["configuration_fingerprint"] != configuration_fingerprint:
        raise SourceMonthQualityMartError(
            "quality metadata configuration_fingerprint does not match configuration"
        )
    sql_sha256 = metadata["sql_sha256"]
    if not isinstance(sql_sha256, str) or _SHA256.fullmatch(sql_sha256) is None:
        raise SourceMonthQualityMartError("quality metadata sql_sha256 is invalid")
    expected_id = compute_source_month_quality_mart_version_id(
        source_month=source_month,
        source_version_id=metadata["source_version_id"],
        clean_version_id=metadata["clean_version_id"],
        fact_version_id=metadata["fact_version_id"],
        transformation_version=transformation_version,
        configuration_fingerprint=configuration_fingerprint,
        sql_sha256=sql_sha256,
    )
    if metadata["mart_version_id"] != expected_id:
        raise SourceMonthQualityMartError(
            "quality metadata mart_version_id does not match deterministic inputs"
        )
    for key in (
        "mart_row_count",
        "source_row_count",
        "clean_row_count",
        "trusted_fact_count",
        "quarantine_count",
    ):
        value = metadata[key]
        if isinstance(value, bool) or not isinstance(value, int) or value < 0:
            raise SourceMonthQualityMartError(
                f"quality metadata {key} must be a non-negative integer"
            )
    if metadata["mart_row_count"] != 1:
        raise SourceMonthQualityMartError(
            "source-month quality metadata mart_row_count must be exactly 1"
        )
    _validate_file_fingerprint_shape(
        metadata["warehouse_database"], "warehouse_database"
    )


def _require_bundle_binding(metadata: dict[str, object], version_path: Path) -> None:
    if version_path.name != f"mart_version_id={metadata['mart_version_id']}":
        raise SourceMonthQualityMartError(
            "source-month quality version directory id is not bound to metadata"
        )
    if version_path.parent.name != f"source_month={metadata['source_month']}":
        raise SourceMonthQualityMartError(
            "source-month quality version directory month is not bound to metadata"
        )


def _require_clean_binding(
    metadata: dict[str, object], *, source_month: str, clean_version_id: str
) -> None:
    if metadata["source_month"] != source_month:
        raise SourceMonthQualityMartError(
            "current Clean metadata belongs to the wrong source_month"
        )
    if metadata["clean_version_id"] != clean_version_id:
        raise SourceMonthQualityMartError(
            "current Clean metadata does not match current clean_version_id"
        )


def _require_input_alignment(
    *,
    source_month: str,
    clean_version_id: str,
    clean_metadata: dict[str, object],
    fact_version_id: str,
    fact_metadata: dict[str, object],
) -> None:
    if fact_metadata["source_month"] != source_month:
        raise SourceMonthQualityMartError(
            "current trusted fact belongs to the wrong source_month"
        )
    if fact_metadata["fact_version_id"] != fact_version_id:
        raise SourceMonthQualityMartError(
            "current trusted fact metadata does not match current fact_version_id"
        )
    if fact_metadata["clean_version_id"] != clean_version_id:
        raise SourceMonthQualityMartError(
            "current trusted fact does not use current Clean version"
        )
    if fact_metadata["source_version_id"] != clean_metadata["source_version_id"]:
        raise SourceMonthQualityMartError(
            "current Clean and trusted fact source_version_id do not match"
        )


def _require_source_lineage(
    *,
    source_record: SourceRecord,
    clean_metadata: dict[str, object],
    raw_root: str | Path,
) -> None:
    if clean_metadata["source_version_id"] != source_record.source_version_id:
        raise SourceMonthQualityMartError(
            "Clean source_version_id does not match registered source record"
        )
    if clean_metadata["source_artifact_sha256"] != source_record.sha256:
        raise SourceMonthQualityMartError(
            "Clean source artifact hash does not match registered source record"
        )
    if clean_metadata["source_artifact_size_bytes"] != source_record.size_bytes:
        raise SourceMonthQualityMartError(
            "Clean source artifact size does not match registered source record"
        )
    verify_raw_artifact(
        raw_root=raw_root,
        source_version_id=source_record.source_version_id,
        expected_size=source_record.size_bytes,
        expected_sha256=source_record.sha256,
    )


def _validate_captured_inputs(
    *,
    manifest: str | Path,
    raw_root: str | Path,
    station_root: str | Path,
    clean_root: Path,
    warehouse_root: Path,
    clean_path: Path,
    fact_path: Path,
    captured_clean_metadata: dict[str, object],
    captured_fact_metadata: dict[str, object],
    captured_source_record: SourceRecord,
) -> None:
    current_clean_metadata = validate_clean_version(clean_path)
    if current_clean_metadata != captured_clean_metadata:
        raise SourceMonthQualityMartError(
            "captured Clean immutable metadata changed during quality build"
        )
    current_fact_metadata = validate_fact_trip_version(
        fact_path,
        manifest=manifest,
        raw_root=raw_root,
        station_root=station_root,
        clean_root=clean_root,
        warehouse_root=warehouse_root,
    )
    if current_fact_metadata != captured_fact_metadata:
        raise SourceMonthQualityMartError(
            "captured trusted fact immutable metadata changed during quality build"
        )
    current_source_record = _find_source_record(
        load_manifest(manifest), captured_source_record.source_version_id
    )
    if current_source_record != captured_source_record:
        raise SourceMonthQualityMartError(
            "captured registered source metadata changed during quality build"
        )
    _require_source_lineage(
        source_record=current_source_record,
        clean_metadata=current_clean_metadata,
        raw_root=raw_root,
    )


def _require_requested_lineage(
    metadata: dict[str, object],
    *,
    source_version_id: str,
    clean_version_id: str,
    fact_version_id: str,
    transformation_version: str,
    configuration: dict[str, object],
    configuration_fingerprint: str,
    sql_sha256: str,
) -> None:
    expected = {
        "source_version_id": source_version_id,
        "clean_version_id": clean_version_id,
        "fact_version_id": fact_version_id,
        "transformation_version": transformation_version,
        "configuration": configuration,
        "configuration_fingerprint": configuration_fingerprint,
        "sql_sha256": sql_sha256,
    }
    for key, value in expected.items():
        if metadata[key] != value:
            raise SourceMonthQualityMartError(
                f"existing same-ID quality mart {key} does not match requested input"
            )


def _require_same_logical_version(
    existing: dict[str, object], candidate: dict[str, object]
) -> None:
    existing_logical = {
        key: value for key, value in existing.items() if key != "warehouse_database"
    }
    candidate_logical = {
        key: value for key, value in candidate.items() if key != "warehouse_database"
    }
    if existing_logical != candidate_logical:
        raise SourceMonthQualityMartError(
            "same quality mart_version_id exists with different logical metadata"
        )


def _schema_version_scalar(schema_versions: object) -> str:
    if not isinstance(schema_versions, (list, tuple)) or not schema_versions:
        raise SourceMonthQualityMartError(
            "Clean schema_versions must contain at least one known schema"
        )
    values = sorted({_require_text(value, "schema_version") for value in schema_versions})
    if len(values) == 1:
        return values[0]
    return "mixed[" + ",".join(values) + "]"


def _find_source_record(
    records: list[SourceRecord], source_version_id: str
) -> SourceRecord:
    matches = [record for record in records if record.source_version_id == source_version_id]
    if len(matches) != 1:
        raise SourceMonthQualityMartError(
            f"source_version_id {source_version_id!r} is not uniquely registered"
        )
    return matches[0]


def _find_source_record_with_index(
    records: list[SourceRecord], source_version_id: str
) -> tuple[int, SourceRecord]:
    matches = [
        (index, record)
        for index, record in enumerate(records)
        if record.source_version_id == source_version_id
    ]
    if len(matches) != 1:
        raise SourceMonthQualityMartError(
            f"source_version_id {source_version_id!r} is not uniquely registered"
        )
    return matches[0]


def _load_clean_pointer(path: Path, source_month: str) -> dict[str, object]:
    payload = _load_json(path, f"current Clean pointer for {source_month}")
    if set(payload) != {"schema_version", "source_month", "clean_version_id"}:
        raise SourceMonthQualityMartError(
            f"current Clean pointer for {source_month} has invalid fields"
        )
    if payload["schema_version"] != 1 or payload["source_month"] != source_month:
        raise SourceMonthQualityMartError(
            f"current Clean pointer for {source_month} has invalid binding"
        )
    _require_version(payload["clean_version_id"], _CLEAN_VERSION, "clean_version_id")
    return payload


def _load_fact_pointer(path: Path, source_month: str) -> dict[str, object]:
    payload = _load_json(path, f"current trusted fact pointer for {source_month}")
    if set(payload) != {"schema_version", "source_month", "fact_version_id"}:
        raise SourceMonthQualityMartError(
            f"current trusted fact pointer for {source_month} has invalid fields"
        )
    if payload["schema_version"] != 1 or payload["source_month"] != source_month:
        raise SourceMonthQualityMartError(
            f"current trusted fact pointer for {source_month} has invalid binding"
        )
    _require_version(payload["fact_version_id"], _FACT_VERSION, "fact_version_id")
    return payload


def _load_mart_pointer_if_present(
    path: Path, source_month: str
) -> dict[str, object] | None:
    if not path.exists():
        return None
    payload = _load_json(path, "current source-month quality mart pointer")
    if set(payload) != {"schema_version", "source_month", "mart_version_id"}:
        raise SourceMonthQualityMartError(
            "current source-month quality mart pointer has invalid fields"
        )
    if (
        payload["schema_version"] != CURRENT_POINTER_SCHEMA_VERSION
        or payload["source_month"] != source_month
    ):
        raise SourceMonthQualityMartError(
            "current source-month quality mart pointer has invalid binding"
        )
    _require_version(payload["mart_version_id"], _MART_VERSION, "mart_version_id")
    return payload


def _write_current_pointer(
    path: Path, source_month: str, mart_version_id: str
) -> None:
    payload = {
        "schema_version": CURRENT_POINTER_SCHEMA_VERSION,
        "source_month": source_month,
        "mart_version_id": mart_version_id,
    }
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, temp_name = tempfile.mkstemp(
        prefix=f".{path.name}.", suffix=".tmp", dir=path.parent
    )
    temp_path = Path(temp_name)
    try:
        with os.fdopen(fd, "w", encoding="utf-8", newline="\n") as handle:
            handle.write(_canonical_json_text(payload) + "\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temp_path, path)
    finally:
        temp_path.unlink(missing_ok=True)


def _require_current_inputs_unchanged(
    *,
    clean_root: Path,
    warehouse_root: Path,
    source_month: str,
    captured_clean_pointer: dict[str, object],
    captured_fact_pointer: dict[str, object],
) -> None:
    if _load_clean_pointer(
        clean_current_pointer_path(clean_root, source_month), source_month
    ) != captured_clean_pointer:
        raise SourceMonthQualityMartError(
            "Clean current pointer changed during source-month quality build"
        )
    if _load_fact_pointer(
        fact_current_pointer_path(warehouse_root, source_month), source_month
    ) != captured_fact_pointer:
        raise SourceMonthQualityMartError(
            "trusted fact current pointer changed during source-month quality build"
        )


@contextmanager
def _publication_locks(
    *,
    clean_root: Path,
    warehouse_root: Path,
    source_month: str,
    mart_version_id: str,
):
    with ExitStack() as stack:
        stack.enter_context(
            _advisory_lock(
                clean_root / "locks" / f"source_month={source_month}.lock",
                label=f"source-month quality {mart_version_id}",
                error_message=f"Clean {source_month} is being published concurrently",
            )
        )
        stack.enter_context(
            _advisory_lock(
                warehouse_root
                / "fact_trip_trusted"
                / "locks"
                / f"source_month={source_month}.lock",
                label=f"source-month quality {mart_version_id}",
                error_message=f"trusted fact {source_month} is being published concurrently",
            )
        )
        stack.enter_context(
            _advisory_lock(
                source_month_quality_mart_root(warehouse_root)
                / "locks"
                / f"source_month={source_month}.lock",
                label=mart_version_id,
                error_message="same-month source quality mart is being published concurrently",
            )
        )
        yield


@contextmanager
def _advisory_lock(path: Path, *, label: str, error_message: str):
    path.parent.mkdir(parents=True, exist_ok=True)
    handle = path.open("a+b")
    try:
        handle.seek(0, os.SEEK_END)
        if handle.tell() == 0:
            handle.write(b"0")
            handle.flush()
            os.fsync(handle.fileno())
        try:
            _lock_nonblocking(handle)
        except (BlockingIOError, OSError) as exc:
            raise SourceMonthQualityMartError(
                f"{error_message}; lock={path}"
            ) from exc
        handle.seek(1)
        handle.truncate()
        handle.write((label + "\n").encode("utf-8"))
        handle.flush()
        os.fsync(handle.fileno())
        yield
    finally:
        try:
            _unlock(handle)
        finally:
            handle.close()


def _lock_nonblocking(handle) -> None:
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


def _unlock(handle) -> None:
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


def _load_sql(path: str | Path | None) -> tuple[str, str]:
    sql_path = Path(path) if path is not None else _DEFAULT_SQL_PATH
    try:
        raw = sql_path.read_bytes()
        text = raw.decode("utf-8")
    except (OSError, UnicodeDecodeError) as exc:
        raise SourceMonthQualityMartError(
            f"cannot read source-month quality SQL {sql_path}: {exc}"
        ) from exc
    canonical = text.replace("\r\n", "\n").replace("\r", "\n")
    return canonical, hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def _load_json(path: Path, label: str) -> dict[str, object]:
    try:
        with path.open("r", encoding="utf-8") as handle:
            payload = json.load(handle, object_pairs_hook=_reject_duplicate_pairs)
    except FileNotFoundError as exc:
        raise SourceMonthQualityMartError(f"missing {label}: {path}") from exc
    except (OSError, UnicodeDecodeError, ValueError) as exc:
        raise SourceMonthQualityMartError(f"cannot read {label} {path}: {exc}") from exc
    if not isinstance(payload, dict):
        raise SourceMonthQualityMartError(f"{label} must be a JSON object")
    return payload


def _reject_duplicate_pairs(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError(f"duplicate JSON key {key!r}")
        result[key] = value
    return result


def _write_json(path: Path, payload: dict[str, object]) -> None:
    path.write_text(_canonical_json_text(payload) + "\n", encoding="utf-8", newline="\n")


def _canonical_json_text(value: object) -> str:
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    )


def _canonical_json_bytes(value: object) -> bytes:
    return _canonical_json_text(value).encode("utf-8")


def _canonical_configuration_object(
    configuration: Mapping[str, object],
) -> dict[str, object]:
    if not isinstance(configuration, Mapping):
        raise SourceMonthQualityMartError("configuration must be a mapping")
    try:
        encoded = _canonical_json_text(dict(configuration))
        decoded = json.loads(encoded)
    except (TypeError, ValueError, json.JSONDecodeError) as exc:
        raise SourceMonthQualityMartError(
            f"configuration must be deterministic JSON data: {exc}"
        ) from exc
    if not isinstance(decoded, dict):
        raise SourceMonthQualityMartError("configuration must encode to a JSON object")
    return decoded


def _fingerprint_file(path: Path) -> tuple[int, str]:
    digest = hashlib.sha256()
    size = 0
    try:
        with path.open("rb") as handle:
            for chunk in iter(lambda: handle.read(1024 * 1024), b""):
                size += len(chunk)
                digest.update(chunk)
    except OSError as exc:
        raise SourceMonthQualityMartError(f"cannot fingerprint {path}: {exc}") from exc
    return size, digest.hexdigest()


def _require_file_fingerprint(
    path: Path, expected: object, *, label: str
) -> None:
    _validate_file_fingerprint_shape(expected, label)
    size, sha256 = _fingerprint_file(path)
    if size != expected["size_bytes"] or sha256 != expected["sha256"]:
        raise SourceMonthQualityMartError(
            f"{label} fingerprint mismatch: expected {expected!r}, "
            f"got size_bytes={size}, sha256={sha256}"
        )


def _validate_file_fingerprint_shape(value: object, label: str) -> None:
    if not isinstance(value, dict) or set(value) != {"size_bytes", "sha256"}:
        raise SourceMonthQualityMartError(
            f"{label} fingerprint must contain size_bytes and sha256"
        )
    size = value["size_bytes"]
    sha = value["sha256"]
    if isinstance(size, bool) or not isinstance(size, int) or size < 0:
        raise SourceMonthQualityMartError(f"{label} size_bytes is invalid")
    if not isinstance(sha, str) or _SHA256.fullmatch(sha) is None:
        raise SourceMonthQualityMartError(f"{label} sha256 is invalid")


def _require_source_month(value: object) -> str:
    if not isinstance(value, str) or _MONTH.fullmatch(value) is None:
        raise SourceMonthQualityMartError("source_month must be YYYY-MM")
    if not CORE_START_MONTH <= value <= CORE_END_MONTH:
        raise SourceMonthQualityMartError(
            f"source_month must be within production range "
            f"{CORE_START_MONTH}..{CORE_END_MONTH}"
        )
    return value


def _require_text(value: object, label: str) -> str:
    if not isinstance(value, str) or not value:
        raise SourceMonthQualityMartError(f"{label} must be a non-empty string")
    return value


def _require_version(value: object, pattern: re.Pattern[str], label: str) -> str:
    if not isinstance(value, str) or pattern.fullmatch(value) is None:
        raise SourceMonthQualityMartError(f"{label} has invalid deterministic identity")
    return value


def _sql_path(path: Path) -> str:
    return str(path.resolve()).replace("'", "''").replace("\\", "/")


def _result(
    status: str,
    metadata: dict[str, object],
    version_path: Path,
    current_path: Path,
) -> SourceMonthQualityMartPublishResult:
    return SourceMonthQualityMartPublishResult(
        status=status,
        source_month=metadata["source_month"],
        source_version_id=metadata["source_version_id"],
        mart_version_id=metadata["mart_version_id"],
        clean_version_id=metadata["clean_version_id"],
        fact_version_id=metadata["fact_version_id"],
        version_path=version_path,
        current_pointer_path=current_path,
        source_row_count=metadata["source_row_count"],
        clean_row_count=metadata["clean_row_count"],
        trusted_fact_count=metadata["trusted_fact_count"],
        quarantine_count=metadata["quarantine_count"],
    )
