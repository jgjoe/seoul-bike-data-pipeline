"""Build, validate, and atomically publish trusted trip fact month versions.

The durable fact layer is month-scoped so a revised/backfilled month can move
independently without rewriting unrelated months. Each immutable version is a
persistent DuckDB database containing fact_trip_trusted; the only mutable state
is a tiny per-month current pointer.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import shutil
import tempfile
from collections.abc import Mapping
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path
from typing import Callable

import duckdb

from .clean_publish import (
    _CLEAN_SCHEMA,
    clean_version_path,
    current_pointer_path,
    validate_clean_version,
)
from .errors import FactTripPublishError, SourceRegistrationError
from .station_history import (
    history_current_pointer_path,
    history_version_path,
    validate_station_history_version,
)
from .trip_station_enrichment import (
    RULE_STATION_HISTORY_UNMATCHED,
    evaluate_trip_station_enrichment,
    load_station_enrichment_sql,
)

FACT_VERSION_PREFIX = "fv1_"
CONFIGURATION_FINGERPRINT_PREFIX = "cfg1_"
CURRENT_POINTER_SCHEMA_VERSION = 1
METADATA_SCHEMA_VERSION = 1

STATUS_PUBLISHED_CURRENT = "PUBLISHED_CURRENT"
STATUS_REUSED_CURRENT = "REUSED_CURRENT"

_FACT_VERSION = re.compile(r"fv1_[0-9a-f]{64}")
_CLEAN_VERSION = re.compile(r"cv1_[0-9a-f]{64}")
_HISTORY_VERSION = re.compile(r"wh1_[0-9a-f]{64}")
_CONFIGURATION_FINGERPRINT = re.compile(r"cfg1_[0-9a-f]{64}")
_MONTH = re.compile(r"20[0-9]{2}-(?:0[1-9]|1[0-2])")
_SHA256 = re.compile(r"[0-9a-f]{64}")
_DEFAULT_SQL_PATH = Path(__file__).with_name("sql") / "fact_trip_trusted.sql"
_STATION_WARNING_REASON = (
    "no applicable observed station-history interval exists at rental date; "
    "future observations are not used"
)

_FACT_SCHEMA = (
    *_CLEAN_SCHEMA,
    ("matched_station_applicable_from", "DATE"),
    ("matched_station_applicable_until", "DATE"),
)

_METADATA_FIELDS = {
    "schema_version",
    "source_month",
    "source_version_id",
    "clean_version_id",
    "history_version_id",
    "transformation_version",
    "configuration",
    "configuration_fingerprint",
    "enrichment_sql_sha256",
    "fact_sql_sha256",
    "fact_version_id",
    "fact_row_count",
    "station_history_match_count",
    "station_history_unmatched_count",
    "warning_row_count",
    "warehouse_database",
}


@dataclass(frozen=True)
class FactTripPublishResult:
    status: str
    source_month: str
    source_version_id: str
    clean_version_id: str
    history_version_id: str
    fact_version_id: str
    version_path: Path
    current_pointer_path: Path
    fact_row_count: int
    station_history_match_count: int
    station_history_unmatched_count: int
    warning_row_count: int


def compute_configuration_fingerprint(configuration: Mapping[str, object]) -> str:
    canonical = _canonical_configuration_object(configuration)
    return CONFIGURATION_FINGERPRINT_PREFIX + hashlib.sha256(
        _canonical_json_bytes(canonical)
    ).hexdigest()


def compute_fact_version_id(
    *,
    source_month: str,
    clean_version_id: str,
    history_version_id: str,
    transformation_version: str,
    configuration_fingerprint: str,
    enrichment_sql_sha256: str,
    fact_sql_sha256: str,
) -> str:
    source_month = _require_month(source_month)
    _require_version(clean_version_id, _CLEAN_VERSION, "clean_version_id")
    _require_version(history_version_id, _HISTORY_VERSION, "history_version_id")
    _require_text(transformation_version, "transformation_version")
    if _CONFIGURATION_FINGERPRINT.fullmatch(configuration_fingerprint) is None:
        raise FactTripPublishError("configuration_fingerprint must use cfg1_<sha256>")
    for value, label in (
        (enrichment_sql_sha256, "enrichment_sql_sha256"),
        (fact_sql_sha256, "fact_sql_sha256"),
    ):
        if _SHA256.fullmatch(value) is None:
            raise FactTripPublishError(f"{label} must be a lowercase SHA-256 digest")
    payload = [
        source_month,
        clean_version_id,
        history_version_id,
        transformation_version,
        configuration_fingerprint,
        enrichment_sql_sha256,
        fact_sql_sha256,
    ]
    return FACT_VERSION_PREFIX + hashlib.sha256(_canonical_json_bytes(payload)).hexdigest()


def fact_version_path(
    warehouse_root: str | Path,
    source_month: str,
    fact_version_id: str,
) -> Path:
    source_month = _require_month(source_month)
    _require_version(fact_version_id, _FACT_VERSION, "fact_version_id")
    return (
        Path(warehouse_root)
        / "fact_trip_trusted"
        / "versions"
        / f"source_month={source_month}"
        / f"fact_version_id={fact_version_id}"
    )


def fact_current_pointer_path(
    warehouse_root: str | Path,
    source_month: str,
) -> Path:
    source_month = _require_month(source_month)
    return (
        Path(warehouse_root)
        / "fact_trip_trusted"
        / "current"
        / f"source_month={source_month}.json"
    )


def publish_current_fact_trip_month(
    *,
    manifest: str | Path,
    raw_root: str | Path,
    station_root: str | Path,
    clean_root: str | Path,
    warehouse_root: str | Path,
    source_month: str,
    transformation_version: str,
    configuration: Mapping[str, object],
    enrichment_sql_path: str | Path | None = None,
    fact_sql_path: str | Path | None = None,
    expected_clean_version_id: str | None = None,
    expected_source_version_id: str | None = None,
    before_current_publish: Callable[[Path], None] | None = None,
) -> FactTripPublishResult:
    """Publish one current Clean month as an immutable trusted fact version."""
    source_month = _require_month(source_month)
    _require_text(transformation_version, "transformation_version")
    canonical_configuration = _canonical_configuration_object(configuration)
    configuration_fingerprint = compute_configuration_fingerprint(canonical_configuration)
    enrichment_sql_text, enrichment_sql_sha256 = load_station_enrichment_sql(
        enrichment_sql_path
    )
    fact_sql_text, fact_sql_sha256 = _load_sql(fact_sql_path)

    clean_pointer_path = current_pointer_path(clean_root, source_month)
    clean_pointer = _load_clean_pointer(clean_pointer_path, source_month)
    history_pointer_path = history_current_pointer_path(warehouse_root)
    history_pointer = _load_history_pointer(history_pointer_path)

    clean_version_id = clean_pointer["clean_version_id"]
    history_version_id = history_pointer["history_version_id"]
    clean_path = clean_version_path(clean_root, source_month, clean_version_id)
    history_path = history_version_path(warehouse_root, history_version_id)
    clean_metadata = _validate_clean_input(
        clean_path,
        source_month=source_month,
        clean_version_id=clean_version_id,
    )
    if (
        expected_clean_version_id is not None
        and clean_version_id != expected_clean_version_id
    ):
        raise FactTripPublishError(
            "current Clean version does not match the caller's expected lineage"
        )
    history_metadata = _validate_history_input(
        history_path,
        history_version_id=history_version_id,
        manifest=manifest,
        raw_root=raw_root,
        station_root=station_root,
    )
    source_version_id = _require_text(
        clean_metadata["source_version_id"], "source_version_id"
    )
    if (
        expected_source_version_id is not None
        and source_version_id != expected_source_version_id
    ):
        raise FactTripPublishError(
            "current Clean source version does not match the caller's expected lineage"
        )

    fact_version_id = compute_fact_version_id(
        source_month=source_month,
        clean_version_id=clean_version_id,
        history_version_id=history_version_id,
        transformation_version=transformation_version,
        configuration_fingerprint=configuration_fingerprint,
        enrichment_sql_sha256=enrichment_sql_sha256,
        fact_sql_sha256=fact_sql_sha256,
    )
    version_path = fact_version_path(warehouse_root, source_month, fact_version_id)
    current_path = fact_current_pointer_path(warehouse_root, source_month)
    initial_pointer = _load_fact_pointer_if_present(current_path, source_month)
    if initial_pointer is not None:
        initial_version_path = fact_version_path(
            warehouse_root, source_month, initial_pointer["fact_version_id"]
        )
        initial_metadata = validate_fact_trip_version(
            initial_version_path,
            manifest=manifest,
            raw_root=raw_root,
            station_root=station_root,
            clean_root=clean_root,
            warehouse_root=warehouse_root,
        )
        _require_bundle_binding(
            initial_metadata,
            expected_source_month=source_month,
            expected_fact_version_id=initial_pointer["fact_version_id"],
            version_path=initial_version_path,
        )

    facts_root = Path(warehouse_root) / "fact_trip_trusted"
    if version_path.exists():
        candidate_metadata = validate_fact_trip_version(
            version_path,
            manifest=manifest,
            raw_root=raw_root,
            station_root=station_root,
            clean_root=clean_root,
            warehouse_root=warehouse_root,
        )
        _require_bundle_binding(
            candidate_metadata,
            expected_source_month=source_month,
            expected_fact_version_id=fact_version_id,
            version_path=version_path,
        )
        _require_candidate_lineage(
            candidate_metadata,
            source_version_id=source_version_id,
            clean_version_id=clean_version_id,
            history_version_id=history_version_id,
            transformation_version=transformation_version,
            configuration=canonical_configuration,
            configuration_fingerprint=configuration_fingerprint,
            enrichment_sql_sha256=enrichment_sql_sha256,
            fact_sql_sha256=fact_sql_sha256,
        )
    else:
        staging_parent = facts_root / "staging"
        staging_parent.mkdir(parents=True, exist_ok=True)
        stage_container = Path(
            tempfile.mkdtemp(prefix="build-", dir=staging_parent)
        )
        stage_path = (
            stage_container
            / f"source_month={source_month}"
            / f"fact_version_id={fact_version_id}"
        )
        stage_path.mkdir(parents=True)
        try:
            summary = _build_stage_database(
                stage_path=stage_path,
                manifest=manifest,
                raw_root=raw_root,
                station_root=station_root,
                clean_path=clean_path,
                history_path=history_path,
                enrichment_sql_text=enrichment_sql_text,
                fact_sql_text=fact_sql_text,
            )
            candidate_metadata = _build_metadata(
                stage_path=stage_path,
                source_month=source_month,
                source_version_id=source_version_id,
                clean_version_id=clean_version_id,
                history_version_id=history_version_id,
                transformation_version=transformation_version,
                configuration=canonical_configuration,
                configuration_fingerprint=configuration_fingerprint,
                enrichment_sql_sha256=enrichment_sql_sha256,
                fact_sql_sha256=fact_sql_sha256,
                fact_version_id=fact_version_id,
                summary=summary,
            )
            _write_json(stage_path / "metadata.json", candidate_metadata)
            validated_stage = validate_fact_trip_version(
                stage_path,
                manifest=manifest,
                raw_root=raw_root,
                station_root=station_root,
                clean_root=clean_root,
                warehouse_root=warehouse_root,
            )
            _require_same_logical_version(candidate_metadata, validated_stage)

            version_path.parent.mkdir(parents=True, exist_ok=True)
            try:
                os.replace(stage_path, version_path)
            except OSError:
                if not version_path.exists():
                    raise
                existing = validate_fact_trip_version(
                    version_path,
                    manifest=manifest,
                    raw_root=raw_root,
                    station_root=station_root,
                    clean_root=clean_root,
                    warehouse_root=warehouse_root,
                )
                _require_bundle_binding(
                    existing,
                    expected_source_month=source_month,
                    expected_fact_version_id=fact_version_id,
                    version_path=version_path,
                )
                _require_same_logical_version(existing, candidate_metadata)
                candidate_metadata = existing
                shutil.rmtree(stage_container)
            else:
                candidate_metadata = validate_fact_trip_version(
                    version_path,
                    manifest=manifest,
                    raw_root=raw_root,
                    station_root=station_root,
                    clean_root=clean_root,
                    warehouse_root=warehouse_root,
                )
                _require_bundle_binding(
                    candidate_metadata,
                    expected_source_month=source_month,
                    expected_fact_version_id=fact_version_id,
                    version_path=version_path,
                )
        finally:
            if stage_container.exists():
                shutil.rmtree(stage_container)

    with _publication_locks(
        clean_root=Path(clean_root),
        warehouse_root=Path(warehouse_root),
        facts_root=facts_root,
        source_month=source_month,
        fact_version_id=fact_version_id,
    ):
        _require_current_lineage_unchanged(
            clean_pointer_path=clean_pointer_path,
            clean_pointer=clean_pointer,
            history_pointer_path=history_pointer_path,
            history_pointer=history_pointer,
            source_month=source_month,
        )
        final_clean_metadata = _validate_clean_input(
            clean_path,
            source_month=source_month,
            clean_version_id=clean_version_id,
        )
        final_history_metadata = _validate_history_input(
            history_path,
            history_version_id=history_version_id,
            manifest=manifest,
            raw_root=raw_root,
            station_root=station_root,
        )
        if final_clean_metadata != clean_metadata:
            raise FactTripPublishError(
                "immutable Clean metadata changed during trusted fact publication"
            )
        if final_history_metadata != history_metadata:
            raise FactTripPublishError(
                "immutable station-history metadata changed during trusted fact publication"
            )
        final_metadata = validate_fact_trip_version(
            version_path,
            manifest=manifest,
            raw_root=raw_root,
            station_root=station_root,
            clean_root=clean_root,
            warehouse_root=warehouse_root,
        )
        _require_bundle_binding(
            final_metadata,
            expected_source_month=source_month,
            expected_fact_version_id=fact_version_id,
            version_path=version_path,
        )
        if final_metadata != candidate_metadata:
            raise FactTripPublishError(
                "immutable trusted fact bundle changed before final publication validation"
            )

        latest_pointer = _load_fact_pointer_if_present(current_path, source_month)
        if latest_pointer is not None and latest_pointer["fact_version_id"] == fact_version_id:
            return _result(STATUS_REUSED_CURRENT, final_metadata, version_path, current_path)
        if latest_pointer != initial_pointer:
            raise FactTripPublishError(
                "trusted fact current pointer changed during build; refusing concurrent overwrite"
            )

        if before_current_publish is not None:
            before_current_publish(version_path)
            _require_current_lineage_unchanged(
                clean_pointer_path=clean_pointer_path,
                clean_pointer=clean_pointer,
                history_pointer_path=history_pointer_path,
                history_pointer=history_pointer,
                source_month=source_month,
            )
            callback_clean_metadata = _validate_clean_input(
                clean_path,
                source_month=source_month,
                clean_version_id=clean_version_id,
            )
            callback_history_metadata = _validate_history_input(
                history_path,
                history_version_id=history_version_id,
                manifest=manifest,
                raw_root=raw_root,
                station_root=station_root,
            )
            if callback_clean_metadata != clean_metadata:
                raise FactTripPublishError(
                    "immutable Clean metadata changed before trusted fact current publish"
                )
            if callback_history_metadata != history_metadata:
                raise FactTripPublishError(
                    "immutable station-history metadata changed before trusted fact current publish"
                )
            callback_metadata = validate_fact_trip_version(
                version_path,
                manifest=manifest,
                raw_root=raw_root,
                station_root=station_root,
                clean_root=clean_root,
                warehouse_root=warehouse_root,
            )
            if callback_metadata != final_metadata:
                raise FactTripPublishError(
                    "immutable trusted fact bundle changed after final validation"
                )
            if _load_fact_pointer_if_present(current_path, source_month) != initial_pointer:
                raise FactTripPublishError(
                    "trusted fact current pointer changed before atomic publish"
                )

        _write_fact_pointer(current_path, source_month, fact_version_id)

    return _result(STATUS_PUBLISHED_CURRENT, final_metadata, version_path, current_path)


def validate_fact_trip_version(
    version_path: str | Path,
    *,
    manifest: str | Path,
    raw_root: str | Path,
    station_root: str | Path,
    clean_root: str | Path,
    warehouse_root: str | Path,
) -> dict[str, object]:
    """Hard-validate one immutable trusted fact version and its input lineage."""
    path = Path(version_path)
    metadata = _load_json(path / "metadata.json", "trusted fact metadata")
    _validate_metadata_identity(metadata)
    _require_bundle_binding(
        metadata,
        expected_source_month=metadata["source_month"],
        expected_fact_version_id=metadata["fact_version_id"],
        version_path=path,
    )

    source_month = metadata["source_month"]
    clean_version_id = metadata["clean_version_id"]
    history_version_id = metadata["history_version_id"]
    clean_path = clean_version_path(clean_root, source_month, clean_version_id)
    history_path = history_version_path(warehouse_root, history_version_id)
    clean_metadata = _validate_clean_input(
        clean_path,
        source_month=source_month,
        clean_version_id=clean_version_id,
    )
    _validate_history_input(
        history_path,
        history_version_id=history_version_id,
        manifest=manifest,
        raw_root=raw_root,
        station_root=station_root,
    )
    if metadata["source_version_id"] != clean_metadata["source_version_id"]:
        raise FactTripPublishError(
            "trusted fact metadata source_version_id does not match Clean lineage"
        )

    database_path = path / "warehouse.duckdb"
    wal_path = Path(str(database_path) + ".wal")
    if wal_path.exists():
        raise FactTripPublishError(
            "trusted fact warehouse has residual WAL and is not finalized"
        )
    _require_file_fingerprint(
        database_path,
        metadata["warehouse_database"],
        label="warehouse.duckdb",
    )
    connection = duckdb.connect(
        config={
            "memory_limit": "3GB",
            "threads": "2",
        }
    )
    try:
        connection.execute(
            f"ATTACH '{_sql_path(database_path)}' AS fact_bundle (READ_ONLY)"
        )
        connection.execute(
            f"ATTACH '{_sql_path(history_path / 'warehouse.duckdb')}' "
            "AS station_history (READ_ONLY)"
        )
        connection.execute(
            f"CREATE VIEW clean_input AS SELECT * FROM "
            f"read_parquet('{_sql_path(clean_path / 'clean.parquet')}', hive_partitioning=false)"
        )
        _require_fact_schema(connection)
        _require_fact_semantics(connection, source_month, clean_metadata)
        counts = connection.execute(
            """
            SELECT
                COUNT(*),
                COUNT(*) FILTER (WHERE station_history_match),
                COUNT(*) FILTER (WHERE NOT station_history_match),
                COUNT(*) FILTER (WHERE dq_warning_count > 0)
            FROM fact_bundle.fact_trip_trusted
            """
        ).fetchone()
    except duckdb.Error as exc:
        raise FactTripPublishError(
            f"cannot validate trusted fact warehouse database: {exc}"
        ) from exc
    finally:
        connection.close()

    expected = {
        "fact_row_count": int(counts[0]),
        "station_history_match_count": int(counts[1]),
        "station_history_unmatched_count": int(counts[2]),
        "warning_row_count": int(counts[3]),
    }
    for key, value in expected.items():
        if metadata[key] != value:
            raise FactTripPublishError(
                f"trusted fact metadata {key}={metadata[key]!r} "
                f"does not match durable value {value!r}"
            )
    return metadata


def _build_stage_database(
    *,
    stage_path: Path,
    manifest: str | Path,
    raw_root: str | Path,
    station_root: str | Path,
    clean_path: Path,
    history_path: Path,
    enrichment_sql_text: str,
    fact_sql_text: str,
):
    database_path = stage_path / "warehouse.duckdb"
    scratch_path = stage_path / "fact_enriched_input.ndjson"
    connection = duckdb.connect(str(database_path))
    scratch_handle = None

    try:
        columns = ", ".join(f'"{name}" {type_name}' for name, type_name in _FACT_SCHEMA)
        connection.execute(f"CREATE TABLE fact_enriched_input ({columns})")
        scratch_handle = scratch_path.open("w", encoding="utf-8", newline="\n")

        def on_row(row: dict[str, object]) -> None:
            scratch_handle.write(
                json.dumps(
                    {name: row[name] for name, _ in _FACT_SCHEMA},
                    ensure_ascii=False,
                    separators=(",", ":"),
                    allow_nan=False,
                    default=_json_scalar,
                )
                + "\n"
            )

        summary = evaluate_trip_station_enrichment(
            manifest=manifest,
            raw_root=raw_root,
            station_root=station_root,
            clean_version_path=clean_path,
            station_history_version_path=history_path,
            sql_text=enrichment_sql_text,
            on_row=on_row,
        )
        scratch_handle.flush()
        os.fsync(scratch_handle.fileno())
        scratch_handle.close()
        scratch_handle = None
        connection.execute(
            f"COPY fact_enriched_input FROM '{_sql_path(scratch_path)}' "
            "(FORMAT JSON)"
        )
        staged_count = connection.execute(
            "SELECT COUNT(*) FROM fact_enriched_input"
        ).fetchone()[0]
        if staged_count != summary.trusted_candidate_count:
            raise FactTripPublishError(
                "staged trusted fact rows do not match enrichment candidate count"
            )
        connection.execute(fact_sql_text)
        _require_materialized_fact_table(connection)
        connection.execute("DROP TABLE fact_enriched_input")
        scratch_path.unlink()
        connection.execute("CHECKPOINT")
        return summary
    except duckdb.Error as exc:
        raise FactTripPublishError(f"trusted fact build failed: {exc}") from exc
    finally:
        if scratch_handle is not None:
            scratch_handle.close()
        connection.close()
        scratch_path.unlink(missing_ok=True)


def _build_metadata(
    *,
    stage_path: Path,
    source_month: str,
    source_version_id: str,
    clean_version_id: str,
    history_version_id: str,
    transformation_version: str,
    configuration: dict[str, object],
    configuration_fingerprint: str,
    enrichment_sql_sha256: str,
    fact_sql_sha256: str,
    fact_version_id: str,
    summary,
) -> dict[str, object]:
    database_path = stage_path / "warehouse.duckdb"
    connection = duckdb.connect(str(database_path), read_only=True)
    try:
        fact_row_count, match_count, unmatched_count, warning_row_count = connection.execute(
            """
            SELECT
                COUNT(*),
                COUNT(*) FILTER (WHERE station_history_match),
                COUNT(*) FILTER (WHERE NOT station_history_match),
                COUNT(*) FILTER (WHERE dq_warning_count > 0)
            FROM fact_trip_trusted
            """
        ).fetchone()
    finally:
        connection.close()
    if fact_row_count != summary.trusted_candidate_count:
        raise FactTripPublishError(
            "fact row count does not match enrichment trusted candidate count"
        )
    if match_count != summary.station_history_match_count:
        raise FactTripPublishError(
            "fact station match count does not match enrichment summary"
        )
    if unmatched_count != summary.station_history_unmatched_count:
        raise FactTripPublishError(
            "fact station unmatched count does not match enrichment summary"
        )
    size_bytes, sha256 = _fingerprint_file(database_path)
    return {
        "schema_version": METADATA_SCHEMA_VERSION,
        "source_month": source_month,
        "source_version_id": source_version_id,
        "clean_version_id": clean_version_id,
        "history_version_id": history_version_id,
        "transformation_version": transformation_version,
        "configuration": configuration,
        "configuration_fingerprint": configuration_fingerprint,
        "enrichment_sql_sha256": enrichment_sql_sha256,
        "fact_sql_sha256": fact_sql_sha256,
        "fact_version_id": fact_version_id,
        "fact_row_count": int(fact_row_count),
        "station_history_match_count": int(match_count),
        "station_history_unmatched_count": int(unmatched_count),
        "warning_row_count": int(warning_row_count),
        "warehouse_database": {
            "size_bytes": size_bytes,
            "sha256": sha256,
        },
    }


def _require_fact_schema(connection: duckdb.DuckDBPyConnection) -> None:
    actual = tuple(
        (row[0], row[1])
        for row in connection.execute(
            "DESCRIBE SELECT * FROM fact_bundle.fact_trip_trusted"
        ).fetchall()
    )
    if actual != _FACT_SCHEMA:
        raise FactTripPublishError(
            f"fact_trip_trusted schema mismatch: expected {_FACT_SCHEMA!r}, got {actual!r}"
        )
    rows = connection.execute(
        """
        SELECT table_type
        FROM information_schema.tables
        WHERE table_catalog = 'fact_bundle'
          AND table_schema = 'main'
          AND table_name = 'fact_trip_trusted'
        """
    ).fetchall()
    if rows != [("BASE TABLE",)]:
        raise FactTripPublishError("fact_trip_trusted must be a materialized base table")


def _require_fact_semantics(
    connection: duckdb.DuckDBPyConnection,
    source_month: str,
    clean_metadata: dict[str, object],
) -> None:
    invalid_issue_field_count, invalid_issue_severity_count = connection.execute(
        """
        SELECT
            COUNT(*) FILTER (
                WHERE issue.rule_id IS NULL OR trim(issue.rule_id) = ''
                   OR issue.severity IS NULL OR trim(issue.severity) = ''
                   OR issue.field IS NULL OR trim(issue.field) = ''
                   OR issue.reason IS NULL OR trim(issue.reason) = ''
            ),
            COUNT(*) FILTER (
                WHERE issue.severity IS NOT NULL
                  AND issue.severity NOT IN ('WARNING', 'QUARANTINE')
            )
        FROM fact_bundle.fact_trip_trusted AS fact,
             UNNEST(fact.dq_issues) AS issues(issue)
        """
    ).fetchone()
    if invalid_issue_field_count:
        raise FactTripPublishError(
            "fact_trip_trusted contains DQ issue with missing/empty rule_id, severity, field, or reason"
        )
    if invalid_issue_severity_count:
        raise FactTripPublishError(
            "fact_trip_trusted contains DQ issue severity outside WARNING/QUARANTINE contract"
        )

    (
        row_count,
        unique_source_rows,
        invalid_required_state,
        invalid_month,
        invalid_station_match,
        invalid_warning_count,
        quarantine_issue_count,
    ) = connection.execute(
        """
        WITH warning_stats AS (
            SELECT
                fact.source_row_id,
                COUNT(DISTINCT issue.rule_id) FILTER (
                    WHERE issue.severity = 'WARNING'
                ) AS expected_warning_count,
                COUNT(*) FILTER (WHERE issue.severity = 'QUARANTINE') AS quarantine_issues
            FROM fact_bundle.fact_trip_trusted AS fact
            LEFT JOIN UNNEST(fact.dq_issues) AS issues(issue) ON TRUE
            GROUP BY fact.source_row_id
        )
        SELECT
            COUNT(*),
            COUNT(DISTINCT fact.source_row_id),
            COUNT(*) FILTER (
                WHERE fact.source_row_id IS NULL
                   OR fact.station_history_match IS NULL
                   OR fact.row_quarantined IS DISTINCT FROM FALSE
                   OR fact.trusted_fact_eligible IS DISTINCT FROM TRUE
                   OR fact.dq_issues IS NULL
                   OR fact.dq_warning_count IS NULL
            ),
            COUNT(*) FILTER (WHERE fact.source_month IS DISTINCT FROM ?),
            COUNT(*) FILTER (
                WHERE fact.station_history_match
                      IS DISTINCT FROM (fact.matched_station_applicable_from IS NOT NULL)
                   OR (
                        fact.matched_station_applicable_from IS NULL
                        AND fact.matched_station_applicable_until IS NOT NULL
                   )
            ),
            COUNT(*) FILTER (
                WHERE fact.dq_warning_count
                      IS DISTINCT FROM COALESCE(stats.expected_warning_count, 0)
            ),
            COALESCE(SUM(stats.quarantine_issues), 0)
        FROM fact_bundle.fact_trip_trusted AS fact
        LEFT JOIN warning_stats AS stats USING (source_row_id)
        """,
        [source_month],
    ).fetchone()
    if row_count != clean_metadata["trusted_fact_eligible_count"]:
        raise FactTripPublishError(
            "fact row count does not match trusted-fact-eligible Clean row count"
        )
    if unique_source_rows != row_count:
        raise FactTripPublishError("fact_trip_trusted source_row_id values are not unique")
    if invalid_required_state:
        raise FactTripPublishError(
            "fact_trip_trusted violates trusted/enriched required row state"
        )
    if invalid_month:
        raise FactTripPublishError("fact_trip_trusted contains row outside source_month")
    if invalid_station_match:
        raise FactTripPublishError(
            "fact_trip_trusted station_history_match does not reconcile with matched interval"
        )
    if invalid_warning_count:
        raise FactTripPublishError(
            "fact_trip_trusted dq_warning_count does not match distinct WARNING rules"
        )
    if quarantine_issue_count:
        raise FactTripPublishError(
            "fact_trip_trusted contains QUARANTINE issue on trusted row"
        )

    comparisons = "\n                   OR ".join(
        f'fact."{name}" IS DISTINCT FROM clean."{name}"'
        for name, _ in _CLEAN_SCHEMA
        if name
        not in {
            "source_row_id",
            "station_history_match",
            "dq_issues",
            "dq_warning_count",
        }
    )
    projection_mismatch = connection.execute(
        f"""
        WITH trusted_clean AS (
            SELECT * FROM clean_input WHERE trusted_fact_eligible
        )
        SELECT COUNT(*)
        FROM trusted_clean AS clean
        FULL OUTER JOIN fact_bundle.fact_trip_trusted AS fact
            ON fact.source_row_id = clean.source_row_id
        WHERE clean.source_row_id IS NULL
           OR fact.source_row_id IS NULL
           OR {comparisons}
        """
    ).fetchone()[0]
    if projection_mismatch:
        raise FactTripPublishError(
            "fact_trip_trusted modified trusted Clean projection or source-row set"
        )

    issue_difference = connection.execute(
        f"""
        WITH trusted_clean AS (
            SELECT * FROM clean_input WHERE trusted_fact_eligible
        ), clean_issues AS (
            SELECT
                clean.source_row_id,
                issue.rule_id,
                issue.severity,
                issue.field,
                issue.reason
            FROM trusted_clean AS clean,
                 UNNEST(clean.dq_issues) AS issues(issue)
        ), fact_base_issues AS (
            SELECT
                fact.source_row_id,
                issue.rule_id,
                issue.severity,
                issue.field,
                issue.reason
            FROM fact_bundle.fact_trip_trusted AS fact,
                 UNNEST(fact.dq_issues) AS issues(issue)
            WHERE issue.rule_id <> '{RULE_STATION_HISTORY_UNMATCHED}'
        )
        SELECT
            (SELECT COUNT(*) FROM (
                SELECT * FROM clean_issues
                EXCEPT ALL
                SELECT * FROM fact_base_issues
            )),
            (SELECT COUNT(*) FROM (
                SELECT * FROM fact_base_issues
                EXCEPT ALL
                SELECT * FROM clean_issues
            ))
        """
    ).fetchone()
    if issue_difference != (0, 0):
        raise FactTripPublishError(
            "fact_trip_trusted does not preserve pre-enrichment Clean DQ issues exactly"
        )

    station_warning_mismatch = connection.execute(
        f"""
        WITH station_warning AS (
            SELECT
                fact.source_row_id,
                COUNT(*) AS warning_count,
                COUNT(*) FILTER (
                    WHERE issue.severity <> 'WARNING'
                       OR issue.field <> 'rent_station_no'
                       OR issue.reason <> ?
                ) AS invalid_warning
            FROM fact_bundle.fact_trip_trusted AS fact,
                 UNNEST(fact.dq_issues) AS issues(issue)
            WHERE issue.rule_id = '{RULE_STATION_HISTORY_UNMATCHED}'
            GROUP BY fact.source_row_id
        )
        SELECT COUNT(*)
        FROM fact_bundle.fact_trip_trusted AS fact
        LEFT JOIN station_warning AS warning USING (source_row_id)
        WHERE (
                fact.station_history_match
                AND COALESCE(warning.warning_count, 0) <> 0
              )
           OR (
                NOT fact.station_history_match
                AND COALESCE(warning.warning_count, 0) <> 1
              )
           OR COALESCE(warning.invalid_warning, 0) <> 0
        """,
        [_STATION_WARNING_REASON],
    ).fetchone()[0]
    if station_warning_mismatch:
        raise FactTripPublishError(
            "fact_trip_trusted station unmatched warning semantics are invalid"
        )

    temporal_mismatch = connection.execute(
        """
        WITH expected AS (
            SELECT
                clean.source_row_id,
                history.applicable_from,
                history.applicable_until
            FROM clean_input AS clean
            LEFT JOIN station_history.dim_station_history AS history
                ON history.station_no = clean.rent_station_no
               AND CAST(clean.rent_at AS DATE) >= history.applicable_from
               AND (
                    history.applicable_until IS NULL
                    OR CAST(clean.rent_at AS DATE) < history.applicable_until
               )
            WHERE clean.trusted_fact_eligible
        )
        SELECT COUNT(*)
        FROM expected
        JOIN fact_bundle.fact_trip_trusted AS fact USING (source_row_id)
        WHERE fact.station_history_match
                IS DISTINCT FROM (expected.applicable_from IS NOT NULL)
           OR fact.matched_station_applicable_from
                IS DISTINCT FROM expected.applicable_from
           OR fact.matched_station_applicable_until
                IS DISTINCT FROM expected.applicable_until
        """
    ).fetchone()[0]
    if temporal_mismatch:
        raise FactTripPublishError(
            "fact_trip_trusted does not match independently recomputed station history applicability"
        )


def _require_materialized_fact_table(connection: duckdb.DuckDBPyConnection) -> None:
    rows = connection.execute(
        """
        SELECT table_type
        FROM information_schema.tables
        WHERE table_schema = 'main'
          AND table_name = 'fact_trip_trusted'
        """
    ).fetchall()
    if rows != [("BASE TABLE",)]:
        raise FactTripPublishError(
            "fact SQL must materialize fact_trip_trusted as a base table"
        )
    actual = tuple(
        (row[0], row[1])
        for row in connection.execute(
            "DESCRIBE SELECT * FROM fact_trip_trusted"
        ).fetchall()
    )
    if actual != _FACT_SCHEMA:
        raise FactTripPublishError(
            f"fact SQL schema mismatch: expected {_FACT_SCHEMA!r}, got {actual!r}"
        )


def _validate_metadata_identity(metadata: dict[str, object]) -> None:
    if set(metadata) != _METADATA_FIELDS:
        raise FactTripPublishError("trusted fact metadata has invalid fields")
    if metadata["schema_version"] != METADATA_SCHEMA_VERSION:
        raise FactTripPublishError("trusted fact metadata has unsupported schema_version")
    source_month = _require_month(metadata["source_month"])
    clean_version_id = _require_version(
        metadata["clean_version_id"], _CLEAN_VERSION, "clean_version_id"
    )
    history_version_id = _require_version(
        metadata["history_version_id"], _HISTORY_VERSION, "history_version_id"
    )
    transformation_version = _require_text(
        metadata["transformation_version"], "transformation_version"
    )
    configuration = _canonical_configuration_object(metadata["configuration"])
    configuration_fingerprint = compute_configuration_fingerprint(configuration)
    if metadata["configuration_fingerprint"] != configuration_fingerprint:
        raise FactTripPublishError(
            "trusted fact metadata configuration_fingerprint does not match configuration"
        )
    enrichment_sql_sha256 = metadata["enrichment_sql_sha256"]
    fact_sql_sha256 = metadata["fact_sql_sha256"]
    if not isinstance(enrichment_sql_sha256, str) or _SHA256.fullmatch(
        enrichment_sql_sha256
    ) is None:
        raise FactTripPublishError("trusted fact metadata enrichment_sql_sha256 is invalid")
    if not isinstance(fact_sql_sha256, str) or _SHA256.fullmatch(fact_sql_sha256) is None:
        raise FactTripPublishError("trusted fact metadata fact_sql_sha256 is invalid")
    expected_fact_version_id = compute_fact_version_id(
        source_month=source_month,
        clean_version_id=clean_version_id,
        history_version_id=history_version_id,
        transformation_version=transformation_version,
        configuration_fingerprint=configuration_fingerprint,
        enrichment_sql_sha256=enrichment_sql_sha256,
        fact_sql_sha256=fact_sql_sha256,
    )
    if metadata["fact_version_id"] != expected_fact_version_id:
        raise FactTripPublishError(
            "trusted fact metadata fact_version_id does not match deterministic inputs"
        )
    _require_text(metadata["source_version_id"], "source_version_id")
    for key in (
        "fact_row_count",
        "station_history_match_count",
        "station_history_unmatched_count",
        "warning_row_count",
    ):
        value = metadata[key]
        if not isinstance(value, int) or isinstance(value, bool) or value < 0:
            raise FactTripPublishError(
                f"trusted fact metadata {key} must be a non-negative integer"
            )
    if (
        metadata["fact_row_count"]
        != metadata["station_history_match_count"]
        + metadata["station_history_unmatched_count"]
    ):
        raise FactTripPublishError(
            "trusted fact metadata match/unmatched counts do not reconcile"
        )
    fingerprint = metadata["warehouse_database"]
    if not isinstance(fingerprint, dict) or set(fingerprint) != {"size_bytes", "sha256"}:
        raise FactTripPublishError(
            "trusted fact metadata warehouse_database fingerprint is malformed"
        )
    if (
        not isinstance(fingerprint["size_bytes"], int)
        or isinstance(fingerprint["size_bytes"], bool)
        or fingerprint["size_bytes"] <= 0
        or not isinstance(fingerprint["sha256"], str)
        or _SHA256.fullmatch(fingerprint["sha256"]) is None
    ):
        raise FactTripPublishError(
            "trusted fact metadata warehouse_database fingerprint values are invalid"
        )


def _validate_clean_input(
    clean_path: Path,
    *,
    source_month: str,
    clean_version_id: str,
) -> dict[str, object]:
    try:
        metadata = validate_clean_version(clean_path)
    except SourceRegistrationError as exc:
        raise FactTripPublishError(f"invalid Clean input for trusted fact: {exc}") from exc
    if metadata["source_month"] != source_month:
        raise FactTripPublishError("Clean bundle source_month does not match fact lineage")
    if metadata["clean_version_id"] != clean_version_id:
        raise FactTripPublishError("Clean bundle version id does not match fact lineage")
    if clean_path.name != f"clean_version_id={clean_version_id}":
        raise FactTripPublishError("Clean bundle version id does not match path")
    if clean_path.parent.name != f"source_month={source_month}":
        raise FactTripPublishError("Clean bundle source_month does not match path")
    return metadata


def _validate_history_input(
    history_path: Path,
    *,
    history_version_id: str,
    manifest: str | Path,
    raw_root: str | Path,
    station_root: str | Path,
) -> dict[str, object]:
    try:
        metadata = validate_station_history_version(
            history_path,
            manifest=manifest,
            raw_root=raw_root,
            station_root=station_root,
        )
    except SourceRegistrationError as exc:
        raise FactTripPublishError(
            f"invalid station-history input for trusted fact: {exc}"
        ) from exc
    if metadata["history_version_id"] != history_version_id:
        raise FactTripPublishError(
            "station-history bundle version id does not match fact lineage"
        )
    if history_path.name != f"history_version_id={history_version_id}":
        raise FactTripPublishError("station-history bundle version id does not match path")
    return metadata


def _require_candidate_lineage(
    metadata: dict[str, object],
    *,
    source_version_id: str,
    clean_version_id: str,
    history_version_id: str,
    transformation_version: str,
    configuration: dict[str, object],
    configuration_fingerprint: str,
    enrichment_sql_sha256: str,
    fact_sql_sha256: str,
) -> None:
    expected = {
        "source_version_id": source_version_id,
        "clean_version_id": clean_version_id,
        "history_version_id": history_version_id,
        "transformation_version": transformation_version,
        "configuration": configuration,
        "configuration_fingerprint": configuration_fingerprint,
        "enrichment_sql_sha256": enrichment_sql_sha256,
        "fact_sql_sha256": fact_sql_sha256,
    }
    for key, value in expected.items():
        if metadata[key] != value:
            raise FactTripPublishError(
                f"existing trusted fact version {key} does not match requested lineage"
            )


def _require_bundle_binding(
    metadata: dict[str, object],
    *,
    expected_source_month: str,
    expected_fact_version_id: str,
    version_path: Path,
) -> None:
    if metadata["source_month"] != expected_source_month:
        raise FactTripPublishError(
            "trusted fact bundle source_month does not match directory/current binding"
        )
    if metadata["fact_version_id"] != expected_fact_version_id:
        raise FactTripPublishError(
            "trusted fact bundle version id does not match directory/current binding"
        )
    if version_path.name != f"fact_version_id={expected_fact_version_id}":
        raise FactTripPublishError("trusted fact version directory id is not bound to metadata")
    if version_path.parent.name != f"source_month={expected_source_month}":
        raise FactTripPublishError("trusted fact version directory month is not bound to metadata")


def _require_same_logical_version(
    existing: dict[str, object],
    candidate: dict[str, object],
) -> None:
    existing_logical = {
        key: value for key, value in existing.items() if key != "warehouse_database"
    }
    candidate_logical = {
        key: value for key, value in candidate.items() if key != "warehouse_database"
    }
    if existing_logical != candidate_logical:
        raise FactTripPublishError(
            "same fact_version_id exists with different logical metadata"
        )


def _require_current_lineage_unchanged(
    *,
    clean_pointer_path: Path,
    clean_pointer: dict[str, object],
    history_pointer_path: Path,
    history_pointer: dict[str, object],
    source_month: str,
) -> None:
    if _load_clean_pointer(clean_pointer_path, source_month) != clean_pointer:
        raise FactTripPublishError(
            "current Clean pointer changed during trusted fact publication"
        )
    if _load_history_pointer(history_pointer_path) != history_pointer:
        raise FactTripPublishError(
            "current station-history pointer changed during trusted fact publication"
        )


def _load_clean_pointer(path: Path, source_month: str) -> dict[str, object]:
    payload = _load_json(path, "current Clean pointer")
    if set(payload) != {"schema_version", "source_month", "clean_version_id"}:
        raise FactTripPublishError(f"current Clean pointer {path} has invalid fields")
    if payload["schema_version"] != 1 or payload["source_month"] != source_month:
        raise FactTripPublishError(
            f"current Clean pointer {path} has invalid version/month binding"
        )
    _require_version(payload["clean_version_id"], _CLEAN_VERSION, "clean_version_id")
    return payload


def _load_history_pointer(path: Path) -> dict[str, object]:
    payload = _load_json(path, "current station-history pointer")
    if set(payload) != {"schema_version", "history_version_id"}:
        raise FactTripPublishError(
            f"current station-history pointer {path} has invalid fields"
        )
    if payload["schema_version"] != 1:
        raise FactTripPublishError(
            f"current station-history pointer {path} has unsupported schema_version"
        )
    _require_version(payload["history_version_id"], _HISTORY_VERSION, "history_version_id")
    return payload


def _load_fact_pointer_if_present(
    path: Path,
    source_month: str,
) -> dict[str, object] | None:
    if not path.exists():
        return None
    payload = _load_json(path, "current trusted fact pointer")
    if set(payload) != {"schema_version", "source_month", "fact_version_id"}:
        raise FactTripPublishError(
            f"current trusted fact pointer {path} has invalid fields"
        )
    if payload["schema_version"] != CURRENT_POINTER_SCHEMA_VERSION:
        raise FactTripPublishError(
            f"current trusted fact pointer {path} has unsupported schema_version"
        )
    if payload["source_month"] != source_month:
        raise FactTripPublishError(
            f"current trusted fact pointer {path} has invalid source_month binding"
        )
    _require_version(payload["fact_version_id"], _FACT_VERSION, "fact_version_id")
    return payload


def _write_fact_pointer(
    path: Path,
    source_month: str,
    fact_version_id: str,
) -> None:
    payload = {
        "schema_version": CURRENT_POINTER_SCHEMA_VERSION,
        "source_month": source_month,
        "fact_version_id": fact_version_id,
    }
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, temp_name = tempfile.mkstemp(
        prefix=f".{path.name}.",
        suffix=".tmp",
        dir=path.parent,
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


@contextmanager
def _publication_locks(
    *,
    clean_root: Path,
    warehouse_root: Path,
    facts_root: Path,
    source_month: str,
    fact_version_id: str,
):
    clean_lock = clean_root / "locks" / f"source_month={source_month}.lock"
    history_lock = warehouse_root / "locks" / "current.lock"
    fact_lock = facts_root / "locks" / f"source_month={source_month}.lock"
    with _advisory_lock(
        clean_lock,
        label=f"trusted fact {fact_version_id}",
        error_message="current Clean month is being published concurrently",
    ):
        with _advisory_lock(
            history_lock,
            label=f"trusted fact {fact_version_id}",
            error_message="station-history current is being published concurrently",
        ):
            with _advisory_lock(
                fact_lock,
                label=fact_version_id,
                error_message="same-month trusted fact is being published concurrently",
            ):
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
            raise FactTripPublishError(f"{error_message}; lock={path}") from exc
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


def _load_sql(sql_path: str | Path | None) -> tuple[str, str]:
    path = Path(sql_path) if sql_path is not None else _DEFAULT_SQL_PATH
    try:
        text = path.read_text(encoding="utf-8")
    except (OSError, UnicodeDecodeError) as exc:
        raise FactTripPublishError(f"cannot read trusted fact SQL {path}: {exc}") from exc
    text = text.replace("\r\n", "\n").replace("\r", "\n")
    if not text.strip():
        raise FactTripPublishError("trusted fact SQL is empty")
    return text, hashlib.sha256(text.encode("utf-8")).hexdigest()


def _require_file_fingerprint(path: Path, payload: object, *, label: str) -> None:
    if not isinstance(payload, dict) or set(payload) != {"size_bytes", "sha256"}:
        raise FactTripPublishError(f"metadata {label} fingerprint is malformed")
    size_bytes, sha256 = _fingerprint_file(path)
    if payload != {"size_bytes": size_bytes, "sha256": sha256}:
        raise FactTripPublishError(f"{label} fingerprint does not match immutable metadata")


def _fingerprint_file(path: Path) -> tuple[int, str]:
    digest = hashlib.sha256()
    size = 0
    try:
        with path.open("rb") as handle:
            while chunk := handle.read(1024 * 1024):
                size += len(chunk)
                digest.update(chunk)
    except OSError as exc:
        raise FactTripPublishError(f"cannot fingerprint {path}: {exc}") from exc
    return size, digest.hexdigest()


def _load_json(path: Path, label: str) -> dict[str, object]:
    try:
        with path.open("r", encoding="utf-8") as handle:
            payload = json.load(handle, object_pairs_hook=_reject_duplicate_pairs)
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise FactTripPublishError(f"cannot read {label} {path}: {exc}") from exc
    if not isinstance(payload, dict):
        raise FactTripPublishError(f"{label} {path} must be a JSON object")
    return payload


def _write_json(path: Path, payload: dict[str, object]) -> None:
    with path.open("w", encoding="utf-8", newline="\n") as handle:
        handle.write(_canonical_json_text(payload) + "\n")
        handle.flush()
        os.fsync(handle.fileno())


def _canonical_configuration_object(
    configuration: Mapping[str, object] | object,
) -> dict[str, object]:
    if not isinstance(configuration, Mapping):
        raise FactTripPublishError("configuration must be a JSON-compatible mapping")
    try:
        encoded = _canonical_json_text(dict(configuration))
        decoded = json.loads(encoded)
    except (TypeError, ValueError) as exc:
        raise FactTripPublishError(f"configuration must be JSON-compatible: {exc}") from exc
    if not isinstance(decoded, dict):
        raise FactTripPublishError("configuration must serialize to a JSON object")
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


def _json_scalar(value: object) -> str:
    if isinstance(value, (datetime, date)):
        return value.isoformat(sep=" ") if isinstance(value, datetime) else value.isoformat()
    if isinstance(value, Decimal):
        return str(value)
    raise TypeError(f"cannot serialize {type(value).__name__} into fact staging JSON")


def _reject_duplicate_pairs(
    pairs: list[tuple[str, object]],
) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise FactTripPublishError(f"duplicate JSON key {key!r} is not allowed")
        result[key] = value
    return result


def _require_month(value: object) -> str:
    if not isinstance(value, str) or _MONTH.fullmatch(value) is None:
        raise FactTripPublishError(f"source_month must be YYYY-MM, got {value!r}")
    return value


def _require_text(value: object, label: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise FactTripPublishError(f"{label} must be a non-empty string")
    return value


def _require_version(
    value: object,
    pattern: re.Pattern[str],
    label: str,
) -> str:
    if not isinstance(value, str) or pattern.fullmatch(value) is None:
        raise FactTripPublishError(f"{label} has invalid deterministic identity")
    return value


def _sql_path(path: Path) -> str:
    return path.resolve().as_posix().replace("'", "''")


def _result(
    status: str,
    metadata: dict[str, object],
    version_path: Path,
    current_path: Path,
) -> FactTripPublishResult:
    return FactTripPublishResult(
        status=status,
        source_month=metadata["source_month"],
        source_version_id=metadata["source_version_id"],
        clean_version_id=metadata["clean_version_id"],
        history_version_id=metadata["history_version_id"],
        fact_version_id=metadata["fact_version_id"],
        version_path=version_path,
        current_pointer_path=current_path,
        fact_row_count=metadata["fact_row_count"],
        station_history_match_count=metadata["station_history_match_count"],
        station_history_unmatched_count=metadata["station_history_unmatched_count"],
        warning_row_count=metadata["warning_row_count"],
    )
