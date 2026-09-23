"""Build, validate, and atomically publish service-month station-day marts."""

from __future__ import annotations

import hashlib
import json
import os
import re
import shutil
import tempfile
from collections.abc import Mapping
from contextlib import ExitStack, contextmanager
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from statistics import median
from typing import Callable

import duckdb

from .date_dimension import (
    date_current_pointer_path,
    date_version_path,
    validate_date_dimension_version,
)
from .errors import StationDayMartError
from .fact_publish import (
    fact_current_pointer_path,
    fact_version_path,
    validate_fact_trip_version,
)

CORE_START_MONTH = "2020-01"
CORE_END_MONTH = "2026-06"

MART_VERSION_PREFIX = "msd1_"
CONFIGURATION_FINGERPRINT_PREFIX = "cfg1_"
CURRENT_POINTER_SCHEMA_VERSION = 1
METADATA_SCHEMA_VERSION = 1
STATUS_PUBLISHED_CURRENT = "PUBLISHED_CURRENT"
STATUS_REUSED_CURRENT = "REUSED_CURRENT"

_MART_VERSION = re.compile(r"msd1_[0-9a-f]{64}")
_FACT_VERSION = re.compile(r"fv1_[0-9a-f]{64}")
_DATE_VERSION = re.compile(r"dv1_[0-9a-f]{64}")
_CONFIGURATION_FINGERPRINT = re.compile(r"cfg1_[0-9a-f]{64}")
_SHA256 = re.compile(r"[0-9a-f]{64}")
_MONTH = re.compile(r"20[0-9]{2}-(?:0[1-9]|1[0-2])")
_DEFAULT_SQL_PATH = Path(__file__).with_name("sql") / "mart_station_day.sql"

_MART_SCHEMA = (
    ("service_date", "DATE"),
    ("station_no", "VARCHAR"),
    ("rental_count", "BIGINT"),
    ("return_count", "BIGINT"),
    ("completed_trip_count", "BIGINT"),
    ("unique_bike_count", "BIGINT"),
    ("avg_usage_min", "DOUBLE"),
    ("median_usage_min", "DOUBLE"),
    ("valid_distance_trip_count", "BIGINT"),
    ("missing_return_count", "BIGINT"),
    ("station_history_match_count", "BIGINT"),
)

_METADATA_FIELDS = {
    "schema_version",
    "service_month",
    "date_version_id",
    "fact_lineage",
    "fact_scan_lineage",
    "transformation_version",
    "configuration",
    "configuration_fingerprint",
    "sql_sha256",
    "mart_version_id",
    "mart_row_count",
    "rental_count_total",
    "return_count_total",
    "completed_trip_count_total",
    "valid_distance_trip_count_total",
    "missing_return_count_total",
    "station_history_match_count_total",
    "warehouse_database",
}


@dataclass(frozen=True)
class StationDayMartPublishResult:
    status: str
    service_month: str
    mart_version_id: str
    date_version_id: str
    fact_lineage: tuple[tuple[str, str], ...]
    fact_scan_lineage: tuple[tuple[str, str], ...]
    version_path: Path
    current_pointer_path: Path
    mart_row_count: int
    rental_count_total: int
    return_count_total: int
    completed_trip_count_total: int
    valid_distance_trip_count_total: int
    missing_return_count_total: int
    station_history_match_count_total: int


@dataclass(frozen=True)
class StationDayRevisionImpact:
    source_month: str
    old_fact_version_id: str
    new_fact_version_id: str
    affected_service_months: tuple[str, ...]


def compute_configuration_fingerprint(configuration: Mapping[str, object]) -> str:
    canonical = _canonical_configuration_object(configuration)
    return CONFIGURATION_FINGERPRINT_PREFIX + hashlib.sha256(
        _canonical_json_bytes(canonical)
    ).hexdigest()


def compute_station_day_mart_version_id(
    *,
    service_month: str,
    date_version_id: str,
    fact_lineage: list[dict[str, str]],
    transformation_version: str,
    configuration_fingerprint: str,
    sql_sha256: str,
) -> str:
    service_month = _require_service_month(service_month)
    _require_version(date_version_id, _DATE_VERSION, "date_version_id")
    normalized_lineage = _require_material_fact_lineage(
        fact_lineage, service_month
    )
    _require_text(transformation_version, "transformation_version")
    if _CONFIGURATION_FINGERPRINT.fullmatch(configuration_fingerprint) is None:
        raise StationDayMartError(
            "configuration_fingerprint must use cfg1_<sha256>"
        )
    if _SHA256.fullmatch(sql_sha256) is None:
        raise StationDayMartError("sql_sha256 must be a lowercase SHA-256 digest")
    payload = [
        service_month,
        date_version_id,
        normalized_lineage,
        transformation_version,
        configuration_fingerprint,
        sql_sha256,
    ]
    return MART_VERSION_PREFIX + hashlib.sha256(
        _canonical_json_bytes(payload)
    ).hexdigest()


def station_day_mart_root(warehouse_root: str | Path) -> Path:
    return Path(warehouse_root) / "mart_station_day"


def station_day_mart_version_path(
    warehouse_root: str | Path,
    service_month: str,
    mart_version_id: str,
) -> Path:
    service_month = _require_service_month(service_month)
    _require_version(mart_version_id, _MART_VERSION, "mart_version_id")
    return (
        station_day_mart_root(warehouse_root)
        / "versions"
        / f"service_month={service_month}"
        / f"mart_version_id={mart_version_id}"
    )


def station_day_mart_current_pointer_path(
    warehouse_root: str | Path,
    service_month: str,
) -> Path:
    service_month = _require_service_month(service_month)
    return (
        station_day_mart_root(warehouse_root)
        / "current"
        / f"service_month={service_month}.json"
    )


def plan_station_day_fact_revision_impact(
    *,
    manifest: str | Path,
    raw_root: str | Path,
    station_root: str | Path,
    clean_root: str | Path,
    warehouse_root: str | Path,
    source_month: str,
    old_fact_version_id: str,
    new_fact_version_id: str,
) -> StationDayRevisionImpact:
    """Return service months that must rebuild for one fact-month revision."""
    source_month = _require_service_month(source_month)
    _require_version(old_fact_version_id, _FACT_VERSION, "old_fact_version_id")
    _require_version(new_fact_version_id, _FACT_VERSION, "new_fact_version_id")
    old_path = fact_version_path(
        warehouse_root, source_month, old_fact_version_id
    )
    new_path = fact_version_path(
        warehouse_root, source_month, new_fact_version_id
    )
    old_metadata = validate_fact_trip_version(
        old_path,
        manifest=manifest,
        raw_root=raw_root,
        station_root=station_root,
        clean_root=clean_root,
        warehouse_root=warehouse_root,
    )
    new_metadata = validate_fact_trip_version(
        new_path,
        manifest=manifest,
        raw_root=raw_root,
        station_root=station_root,
        clean_root=clean_root,
        warehouse_root=warehouse_root,
    )
    if (
        old_metadata["source_month"] != source_month
        or new_metadata["source_month"] != source_month
    ):
        raise StationDayMartError(
            "revision impact fact versions must belong to source_month"
        )
    affected = {source_month}
    affected.update(_completed_return_service_months(old_path))
    affected.update(_completed_return_service_months(new_path))
    return StationDayRevisionImpact(
        source_month=source_month,
        old_fact_version_id=old_fact_version_id,
        new_fact_version_id=new_fact_version_id,
        affected_service_months=tuple(sorted(affected)),
    )


def publish_current_station_day_mart(
    *,
    manifest: str | Path,
    raw_root: str | Path,
    station_root: str | Path,
    clean_root: str | Path,
    warehouse_root: str | Path,
    service_month: str,
    transformation_version: str,
    configuration: Mapping[str, object],
    sql_path: str | Path | None = None,
    before_current_publish: Callable[[Path], None] | None = None,
) -> StationDayMartPublishResult:
    """Publish one service-month station-day mart from current fact/date lineage."""
    service_month = _require_service_month(service_month)
    _require_text(transformation_version, "transformation_version")
    canonical_configuration = _canonical_configuration_object(configuration)
    configuration_fingerprint = compute_configuration_fingerprint(
        canonical_configuration
    )
    sql_text, sql_sha256 = _load_sql(sql_path)
    warehouse_root = Path(warehouse_root)
    fact_months = _months_through(service_month)

    date_pointer = _load_date_pointer(date_current_pointer_path(warehouse_root))
    date_version_id = date_pointer["date_version_id"]
    date_path = date_version_path(warehouse_root, date_version_id)
    captured_date_metadata = validate_date_dimension_version(date_path)

    fact_scan_lineage: list[dict[str, str]] = []
    fact_paths: dict[str, Path] = {}
    captured_fact_pointers: dict[str, dict[str, object]] = {}
    captured_fact_metadata: dict[str, dict[str, object]] = {}
    for source_month in fact_months:
        pointer = _load_fact_pointer(
            fact_current_pointer_path(warehouse_root, source_month),
            source_month,
        )
        fact_version_id = pointer["fact_version_id"]
        version_path = fact_version_path(
            warehouse_root, source_month, fact_version_id
        )
        fact_metadata = validate_fact_trip_version(
            version_path,
            manifest=manifest,
            raw_root=raw_root,
            station_root=station_root,
            clean_root=clean_root,
            warehouse_root=warehouse_root,
        )
        fact_scan_lineage.append(
            {
                "source_month": source_month,
                "fact_version_id": fact_version_id,
            }
        )
        fact_paths[source_month] = version_path
        captured_fact_pointers[source_month] = pointer
        captured_fact_metadata[source_month] = fact_metadata

    fact_lineage = _material_fact_lineage(
        service_month=service_month,
        fact_paths=fact_paths,
        fact_scan_lineage=fact_scan_lineage,
    )

    mart_version_id = compute_station_day_mart_version_id(
        service_month=service_month,
        date_version_id=date_version_id,
        fact_lineage=fact_lineage,
        transformation_version=transformation_version,
        configuration_fingerprint=configuration_fingerprint,
        sql_sha256=sql_sha256,
    )
    version_path = station_day_mart_version_path(
        warehouse_root, service_month, mart_version_id
    )
    current_path = station_day_mart_current_pointer_path(
        warehouse_root, service_month
    )
    initial_pointer = _load_mart_pointer_if_present(current_path, service_month)
    if initial_pointer is not None:
        validate_station_day_mart_version(
            station_day_mart_version_path(
                warehouse_root,
                service_month,
                initial_pointer["mart_version_id"],
            ),
            manifest=manifest,
            raw_root=raw_root,
            station_root=station_root,
            clean_root=clean_root,
            warehouse_root=warehouse_root,
        )

    mart_root = station_day_mart_root(warehouse_root)
    if version_path.exists():
        candidate_metadata = validate_station_day_mart_version(
            version_path,
            manifest=manifest,
            raw_root=raw_root,
            station_root=station_root,
            clean_root=clean_root,
            warehouse_root=warehouse_root,
        )
        _require_requested_lineage(
            candidate_metadata,
            date_version_id=date_version_id,
            fact_lineage=fact_lineage,
            transformation_version=transformation_version,
            configuration=canonical_configuration,
            configuration_fingerprint=configuration_fingerprint,
            sql_sha256=sql_sha256,
        )
    else:
        staging_root = mart_root / "staging"
        staging_root.mkdir(parents=True, exist_ok=True)
        stage_container = Path(tempfile.mkdtemp(prefix="build-", dir=staging_root))
        stage_month_path = stage_container / f"service_month={service_month}"
        stage_path = stage_month_path / f"mart_version_id={mart_version_id}"
        stage_path.mkdir(parents=True)
        try:
            _build_stage_database(
                stage_path=stage_path,
                service_month=service_month,
                fact_paths=fact_paths,
                sql_text=sql_text,
            )
            candidate_metadata = _build_metadata(
                stage_path=stage_path,
                service_month=service_month,
                date_version_id=date_version_id,
                fact_lineage=fact_lineage,
                fact_scan_lineage=fact_scan_lineage,
                transformation_version=transformation_version,
                configuration=canonical_configuration,
                configuration_fingerprint=configuration_fingerprint,
                sql_sha256=sql_sha256,
                mart_version_id=mart_version_id,
            )
            _write_json(stage_path / "metadata.json", candidate_metadata)
            validated_stage = validate_station_day_mart_version(
                stage_path,
                manifest=manifest,
                raw_root=raw_root,
                station_root=station_root,
                clean_root=clean_root,
                warehouse_root=warehouse_root,
            )
            if validated_stage != candidate_metadata:
                raise StationDayMartError(
                    "validated station-day stage metadata changed unexpectedly"
                )
            version_path.parent.mkdir(parents=True, exist_ok=True)
            try:
                os.replace(stage_path, version_path)
            except OSError:
                if not version_path.exists():
                    raise
                existing = validate_station_day_mart_version(
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
        warehouse_root=warehouse_root,
        service_month=service_month,
        fact_months=fact_months,
        mart_version_id=mart_version_id,
    ):
        _require_current_inputs_unchanged(
            warehouse_root=warehouse_root,
            captured_date_pointer=date_pointer,
            captured_fact_pointers=captured_fact_pointers,
        )
        _validate_captured_immutable_inputs(
            manifest=manifest,
            raw_root=raw_root,
            station_root=station_root,
            clean_root=clean_root,
            warehouse_root=warehouse_root,
            date_path=date_path,
            fact_paths=fact_paths,
            captured_date_metadata=captured_date_metadata,
            captured_fact_metadata=captured_fact_metadata,
        )
        latest_pointer = _load_mart_pointer_if_present(
            current_path, service_month
        )
        final_metadata = validate_station_day_mart_version(
            version_path,
            manifest=manifest,
            raw_root=raw_root,
            station_root=station_root,
            clean_root=clean_root,
            warehouse_root=warehouse_root,
        )
        _require_mart_matches_fact_paths(
            version_path=version_path,
            service_month=service_month,
            fact_paths=fact_paths,
        )
        if final_metadata != candidate_metadata:
            raise StationDayMartError(
                "station-day immutable bundle changed before current publish"
            )
        if (
            latest_pointer is not None
            and latest_pointer["mart_version_id"] == mart_version_id
        ):
            return _result(
                STATUS_REUSED_CURRENT,
                final_metadata,
                version_path,
                current_path,
            )
        if latest_pointer != initial_pointer:
            raise StationDayMartError(
                "station-day current pointer changed during build; refusing rollback"
            )
        if before_current_publish is not None:
            before_current_publish(version_path)
            _require_current_inputs_unchanged(
                warehouse_root=warehouse_root,
                captured_date_pointer=date_pointer,
                captured_fact_pointers=captured_fact_pointers,
            )
            _validate_captured_immutable_inputs(
                manifest=manifest,
                raw_root=raw_root,
                station_root=station_root,
                clean_root=clean_root,
                warehouse_root=warehouse_root,
                date_path=date_path,
                fact_paths=fact_paths,
                captured_date_metadata=captured_date_metadata,
                captured_fact_metadata=captured_fact_metadata,
            )
            callback_metadata = validate_station_day_mart_version(
                version_path,
                manifest=manifest,
                raw_root=raw_root,
                station_root=station_root,
                clean_root=clean_root,
                warehouse_root=warehouse_root,
            )
            _require_mart_matches_fact_paths(
                version_path=version_path,
                service_month=service_month,
                fact_paths=fact_paths,
            )
            if callback_metadata != final_metadata:
                raise StationDayMartError(
                    "station-day immutable bundle changed before atomic current publish"
                )
            if _load_mart_pointer_if_present(current_path, service_month) != initial_pointer:
                raise StationDayMartError(
                    "station-day current pointer changed before atomic publish"
                )
        _write_current_pointer(current_path, service_month, mart_version_id)
    return _result(
        STATUS_PUBLISHED_CURRENT, final_metadata, version_path, current_path
    )


def validate_station_day_mart_version(
    version_path: str | Path,
    *,
    manifest: str | Path,
    raw_root: str | Path,
    station_root: str | Path,
    clean_root: str | Path,
    warehouse_root: str | Path,
) -> dict[str, object]:
    """Hard-validate one immutable station-day mart and its immutable lineage."""
    path = Path(version_path)
    metadata = _load_json(path / "metadata.json", "station-day mart metadata")
    _validate_metadata_identity(metadata)
    _require_bundle_binding(metadata, path)

    service_month = metadata["service_month"]
    date_version_id = metadata["date_version_id"]
    date_path = date_version_path(warehouse_root, date_version_id)
    validate_date_dimension_version(date_path)

    fact_paths: dict[str, Path] = {}
    for lineage in metadata["fact_scan_lineage"]:
        source_month = lineage["source_month"]
        fact_version_id = lineage["fact_version_id"]
        fact_path = fact_version_path(
            warehouse_root, source_month, fact_version_id
        )
        validate_fact_trip_version(
            fact_path,
            manifest=manifest,
            raw_root=raw_root,
            station_root=station_root,
            clean_root=clean_root,
            warehouse_root=warehouse_root,
        )
        fact_paths[source_month] = fact_path
    expected_material_lineage = _material_fact_lineage(
        service_month=service_month,
        fact_paths=fact_paths,
        fact_scan_lineage=metadata["fact_scan_lineage"],
    )
    if metadata["fact_lineage"] != expected_material_lineage:
        raise StationDayMartError(
            "station-day material fact_lineage does not match independently "
            "recomputed contributors"
        )

    database_path = path / "warehouse.duckdb"
    wal_path = Path(str(database_path) + ".wal")
    if wal_path.exists():
        raise StationDayMartError(
            "station-day warehouse has residual WAL and is not finalized"
        )
    _require_file_fingerprint(
        database_path,
        metadata["warehouse_database"],
        label="warehouse.duckdb",
    )
    try:
        connection = duckdb.connect(str(database_path), read_only=True)
    except duckdb.Error as exc:
        raise StationDayMartError(
            f"cannot open station-day warehouse database: {exc}"
        ) from exc
    try:
        _require_mart_schema(connection)
        actual_rows = connection.execute(
            """
            SELECT
                service_date,
                station_no,
                rental_count,
                return_count,
                completed_trip_count,
                unique_bike_count,
                avg_usage_min,
                median_usage_min,
                valid_distance_trip_count,
                missing_return_count,
                station_history_match_count
            FROM mart_station_day
            ORDER BY service_date, station_no
            """
        ).fetchall()
    except duckdb.Error as exc:
        raise StationDayMartError(
            f"cannot validate station-day warehouse database: {exc}"
        ) from exc
    finally:
        connection.close()

    expected_rows = _expected_rows(
        service_month=service_month,
        fact_paths=fact_paths,
    )
    _require_rows_equal(actual_rows, expected_rows)
    stats = _summarize_rows(actual_rows)
    for key, value in stats.items():
        if metadata[key] != value:
            raise StationDayMartError(
                f"station-day metadata {key}={metadata[key]!r} "
                f"does not match durable value {value!r}"
            )
    return metadata


def _build_stage_database(
    *,
    stage_path: Path,
    service_month: str,
    fact_paths: dict[str, Path],
    sql_text: str,
) -> None:
    database_path = stage_path / "warehouse.duckdb"
    connection = duckdb.connect(str(database_path))
    start_date, end_date = _month_bounds(service_month)
    try:
        connection.execute(
            """
            CREATE TABLE rental_fact_input (
                bike_id VARCHAR NOT NULL,
                rent_at TIMESTAMP NOT NULL,
                rent_station_no VARCHAR NOT NULL,
                usage_min BIGINT,
                is_completed_trip BOOLEAN NOT NULL,
                distance_metric_eligible BOOLEAN NOT NULL,
                station_history_match BOOLEAN NOT NULL
            )
            """
        )
        connection.execute(
            """
            CREATE TABLE return_event_input (
                return_at TIMESTAMP NOT NULL,
                return_station_no VARCHAR NOT NULL
            )
            """
        )
        for source_month in _months_through(service_month):
            fact_path = fact_paths[source_month] / "warehouse.duckdb"
            connection.execute(
                f"ATTACH '{_sql_path(fact_path)}' AS source_fact (READ_ONLY)"
            )
            try:
                if source_month == service_month:
                    connection.execute(
                        """
                        INSERT INTO rental_fact_input
                        SELECT
                            bike_id,
                            rent_at,
                            rent_station_no,
                            usage_min,
                            is_completed_trip,
                            distance_metric_eligible,
                            station_history_match
                        FROM source_fact.fact_trip_trusted
                        """
                    )
                connection.execute(
                    """
                    INSERT INTO return_event_input
                    SELECT return_at, return_station_no
                    FROM source_fact.fact_trip_trusted
                    WHERE is_completed_trip
                      AND return_at >= ?
                      AND return_at < ?
                    """,
                    [start_date, end_date],
                )
            finally:
                connection.execute("DETACH source_fact")
        connection.execute(sql_text)
        _require_mart_schema(connection)
        connection.execute("CHECKPOINT")
    except duckdb.Error as exc:
        raise StationDayMartError(
            f"station-day mart SQL build failed: {exc}"
        ) from exc
    finally:
        connection.close()
    if Path(str(database_path) + ".wal").exists():
        raise StationDayMartError(
            "station-day mart build left a residual DuckDB WAL"
        )


def _build_metadata(
    *,
    stage_path: Path,
    service_month: str,
    date_version_id: str,
    fact_lineage: list[dict[str, str]],
    fact_scan_lineage: list[dict[str, str]],
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
            """
            SELECT
                service_date,
                station_no,
                rental_count,
                return_count,
                completed_trip_count,
                unique_bike_count,
                avg_usage_min,
                median_usage_min,
                valid_distance_trip_count,
                missing_return_count,
                station_history_match_count
            FROM mart_station_day
            ORDER BY service_date, station_no
            """
        ).fetchall()
    finally:
        connection.close()
    size_bytes, sha256 = _fingerprint_file(database_path)
    return {
        "schema_version": METADATA_SCHEMA_VERSION,
        "service_month": service_month,
        "date_version_id": date_version_id,
        "fact_lineage": fact_lineage,
        "fact_scan_lineage": fact_scan_lineage,
        "transformation_version": transformation_version,
        "configuration": configuration,
        "configuration_fingerprint": configuration_fingerprint,
        "sql_sha256": sql_sha256,
        "mart_version_id": mart_version_id,
        **_summarize_rows(rows),
        "warehouse_database": {"size_bytes": size_bytes, "sha256": sha256},
    }


def _require_mart_schema(connection: duckdb.DuckDBPyConnection) -> None:
    rows = connection.execute(
        """
        SELECT table_type
        FROM information_schema.tables
        WHERE table_schema = 'main' AND table_name = 'mart_station_day'
        """
    ).fetchall()
    if rows != [("BASE TABLE",)]:
        raise StationDayMartError(
            "mart_station_day must be a materialized base table"
        )
    actual = tuple(
        (row[0], row[1])
        for row in connection.execute(
            "DESCRIBE SELECT * FROM mart_station_day"
        ).fetchall()
    )
    if actual != _MART_SCHEMA:
        raise StationDayMartError(
            f"mart_station_day schema mismatch: expected {_MART_SCHEMA!r}, got {actual!r}"
        )


def _expected_rows(
    *,
    service_month: str,
    fact_paths: dict[str, Path],
) -> list[tuple[object, ...]]:
    service_month = _require_service_month(service_month)
    start_date, end_date = _month_bounds(service_month)
    metrics: dict[tuple[date, str], dict[str, object]] = {}

    rental_path = fact_paths[service_month] / "warehouse.duckdb"
    rental_connection = duckdb.connect(str(rental_path), read_only=True)
    try:
        rental_cursor = rental_connection.execute(
            """
            SELECT
                bike_id,
                rent_at,
                rent_station_no,
                usage_min,
                is_completed_trip,
                distance_metric_eligible,
                station_history_match
            FROM fact_trip_trusted
            ORDER BY CAST(rent_at AS DATE), rent_station_no, source_row_id
            """
        )
        current_key: tuple[date, str] | None = None
        rental_count = 0
        completed_trip_count = 0
        bikes: set[str] = set()
        usage_values: list[int] = []
        valid_distance_trip_count = 0
        missing_return_count = 0
        station_history_match_count = 0

        while True:
            batch = rental_cursor.fetchmany(10_000)
            if not batch:
                break
            for (
                bike_id,
                rent_at,
                station_no,
                usage_min,
                is_completed_trip,
                distance_metric_eligible,
                station_history_match,
            ) in batch:
                service_date = rent_at.date()
                if not (start_date <= service_date < end_date):
                    raise StationDayMartError(
                        "rental fact lineage contains row outside service_month"
                    )
                key = (service_date, station_no)
                if current_key is not None and key != current_key:
                    _finalize_rental_metric_group(
                        metrics,
                        key=current_key,
                        rental_count=rental_count,
                        completed_trip_count=completed_trip_count,
                        bikes=bikes,
                        usage_values=usage_values,
                        valid_distance_trip_count=valid_distance_trip_count,
                        missing_return_count=missing_return_count,
                        station_history_match_count=station_history_match_count,
                    )
                    rental_count = 0
                    completed_trip_count = 0
                    bikes = set()
                    usage_values = []
                    valid_distance_trip_count = 0
                    missing_return_count = 0
                    station_history_match_count = 0
                current_key = key
                rental_count += 1
                completed_trip_count += int(is_completed_trip)
                bikes.add(bike_id)
                if usage_min is not None:
                    usage_values.append(int(usage_min))
                valid_distance_trip_count += int(distance_metric_eligible)
                missing_return_count += int(not is_completed_trip)
                station_history_match_count += int(station_history_match)

        if current_key is not None:
            _finalize_rental_metric_group(
                metrics,
                key=current_key,
                rental_count=rental_count,
                completed_trip_count=completed_trip_count,
                bikes=bikes,
                usage_values=usage_values,
                valid_distance_trip_count=valid_distance_trip_count,
                missing_return_count=missing_return_count,
                station_history_match_count=station_history_match_count,
            )
    finally:
        rental_connection.close()

    for source_month in _months_through(service_month):
        fact_path = fact_paths[source_month] / "warehouse.duckdb"
        connection = duckdb.connect(str(fact_path), read_only=True)
        try:
            return_cursor = connection.execute(
                """
                SELECT return_at, return_station_no
                FROM fact_trip_trusted
                WHERE is_completed_trip
                  AND return_at >= ?
                  AND return_at < ?
                ORDER BY return_at, return_station_no, source_row_id
                """,
                [start_date, end_date],
            )
            while True:
                batch = return_cursor.fetchmany(10_000)
                if not batch:
                    break
                for return_at, station_no in batch:
                    state = _metric_state(metrics, return_at.date(), station_no)
                    state["return_count"] += 1
        finally:
            connection.close()

    output: list[tuple[object, ...]] = []
    for (service_date, station_no), state in sorted(metrics.items()):
        output.append(
            (
                service_date,
                station_no,
                state["rental_count"],
                state["return_count"],
                state["completed_trip_count"],
                state["unique_bike_count"],
                state["avg_usage_min"],
                state["median_usage_min"],
                state["valid_distance_trip_count"],
                state["missing_return_count"],
                state["station_history_match_count"],
            )
        )
    return output


def _metric_state(
    metrics: dict[tuple[date, str], dict[str, object]],
    service_date: date,
    station_no: str,
) -> dict[str, object]:
    key = (service_date, station_no)
    if key not in metrics:
        metrics[key] = {
            "rental_count": 0,
            "return_count": 0,
            "completed_trip_count": 0,
            "unique_bike_count": 0,
            "avg_usage_min": None,
            "median_usage_min": None,
            "valid_distance_trip_count": 0,
            "missing_return_count": 0,
            "station_history_match_count": 0,
        }
    return metrics[key]


def _finalize_rental_metric_group(
    metrics: dict[tuple[date, str], dict[str, object]],
    *,
    key: tuple[date, str],
    rental_count: int,
    completed_trip_count: int,
    bikes: set[str],
    usage_values: list[int],
    valid_distance_trip_count: int,
    missing_return_count: int,
    station_history_match_count: int,
) -> None:
    state = _metric_state(metrics, key[0], key[1])
    state["rental_count"] = rental_count
    state["completed_trip_count"] = completed_trip_count
    state["unique_bike_count"] = len(bikes)
    state["avg_usage_min"] = (
        float(sum(usage_values) / len(usage_values)) if usage_values else None
    )
    state["median_usage_min"] = (
        float(median(usage_values)) if usage_values else None
    )
    state["valid_distance_trip_count"] = valid_distance_trip_count
    state["missing_return_count"] = missing_return_count
    state["station_history_match_count"] = station_history_match_count


def _require_rows_equal(
    actual: list[tuple[object, ...]],
    expected: list[tuple[object, ...]],
) -> None:
    if len(actual) != len(expected):
        raise StationDayMartError(
            "mart_station_day row count does not match independent recomputation"
        )
    for actual_row, expected_row in zip(actual, expected, strict=True):
        for index, (actual_value, expected_value) in enumerate(
            zip(actual_row, expected_row, strict=True)
        ):
            if index in (6, 7):
                if actual_value is None or expected_value is None:
                    if actual_value is expected_value:
                        continue
                    raise StationDayMartError(
                        "mart_station_day aggregate values do not match independent recomputation"
                    )
                if float(actual_value) == float(expected_value):
                    continue
                raise StationDayMartError(
                    "mart_station_day aggregate values do not match independent recomputation"
                )
            if actual_value != expected_value:
                raise StationDayMartError(
                    "mart_station_day aggregate values do not match independent recomputation"
                )


def _summarize_rows(rows: list[tuple[object, ...]]) -> dict[str, int]:
    return {
        "mart_row_count": len(rows),
        "rental_count_total": sum(int(row[2]) for row in rows),
        "return_count_total": sum(int(row[3]) for row in rows),
        "completed_trip_count_total": sum(int(row[4]) for row in rows),
        "valid_distance_trip_count_total": sum(int(row[8]) for row in rows),
        "missing_return_count_total": sum(int(row[9]) for row in rows),
        "station_history_match_count_total": sum(
            int(row[10]) for row in rows
        ),
    }


def _validate_metadata_identity(metadata: dict[str, object]) -> None:
    if set(metadata) != _METADATA_FIELDS:
        raise StationDayMartError("station-day metadata has invalid fields")
    if metadata["schema_version"] != METADATA_SCHEMA_VERSION:
        raise StationDayMartError(
            "station-day metadata has unsupported schema_version"
        )
    service_month = _require_service_month(metadata["service_month"])
    date_version_id = _require_version(
        metadata["date_version_id"], _DATE_VERSION, "date_version_id"
    )
    fact_scan_lineage = _require_fact_scan_lineage(
        metadata["fact_scan_lineage"], service_month
    )
    fact_lineage = _require_material_fact_lineage(
        metadata["fact_lineage"], service_month
    )
    scan_by_month = {
        item["source_month"]: item["fact_version_id"]
        for item in fact_scan_lineage
    }
    for item in fact_lineage:
        if scan_by_month.get(item["source_month"]) != item["fact_version_id"]:
            raise StationDayMartError(
                "material fact_lineage must be a subset of fact_scan_lineage"
            )
    transformation_version = _require_text(
        metadata["transformation_version"], "transformation_version"
    )
    configuration = _canonical_configuration_object(metadata["configuration"])
    configuration_fingerprint = compute_configuration_fingerprint(configuration)
    if metadata["configuration_fingerprint"] != configuration_fingerprint:
        raise StationDayMartError(
            "station-day configuration fingerprint does not match configuration"
        )
    sql_sha256 = metadata["sql_sha256"]
    if not isinstance(sql_sha256, str) or _SHA256.fullmatch(sql_sha256) is None:
        raise StationDayMartError("station-day metadata sql_sha256 is invalid")
    expected_id = compute_station_day_mart_version_id(
        service_month=service_month,
        date_version_id=date_version_id,
        fact_lineage=fact_lineage,
        transformation_version=transformation_version,
        configuration_fingerprint=configuration_fingerprint,
        sql_sha256=sql_sha256,
    )
    if metadata["mart_version_id"] != expected_id:
        raise StationDayMartError(
            "station-day metadata mart_version_id does not match deterministic inputs"
        )
    for field in (
        "mart_row_count",
        "rental_count_total",
        "return_count_total",
        "completed_trip_count_total",
        "valid_distance_trip_count_total",
        "missing_return_count_total",
        "station_history_match_count_total",
    ):
        value = metadata[field]
        if (
            not isinstance(value, int)
            or isinstance(value, bool)
            or value < 0
        ):
            raise StationDayMartError(
                f"station-day metadata {field} must be a non-negative integer"
            )
    fingerprint = metadata["warehouse_database"]
    if (
        not isinstance(fingerprint, dict)
        or set(fingerprint) != {"size_bytes", "sha256"}
        or not isinstance(fingerprint["size_bytes"], int)
        or isinstance(fingerprint["size_bytes"], bool)
        or fingerprint["size_bytes"] <= 0
        or not isinstance(fingerprint["sha256"], str)
        or _SHA256.fullmatch(fingerprint["sha256"]) is None
    ):
        raise StationDayMartError(
            "station-day warehouse fingerprint is malformed"
        )


def _require_bundle_binding(
    metadata: dict[str, object],
    version_path: Path,
) -> None:
    service_month = metadata["service_month"]
    mart_version_id = metadata["mart_version_id"]
    if version_path.name != f"mart_version_id={mart_version_id}":
        raise StationDayMartError(
            "station-day version directory id is not bound to metadata"
        )
    if version_path.parent.name != f"service_month={service_month}":
        raise StationDayMartError(
            "station-day version directory month is not bound to metadata"
        )


def _require_requested_lineage(
    metadata: dict[str, object],
    *,
    date_version_id: str,
    fact_lineage: list[dict[str, str]],
    transformation_version: str,
    configuration: dict[str, object],
    configuration_fingerprint: str,
    sql_sha256: str,
) -> None:
    expected = {
        "date_version_id": date_version_id,
        "fact_lineage": fact_lineage,
        "transformation_version": transformation_version,
        "configuration": configuration,
        "configuration_fingerprint": configuration_fingerprint,
        "sql_sha256": sql_sha256,
    }
    for key, value in expected.items():
        if metadata[key] != value:
            raise StationDayMartError(
                f"existing station-day version {key} does not match requested lineage"
            )


def _require_same_logical_version(
    existing: dict[str, object],
    candidate: dict[str, object],
) -> None:
    ignored = {"warehouse_database", "fact_scan_lineage"}
    existing_logical = {
        key: value for key, value in existing.items() if key not in ignored
    }
    candidate_logical = {
        key: value for key, value in candidate.items() if key not in ignored
    }
    if existing_logical != candidate_logical:
        raise StationDayMartError(
            "same mart_version_id exists with different logical metadata"
        )


def _load_date_pointer(path: Path) -> dict[str, object]:
    payload = _load_json(path, "current date dimension pointer")
    if set(payload) != {"schema_version", "date_version_id"}:
        raise StationDayMartError(
            "current date dimension pointer has invalid fields"
        )
    if payload["schema_version"] != 1:
        raise StationDayMartError(
            "current date dimension pointer has unsupported schema_version"
        )
    _require_version(payload["date_version_id"], _DATE_VERSION, "date_version_id")
    return payload


def _load_fact_pointer(path: Path, source_month: str) -> dict[str, object]:
    payload = _load_json(path, f"current trusted fact pointer for {source_month}")
    if set(payload) != {
        "schema_version",
        "source_month",
        "fact_version_id",
    }:
        raise StationDayMartError(
            f"current trusted fact pointer for {source_month} has invalid fields"
        )
    if payload["schema_version"] != 1:
        raise StationDayMartError(
            f"current trusted fact pointer for {source_month} has unsupported schema_version"
        )
    if payload["source_month"] != source_month:
        raise StationDayMartError(
            f"current trusted fact pointer for {source_month} has invalid month binding"
        )
    _require_version(payload["fact_version_id"], _FACT_VERSION, "fact_version_id")
    return payload


def _load_mart_pointer_if_present(
    path: Path,
    service_month: str,
) -> dict[str, object] | None:
    if not path.exists():
        return None
    payload = _load_json(path, "current station-day mart pointer")
    if set(payload) != {
        "schema_version",
        "service_month",
        "mart_version_id",
    }:
        raise StationDayMartError(
            "current station-day mart pointer has invalid fields"
        )
    if payload["schema_version"] != CURRENT_POINTER_SCHEMA_VERSION:
        raise StationDayMartError(
            "current station-day mart pointer has unsupported schema_version"
        )
    if payload["service_month"] != service_month:
        raise StationDayMartError(
            "current station-day mart pointer has invalid month binding"
        )
    _require_version(
        payload["mart_version_id"], _MART_VERSION, "mart_version_id"
    )
    return payload


def _write_current_pointer(
    path: Path,
    service_month: str,
    mart_version_id: str,
) -> None:
    payload = {
        "schema_version": CURRENT_POINTER_SCHEMA_VERSION,
        "service_month": service_month,
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
    warehouse_root: Path,
    captured_date_pointer: dict[str, object],
    captured_fact_pointers: dict[str, dict[str, object]],
) -> None:
    if _load_date_pointer(
        date_current_pointer_path(warehouse_root)
    ) != captured_date_pointer:
        raise StationDayMartError(
            "date dimension current pointer changed during station-day build"
        )
    for source_month, captured in captured_fact_pointers.items():
        latest = _load_fact_pointer(
            fact_current_pointer_path(warehouse_root, source_month),
            source_month,
        )
        if latest != captured:
            raise StationDayMartError(
                f"trusted fact current pointer changed during station-day build: {source_month}"
            )


def _validate_captured_immutable_inputs(
    *,
    manifest: str | Path,
    raw_root: str | Path,
    station_root: str | Path,
    clean_root: str | Path,
    warehouse_root: Path,
    date_path: Path,
    fact_paths: dict[str, Path],
    captured_date_metadata: dict[str, object],
    captured_fact_metadata: dict[str, dict[str, object]],
) -> None:
    current_date_metadata = validate_date_dimension_version(date_path)
    if current_date_metadata != captured_date_metadata:
        raise StationDayMartError(
            "captured date dimension immutable metadata changed during station-day build"
        )
    for source_month in sorted(fact_paths):
        current_fact_metadata = validate_fact_trip_version(
            fact_paths[source_month],
            manifest=manifest,
            raw_root=raw_root,
            station_root=station_root,
            clean_root=clean_root,
            warehouse_root=warehouse_root,
        )
        if current_fact_metadata != captured_fact_metadata[source_month]:
            raise StationDayMartError(
                "captured trusted fact immutable metadata changed during station-day build: "
                f"{source_month}"
            )


@contextmanager
def _publication_locks(
    *,
    warehouse_root: Path,
    service_month: str,
    fact_months: list[str],
    mart_version_id: str,
):
    mart_root = station_day_mart_root(warehouse_root)
    with ExitStack() as stack:
        stack.enter_context(
            _advisory_lock(
                warehouse_root / "dim_date" / "locks" / "current.lock",
                label=f"station-day {mart_version_id}",
                error_message="date dimension current is being published concurrently",
            )
        )
        for source_month in fact_months:
            stack.enter_context(
                _advisory_lock(
                    warehouse_root
                    / "fact_trip_trusted"
                    / "locks"
                    / f"source_month={source_month}.lock",
                    label=f"station-day {mart_version_id}",
                    error_message=(
                        f"trusted fact {source_month} is being published concurrently"
                    ),
                )
            )
        stack.enter_context(
            _advisory_lock(
                mart_root / "locks" / f"service_month={service_month}.lock",
                label=mart_version_id,
                error_message="same-month station-day mart is being published concurrently",
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
            raise StationDayMartError(
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
        raise StationDayMartError(
            f"cannot read mart_station_day SQL {path}: {exc}"
        ) from exc
    text = text.replace("\r\n", "\n").replace("\r", "\n")
    if not text.strip():
        raise StationDayMartError("mart_station_day SQL is empty")
    return text, hashlib.sha256(text.encode("utf-8")).hexdigest()


def _require_file_fingerprint(
    path: Path,
    payload: object,
    *,
    label: str,
) -> None:
    if not isinstance(payload, dict) or set(payload) != {"size_bytes", "sha256"}:
        raise StationDayMartError(
            f"metadata {label} fingerprint is malformed"
        )
    size_bytes, sha256 = _fingerprint_file(path)
    if payload != {"size_bytes": size_bytes, "sha256": sha256}:
        raise StationDayMartError(
            f"{label} fingerprint does not match immutable metadata"
        )


def _fingerprint_file(path: Path) -> tuple[int, str]:
    digest = hashlib.sha256()
    size = 0
    try:
        with path.open("rb") as handle:
            while chunk := handle.read(1024 * 1024):
                size += len(chunk)
                digest.update(chunk)
    except OSError as exc:
        raise StationDayMartError(f"cannot fingerprint {path}: {exc}") from exc
    return size, digest.hexdigest()


def _load_json(path: Path, label: str) -> dict[str, object]:
    try:
        with path.open("r", encoding="utf-8") as handle:
            payload = json.load(handle, object_pairs_hook=_reject_duplicate_pairs)
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise StationDayMartError(f"cannot read {label} {path}: {exc}") from exc
    if not isinstance(payload, dict):
        raise StationDayMartError(f"{label} {path} must be a JSON object")
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
        raise StationDayMartError(
            "configuration must be a JSON-compatible mapping"
        )
    try:
        encoded = _canonical_json_text(dict(configuration))
        decoded = json.loads(encoded)
    except (TypeError, ValueError) as exc:
        raise StationDayMartError(
            f"configuration must be JSON-compatible: {exc}"
        ) from exc
    if not isinstance(decoded, dict):
        raise StationDayMartError(
            "configuration must serialize to a JSON object"
        )
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


def _reject_duplicate_pairs(
    pairs: list[tuple[str, object]],
) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise StationDayMartError(
                f"duplicate JSON key {key!r} is not allowed"
            )
        result[key] = value
    return result


def _require_text(value: object, label: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise StationDayMartError(f"{label} must be a non-empty string")
    return value


def _require_version(
    value: object,
    pattern: re.Pattern[str],
    label: str,
) -> str:
    if not isinstance(value, str) or pattern.fullmatch(value) is None:
        raise StationDayMartError(f"{label} has invalid deterministic identity")
    return value


def _require_service_month(value: object) -> str:
    if not isinstance(value, str) or _MONTH.fullmatch(value) is None:
        raise StationDayMartError("service_month must use YYYY-MM")
    if value < CORE_START_MONTH or value > CORE_END_MONTH:
        raise StationDayMartError(
            f"service_month must be within {CORE_START_MONTH}..{CORE_END_MONTH}"
        )
    return value


def _require_fact_scan_lineage(
    value: object,
    service_month: str,
) -> list[dict[str, str]]:
    if not isinstance(value, list):
        raise StationDayMartError("fact_scan_lineage must be a list")
    expected_months = _months_through(service_month)
    if len(value) != len(expected_months):
        raise StationDayMartError(
            "fact_scan_lineage must cover every production source month through service_month"
        )
    normalized: list[dict[str, str]] = []
    for expected_month, item in zip(expected_months, value, strict=True):
        if not isinstance(item, dict) or set(item) != {
            "source_month",
            "fact_version_id",
        }:
            raise StationDayMartError("fact_scan_lineage entry has invalid fields")
        if item["source_month"] != expected_month:
            raise StationDayMartError(
                "fact_scan_lineage months must be complete and ordered"
            )
        fact_version_id = _require_version(
            item["fact_version_id"], _FACT_VERSION, "fact_version_id"
        )
        normalized.append(
            {
                "source_month": expected_month,
                "fact_version_id": fact_version_id,
            }
        )
    return normalized


def _require_material_fact_lineage(
    value: object,
    service_month: str,
) -> list[dict[str, str]]:
    if not isinstance(value, list) or not value:
        raise StationDayMartError("fact_lineage must be a non-empty list")
    allowed_months = set(_months_through(service_month))
    normalized: list[dict[str, str]] = []
    previous_month: str | None = None
    seen: set[str] = set()
    for item in value:
        if not isinstance(item, dict) or set(item) != {
            "source_month",
            "fact_version_id",
        }:
            raise StationDayMartError("fact_lineage entry has invalid fields")
        source_month = item["source_month"]
        if source_month not in allowed_months:
            raise StationDayMartError(
                "fact_lineage contains month outside service-month dependency range"
            )
        if source_month in seen or (
            previous_month is not None and source_month <= previous_month
        ):
            raise StationDayMartError(
                "fact_lineage months must be unique and ordered"
            )
        fact_version_id = _require_version(
            item["fact_version_id"], _FACT_VERSION, "fact_version_id"
        )
        normalized.append(
            {
                "source_month": source_month,
                "fact_version_id": fact_version_id,
            }
        )
        seen.add(source_month)
        previous_month = source_month
    if service_month not in seen:
        raise StationDayMartError(
            "fact_lineage must include the rental fact for service_month"
        )
    return normalized


def _material_fact_lineage(
    *,
    service_month: str,
    fact_paths: dict[str, Path],
    fact_scan_lineage: list[dict[str, str]],
) -> list[dict[str, str]]:
    start_date, end_date = _month_bounds(service_month)
    material_months = {service_month}
    for source_month in _months_through(service_month):
        connection = duckdb.connect(
            str(fact_paths[source_month] / "warehouse.duckdb"),
            read_only=True,
        )
        try:
            count = int(
                connection.execute(
                    """
                    SELECT COUNT(*)
                    FROM fact_trip_trusted
                    WHERE is_completed_trip
                      AND return_at >= ?
                      AND return_at < ?
                    """,
                    [start_date, end_date],
                ).fetchone()[0]
            )
        finally:
            connection.close()
        if count:
            material_months.add(source_month)
    return [
        item
        for item in fact_scan_lineage
        if item["source_month"] in material_months
    ]


def _completed_return_service_months(fact_path: Path) -> set[str]:
    connection = duckdb.connect(
        str(fact_path / "warehouse.duckdb"), read_only=True
    )
    try:
        values = connection.execute(
            """
            SELECT return_at
            FROM fact_trip_trusted
            WHERE is_completed_trip
            """
        ).fetchall()
    finally:
        connection.close()
    return {
        value[0].strftime("%Y-%m")
        for value in values
        if value[0] is not None
        and CORE_START_MONTH <= value[0].strftime("%Y-%m") <= CORE_END_MONTH
    }


def _require_mart_matches_fact_paths(
    *,
    version_path: Path,
    service_month: str,
    fact_paths: dict[str, Path],
) -> None:
    database_path = version_path / "warehouse.duckdb"
    connection = duckdb.connect(str(database_path), read_only=True)
    try:
        actual_rows = connection.execute(
            """
            SELECT
                service_date,
                station_no,
                rental_count,
                return_count,
                completed_trip_count,
                unique_bike_count,
                avg_usage_min,
                median_usage_min,
                valid_distance_trip_count,
                missing_return_count,
                station_history_match_count
            FROM mart_station_day
            ORDER BY service_date, station_no
            """
        ).fetchall()
    finally:
        connection.close()
    expected_rows = _expected_rows(
        service_month=service_month,
        fact_paths=fact_paths,
    )
    _require_rows_equal(actual_rows, expected_rows)


def _months_through(service_month: str) -> list[str]:
    service_month = _require_service_month(service_month)
    start_year, start_month = map(int, CORE_START_MONTH.split("-"))
    end_year, end_month = map(int, service_month.split("-"))
    output: list[str] = []
    year, month = start_year, start_month
    while (year, month) <= (end_year, end_month):
        output.append(f"{year:04d}-{month:02d}")
        month += 1
        if month == 13:
            year += 1
            month = 1
    return output


def _month_bounds(service_month: str) -> tuple[date, date]:
    service_month = _require_service_month(service_month)
    year, month = map(int, service_month.split("-"))
    start = date(year, month, 1)
    if month == 12:
        end = date(year + 1, 1, 1)
    else:
        end = date(year, month + 1, 1)
    return start, end


def _sql_path(path: Path) -> str:
    return str(path).replace("\\", "/").replace("'", "''")


def _result(
    status: str,
    metadata: dict[str, object],
    version_path: Path,
    current_path: Path,
) -> StationDayMartPublishResult:
    return StationDayMartPublishResult(
        status=status,
        service_month=metadata["service_month"],
        mart_version_id=metadata["mart_version_id"],
        date_version_id=metadata["date_version_id"],
        fact_lineage=tuple(
            (item["source_month"], item["fact_version_id"])
            for item in metadata["fact_lineage"]
        ),
        fact_scan_lineage=tuple(
            (item["source_month"], item["fact_version_id"])
            for item in metadata["fact_scan_lineage"]
        ),
        version_path=version_path,
        current_pointer_path=current_path,
        mart_row_count=metadata["mart_row_count"],
        rental_count_total=metadata["rental_count_total"],
        return_count_total=metadata["return_count_total"],
        completed_trip_count_total=metadata["completed_trip_count_total"],
        valid_distance_trip_count_total=metadata[
            "valid_distance_trip_count_total"
        ],
        missing_return_count_total=metadata["missing_return_count_total"],
        station_history_match_count_total=metadata[
            "station_history_match_count_total"
        ],
    )
