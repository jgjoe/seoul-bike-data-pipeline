"""Build, validate, and atomically publish service-month OD-day marts."""

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
from datetime import date
from itertools import zip_longest
from pathlib import Path
from statistics import median
from typing import Callable, Iterable, Iterator

import duckdb

from .date_dimension import (
    date_current_pointer_path,
    date_version_path,
    validate_date_dimension_version,
)
from .errors import OdDayMartError
from .fact_publish import (
    fact_current_pointer_path,
    fact_version_path,
    validate_fact_trip_version,
)

CORE_START_MONTH = "2020-01"
CORE_END_MONTH = "2026-06"

MART_VERSION_PREFIX = "mod1_"
CONFIGURATION_FINGERPRINT_PREFIX = "cfg1_"
CURRENT_POINTER_SCHEMA_VERSION = 1
METADATA_SCHEMA_VERSION = 1
STATUS_PUBLISHED_CURRENT = "PUBLISHED_CURRENT"
STATUS_REUSED_CURRENT = "REUSED_CURRENT"

_MART_VERSION = re.compile(r"mod1_[0-9a-f]{64}")
_FACT_VERSION = re.compile(r"fv1_[0-9a-f]{64}")
_DATE_VERSION = re.compile(r"dv1_[0-9a-f]{64}")
_CONFIGURATION_FINGERPRINT = re.compile(r"cfg1_[0-9a-f]{64}")
_SHA256 = re.compile(r"[0-9a-f]{64}")
_MONTH = re.compile(r"20[0-9]{2}-(?:0[1-9]|1[0-2])")
_DEFAULT_SQL_PATH = Path(__file__).with_name("sql") / "mart_od_day.sql"

_MART_SCHEMA = (
    ("service_date", "DATE"),
    ("origin_station_no", "VARCHAR"),
    ("destination_station_no", "VARCHAR"),
    ("trip_count", "BIGINT"),
    ("unique_bike_count", "BIGINT"),
    ("avg_usage_min", "DOUBLE"),
    ("median_usage_min", "DOUBLE"),
    ("valid_distance_trip_count", "BIGINT"),
    ("valid_distance_avg", "DOUBLE"),
    ("valid_distance_median", "DOUBLE"),
)

_METADATA_FIELDS = {
    "schema_version",
    "service_month",
    "date_version_id",
    "fact_version_id",
    "transformation_version",
    "configuration",
    "configuration_fingerprint",
    "sql_sha256",
    "mart_version_id",
    "mart_row_count",
    "trip_count_total",
    "valid_distance_trip_count_total",
    "warehouse_database",
}


@dataclass(frozen=True)
class OdDayMartPublishResult:
    status: str
    service_month: str
    mart_version_id: str
    date_version_id: str
    fact_version_id: str
    version_path: Path
    current_pointer_path: Path
    mart_row_count: int
    trip_count_total: int
    valid_distance_trip_count_total: int


@dataclass(frozen=True)
class OdDayRevisionImpact:
    source_month: str
    old_fact_version_id: str
    new_fact_version_id: str
    affected_service_months: tuple[str, ...]


def compute_configuration_fingerprint(configuration: Mapping[str, object]) -> str:
    canonical = _canonical_configuration_object(configuration)
    return CONFIGURATION_FINGERPRINT_PREFIX + hashlib.sha256(
        _canonical_json_bytes(canonical)
    ).hexdigest()


def compute_od_day_mart_version_id(
    *,
    service_month: str,
    date_version_id: str,
    fact_version_id: str,
    transformation_version: str,
    configuration_fingerprint: str,
    sql_sha256: str,
) -> str:
    service_month = _require_service_month(service_month)
    _require_version(date_version_id, _DATE_VERSION, "date_version_id")
    _require_version(fact_version_id, _FACT_VERSION, "fact_version_id")
    _require_text(transformation_version, "transformation_version")
    if _CONFIGURATION_FINGERPRINT.fullmatch(configuration_fingerprint) is None:
        raise OdDayMartError("configuration_fingerprint must use cfg1_<sha256>")
    if _SHA256.fullmatch(sql_sha256) is None:
        raise OdDayMartError("sql_sha256 must be a lowercase SHA-256 digest")
    payload = [
        service_month,
        date_version_id,
        fact_version_id,
        transformation_version,
        configuration_fingerprint,
        sql_sha256,
    ]
    return MART_VERSION_PREFIX + hashlib.sha256(
        _canonical_json_bytes(payload)
    ).hexdigest()


def od_day_mart_root(warehouse_root: str | Path) -> Path:
    return Path(warehouse_root) / "mart_od_day"


def od_day_mart_version_path(
    warehouse_root: str | Path,
    service_month: str,
    mart_version_id: str,
) -> Path:
    service_month = _require_service_month(service_month)
    _require_version(mart_version_id, _MART_VERSION, "mart_version_id")
    return (
        od_day_mart_root(warehouse_root)
        / "versions"
        / f"service_month={service_month}"
        / f"mart_version_id={mart_version_id}"
    )


def od_day_mart_current_pointer_path(
    warehouse_root: str | Path, service_month: str
) -> Path:
    service_month = _require_service_month(service_month)
    return (
        od_day_mart_root(warehouse_root)
        / "current"
        / f"service_month={service_month}.json"
    )


def plan_od_day_fact_revision_impact(
    *,
    manifest: str | Path,
    raw_root: str | Path,
    station_root: str | Path,
    clean_root: str | Path,
    warehouse_root: str | Path,
    source_month: str,
    old_fact_version_id: str,
    new_fact_version_id: str,
) -> OdDayRevisionImpact:
    """OD-day is rental-cohort scoped, so a fact revision affects its own month only."""
    source_month = _require_service_month(source_month)
    _require_version(old_fact_version_id, _FACT_VERSION, "old_fact_version_id")
    _require_version(new_fact_version_id, _FACT_VERSION, "new_fact_version_id")
    for fact_version_id in (old_fact_version_id, new_fact_version_id):
        metadata = validate_fact_trip_version(
            fact_version_path(warehouse_root, source_month, fact_version_id),
            manifest=manifest,
            raw_root=raw_root,
            station_root=station_root,
            clean_root=clean_root,
            warehouse_root=warehouse_root,
        )
        if metadata["source_month"] != source_month:
            raise OdDayMartError(
                "revision impact fact versions must belong to source_month"
            )
    return OdDayRevisionImpact(
        source_month=source_month,
        old_fact_version_id=old_fact_version_id,
        new_fact_version_id=new_fact_version_id,
        affected_service_months=(source_month,),
    )


def publish_current_od_day_mart(
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
) -> OdDayMartPublishResult:
    service_month = _require_service_month(service_month)
    _require_text(transformation_version, "transformation_version")
    canonical_configuration = _canonical_configuration_object(configuration)
    configuration_fingerprint = compute_configuration_fingerprint(
        canonical_configuration
    )
    sql_text, sql_sha256 = _load_sql(sql_path)
    warehouse_root = Path(warehouse_root)

    date_pointer = _load_date_pointer(date_current_pointer_path(warehouse_root))
    date_version_id = date_pointer["date_version_id"]
    date_path = date_version_path(warehouse_root, date_version_id)
    captured_date_metadata = validate_date_dimension_version(date_path)

    fact_pointer = _load_fact_pointer(
        fact_current_pointer_path(warehouse_root, service_month), service_month
    )
    fact_version_id = fact_pointer["fact_version_id"]
    fact_path = fact_version_path(warehouse_root, service_month, fact_version_id)
    captured_fact_metadata = validate_fact_trip_version(
        fact_path,
        manifest=manifest,
        raw_root=raw_root,
        station_root=station_root,
        clean_root=clean_root,
        warehouse_root=warehouse_root,
    )

    mart_version_id = compute_od_day_mart_version_id(
        service_month=service_month,
        date_version_id=date_version_id,
        fact_version_id=fact_version_id,
        transformation_version=transformation_version,
        configuration_fingerprint=configuration_fingerprint,
        sql_sha256=sql_sha256,
    )
    version_path = od_day_mart_version_path(
        warehouse_root, service_month, mart_version_id
    )
    current_path = od_day_mart_current_pointer_path(warehouse_root, service_month)
    initial_pointer = _load_mart_pointer_if_present(current_path, service_month)
    if initial_pointer is not None:
        validate_od_day_mart_version(
            od_day_mart_version_path(
                warehouse_root, service_month, initial_pointer["mart_version_id"]
            ),
            manifest=manifest,
            raw_root=raw_root,
            station_root=station_root,
            clean_root=clean_root,
            warehouse_root=warehouse_root,
        )

    mart_root = od_day_mart_root(warehouse_root)
    if version_path.exists():
        candidate_metadata = validate_od_day_mart_version(
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
            / f"service_month={service_month}"
            / f"mart_version_id={mart_version_id}"
        )
        stage_path.mkdir(parents=True)
        try:
            _build_stage_database(
                stage_path=stage_path,
                service_month=service_month,
                fact_path=fact_path,
                sql_text=sql_text,
            )
            candidate_metadata = _build_metadata(
                stage_path=stage_path,
                service_month=service_month,
                date_version_id=date_version_id,
                fact_version_id=fact_version_id,
                transformation_version=transformation_version,
                configuration=canonical_configuration,
                configuration_fingerprint=configuration_fingerprint,
                sql_sha256=sql_sha256,
                mart_version_id=mart_version_id,
            )
            _write_json(stage_path / "metadata.json", candidate_metadata)
            validated_stage = validate_od_day_mart_version(
                stage_path,
                manifest=manifest,
                raw_root=raw_root,
                station_root=station_root,
                clean_root=clean_root,
                warehouse_root=warehouse_root,
            )
            if validated_stage != candidate_metadata:
                raise OdDayMartError(
                    "validated OD-day stage metadata changed unexpectedly"
                )
            version_path.parent.mkdir(parents=True, exist_ok=True)
            try:
                os.replace(stage_path, version_path)
            except OSError:
                if not version_path.exists():
                    raise
                existing = validate_od_day_mart_version(
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
        mart_version_id=mart_version_id,
    ):
        _require_current_inputs_unchanged(
            warehouse_root=warehouse_root,
            service_month=service_month,
            captured_date_pointer=date_pointer,
            captured_fact_pointer=fact_pointer,
        )
        _validate_captured_immutable_inputs(
            manifest=manifest,
            raw_root=raw_root,
            station_root=station_root,
            clean_root=clean_root,
            warehouse_root=warehouse_root,
            date_path=date_path,
            fact_path=fact_path,
            captured_date_metadata=captured_date_metadata,
            captured_fact_metadata=captured_fact_metadata,
        )
        latest_pointer = _load_mart_pointer_if_present(current_path, service_month)
        final_metadata = validate_od_day_mart_version(
            version_path,
            manifest=manifest,
            raw_root=raw_root,
            station_root=station_root,
            clean_root=clean_root,
            warehouse_root=warehouse_root,
        )
        _require_mart_matches_fact_path(
            version_path=version_path,
            service_month=service_month,
            fact_path=fact_path,
        )
        if final_metadata != candidate_metadata:
            raise OdDayMartError("OD-day immutable bundle changed before current publish")
        if latest_pointer is not None and latest_pointer["mart_version_id"] == mart_version_id:
            return _result(
                STATUS_REUSED_CURRENT, final_metadata, version_path, current_path
            )
        if latest_pointer != initial_pointer:
            raise OdDayMartError(
                "OD-day current pointer changed during build; refusing rollback"
            )
        if before_current_publish is not None:
            before_current_publish(version_path)
            _require_current_inputs_unchanged(
                warehouse_root=warehouse_root,
                service_month=service_month,
                captured_date_pointer=date_pointer,
                captured_fact_pointer=fact_pointer,
            )
            _validate_captured_immutable_inputs(
                manifest=manifest,
                raw_root=raw_root,
                station_root=station_root,
                clean_root=clean_root,
                warehouse_root=warehouse_root,
                date_path=date_path,
                fact_path=fact_path,
                captured_date_metadata=captured_date_metadata,
                captured_fact_metadata=captured_fact_metadata,
            )
            callback_metadata = validate_od_day_mart_version(
                version_path,
                manifest=manifest,
                raw_root=raw_root,
                station_root=station_root,
                clean_root=clean_root,
                warehouse_root=warehouse_root,
            )
            _require_mart_matches_fact_path(
                version_path=version_path,
                service_month=service_month,
                fact_path=fact_path,
            )
            if callback_metadata != final_metadata:
                raise OdDayMartError(
                    "OD-day immutable bundle changed before atomic current publish"
                )
            if _load_mart_pointer_if_present(current_path, service_month) != initial_pointer:
                raise OdDayMartError(
                    "OD-day current pointer changed before atomic publish"
                )
        _write_current_pointer(current_path, service_month, mart_version_id)
    return _result(STATUS_PUBLISHED_CURRENT, final_metadata, version_path, current_path)


def validate_od_day_mart_version(
    version_path: str | Path,
    *,
    manifest: str | Path,
    raw_root: str | Path,
    station_root: str | Path,
    clean_root: str | Path,
    warehouse_root: str | Path,
) -> dict[str, object]:
    path = Path(version_path)
    metadata = _load_json(path / "metadata.json", "OD-day mart metadata")
    _validate_metadata_identity(metadata)
    _require_bundle_binding(metadata, path)
    service_month = metadata["service_month"]

    validate_date_dimension_version(
        date_version_path(warehouse_root, metadata["date_version_id"])
    )
    fact_path = fact_version_path(
        warehouse_root, service_month, metadata["fact_version_id"]
    )
    fact_metadata = validate_fact_trip_version(
        fact_path,
        manifest=manifest,
        raw_root=raw_root,
        station_root=station_root,
        clean_root=clean_root,
        warehouse_root=warehouse_root,
    )
    if fact_metadata["source_month"] != service_month:
        raise OdDayMartError("OD-day fact lineage does not match service_month")

    database_path = path / "warehouse.duckdb"
    if Path(str(database_path) + ".wal").exists():
        raise OdDayMartError("OD-day warehouse has residual WAL and is not finalized")
    _require_file_fingerprint(
        database_path, metadata["warehouse_database"], label="warehouse.duckdb"
    )
    try:
        connection = duckdb.connect(str(database_path), read_only=True)
    except duckdb.Error as exc:
        raise OdDayMartError(f"cannot open OD-day warehouse database: {exc}") from exc
    try:
        _require_mart_schema(connection)
        _require_rows_equal(
            _iter_mart_rows(connection),
            _iter_expected_rows(service_month=service_month, fact_path=fact_path),
        )
        stats = _summarize_table(connection)
    except duckdb.Error as exc:
        raise OdDayMartError(f"cannot validate OD-day warehouse database: {exc}") from exc
    finally:
        connection.close()
    for key, value in stats.items():
        if metadata[key] != value:
            raise OdDayMartError(
                f"OD-day metadata {key}={metadata[key]!r} does not match durable table {value!r}"
            )
    return metadata


def _build_stage_database(
    *, stage_path: Path, service_month: str, fact_path: Path, sql_text: str
) -> None:
    database_path = stage_path / "warehouse.duckdb"
    connection = duckdb.connect(str(database_path))
    try:
        connection.execute(
            """
            CREATE TABLE od_fact_input (
                bike_id VARCHAR NOT NULL,
                rent_at TIMESTAMP NOT NULL,
                rent_station_no VARCHAR NOT NULL,
                return_station_no VARCHAR,
                usage_min BIGINT,
                distance_m DECIMAL(38,18),
                is_completed_trip BOOLEAN NOT NULL,
                distance_metric_eligible BOOLEAN NOT NULL
            )
            """
        )
        connection.execute(
            f"ATTACH '{_sql_path(fact_path / 'warehouse.duckdb')}' AS source_fact (READ_ONLY)"
        )
        try:
            connection.execute(
                """
                INSERT INTO od_fact_input
                SELECT
                    bike_id,
                    rent_at,
                    rent_station_no,
                    return_station_no,
                    usage_min,
                    distance_m,
                    is_completed_trip,
                    distance_metric_eligible
                FROM source_fact.fact_trip_trusted
                """
            )
        finally:
            connection.execute("DETACH source_fact")
        connection.execute(sql_text)
        _require_mart_schema(connection)
        connection.execute("CHECKPOINT")
    except duckdb.Error as exc:
        raise OdDayMartError(f"OD-day mart SQL build failed: {exc}") from exc
    finally:
        connection.close()
    if Path(str(database_path) + ".wal").exists():
        raise OdDayMartError("OD-day mart build left a residual DuckDB WAL")


def _build_metadata(
    *,
    stage_path: Path,
    service_month: str,
    date_version_id: str,
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
        stats = _summarize_table(connection)
    finally:
        connection.close()
    size_bytes, sha256 = _fingerprint_file(database_path)
    return {
        "schema_version": METADATA_SCHEMA_VERSION,
        "service_month": service_month,
        "date_version_id": date_version_id,
        "fact_version_id": fact_version_id,
        "transformation_version": transformation_version,
        "configuration": configuration,
        "configuration_fingerprint": configuration_fingerprint,
        "sql_sha256": sql_sha256,
        "mart_version_id": mart_version_id,
        **stats,
        "warehouse_database": {"size_bytes": size_bytes, "sha256": sha256},
    }


def _require_mart_schema(connection: duckdb.DuckDBPyConnection) -> None:
    table_rows = connection.execute(
        """
        SELECT table_type FROM information_schema.tables
        WHERE table_schema = 'main' AND table_name = 'mart_od_day'
        """
    ).fetchall()
    if table_rows != [("BASE TABLE",)]:
        raise OdDayMartError("mart_od_day must be a materialized base table")
    actual = tuple(
        (row[0], row[1])
        for row in connection.execute("DESCRIBE SELECT * FROM mart_od_day").fetchall()
    )
    if actual != _MART_SCHEMA:
        raise OdDayMartError(
            f"mart_od_day schema mismatch: expected {_MART_SCHEMA!r}, got {actual!r}"
        )


def _expected_rows(
    *, service_month: str, fact_path: Path
) -> list[tuple[object, ...]]:
    return list(_iter_expected_rows(service_month=service_month, fact_path=fact_path))


def _iter_expected_rows(
    *, service_month: str, fact_path: Path
) -> Iterator[tuple[object, ...]]:
    start_date, end_date = _month_bounds(service_month)
    connection = duckdb.connect(str(fact_path / "warehouse.duckdb"), read_only=True)
    try:
        out_of_month = connection.execute(
            """
            SELECT COUNT(*)
            FROM fact_trip_trusted
            WHERE CAST(rent_at AS DATE) < ?
               OR CAST(rent_at AS DATE) >= ?
            """,
            [start_date, end_date],
        ).fetchone()[0]
        if out_of_month:
            raise OdDayMartError("rental fact lineage contains row outside service_month")

        cursor = connection.execute(
            """
            SELECT
                bike_id,
                rent_at,
                rent_station_no,
                return_station_no,
                usage_min,
                distance_m,
                is_completed_trip,
                distance_metric_eligible
            FROM fact_trip_trusted
            WHERE is_completed_trip
            ORDER BY
                CAST(rent_at AS DATE),
                rent_station_no,
                return_station_no,
                source_row_id
            """
        )

        current_key: tuple[date, str, str] | None = None
        trip_count = 0
        bikes: set[str] = set()
        usage_values: list[int] = []
        distance_values: list[object] = []
        while True:
            batch = cursor.fetchmany(10_000)
            if not batch:
                break
            for (
                bike_id,
                rent_at,
                origin,
                destination,
                usage_min,
                distance_m,
                completed,
                distance_eligible,
            ) in batch:
                if not completed:
                    raise OdDayMartError(
                        "completed-trip OD stream unexpectedly contains incomplete row"
                    )
                service_date = rent_at.date()
                if destination is None:
                    raise OdDayMartError(
                        "completed trusted fact row is missing destination station"
                    )
                key = (service_date, origin, destination)
                if current_key is not None and key != current_key:
                    yield _finalize_expected_group(
                        current_key,
                        trip_count=trip_count,
                        bikes=bikes,
                        usage_values=usage_values,
                        distance_values=distance_values,
                    )
                    trip_count = 0
                    bikes = set()
                    usage_values = []
                    distance_values = []
                current_key = key
                trip_count += 1
                bikes.add(bike_id)
                if usage_min is not None:
                    usage_values.append(int(usage_min))
                if distance_eligible:
                    if distance_m is None:
                        raise OdDayMartError(
                            "distance-eligible trusted fact row is missing distance_m"
                        )
                    distance_values.append(distance_m)

        if current_key is not None:
            yield _finalize_expected_group(
                current_key,
                trip_count=trip_count,
                bikes=bikes,
                usage_values=usage_values,
                distance_values=distance_values,
            )
    finally:
        connection.close()


def _finalize_expected_group(
    key: tuple[date, str, str],
    *,
    trip_count: int,
    bikes: set[str],
    usage_values: list[int],
    distance_values: list[object],
) -> tuple[object, ...]:
    return (
        *key,
        trip_count,
        len(bikes),
        float(sum(usage_values) / len(usage_values)) if usage_values else None,
        float(median(usage_values)) if usage_values else None,
        len(distance_values),
        float(sum(distance_values) / len(distance_values))
        if distance_values
        else None,
        float(median(distance_values)) if distance_values else None,
    )


def _iter_mart_rows(
    connection: duckdb.DuckDBPyConnection,
) -> Iterator[tuple[object, ...]]:
    cursor = connection.execute(
        """
        SELECT
            service_date,
            origin_station_no,
            destination_station_no,
            trip_count,
            unique_bike_count,
            avg_usage_min,
            median_usage_min,
            valid_distance_trip_count,
            valid_distance_avg,
            valid_distance_median
        FROM mart_od_day
        ORDER BY service_date, origin_station_no, destination_station_no
        """
    )
    while True:
        batch = cursor.fetchmany(10_000)
        if not batch:
            break
        yield from batch


def _require_rows_equal(
    actual: Iterable[tuple[object, ...]], expected: Iterable[tuple[object, ...]]
) -> None:
    sentinel = object()
    float_indexes = {5, 6, 8, 9}
    for actual_row, expected_row in zip_longest(
        actual, expected, fillvalue=sentinel
    ):
        if actual_row is sentinel or expected_row is sentinel:
            raise OdDayMartError(
                "mart_od_day row count does not match independent recomputation"
            )
        assert isinstance(actual_row, tuple)
        assert isinstance(expected_row, tuple)
        for index, (actual_value, expected_value) in enumerate(
            zip(actual_row, expected_row, strict=True)
        ):
            if index in float_indexes:
                if actual_value is None or expected_value is None:
                    if actual_value is expected_value:
                        continue
                    raise OdDayMartError(
                        "mart_od_day aggregate values do not match independent recomputation"
                    )
                actual_float = float(actual_value)
                expected_float = float(expected_value)
                tolerance = max(math.ulp(expected_float) * 2, 1e-12)
                if math.isclose(
                    actual_float,
                    expected_float,
                    rel_tol=0.0,
                    abs_tol=tolerance,
                ):
                    continue
                raise OdDayMartError(
                    "mart_od_day aggregate values do not match independent recomputation"
                )
            if actual_value != expected_value:
                raise OdDayMartError(
                    "mart_od_day aggregate values do not match independent recomputation"
                )


def _summarize_table(connection: duckdb.DuckDBPyConnection) -> dict[str, int]:
    row = connection.execute(
        """
        SELECT
            COUNT(*),
            COALESCE(SUM(trip_count), 0),
            COALESCE(SUM(valid_distance_trip_count), 0)
        FROM mart_od_day
        """
    ).fetchone()
    return {
        "mart_row_count": int(row[0]),
        "trip_count_total": int(row[1]),
        "valid_distance_trip_count_total": int(row[2]),
    }


def _validate_metadata_identity(metadata: dict[str, object]) -> None:
    if set(metadata) != _METADATA_FIELDS:
        raise OdDayMartError("OD-day metadata has invalid fields")
    if metadata["schema_version"] != METADATA_SCHEMA_VERSION:
        raise OdDayMartError("OD-day metadata has unsupported schema_version")
    service_month = _require_service_month(metadata["service_month"])
    date_version_id = _require_version(
        metadata["date_version_id"], _DATE_VERSION, "date_version_id"
    )
    fact_version_id = _require_version(
        metadata["fact_version_id"], _FACT_VERSION, "fact_version_id"
    )
    transformation_version = _require_text(
        metadata["transformation_version"], "transformation_version"
    )
    configuration = _canonical_configuration_object(metadata["configuration"])
    configuration_fingerprint = compute_configuration_fingerprint(configuration)
    if metadata["configuration_fingerprint"] != configuration_fingerprint:
        raise OdDayMartError(
            "OD-day configuration fingerprint does not match configuration"
        )
    sql_sha256 = metadata["sql_sha256"]
    if not isinstance(sql_sha256, str) or _SHA256.fullmatch(sql_sha256) is None:
        raise OdDayMartError("OD-day metadata sql_sha256 is invalid")
    expected_id = compute_od_day_mart_version_id(
        service_month=service_month,
        date_version_id=date_version_id,
        fact_version_id=fact_version_id,
        transformation_version=transformation_version,
        configuration_fingerprint=configuration_fingerprint,
        sql_sha256=sql_sha256,
    )
    if metadata["mart_version_id"] != expected_id:
        raise OdDayMartError(
            "OD-day metadata mart_version_id does not match deterministic inputs"
        )
    for field in (
        "mart_row_count",
        "trip_count_total",
        "valid_distance_trip_count_total",
    ):
        value = metadata[field]
        if not isinstance(value, int) or isinstance(value, bool) or value < 0:
            raise OdDayMartError(
                f"OD-day metadata {field} must be a non-negative integer"
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
        raise OdDayMartError("OD-day warehouse fingerprint is malformed")


def _require_bundle_binding(metadata: dict[str, object], path: Path) -> None:
    if path.name != f"mart_version_id={metadata['mart_version_id']}":
        raise OdDayMartError("OD-day version directory id is not bound to metadata")
    if path.parent.name != f"service_month={metadata['service_month']}":
        raise OdDayMartError("OD-day version directory month is not bound to metadata")


def _require_requested_lineage(
    metadata: dict[str, object],
    *,
    date_version_id: str,
    fact_version_id: str,
    transformation_version: str,
    configuration: dict[str, object],
    configuration_fingerprint: str,
    sql_sha256: str,
) -> None:
    expected = {
        "date_version_id": date_version_id,
        "fact_version_id": fact_version_id,
        "transformation_version": transformation_version,
        "configuration": configuration,
        "configuration_fingerprint": configuration_fingerprint,
        "sql_sha256": sql_sha256,
    }
    for key, value in expected.items():
        if metadata[key] != value:
            raise OdDayMartError(
                f"existing OD-day version {key} does not match requested lineage"
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
        raise OdDayMartError(
            "same mart_version_id exists with different logical metadata"
        )


def _load_date_pointer(path: Path) -> dict[str, object]:
    payload = _load_json(path, "current date dimension pointer")
    if set(payload) != {"schema_version", "date_version_id"} or payload["schema_version"] != 1:
        raise OdDayMartError("current date dimension pointer is invalid")
    _require_version(payload["date_version_id"], _DATE_VERSION, "date_version_id")
    return payload


def _load_fact_pointer(path: Path, source_month: str) -> dict[str, object]:
    payload = _load_json(path, f"current trusted fact pointer for {source_month}")
    if set(payload) != {"schema_version", "source_month", "fact_version_id"}:
        raise OdDayMartError(
            f"current trusted fact pointer for {source_month} has invalid fields"
        )
    if payload["schema_version"] != 1 or payload["source_month"] != source_month:
        raise OdDayMartError(
            f"current trusted fact pointer for {source_month} has invalid binding"
        )
    _require_version(payload["fact_version_id"], _FACT_VERSION, "fact_version_id")
    return payload


def _load_mart_pointer_if_present(
    path: Path, service_month: str
) -> dict[str, object] | None:
    if not path.exists():
        return None
    payload = _load_json(path, "current OD-day mart pointer")
    if set(payload) != {"schema_version", "service_month", "mart_version_id"}:
        raise OdDayMartError("current OD-day mart pointer has invalid fields")
    if (
        payload["schema_version"] != CURRENT_POINTER_SCHEMA_VERSION
        or payload["service_month"] != service_month
    ):
        raise OdDayMartError("current OD-day mart pointer has invalid binding")
    _require_version(payload["mart_version_id"], _MART_VERSION, "mart_version_id")
    return payload


def _write_current_pointer(path: Path, service_month: str, mart_version_id: str) -> None:
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
    service_month: str,
    captured_date_pointer: dict[str, object],
    captured_fact_pointer: dict[str, object],
) -> None:
    if _load_date_pointer(date_current_pointer_path(warehouse_root)) != captured_date_pointer:
        raise OdDayMartError("date dimension current pointer changed during OD-day build")
    if _load_fact_pointer(
        fact_current_pointer_path(warehouse_root, service_month), service_month
    ) != captured_fact_pointer:
        raise OdDayMartError("trusted fact current pointer changed during OD-day build")


def _validate_captured_immutable_inputs(
    *,
    manifest: str | Path,
    raw_root: str | Path,
    station_root: str | Path,
    clean_root: str | Path,
    warehouse_root: Path,
    date_path: Path,
    fact_path: Path,
    captured_date_metadata: dict[str, object],
    captured_fact_metadata: dict[str, object],
) -> None:
    if validate_date_dimension_version(date_path) != captured_date_metadata:
        raise OdDayMartError(
            "captured date dimension immutable metadata changed during OD-day build"
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
        raise OdDayMartError(
            "captured trusted fact immutable metadata changed during OD-day build"
        )


@contextmanager
def _publication_locks(
    *, warehouse_root: Path, service_month: str, mart_version_id: str
):
    with ExitStack() as stack:
        stack.enter_context(
            _advisory_lock(
                warehouse_root / "dim_date" / "locks" / "current.lock",
                label=f"OD-day {mart_version_id}",
                error_message="date dimension current is being published concurrently",
            )
        )
        stack.enter_context(
            _advisory_lock(
                warehouse_root
                / "fact_trip_trusted"
                / "locks"
                / f"source_month={service_month}.lock",
                label=f"OD-day {mart_version_id}",
                error_message=f"trusted fact {service_month} is being published concurrently",
            )
        )
        stack.enter_context(
            _advisory_lock(
                od_day_mart_root(warehouse_root)
                / "locks"
                / f"service_month={service_month}.lock",
                label=mart_version_id,
                error_message="same-month OD-day mart is being published concurrently",
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
            raise OdDayMartError(f"{error_message}; lock={path}") from exc
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
        raise OdDayMartError(f"cannot read mart_od_day SQL {path}: {exc}") from exc
    text = text.replace("\r\n", "\n").replace("\r", "\n")
    if not text.strip():
        raise OdDayMartError("mart_od_day SQL is empty")
    return text, hashlib.sha256(text.encode("utf-8")).hexdigest()


def _require_file_fingerprint(path: Path, payload: object, *, label: str) -> None:
    if not isinstance(payload, dict) or set(payload) != {"size_bytes", "sha256"}:
        raise OdDayMartError(f"metadata {label} fingerprint is malformed")
    size_bytes, sha256 = _fingerprint_file(path)
    if payload != {"size_bytes": size_bytes, "sha256": sha256}:
        raise OdDayMartError(f"{label} fingerprint does not match immutable metadata")


def _fingerprint_file(path: Path) -> tuple[int, str]:
    digest = hashlib.sha256()
    size = 0
    try:
        with path.open("rb") as handle:
            while chunk := handle.read(1024 * 1024):
                size += len(chunk)
                digest.update(chunk)
    except OSError as exc:
        raise OdDayMartError(f"cannot fingerprint {path}: {exc}") from exc
    return size, digest.hexdigest()


def _load_json(path: Path, label: str) -> dict[str, object]:
    try:
        with path.open("r", encoding="utf-8") as handle:
            payload = json.load(handle, object_pairs_hook=_reject_duplicate_pairs)
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise OdDayMartError(f"cannot read {label} {path}: {exc}") from exc
    if not isinstance(payload, dict):
        raise OdDayMartError(f"{label} {path} must be a JSON object")
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
        raise OdDayMartError("configuration must be a JSON-compatible mapping")
    try:
        encoded = _canonical_json_text(dict(configuration))
        decoded = json.loads(encoded)
    except (TypeError, ValueError) as exc:
        raise OdDayMartError(f"configuration must be JSON-compatible: {exc}") from exc
    if not isinstance(decoded, dict):
        raise OdDayMartError("configuration must serialize to a JSON object")
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


def _reject_duplicate_pairs(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise OdDayMartError(f"duplicate JSON key {key!r} is not allowed")
        result[key] = value
    return result


def _require_text(value: object, label: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise OdDayMartError(f"{label} must be a non-empty string")
    return value


def _require_version(
    value: object, pattern: re.Pattern[str], label: str
) -> str:
    if not isinstance(value, str) or pattern.fullmatch(value) is None:
        raise OdDayMartError(f"{label} has invalid deterministic identity")
    return value


def _require_service_month(value: object) -> str:
    if not isinstance(value, str) or _MONTH.fullmatch(value) is None:
        raise OdDayMartError("service_month must use YYYY-MM")
    if value < CORE_START_MONTH or value > CORE_END_MONTH:
        raise OdDayMartError(
            f"service_month must be within {CORE_START_MONTH}..{CORE_END_MONTH}"
        )
    return value


def _month_bounds(service_month: str) -> tuple[date, date]:
    service_month = _require_service_month(service_month)
    year, month = map(int, service_month.split("-"))
    start = date(year, month, 1)
    end = date(year + 1, 1, 1) if month == 12 else date(year, month + 1, 1)
    return start, end


def _sql_path(path: Path) -> str:
    return str(path).replace("\\", "/").replace("'", "''")


def _require_mart_matches_fact_path(
    *, version_path: Path, service_month: str, fact_path: Path
) -> None:
    connection = duckdb.connect(str(version_path / "warehouse.duckdb"), read_only=True)
    try:
        _require_rows_equal(
            _iter_mart_rows(connection),
            _iter_expected_rows(service_month=service_month, fact_path=fact_path),
        )
    finally:
        connection.close()


def _result(
    status: str,
    metadata: dict[str, object],
    version_path: Path,
    current_path: Path,
) -> OdDayMartPublishResult:
    return OdDayMartPublishResult(
        status=status,
        service_month=metadata["service_month"],
        mart_version_id=metadata["mart_version_id"],
        date_version_id=metadata["date_version_id"],
        fact_version_id=metadata["fact_version_id"],
        version_path=version_path,
        current_pointer_path=current_path,
        mart_row_count=metadata["mart_row_count"],
        trip_count_total=metadata["trip_count_total"],
        valid_distance_trip_count_total=metadata[
            "valid_distance_trip_count_total"
        ],
    )
