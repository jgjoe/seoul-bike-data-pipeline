"""Build and atomically publish the snapshot-observed station history dimension.

Python owns control flow, lineage validation, failure handling, and publication.
The business transformation itself is plain SQL executed by DuckDB, matching
PRD §26/§27/§29.  Trip enrichment intentionally remains a later slice.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import shutil
import tempfile
from collections import defaultdict
from collections.abc import Mapping
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import date
from decimal import Decimal
from pathlib import Path
from typing import Callable

import duckdb

from .errors import StationHistoryError
from .station_snapshot import (
    PRECISION_DAY,
    PRECISION_MONTH,
    station_current_set_lock,
    station_version_path,
    validate_station_version,
)

HISTORY_VERSION_PREFIX = "wh1_"
CONFIGURATION_FINGERPRINT_PREFIX = "cfg1_"
CURRENT_POINTER_SCHEMA_VERSION = 1
METADATA_SCHEMA_VERSION = 1

STATUS_PUBLISHED_CURRENT = "PUBLISHED_CURRENT"
STATUS_REUSED_CURRENT = "REUSED_CURRENT"

_HISTORY_VERSION = re.compile(r"wh1_[0-9a-f]{64}")
_CONFIGURATION_FINGERPRINT = re.compile(r"cfg1_[0-9a-f]{64}")
_CURRENT_POINTER_FILE = re.compile(r"observation_period=(20[0-9]{2}-(?:[0-9]{2}|[0-9]{2}-[0-9]{2}))\.json")
_DEFAULT_SQL_PATH = Path(__file__).with_name("sql") / "dim_station_history.sql"

_HISTORY_SCHEMA = (
    ("station_no", "VARCHAR"),
    ("applicable_from", "DATE"),
    ("applicable_until", "DATE"),
    ("station_name", "VARCHAR"),
    ("district", "VARCHAR"),
    ("address", "VARCHAR"),
    ("latitude", "DECIMAL(18,8)"),
    ("longitude", "DECIMAL(18,8)"),
    ("lcd_capacity", "INTEGER"),
    ("qr_capacity", "INTEGER"),
    ("operation_mode", "VARCHAR"),
    ("observation_periods", "VARCHAR[]"),
    ("station_version_ids", "VARCHAR[]"),
    ("source_version_ids", "VARCHAR[]"),
    ("observation_count", "INTEGER"),
)


@dataclass(frozen=True)
class StationHistoryInput:
    snapshot_seq: int
    observation_period: str
    observation_precision: str
    applicable_from: date
    station_version_id: str
    source_version_id: str
    observation_count: int
    version_path: Path


@dataclass(frozen=True)
class StationHistoryInterval:
    station_no: str
    applicable_from: date
    applicable_until: date | None
    station_name: str | None
    district: str | None
    address: str | None
    latitude: Decimal | None
    longitude: Decimal | None
    lcd_capacity: int | None
    qr_capacity: int | None
    operation_mode: str | None
    observation_periods: tuple[str, ...]
    station_version_ids: tuple[str, ...]
    source_version_ids: tuple[str, ...]
    observation_count: int


@dataclass(frozen=True)
class StationHistoryPublishResult:
    status: str
    history_version_id: str
    version_path: Path
    current_pointer_path: Path
    snapshot_count: int
    input_observation_count: int
    history_row_count: int
    distinct_station_count: int
    open_interval_count: int


def compute_applicable_from(observation_period: str, observation_precision: str) -> date:
    if observation_precision == PRECISION_DAY:
        try:
            value = date.fromisoformat(observation_period)
        except ValueError as exc:
            raise StationHistoryError(
                f"DAY observation period must be YYYY-MM-DD, got {observation_period!r}"
            ) from exc
        if len(observation_period) != 10:
            raise StationHistoryError(
                f"DAY observation period must be YYYY-MM-DD, got {observation_period!r}"
            )
        return value
    if observation_precision == PRECISION_MONTH:
        match = re.fullmatch(r"(20[0-9]{2})-(0[1-9]|1[0-2])", observation_period)
        if match is None:
            raise StationHistoryError(
                f"MONTH observation period must be YYYY-MM, got {observation_period!r}"
            )
        year = int(match.group(1))
        month = int(match.group(2))
        return date(year + (1 if month == 12 else 0), 1 if month == 12 else month + 1, 1)
    raise StationHistoryError(
        f"unsupported observation_precision {observation_precision!r}"
    )


def compute_configuration_fingerprint(configuration: Mapping[str, object]) -> str:
    canonical = _canonical_configuration_object(configuration)
    return CONFIGURATION_FINGERPRINT_PREFIX + hashlib.sha256(
        _canonical_json_bytes(canonical)
    ).hexdigest()


def compute_history_version_id(
    *,
    input_snapshots: list[dict[str, object]],
    transformation_version: str,
    configuration_fingerprint: str,
    sql_sha256: str,
) -> str:
    _require_text(transformation_version, "transformation_version")
    if _CONFIGURATION_FINGERPRINT.fullmatch(configuration_fingerprint) is None:
        raise StationHistoryError("configuration_fingerprint must use cfg1_<sha256>")
    if re.fullmatch(r"[0-9a-f]{64}", sql_sha256) is None:
        raise StationHistoryError("sql_sha256 must be a lowercase SHA-256 hex digest")
    payload = [
        input_snapshots,
        transformation_version,
        configuration_fingerprint,
        sql_sha256,
    ]
    return HISTORY_VERSION_PREFIX + hashlib.sha256(_canonical_json_bytes(payload)).hexdigest()


def history_version_path(
    warehouse_root: str | Path, history_version_id: str
) -> Path:
    if _HISTORY_VERSION.fullmatch(history_version_id) is None:
        raise StationHistoryError("history_version_id must use wh1_<sha256>")
    return Path(warehouse_root) / "versions" / f"history_version_id={history_version_id}"


def history_current_pointer_path(warehouse_root: str | Path) -> Path:
    return Path(warehouse_root) / "current.json"


def publish_station_history(
    *,
    manifest: str | Path,
    raw_root: str | Path,
    station_root: str | Path,
    warehouse_root: str | Path,
    transformation_version: str,
    configuration: Mapping[str, object],
    sql_path: str | Path | None = None,
    before_current_publish: Callable[[Path], None] | None = None,
) -> StationHistoryPublishResult:
    _require_text(transformation_version, "transformation_version")
    canonical_configuration = _canonical_configuration_object(configuration)
    configuration_fingerprint = compute_configuration_fingerprint(canonical_configuration)
    sql_text, sql_sha256 = _load_sql(sql_path)
    inputs = _capture_current_snapshots(
        manifest=manifest, raw_root=raw_root, station_root=station_root
    )
    input_payload = _input_snapshot_payload(inputs)
    history_version_id = compute_history_version_id(
        input_snapshots=input_payload,
        transformation_version=transformation_version,
        configuration_fingerprint=configuration_fingerprint,
        sql_sha256=sql_sha256,
    )
    root = Path(warehouse_root)
    version_path = history_version_path(root, history_version_id)
    current_path = history_current_pointer_path(root)
    initial_pointer = _load_current_pointer(current_path)
    if initial_pointer is not None:
        current_version_path = history_version_path(
            root, initial_pointer["history_version_id"]
        )
        current_metadata = validate_station_history_version(
            current_version_path,
            manifest=manifest,
            raw_root=raw_root,
            station_root=station_root,
        )
        _require_bundle_binding(
            current_metadata, initial_pointer["history_version_id"]
        )

    staging_parent = root / "staging"
    staging_parent.mkdir(parents=True, exist_ok=True)
    stage_path = Path(
        tempfile.mkdtemp(
            prefix=f"history_version_id={history_version_id}-",
            dir=staging_parent,
        )
    )
    try:
        _build_stage_database(
            stage_path=stage_path,
            inputs=inputs,
            sql_text=sql_text,
        )
        metadata = _build_metadata(
            stage_path=stage_path,
            history_version_id=history_version_id,
            transformation_version=transformation_version,
            configuration=canonical_configuration,
            configuration_fingerprint=configuration_fingerprint,
            sql_sha256=sql_sha256,
            inputs=inputs,
        )
        _write_json(stage_path / "metadata.json", metadata)
        validated = validate_station_history_version(
            stage_path,
            manifest=manifest,
            raw_root=raw_root,
            station_root=station_root,
        )
        _require_bundle_binding(validated, history_version_id)

        version_reused = False
        if version_path.exists():
            existing = validate_station_history_version(
                version_path,
                manifest=manifest,
                raw_root=raw_root,
                station_root=station_root,
            )
            _require_bundle_binding(existing, history_version_id)
            _require_same_logical_version(existing, validated)
            version_reused = True
            shutil.rmtree(stage_path)
        else:
            version_path.parent.mkdir(parents=True, exist_ok=True)
            try:
                os.replace(stage_path, version_path)
            except OSError:
                if not version_path.exists():
                    raise
                existing = validate_station_history_version(
                    version_path,
                    manifest=manifest,
                    raw_root=raw_root,
                    station_root=station_root,
                )
                _require_bundle_binding(existing, history_version_id)
                _require_same_logical_version(existing, validated)
                version_reused = True
                shutil.rmtree(stage_path)

        _require_station_inputs_unchanged(
            expected=input_payload,
            manifest=manifest,
            raw_root=raw_root,
            station_root=station_root,
        )
        pointer = _load_current_pointer(current_path)
        if pointer is not None and pointer["history_version_id"] == history_version_id:
            return _result(
                STATUS_REUSED_CURRENT, validated, version_path, current_path
            )
        if pointer != initial_pointer:
            raise StationHistoryError(
                "warehouse current pointer changed during build; refusing concurrent overwrite"
            )
        if before_current_publish is not None:
            before_current_publish(version_path)
        with _publish_lock(root, history_version_id):
            with station_current_set_lock(station_root):
                latest_pointer = _load_current_pointer(current_path)
                if latest_pointer != initial_pointer:
                    if (
                        latest_pointer is not None
                        and latest_pointer["history_version_id"] == history_version_id
                    ):
                        _require_station_inputs_unchanged(
                            expected=input_payload,
                            manifest=manifest,
                            raw_root=raw_root,
                            station_root=station_root,
                        )
                        final_metadata = validate_station_history_version(
                            version_path,
                            manifest=manifest,
                            raw_root=raw_root,
                            station_root=station_root,
                        )
                        _require_bundle_binding(final_metadata, history_version_id)
                        _require_same_logical_version(final_metadata, validated)
                        return _result(
                            STATUS_REUSED_CURRENT,
                            final_metadata,
                            version_path,
                            current_path,
                        )
                    raise StationHistoryError(
                        "warehouse current pointer changed before atomic publish; refusing concurrent overwrite"
                    )
                _require_station_inputs_unchanged(
                    expected=input_payload,
                    manifest=manifest,
                    raw_root=raw_root,
                    station_root=station_root,
                )
                final_metadata = validate_station_history_version(
                    version_path,
                    manifest=manifest,
                    raw_root=raw_root,
                    station_root=station_root,
                )
                _require_bundle_binding(final_metadata, history_version_id)
                _require_same_logical_version(final_metadata, validated)
                _write_current_pointer(current_path, history_version_id)
        return _result(
            STATUS_PUBLISHED_CURRENT, validated, version_path, current_path
        )
    except Exception:
        raise


def validate_station_history_version(
    version_path: str | Path,
    *,
    manifest: str | Path,
    raw_root: str | Path,
    station_root: str | Path,
) -> dict[str, object]:
    path = Path(version_path)
    metadata = _load_metadata(path / "metadata.json")
    _validate_metadata_identity(metadata)
    inputs = _inputs_from_metadata(
        metadata=metadata,
        manifest=manifest,
        raw_root=raw_root,
        station_root=station_root,
    )
    _require_database_fingerprint(
        path / "warehouse.duckdb", metadata["warehouse_database"]
    )
    connection = duckdb.connect(str(path / "warehouse.duckdb"), read_only=True)
    try:
        _require_schema(connection)
        invalid_interval_count = connection.execute(
            """
            SELECT COUNT(*)
            FROM dim_station_history
            WHERE station_no IS NULL OR station_no = ''
               OR applicable_from IS NULL
               OR (
                   applicable_until IS NOT NULL
                   AND applicable_until <= applicable_from
               )
               OR observation_count <= 0
               OR array_length(observation_periods) <> observation_count
               OR array_length(station_version_ids) <> observation_count
               OR array_length(source_version_ids) <> observation_count
            """
        ).fetchone()[0]
        if invalid_interval_count:
            raise StationHistoryError(
                "dim_station_history contains malformed interval/provenance rows"
            )
        overlap_count = connection.execute(
            """
            WITH ordered AS (
                SELECT
                    station_no,
                    applicable_from,
                    applicable_until,
                    lag(applicable_until) OVER (
                        PARTITION BY station_no ORDER BY applicable_from
                    ) AS previous_until
                FROM dim_station_history
            )
            SELECT COUNT(*)
            FROM ordered
            WHERE previous_until IS NULL AND applicable_from <> (
                    SELECT min(d2.applicable_from)
                    FROM dim_station_history AS d2
                    WHERE d2.station_no = ordered.station_no
                )
               OR previous_until IS NOT NULL
                  AND applicable_from < previous_until
            """
        ).fetchone()[0]
        if overlap_count:
            raise StationHistoryError(
                "dim_station_history contains overlapping station intervals"
            )
        counts = connection.execute(
            """
            SELECT
                COUNT(*),
                COUNT(DISTINCT station_no),
                COUNT(*) FILTER (WHERE applicable_until IS NULL)
            FROM dim_station_history
            """
        ).fetchone()
        durable_rows = connection.execute(
            """
            SELECT *
            FROM dim_station_history
            ORDER BY length(station_no), station_no, applicable_from
            """
        ).fetchall()
    except duckdb.Error as exc:
        raise StationHistoryError(
            f"cannot validate station history warehouse database: {exc}"
        ) from exc
    finally:
        connection.close()
    expected_counts = {
        "history_row_count": int(counts[0]),
        "distinct_station_count": int(counts[1]),
        "open_interval_count": int(counts[2]),
        "snapshot_count": len(inputs),
        "input_observation_count": sum(item.observation_count for item in inputs),
    }
    for key, actual in expected_counts.items():
        if metadata[key] != actual:
            raise StationHistoryError(
                f"metadata {key}={metadata[key]!r} does not match durable value {actual!r}"
            )
    expected = _compute_expected_history(inputs)
    expected_signatures = [_interval_signature(row) for row in expected]
    durable_signatures = [_durable_interval_signature(row) for row in durable_rows]
    if durable_signatures != expected_signatures:
        raise StationHistoryError(
            "dim_station_history does not exactly match independently recomputed snapshot history"
        )
    return metadata


def _build_stage_database(
    *,
    stage_path: Path,
    inputs: list[StationHistoryInput],
    sql_text: str,
) -> None:
    database_path = stage_path / "warehouse.duckdb"
    connection = duckdb.connect(str(database_path))
    try:
        connection.execute(
            """
            CREATE TABLE station_snapshot_catalog (
                snapshot_seq INTEGER NOT NULL,
                observation_period VARCHAR NOT NULL,
                observation_precision VARCHAR NOT NULL,
                applicable_from DATE NOT NULL,
                station_version_id VARCHAR NOT NULL,
                source_version_id VARCHAR NOT NULL,
                observation_count INTEGER NOT NULL
            )
            """
        )
        connection.execute(
            """
            CREATE TABLE station_observation_input (
                snapshot_seq INTEGER NOT NULL,
                observation_period VARCHAR NOT NULL,
                observation_precision VARCHAR NOT NULL,
                applicable_from DATE NOT NULL,
                station_version_id VARCHAR NOT NULL,
                source_version_id VARCHAR NOT NULL,
                station_no VARCHAR NOT NULL,
                station_name VARCHAR,
                district VARCHAR,
                address VARCHAR,
                latitude DECIMAL(18,8),
                longitude DECIMAL(18,8),
                lcd_capacity INTEGER,
                qr_capacity INTEGER,
                operation_mode VARCHAR
            )
            """
        )
        for item in inputs:
            connection.execute(
                """
                INSERT INTO station_snapshot_catalog VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    item.snapshot_seq,
                    item.observation_period,
                    item.observation_precision,
                    item.applicable_from,
                    item.station_version_id,
                    item.source_version_id,
                    item.observation_count,
                ],
            )
            observations_path = item.version_path / "observations.parquet"
            connection.execute(
                f"""
                INSERT INTO station_observation_input
                SELECT
                    ?::INTEGER,
                    ?::VARCHAR,
                    ?::VARCHAR,
                    ?::DATE,
                    ?::VARCHAR,
                    source_version_id,
                    station_no,
                    station_name,
                    district,
                    address,
                    latitude,
                    longitude,
                    lcd_capacity,
                    qr_capacity,
                    operation_mode
                FROM read_parquet('{_sql_path(observations_path)}', hive_partitioning=false)
                """,
                [
                    item.snapshot_seq,
                    item.observation_period,
                    item.observation_precision,
                    item.applicable_from,
                    item.station_version_id,
                ],
            )
        connection.execute(sql_text)
        connection.execute("DROP TABLE station_observation_input")
        connection.execute("DROP TABLE station_snapshot_catalog")
        connection.execute("CHECKPOINT")
    except duckdb.Error as exc:
        raise StationHistoryError(
            f"dim_station_history SQL build failed: {exc}"
        ) from exc
    finally:
        connection.close()
    wal_path = Path(str(database_path) + ".wal")
    if wal_path.exists():
        raise StationHistoryError(
            f"DuckDB WAL remains after checkpoint/close: {wal_path}"
        )


def _build_metadata(
    *,
    stage_path: Path,
    history_version_id: str,
    transformation_version: str,
    configuration: dict[str, object],
    configuration_fingerprint: str,
    sql_sha256: str,
    inputs: list[StationHistoryInput],
) -> dict[str, object]:
    connection = duckdb.connect(
        str(stage_path / "warehouse.duckdb"), read_only=True
    )
    try:
        history_row_count, distinct_station_count, open_interval_count = (
            connection.execute(
                """
                SELECT
                    COUNT(*),
                    COUNT(DISTINCT station_no),
                    COUNT(*) FILTER (WHERE applicable_until IS NULL)
                FROM dim_station_history
                """
            ).fetchone()
        )
    except duckdb.Error as exc:
        raise StationHistoryError(
            f"cannot summarize station history database: {exc}"
        ) from exc
    finally:
        connection.close()
    database_size, database_sha = _fingerprint_file(
        stage_path / "warehouse.duckdb"
    )
    return {
        "schema_version": METADATA_SCHEMA_VERSION,
        "history_version_id": history_version_id,
        "transformation_version": transformation_version,
        "configuration": configuration,
        "configuration_fingerprint": configuration_fingerprint,
        "sql_sha256": sql_sha256,
        "input_snapshots": _input_snapshot_payload(inputs),
        "snapshot_count": len(inputs),
        "input_observation_count": sum(item.observation_count for item in inputs),
        "history_row_count": int(history_row_count),
        "distinct_station_count": int(distinct_station_count),
        "open_interval_count": int(open_interval_count),
        "warehouse_database": {
            "size_bytes": database_size,
            "sha256": database_sha,
        },
    }


def _capture_current_snapshots(
    *,
    manifest: str | Path,
    raw_root: str | Path,
    station_root: str | Path,
) -> list[StationHistoryInput]:
    current_dir = Path(station_root) / "current"
    if not current_dir.is_dir():
        raise StationHistoryError(
            f"station current pointer directory does not exist: {current_dir}"
        )
    candidates: list[tuple[str, dict[str, object], dict[str, object], Path]] = []
    for pointer_path in sorted(current_dir.glob("observation_period=*.json")):
        match = _CURRENT_POINTER_FILE.fullmatch(pointer_path.name)
        if match is None:
            raise StationHistoryError(
                f"invalid station current pointer filename {pointer_path.name!r}"
            )
        observation_period = match.group(1)
        pointer = _load_station_pointer(pointer_path, observation_period)
        version_path = station_version_path(
            station_root,
            observation_period,
            pointer["station_version_id"],
        )
        metadata = validate_station_version(
            version_path,
            manifest=manifest,
            raw_root=raw_root,
        )
        if metadata["observation_period"] != observation_period:
            raise StationHistoryError(
                "station current pointer period does not match validated station bundle"
            )
        if metadata["station_version_id"] != pointer["station_version_id"]:
            raise StationHistoryError(
                "station current pointer version does not match validated station bundle"
            )
        candidates.append(
            (observation_period, pointer, metadata, version_path)
        )
    if not candidates:
        raise StationHistoryError("no station current snapshots are published")

    sortable: list[
        tuple[date, str, dict[str, object], dict[str, object], Path]
    ] = []
    for period, pointer, metadata, version_path in candidates:
        applicable = compute_applicable_from(
            period, metadata["observation_precision"]
        )
        sortable.append(
            (applicable, period, pointer, metadata, version_path)
        )
    sortable.sort(key=lambda item: (item[0], item[1]))
    seen_applicable: set[date] = set()
    result: list[StationHistoryInput] = []
    for seq, (applicable, period, pointer, metadata, version_path) in enumerate(
        sortable, 1
    ):
        if applicable in seen_applicable:
            raise StationHistoryError(
                f"multiple station snapshots share applicability boundary {applicable}"
            )
        seen_applicable.add(applicable)
        result.append(
            StationHistoryInput(
                snapshot_seq=seq,
                observation_period=period,
                observation_precision=metadata["observation_precision"],
                applicable_from=applicable,
                station_version_id=pointer["station_version_id"],
                source_version_id=metadata["source_version_id"],
                observation_count=metadata["observation_count"],
                version_path=version_path,
            )
        )
    return result


def _inputs_from_metadata(
    *,
    metadata: dict[str, object],
    manifest: str | Path,
    raw_root: str | Path,
    station_root: str | Path,
) -> list[StationHistoryInput]:
    raw_inputs = metadata["input_snapshots"]
    if not isinstance(raw_inputs, list) or not raw_inputs:
        raise StationHistoryError(
            "history metadata input_snapshots must be a non-empty list"
        )
    result: list[StationHistoryInput] = []
    seen_applicable: set[date] = set()
    for expected_seq, payload in enumerate(raw_inputs, 1):
        if not isinstance(payload, dict) or set(payload) != {
            "snapshot_seq",
            "observation_period",
            "observation_precision",
            "applicable_from",
            "station_version_id",
            "source_version_id",
            "observation_count",
        }:
            raise StationHistoryError(
                "history metadata input snapshot has invalid fields"
            )
        if payload["snapshot_seq"] != expected_seq:
            raise StationHistoryError(
                "history metadata input snapshot sequence is not contiguous"
            )
        try:
            applicable = date.fromisoformat(payload["applicable_from"])
        except (TypeError, ValueError) as exc:
            raise StationHistoryError(
                "history metadata input snapshot has invalid applicable_from"
            ) from exc
        derived = compute_applicable_from(
            payload["observation_period"], payload["observation_precision"]
        )
        if applicable != derived:
            raise StationHistoryError(
                "history metadata applicable_from does not match observation precision/period"
            )
        if applicable in seen_applicable:
            raise StationHistoryError(
                "history metadata contains duplicate applicability boundaries"
            )
        seen_applicable.add(applicable)
        version_path = station_version_path(
            station_root,
            payload["observation_period"],
            payload["station_version_id"],
        )
        station_metadata = validate_station_version(
            version_path,
            manifest=manifest,
            raw_root=raw_root,
        )
        if (
            station_metadata["observation_period"] != payload["observation_period"]
            or station_metadata["station_version_id"] != payload["station_version_id"]
            or station_metadata["source_version_id"] != payload["source_version_id"]
            or station_metadata["observation_count"] != payload["observation_count"]
            or station_metadata["observation_precision"]
            != payload["observation_precision"]
        ):
            raise StationHistoryError(
                "history metadata input snapshot does not match validated station bundle"
            )
        result.append(
            StationHistoryInput(
                snapshot_seq=expected_seq,
                observation_period=payload["observation_period"],
                observation_precision=payload["observation_precision"],
                applicable_from=applicable,
                station_version_id=payload["station_version_id"],
                source_version_id=payload["source_version_id"],
                observation_count=payload["observation_count"],
                version_path=version_path,
            )
        )
    if [item.applicable_from for item in result] != sorted(
        item.applicable_from for item in result
    ):
        raise StationHistoryError(
            "history metadata input snapshots are not ordered by applicability"
        )
    return result


def _compute_expected_history(
    inputs: list[StationHistoryInput],
) -> list[StationHistoryInterval]:
    observations: dict[
        str,
        list[tuple[StationHistoryInput, tuple[object, ...]]],
    ] = defaultdict(list)
    connection = duckdb.connect()
    try:
        for item in inputs:
            rows = connection.execute(
                f"""
                SELECT
                    station_no,
                    station_name,
                    district,
                    address,
                    latitude,
                    longitude,
                    lcd_capacity,
                    qr_capacity,
                    operation_mode
                FROM read_parquet(
                    '{_sql_path(item.version_path / "observations.parquet")}',
                    hive_partitioning=false
                )
                ORDER BY length(station_no), station_no
                """
            ).fetchall()
            if len(rows) != item.observation_count:
                raise StationHistoryError(
                    "validated station input observation count changed during history validation"
                )
            for row in rows:
                observations[row[0]].append((item, tuple(row[1:])))
    except duckdb.Error as exc:
        raise StationHistoryError(
            f"cannot recompute station history from input snapshots: {exc}"
        ) from exc
    finally:
        connection.close()

    result: list[StationHistoryInterval] = []
    for station_no, station_observations in observations.items():
        station_observations.sort(key=lambda item: item[0].snapshot_seq)
        group: list[tuple[StationHistoryInput, tuple[object, ...]]] = []
        previous_seq: int | None = None
        previous_state: tuple[object, ...] | None = None
        for observation in station_observations:
            item, state = observation
            starts_new = (
                previous_seq is None
                or item.snapshot_seq != previous_seq + 1
                or state != previous_state
            )
            if starts_new and group:
                result.append(
                    _interval_from_group(station_no, group, inputs)
                )
                group = []
            group.append(observation)
            previous_seq = item.snapshot_seq
            previous_state = state
        if group:
            result.append(_interval_from_group(station_no, group, inputs))
    result.sort(
        key=lambda row: (len(row.station_no), row.station_no, row.applicable_from)
    )
    return result


def _interval_from_group(
    station_no: str,
    group: list[tuple[StationHistoryInput, tuple[object, ...]]],
    inputs: list[StationHistoryInput],
) -> StationHistoryInterval:
    first_input, state = group[0]
    last_input = group[-1][0]
    next_index = last_input.snapshot_seq
    applicable_until = (
        inputs[next_index].applicable_from if next_index < len(inputs) else None
    )
    return StationHistoryInterval(
        station_no=station_no,
        applicable_from=first_input.applicable_from,
        applicable_until=applicable_until,
        station_name=state[0],
        district=state[1],
        address=state[2],
        latitude=state[3],
        longitude=state[4],
        lcd_capacity=state[5],
        qr_capacity=state[6],
        operation_mode=state[7],
        observation_periods=tuple(item.observation_period for item, _ in group),
        station_version_ids=tuple(item.station_version_id for item, _ in group),
        source_version_ids=tuple(item.source_version_id for item, _ in group),
        observation_count=len(group),
    )


def _interval_signature(row: StationHistoryInterval) -> tuple[object, ...]:
    return (
        row.station_no,
        row.applicable_from,
        row.applicable_until,
        row.station_name,
        row.district,
        row.address,
        row.latitude,
        row.longitude,
        row.lcd_capacity,
        row.qr_capacity,
        row.operation_mode,
        list(row.observation_periods),
        list(row.station_version_ids),
        list(row.source_version_ids),
        row.observation_count,
    )


def _durable_interval_signature(row: tuple[object, ...]) -> tuple[object, ...]:
    durable = list(row)
    durable[11] = list(durable[11])
    durable[12] = list(durable[12])
    durable[13] = list(durable[13])
    return tuple(durable)


def _input_snapshot_payload(
    inputs: list[StationHistoryInput],
) -> list[dict[str, object]]:
    return [
        {
            "snapshot_seq": item.snapshot_seq,
            "observation_period": item.observation_period,
            "observation_precision": item.observation_precision,
            "applicable_from": item.applicable_from.isoformat(),
            "station_version_id": item.station_version_id,
            "source_version_id": item.source_version_id,
            "observation_count": item.observation_count,
        }
        for item in inputs
    ]


def _require_station_inputs_unchanged(
    *,
    expected: list[dict[str, object]],
    manifest: str | Path,
    raw_root: str | Path,
    station_root: str | Path,
) -> None:
    actual = _input_snapshot_payload(
        _capture_current_snapshots(
            manifest=manifest,
            raw_root=raw_root,
            station_root=station_root,
        )
    )
    if actual != expected:
        raise StationHistoryError(
            "station current snapshot set changed during history build; current warehouse unchanged"
        )


def _load_sql(sql_path: str | Path | None) -> tuple[str, str]:
    path = Path(sql_path) if sql_path is not None else _DEFAULT_SQL_PATH
    try:
        raw = path.read_bytes()
    except OSError as exc:
        raise StationHistoryError(f"cannot read station history SQL {path}: {exc}") from exc
    try:
        text = raw.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise StationHistoryError("station history SQL must be UTF-8") from exc
    # SQL source identity is textual, not checkout-EOL identity. Git on the
    # Windows host may materialize CRLF while the WSL2 reference runtime uses
    # LF; normalize both CRLF and lone CR before hashing *and* execution so the
    # same logical transformation has one cross-platform version identity.
    text = text.replace("\r\n", "\n").replace("\r", "\n")
    if not text.strip():
        raise StationHistoryError("station history SQL is empty")
    return text, hashlib.sha256(text.encode("utf-8")).hexdigest()


def _load_station_pointer(
    path: Path, observation_period: str
) -> dict[str, object]:
    payload = _load_json(path, "station current pointer")
    if set(payload) != {
        "schema_version",
        "observation_period",
        "station_version_id",
    }:
        raise StationHistoryError(
            f"station current pointer {path} has invalid fields"
        )
    if payload["schema_version"] != 1:
        raise StationHistoryError(
            f"station current pointer {path} has unsupported schema_version"
        )
    if payload["observation_period"] != observation_period:
        raise StationHistoryError(
            f"station current pointer {path} has wrong observation_period"
        )
    if not isinstance(payload["station_version_id"], str) or re.fullmatch(
        r"stv1_[0-9a-f]{64}", payload["station_version_id"]
    ) is None:
        raise StationHistoryError(
            f"station current pointer {path} has invalid station_version_id"
        )
    return payload


def _validate_metadata_identity(metadata: dict[str, object]) -> None:
    required = {
        "schema_version",
        "history_version_id",
        "transformation_version",
        "configuration",
        "configuration_fingerprint",
        "sql_sha256",
        "input_snapshots",
        "snapshot_count",
        "input_observation_count",
        "history_row_count",
        "distinct_station_count",
        "open_interval_count",
        "warehouse_database",
    }
    if set(metadata) != required:
        raise StationHistoryError(
            "station history metadata fields do not match schema"
        )
    if metadata["schema_version"] != METADATA_SCHEMA_VERSION:
        raise StationHistoryError(
            "unsupported station history metadata schema_version"
        )
    configuration = _canonical_configuration_object(metadata["configuration"])
    configuration_fingerprint = compute_configuration_fingerprint(configuration)
    if metadata["configuration_fingerprint"] != configuration_fingerprint:
        raise StationHistoryError(
            "history metadata configuration_fingerprint does not match configuration"
        )
    input_snapshots = metadata["input_snapshots"]
    if not isinstance(input_snapshots, list):
        raise StationHistoryError("history metadata input_snapshots must be a list")
    history_version_id = compute_history_version_id(
        input_snapshots=input_snapshots,
        transformation_version=_require_text(
            metadata["transformation_version"], "transformation_version"
        ),
        configuration_fingerprint=configuration_fingerprint,
        sql_sha256=metadata["sql_sha256"],
    )
    if metadata["history_version_id"] != history_version_id:
        raise StationHistoryError(
            "history metadata history_version_id does not match identity inputs"
        )


def _require_schema(connection: duckdb.DuckDBPyConnection) -> None:
    actual = tuple(
        (row[0], row[1])
        for row in connection.execute(
            "DESCRIBE SELECT * FROM dim_station_history"
        ).fetchall()
    )
    if actual != _HISTORY_SCHEMA:
        raise StationHistoryError(
            f"dim_station_history schema mismatch: expected {_HISTORY_SCHEMA!r}, got {actual!r}"
        )


def _require_database_fingerprint(path: Path, payload: object) -> None:
    if not isinstance(payload, dict) or set(payload) != {"size_bytes", "sha256"}:
        raise StationHistoryError(
            "history metadata warehouse_database fingerprint is malformed"
        )
    size, sha = _fingerprint_file(path)
    if payload["size_bytes"] != size or payload["sha256"] != sha:
        raise StationHistoryError(
            "warehouse.duckdb fingerprint does not match metadata"
        )


def _require_bundle_binding(
    metadata: dict[str, object], history_version_id: str
) -> None:
    if metadata["history_version_id"] != history_version_id:
        raise StationHistoryError(
            "history bundle version id does not match path/pointer"
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
        raise StationHistoryError(
            "same history_version_id exists with different logical metadata"
        )


def _load_metadata(path: Path) -> dict[str, object]:
    return _load_json(path, "station history metadata")


def _load_current_pointer(path: Path) -> dict[str, object] | None:
    if not path.exists():
        return None
    payload = _load_json(path, "warehouse current pointer")
    if set(payload) != {"schema_version", "history_version_id"}:
        raise StationHistoryError(
            f"warehouse current pointer {path} has invalid fields"
        )
    if payload["schema_version"] != CURRENT_POINTER_SCHEMA_VERSION:
        raise StationHistoryError(
            f"warehouse current pointer {path} has unsupported schema_version"
        )
    if not isinstance(payload["history_version_id"], str) or _HISTORY_VERSION.fullmatch(
        payload["history_version_id"]
    ) is None:
        raise StationHistoryError(
            f"warehouse current pointer {path} has invalid history_version_id"
        )
    return payload


def _write_current_pointer(path: Path, history_version_id: str) -> None:
    payload = {
        "schema_version": CURRENT_POINTER_SCHEMA_VERSION,
        "history_version_id": history_version_id,
    }
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, temp_name = tempfile.mkstemp(
        dir=path.parent, prefix=f".{path.name}.", suffix=".tmp"
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
def _publish_lock(root: Path, history_version_id: str):
    lock_path = root / "locks" / "current.lock"
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    handle = lock_path.open("a+b")
    try:
        handle.seek(0, os.SEEK_END)
        if handle.tell() == 0:
            handle.write(b"0")
            handle.flush()
            os.fsync(handle.fileno())
        try:
            _lock_nonblocking(handle)
        except (BlockingIOError, OSError) as exc:
            raise StationHistoryError(
                f"another warehouse publish holds lock {lock_path}"
            ) from exc
        handle.seek(1)
        handle.truncate()
        handle.write((history_version_id + "\n").encode("utf-8"))
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


def _write_json(path: Path, payload: dict[str, object]) -> None:
    with path.open("w", encoding="utf-8", newline="\n") as handle:
        handle.write(_canonical_json_text(payload) + "\n")
        handle.flush()
        os.fsync(handle.fileno())


def _load_json(path: Path, label: str) -> dict[str, object]:
    try:
        text = path.read_text(encoding="utf-8")
        payload = json.loads(text, object_pairs_hook=_reject_duplicate_pairs)
    except (OSError, json.JSONDecodeError) as exc:
        raise StationHistoryError(f"cannot read {label} {path}: {exc}") from exc
    if not isinstance(payload, dict):
        raise StationHistoryError(f"{label} {path} must be a JSON object")
    return payload


def _fingerprint_file(path: Path) -> tuple[int, str]:
    digest = hashlib.sha256()
    size = 0
    try:
        with path.open("rb") as handle:
            while chunk := handle.read(1024 * 1024):
                size += len(chunk)
                digest.update(chunk)
    except OSError as exc:
        raise StationHistoryError(f"cannot fingerprint {path}: {exc}") from exc
    return size, digest.hexdigest()


def _canonical_configuration_object(
    configuration: Mapping[str, object],
) -> dict[str, object]:
    if not isinstance(configuration, Mapping):
        raise StationHistoryError(
            "configuration must be a JSON-compatible mapping"
        )
    try:
        encoded = _canonical_json_text(dict(configuration))
        decoded = json.loads(encoded)
    except (TypeError, ValueError) as exc:
        raise StationHistoryError(
            f"configuration must be JSON-compatible: {exc}"
        ) from exc
    if not isinstance(decoded, dict):
        raise StationHistoryError(
            "configuration must serialize to a JSON object"
        )
    return decoded


def _canonical_json_text(value: object) -> str:
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    )


def _canonical_json_bytes(value: object) -> bytes:
    return _canonical_json_text(value).encode("utf-8")


def _require_text(value: object, name: str) -> str:
    if not isinstance(value, str) or not value:
        raise StationHistoryError(f"{name} must be a non-empty string")
    return value


def _reject_duplicate_pairs(
    pairs: list[tuple[str, object]]
) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise StationHistoryError(f"duplicate JSON key {key!r} is not allowed")
        result[key] = value
    return result


def _sql_path(path: Path) -> str:
    return path.resolve().as_posix().replace("'", "''")


def _result(
    status: str,
    metadata: dict[str, object],
    version_path: Path,
    current_path: Path,
) -> StationHistoryPublishResult:
    return StationHistoryPublishResult(
        status=status,
        history_version_id=metadata["history_version_id"],
        version_path=version_path,
        current_pointer_path=current_path,
        snapshot_count=metadata["snapshot_count"],
        input_observation_count=metadata["input_observation_count"],
        history_row_count=metadata["history_row_count"],
        distinct_station_count=metadata["distinct_station_count"],
        open_interval_count=metadata["open_interval_count"],
    )
