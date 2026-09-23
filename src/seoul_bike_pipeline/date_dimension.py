"""Build, validate, and atomically publish the deterministic production date dimension."""

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
from datetime import date, timedelta
from pathlib import Path
from typing import Callable

import duckdb

from .errors import DateDimensionError

DATE_COVERAGE_START = date(2020, 1, 1)
DATE_COVERAGE_END = date(2026, 6, 30)
DATE_ROW_COUNT = 2373
DAY_OF_WEEK_CONVENTION = "ISO-8601-MONDAY-1"
WEEKEND_DAYS = (6, 7)

DATE_VERSION_PREFIX = "dv1_"
CONFIGURATION_FINGERPRINT_PREFIX = "cfg1_"
CURRENT_POINTER_SCHEMA_VERSION = 1
METADATA_SCHEMA_VERSION = 1
STATUS_PUBLISHED_CURRENT = "PUBLISHED_CURRENT"
STATUS_REUSED_CURRENT = "REUSED_CURRENT"

_DATE_VERSION = re.compile(r"dv1_[0-9a-f]{64}")
_CONFIGURATION_FINGERPRINT = re.compile(r"cfg1_[0-9a-f]{64}")
_SHA256 = re.compile(r"[0-9a-f]{64}")
_DEFAULT_SQL_PATH = Path(__file__).with_name("sql") / "dim_date.sql"
_DATE_SCHEMA = (
    ("date", "DATE"),
    ("year", "INTEGER"),
    ("month", "INTEGER"),
    ("day", "INTEGER"),
    ("day_of_week", "INTEGER"),
    ("weekend_flag", "BOOLEAN"),
)
_METADATA_FIELDS = {
    "schema_version",
    "coverage_start",
    "coverage_end",
    "day_of_week_convention",
    "weekend_days",
    "transformation_version",
    "configuration",
    "configuration_fingerprint",
    "sql_sha256",
    "date_version_id",
    "date_row_count",
    "warehouse_database",
}


@dataclass(frozen=True)
class DateDimensionPublishResult:
    status: str
    date_version_id: str
    version_path: Path
    current_pointer_path: Path
    coverage_start: date
    coverage_end: date
    date_row_count: int


def compute_configuration_fingerprint(configuration: Mapping[str, object]) -> str:
    canonical = _canonical_configuration_object(configuration)
    return CONFIGURATION_FINGERPRINT_PREFIX + hashlib.sha256(
        _canonical_json_bytes(canonical)
    ).hexdigest()


def compute_date_version_id(
    *,
    transformation_version: str,
    configuration_fingerprint: str,
    sql_sha256: str,
) -> str:
    _require_text(transformation_version, "transformation_version")
    if _CONFIGURATION_FINGERPRINT.fullmatch(configuration_fingerprint) is None:
        raise DateDimensionError("configuration_fingerprint must use cfg1_<sha256>")
    if _SHA256.fullmatch(sql_sha256) is None:
        raise DateDimensionError("sql_sha256 must be a lowercase SHA-256 digest")
    payload = [
        DATE_COVERAGE_START.isoformat(),
        DATE_COVERAGE_END.isoformat(),
        DAY_OF_WEEK_CONVENTION,
        list(WEEKEND_DAYS),
        transformation_version,
        configuration_fingerprint,
        sql_sha256,
    ]
    return DATE_VERSION_PREFIX + hashlib.sha256(_canonical_json_bytes(payload)).hexdigest()


def date_dimension_root(warehouse_root: str | Path) -> Path:
    return Path(warehouse_root) / "dim_date"


def date_version_path(warehouse_root: str | Path, date_version_id: str) -> Path:
    _require_version(date_version_id)
    return (
        date_dimension_root(warehouse_root)
        / "versions"
        / f"date_version_id={date_version_id}"
    )


def date_current_pointer_path(warehouse_root: str | Path) -> Path:
    return date_dimension_root(warehouse_root) / "current.json"


def publish_date_dimension(
    *,
    warehouse_root: str | Path,
    transformation_version: str,
    configuration: Mapping[str, object],
    sql_path: str | Path | None = None,
    before_current_publish: Callable[[Path], None] | None = None,
) -> DateDimensionPublishResult:
    _require_text(transformation_version, "transformation_version")
    canonical_configuration = _canonical_configuration_object(configuration)
    configuration_fingerprint = compute_configuration_fingerprint(canonical_configuration)
    sql_text, sql_sha256 = _load_sql(sql_path)
    date_version_id = compute_date_version_id(
        transformation_version=transformation_version,
        configuration_fingerprint=configuration_fingerprint,
        sql_sha256=sql_sha256,
    )
    version_path = date_version_path(warehouse_root, date_version_id)
    current_path = date_current_pointer_path(warehouse_root)
    initial_pointer = _load_current_pointer_if_present(current_path)

    if initial_pointer is not None:
        validate_date_dimension_version(
            date_version_path(warehouse_root, initial_pointer["date_version_id"])
        )

    root = date_dimension_root(warehouse_root)
    if version_path.exists():
        candidate_metadata = validate_date_dimension_version(version_path)
        _require_requested_lineage(
            candidate_metadata,
            transformation_version=transformation_version,
            configuration=canonical_configuration,
            configuration_fingerprint=configuration_fingerprint,
            sql_sha256=sql_sha256,
        )
    else:
        staging_root = root / "staging"
        staging_root.mkdir(parents=True, exist_ok=True)
        stage_container = Path(tempfile.mkdtemp(prefix="build-", dir=staging_root))
        stage_path = stage_container / f"date_version_id={date_version_id}"
        stage_path.mkdir()
        try:
            _build_stage(stage_path, sql_text)
            candidate_metadata = _build_metadata(
                stage_path=stage_path,
                transformation_version=transformation_version,
                configuration=canonical_configuration,
                configuration_fingerprint=configuration_fingerprint,
                sql_sha256=sql_sha256,
                date_version_id=date_version_id,
            )
            _write_json(stage_path / "metadata.json", candidate_metadata)
            validated_stage = validate_date_dimension_version(stage_path)
            if validated_stage != candidate_metadata:
                raise DateDimensionError(
                    "validated date dimension stage metadata changed unexpectedly"
                )
            version_path.parent.mkdir(parents=True, exist_ok=True)
            try:
                os.replace(stage_path, version_path)
            except OSError:
                if not version_path.exists():
                    raise
                existing = validate_date_dimension_version(version_path)
                _require_same_logical_version(existing, candidate_metadata)
                candidate_metadata = existing
        finally:
            if stage_container.exists():
                shutil.rmtree(stage_container)

    with _publish_lock(root, date_version_id):
        latest_pointer = _load_current_pointer_if_present(current_path)
        final_metadata = validate_date_dimension_version(version_path)
        if final_metadata != candidate_metadata:
            raise DateDimensionError(
                "date dimension immutable bundle changed before current publish"
            )
        if (
            latest_pointer is not None
            and latest_pointer["date_version_id"] == date_version_id
        ):
            return _result(
                STATUS_REUSED_CURRENT, final_metadata, version_path, current_path
            )
        if latest_pointer != initial_pointer:
            raise DateDimensionError(
                "date dimension current pointer changed during build; refusing rollback"
            )
        if before_current_publish is not None:
            before_current_publish(version_path)
            callback_metadata = validate_date_dimension_version(version_path)
            if callback_metadata != final_metadata:
                raise DateDimensionError(
                    "date dimension immutable bundle changed before atomic current publish"
                )
            if _load_current_pointer_if_present(current_path) != initial_pointer:
                raise DateDimensionError(
                    "date dimension current pointer changed before atomic publish"
                )
        _write_current_pointer(current_path, date_version_id)
    return _result(STATUS_PUBLISHED_CURRENT, final_metadata, version_path, current_path)


def validate_date_dimension_version(
    version_path: str | Path,
) -> dict[str, object]:
    path = Path(version_path)
    metadata = _load_json(path / "metadata.json", "date dimension metadata")
    _validate_metadata_identity(metadata)
    _require_bundle_binding(metadata, path)

    database_path = path / "warehouse.duckdb"
    wal_path = Path(str(database_path) + ".wal")
    if wal_path.exists():
        raise DateDimensionError(
            "date dimension warehouse has residual WAL and is not finalized"
        )
    _require_file_fingerprint(
        database_path, metadata["warehouse_database"], label="warehouse.duckdb"
    )
    try:
        connection = duckdb.connect(str(database_path), read_only=True)
    except duckdb.Error as exc:
        raise DateDimensionError(
            f"cannot open date dimension warehouse database: {exc}"
        ) from exc
    try:
        _require_schema(connection)
        _require_semantics(connection)
        row_count = int(
            connection.execute("SELECT COUNT(*) FROM dim_date").fetchone()[0]
        )
    except duckdb.Error as exc:
        raise DateDimensionError(
            f"cannot validate date dimension warehouse database: {exc}"
        ) from exc
    finally:
        connection.close()
    if metadata["date_row_count"] != row_count:
        raise DateDimensionError(
            "date dimension metadata row count does not match durable table"
        )
    return metadata


def _build_stage(stage_path: Path, sql_text: str) -> None:
    database_path = stage_path / "warehouse.duckdb"
    connection = duckdb.connect(str(database_path))
    try:
        connection.execute(sql_text)
        _require_materialized_table(connection)
        _require_semantics(connection)
        connection.execute("CHECKPOINT")
    except duckdb.Error as exc:
        raise DateDimensionError(f"date dimension SQL build failed: {exc}") from exc
    finally:
        connection.close()
    if Path(str(database_path) + ".wal").exists():
        raise DateDimensionError("date dimension build left a residual DuckDB WAL")


def _build_metadata(
    *,
    stage_path: Path,
    transformation_version: str,
    configuration: dict[str, object],
    configuration_fingerprint: str,
    sql_sha256: str,
    date_version_id: str,
) -> dict[str, object]:
    database_path = stage_path / "warehouse.duckdb"
    size_bytes, sha256 = _fingerprint_file(database_path)
    return {
        "schema_version": METADATA_SCHEMA_VERSION,
        "coverage_start": DATE_COVERAGE_START.isoformat(),
        "coverage_end": DATE_COVERAGE_END.isoformat(),
        "day_of_week_convention": DAY_OF_WEEK_CONVENTION,
        "weekend_days": list(WEEKEND_DAYS),
        "transformation_version": transformation_version,
        "configuration": configuration,
        "configuration_fingerprint": configuration_fingerprint,
        "sql_sha256": sql_sha256,
        "date_version_id": date_version_id,
        "date_row_count": DATE_ROW_COUNT,
        "warehouse_database": {"size_bytes": size_bytes, "sha256": sha256},
    }


def _require_schema(connection: duckdb.DuckDBPyConnection) -> None:
    rows = connection.execute(
        """
        SELECT table_type
        FROM information_schema.tables
        WHERE table_schema = 'main' AND table_name = 'dim_date'
        """
    ).fetchall()
    if rows != [("BASE TABLE",)]:
        raise DateDimensionError("dim_date must be a materialized base table")
    actual = tuple(
        (row[0], row[1])
        for row in connection.execute("DESCRIBE SELECT * FROM dim_date").fetchall()
    )
    if actual != _DATE_SCHEMA:
        raise DateDimensionError(
            f"dim_date schema mismatch: expected {_DATE_SCHEMA!r}, got {actual!r}"
        )


def _require_materialized_table(connection: duckdb.DuckDBPyConnection) -> None:
    _require_schema(connection)


def _require_semantics(connection: duckdb.DuckDBPyConnection) -> None:
    _require_schema(connection)
    actual = connection.execute(
        """
        SELECT date, year, month, day, day_of_week, weekend_flag
        FROM dim_date
        ORDER BY date
        """
    ).fetchall()
    expected: list[tuple[date, int, int, int, int, bool]] = []
    current = DATE_COVERAGE_START
    while current <= DATE_COVERAGE_END:
        iso_weekday = current.isoweekday()
        expected.append(
            (
                current,
                current.year,
                current.month,
                current.day,
                iso_weekday,
                iso_weekday in WEEKEND_DAYS,
            )
        )
        current += timedelta(days=1)
    if len(actual) != DATE_ROW_COUNT or len(expected) != DATE_ROW_COUNT:
        raise DateDimensionError(
            "dim_date does not have exactly one row per calendar date"
        )
    if actual != expected:
        raise DateDimensionError(
            "dim_date does not exactly match independently recomputed calendar semantics"
        )


def _validate_metadata_identity(metadata: dict[str, object]) -> None:
    if set(metadata) != _METADATA_FIELDS:
        raise DateDimensionError("date dimension metadata has invalid fields")
    if metadata["schema_version"] != METADATA_SCHEMA_VERSION:
        raise DateDimensionError(
            "date dimension metadata has unsupported schema_version"
        )
    if metadata["coverage_start"] != DATE_COVERAGE_START.isoformat():
        raise DateDimensionError("date dimension metadata has invalid coverage_start")
    if metadata["coverage_end"] != DATE_COVERAGE_END.isoformat():
        raise DateDimensionError("date dimension metadata has invalid coverage_end")
    if metadata["day_of_week_convention"] != DAY_OF_WEEK_CONVENTION:
        raise DateDimensionError(
            "date dimension metadata has invalid weekday convention"
        )
    if metadata["weekend_days"] != list(WEEKEND_DAYS):
        raise DateDimensionError(
            "date dimension metadata has invalid weekend definition"
        )
    transformation_version = _require_text(
        metadata["transformation_version"], "transformation_version"
    )
    configuration = _canonical_configuration_object(metadata["configuration"])
    configuration_fingerprint = compute_configuration_fingerprint(configuration)
    if metadata["configuration_fingerprint"] != configuration_fingerprint:
        raise DateDimensionError(
            "date dimension configuration fingerprint does not match configuration"
        )
    sql_sha256 = metadata["sql_sha256"]
    if not isinstance(sql_sha256, str) or _SHA256.fullmatch(sql_sha256) is None:
        raise DateDimensionError("date dimension metadata sql_sha256 is invalid")
    expected_id = compute_date_version_id(
        transformation_version=transformation_version,
        configuration_fingerprint=configuration_fingerprint,
        sql_sha256=sql_sha256,
    )
    if metadata["date_version_id"] != expected_id:
        raise DateDimensionError(
            "date dimension metadata date_version_id does not match deterministic inputs"
        )
    if metadata["date_row_count"] != DATE_ROW_COUNT:
        raise DateDimensionError(
            "date dimension metadata date_row_count is invalid"
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
        raise DateDimensionError(
            "date dimension warehouse fingerprint is malformed"
        )


def _require_bundle_binding(metadata: dict[str, object], path: Path) -> None:
    date_version_id = metadata["date_version_id"]
    if path.name != f"date_version_id={date_version_id}":
        raise DateDimensionError(
            "date dimension directory version id is not bound to metadata"
        )


def _require_requested_lineage(
    metadata: dict[str, object],
    *,
    transformation_version: str,
    configuration: dict[str, object],
    configuration_fingerprint: str,
    sql_sha256: str,
) -> None:
    expected = {
        "transformation_version": transformation_version,
        "configuration": configuration,
        "configuration_fingerprint": configuration_fingerprint,
        "sql_sha256": sql_sha256,
    }
    for key, value in expected.items():
        if metadata[key] != value:
            raise DateDimensionError(
                f"existing date dimension version {key} does not match requested inputs"
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
        raise DateDimensionError(
            "same date_version_id exists with different logical metadata"
        )


def _load_current_pointer_if_present(path: Path) -> dict[str, object] | None:
    if not path.exists():
        return None
    payload = _load_json(path, "current date dimension pointer")
    if set(payload) != {"schema_version", "date_version_id"}:
        raise DateDimensionError(
            "current date dimension pointer has invalid fields"
        )
    if payload["schema_version"] != CURRENT_POINTER_SCHEMA_VERSION:
        raise DateDimensionError(
            "current date dimension pointer has unsupported schema_version"
        )
    _require_version(payload["date_version_id"])
    return payload


def _write_current_pointer(path: Path, date_version_id: str) -> None:
    payload = {
        "schema_version": CURRENT_POINTER_SCHEMA_VERSION,
        "date_version_id": date_version_id,
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


@contextmanager
def _publish_lock(root: Path, date_version_id: str):
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
            raise DateDimensionError(
                "date dimension current is being published concurrently; "
                f"lock={lock_path}"
            ) from exc
        handle.seek(1)
        handle.truncate()
        handle.write((date_version_id + "\n").encode("utf-8"))
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
        raise DateDimensionError(f"cannot read dim_date SQL {path}: {exc}") from exc
    text = text.replace("\r\n", "\n").replace("\r", "\n")
    if not text.strip():
        raise DateDimensionError("dim_date SQL is empty")
    return text, hashlib.sha256(text.encode("utf-8")).hexdigest()


def _require_file_fingerprint(path: Path, payload: object, *, label: str) -> None:
    if not isinstance(payload, dict) or set(payload) != {"size_bytes", "sha256"}:
        raise DateDimensionError(f"metadata {label} fingerprint is malformed")
    size_bytes, sha256 = _fingerprint_file(path)
    if payload != {"size_bytes": size_bytes, "sha256": sha256}:
        raise DateDimensionError(
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
        raise DateDimensionError(f"cannot fingerprint {path}: {exc}") from exc
    return size, digest.hexdigest()


def _load_json(path: Path, label: str) -> dict[str, object]:
    try:
        with path.open("r", encoding="utf-8") as handle:
            payload = json.load(handle, object_pairs_hook=_reject_duplicate_pairs)
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise DateDimensionError(f"cannot read {label} {path}: {exc}") from exc
    if not isinstance(payload, dict):
        raise DateDimensionError(f"{label} {path} must be a JSON object")
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
        raise DateDimensionError(
            "configuration must be a JSON-compatible mapping"
        )
    try:
        encoded = _canonical_json_text(dict(configuration))
        decoded = json.loads(encoded)
    except (TypeError, ValueError) as exc:
        raise DateDimensionError(
            f"configuration must be JSON-compatible: {exc}"
        ) from exc
    if not isinstance(decoded, dict):
        raise DateDimensionError(
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
            raise DateDimensionError(
                f"duplicate JSON key {key!r} is not allowed"
            )
        result[key] = value
    return result


def _require_text(value: object, label: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise DateDimensionError(f"{label} must be a non-empty string")
    return value


def _require_version(value: object) -> str:
    if not isinstance(value, str) or _DATE_VERSION.fullmatch(value) is None:
        raise DateDimensionError(
            "date_version_id has invalid deterministic identity"
        )
    return value


def _result(
    status: str,
    metadata: dict[str, object],
    version_path: Path,
    current_path: Path,
) -> DateDimensionPublishResult:
    return DateDimensionPublishResult(
        status=status,
        date_version_id=metadata["date_version_id"],
        version_path=version_path,
        current_pointer_path=current_path,
        coverage_start=DATE_COVERAGE_START,
        coverage_end=DATE_COVERAGE_END,
        date_row_count=metadata["date_row_count"],
    )
