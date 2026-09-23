"""Station snapshot ingestion, same-snapshot consolidation, and immutable publish.

This module implements D-005 / PRD Addendum 002 and intentionally stops before
cross-snapshot dim_station_history construction.
"""

from __future__ import annotations

import csv
import hashlib
import io
import json
import os
import re
import shutil
import tempfile
import zipfile
from collections import Counter, defaultdict
from collections.abc import Mapping
from contextlib import contextmanager
from dataclasses import asdict, dataclass
from datetime import date, timedelta
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Callable, Iterator
from xml.etree import ElementTree as ET

import duckdb

from .canonical_dq import DqIssue, SEVERITY_QUARANTINE, SEVERITY_WARNING
from .csv_source import compute_raw_row_id, compute_row_content_sha256
from .errors import RawArtifactError, StationSnapshotError
from .manifest import SourceRecord, load_manifest
from .raw import verify_raw_artifact

STATION_DATASET_ID = "station_snapshot"
STATION_SCHEMA_VERSION = "station_snapshot_v1"
STATION_VERSION_PREFIX = "stv1_"
CONFIGURATION_FINGERPRINT_PREFIX = "cfg1_"
CURRENT_POINTER_SCHEMA_VERSION = 1
METADATA_SCHEMA_VERSION = 1

STATUS_PUBLISHED_CURRENT = "PUBLISHED_CURRENT"
STATUS_REUSED_CURRENT = "REUSED_CURRENT"
STATUS_BUILT_HISTORICAL = "BUILT_HISTORICAL"
STATUS_REUSED_HISTORICAL = "REUSED_HISTORICAL"

PRECISION_DAY = "DAY"
PRECISION_MONTH = "MONTH"

RULE_STATION_NO_INVALID = "station_no_invalid"
RULE_TEXT_MISSING = "station_attribute_missing"
RULE_COORDINATE_INVALID = "station_coordinate_invalid"
RULE_CAPACITY_INVALID = "station_capacity_invalid"
RULE_OPERATION_MODE_INVALID = "station_operation_mode_invalid"
RULE_MODE_CAPACITY_MISMATCH = "station_mode_capacity_mismatch"
RULE_EXACT_DUPLICATE = "station_exact_duplicate"
RULE_IDENTITY_CONFLICT = "station_identity_conflict"
RULE_CAPACITY_CONFLICT = "station_capacity_conflict"

_ASCII_DIGITS = re.compile(r"[0-9]+")
_MONTH_PERIOD = re.compile(r"20\d{2}-(?:0[1-9]|1[0-2])")
_DAY_PERIOD = re.compile(r"20\d{2}-(?:0[1-9]|1[0-2])-(?:0[1-9]|[12]\d|3[01])")
_STATION_VERSION = re.compile(r"stv1_[0-9a-f]{64}")
_CONFIGURATION_FINGERPRINT = re.compile(r"cfg1_[0-9a-f]{64}")
_FILENAME_DAY = re.compile(r"\((\d{2})\.(\d{1,2})\.(\d{1,2})\s*기준\)")
_FILENAME_MONTH = re.compile(r"\((\d{2})\.(\d{1,2})월\s*기준\)")
_NS = {"m": "http://schemas.openxmlformats.org/spreadsheetml/2006/main"}

_HEADER = (
    ("대여소번호", "보관소(대여소)명", "소재지(위치)", "", "", "", "설치시기", "설치형태", "", "운영방식"),
    ("", "", "", "", "", "", "", "LCD", "QR", ""),
    ("", "", "자치구", "상세주소", "위도", "경도", "", "", "", ""),
    ("", "", "", "", "", "", "", "거치대수", "거치대수", ""),
    ("", "", "", "", "", "", "", "", "", ""),
)
_ISSUE_JSON_SCHEMA = (
    '[{"rule_id":"VARCHAR","severity":"VARCHAR","field":"VARCHAR","reason":"VARCHAR"}]'
)
_OBS_SCHEMA = (
    ("source_version_id", "VARCHAR"),
    ("observation_period", "VARCHAR"),
    ("observation_precision", "VARCHAR"),
    ("station_no", "VARCHAR"),
    ("station_name", "VARCHAR"),
    ("district", "VARCHAR"),
    ("address", "VARCHAR"),
    ("latitude", "DECIMAL(18,8)"),
    ("longitude", "DECIMAL(18,8)"),
    ("lcd_capacity", "INTEGER"),
    ("qr_capacity", "INTEGER"),
    ("operation_mode", "VARCHAR"),
    ("source_row_ids", "VARCHAR[]"),
    ("source_row_numbers", "BIGINT[]"),
    ("source_row_count", "INTEGER"),
    ("exact_duplicate_count", "INTEGER"),
    ("installation_values", "VARCHAR[]"),
    ("dq_issues", "STRUCT(rule_id VARCHAR, severity VARCHAR, field VARCHAR, reason VARCHAR)[]"),
    ("dq_warning_count", "INTEGER"),
)
_QUARANTINE_SCHEMA = (
    ("source_row_id", "VARCHAR"),
    ("source_version_id", "VARCHAR"),
    ("source_row_number", "BIGINT"),
    ("observation_period", "VARCHAR"),
    ("station_no_raw", "VARCHAR"),
    ("station_no", "VARCHAR"),
    ("station_group_ref", "VARCHAR"),
    ("rule_id", "VARCHAR"),
    ("severity", "VARCHAR"),
    ("field", "VARCHAR"),
    ("reason", "VARCHAR"),
)


@dataclass(frozen=True)
class StationSourceRow:
    source_row_id: str
    source_version_id: str
    source_row_number: int
    row_content_sha256: str
    station_no_raw: str | None
    station_no: str | None
    station_name: str | None
    district: str | None
    address: str | None
    latitude: Decimal | None
    longitude: Decimal | None
    installation_value: str | None
    lcd_capacity: int | None
    qr_capacity: int | None
    operation_mode_raw: str | None
    operation_mode: str | None
    dq_issues: tuple[DqIssue, ...]
    row_quarantined: bool


@dataclass(frozen=True)
class StationObservation:
    source_version_id: str
    observation_period: str
    observation_precision: str
    station_no: str
    station_name: str | None
    district: str | None
    address: str | None
    latitude: Decimal | None
    longitude: Decimal | None
    lcd_capacity: int | None
    qr_capacity: int | None
    operation_mode: str | None
    source_row_ids: tuple[str, ...]
    source_row_numbers: tuple[int, ...]
    source_row_count: int
    exact_duplicate_count: int
    installation_values: tuple[str, ...]
    dq_issues: tuple[DqIssue, ...]
    dq_warning_count: int


@dataclass(frozen=True)
class StationSnapshotSummary:
    source_version_id: str
    observation_period: str
    observation_precision: str
    source_row_count: int
    observation_count: int
    observation_source_row_count: int
    quarantined_source_row_count: int
    conflict_group_count: int
    exact_duplicate_group_count: int
    exact_duplicate_source_row_count: int
    warning_observation_count: int


@dataclass(frozen=True)
class StationPublishResult:
    status: str
    observation_period: str
    source_version_id: str
    station_version_id: str
    version_path: Path
    current_pointer_path: Path
    source_row_count: int
    observation_count: int
    quarantined_source_row_count: int
    quarantine_issue_count: int


def compute_configuration_fingerprint(configuration: Mapping[str, object]) -> str:
    canonical = _canonical_configuration_object(configuration)
    return CONFIGURATION_FINGERPRINT_PREFIX + hashlib.sha256(
        _canonical_json_bytes(canonical)
    ).hexdigest()


def compute_station_version_id(
    *,
    observation_period: str,
    source_version_id: str,
    transformation_version: str,
    configuration_fingerprint: str,
) -> str:
    _require_observation_period(observation_period)
    _require_text(source_version_id, "source_version_id")
    _require_text(transformation_version, "transformation_version")
    if _CONFIGURATION_FINGERPRINT.fullmatch(configuration_fingerprint) is None:
        raise StationSnapshotError("configuration_fingerprint must use cfg1_<sha256>")
    payload = [
        observation_period,
        source_version_id,
        transformation_version,
        configuration_fingerprint,
    ]
    return STATION_VERSION_PREFIX + hashlib.sha256(_canonical_json_bytes(payload)).hexdigest()


def station_version_path(
    station_root: str | Path, observation_period: str, station_version_id: str
) -> Path:
    _require_observation_period(observation_period)
    if _STATION_VERSION.fullmatch(station_version_id) is None:
        raise StationSnapshotError("station_version_id must use stv1_<sha256>")
    return (
        Path(station_root)
        / "versions"
        / f"observation_period={observation_period}"
        / f"station_version_id={station_version_id}"
    )


def station_current_pointer_path(station_root: str | Path, observation_period: str) -> Path:
    _require_observation_period(observation_period)
    return Path(station_root) / "current" / f"observation_period={observation_period}.json"


def parse_observation_period_from_filename(filename: str) -> tuple[str, str]:
    match = _FILENAME_DAY.search(filename)
    if match:
        year, month, day = (int(part) for part in match.groups())
        value = date(2000 + year, month, day)
        return value.isoformat(), PRECISION_DAY
    match = _FILENAME_MONTH.search(filename)
    if match:
        year, month = (int(part) for part in match.groups())
        value = date(2000 + year, month, 1)
        return value.strftime("%Y-%m"), PRECISION_MONTH
    raise StationSnapshotError(
        f"station snapshot filename does not contain a supported observation period: {filename!r}"
    )


def read_and_consolidate_station_snapshot(
    *,
    manifest: str | Path,
    raw_root: str | Path,
    source_version_id: str,
    on_observation: Callable[[StationObservation], None] | None = None,
    on_quarantined_row: Callable[[StationSourceRow], None] | None = None,
) -> StationSnapshotSummary:
    records = load_manifest(manifest)
    _, source = _find_source_record(records, source_version_id)
    _require_station_source(source)
    observation_period, precision = parse_observation_period_from_filename(
        source.published_filename
    )
    if source.logical_period != observation_period:
        raise StationSnapshotError(
            f"station source logical_period {source.logical_period!r} does not match "
            f"published filename observation period {observation_period!r}"
        )
    raw_path = _verify_station_raw(source, raw_root)
    raw_rows = list(_read_source_rows(source, raw_path))
    if not raw_rows:
        raise StationSnapshotError("station snapshot contains zero data rows")
    rows = [
        _canonicalize_source_row(source, row_no, values, source_format)
        for row_no, values, source_format in raw_rows
    ]
    groups: dict[str, list[StationSourceRow]] = defaultdict(list)
    quarantined: list[StationSourceRow] = []
    for row in rows:
        if row.station_no is None or row.row_quarantined:
            quarantined.append(row)
        else:
            groups[row.station_no].append(row)
    observations: list[StationObservation] = []
    conflict_groups = 0
    exact_groups = 0
    exact_source_rows = 0
    for station_no, group in sorted(groups.items(), key=lambda item: (len(item[0]), item[0])):
        observation, group_quarantine, duplicate_count = _consolidate_group(
            source, observation_period, precision, station_no, group
        )
        if duplicate_count:
            exact_groups += 1
            exact_source_rows += duplicate_count
        if group_quarantine:
            conflict_groups += 1
            quarantined.extend(group_quarantine)
        elif observation is not None:
            observations.append(observation)
    quarantined_ids = {row.source_row_id for row in quarantined}
    observation_source_rows = sum(obs.source_row_count for obs in observations)
    if len(quarantined_ids) + observation_source_rows != len(rows):
        raise StationSnapshotError(
            "station snapshot reconciliation failed: source rows are not exactly "
            "observation contributors plus quarantined rows"
        )
    if on_observation is not None:
        for observation in observations:
            on_observation(observation)
    if on_quarantined_row is not None:
        for row in sorted(quarantined, key=lambda item: item.source_row_number):
            on_quarantined_row(row)
    return StationSnapshotSummary(
        source_version_id=source.source_version_id,
        observation_period=observation_period,
        observation_precision=precision,
        source_row_count=len(rows),
        observation_count=len(observations),
        observation_source_row_count=observation_source_rows,
        quarantined_source_row_count=len(quarantined_ids),
        conflict_group_count=conflict_groups,
        exact_duplicate_group_count=exact_groups,
        exact_duplicate_source_row_count=exact_source_rows,
        warning_observation_count=sum(bool(obs.dq_issues) for obs in observations),
    )


def publish_station_snapshot(
    *,
    manifest: str | Path,
    raw_root: str | Path,
    station_root: str | Path,
    source_version_id: str,
    transformation_version: str,
    configuration: Mapping[str, object],
    before_current_publish: Callable[[Path], None] | None = None,
) -> StationPublishResult:
    records = load_manifest(manifest)
    source_index, source = _find_source_record(records, source_version_id)
    _require_station_source(source)
    observation_period, _ = parse_observation_period_from_filename(source.published_filename)
    if source.logical_period != observation_period:
        raise StationSnapshotError("station source logical_period does not match published filename")
    canonical_configuration = _canonical_configuration_object(configuration)
    config_fingerprint = compute_configuration_fingerprint(canonical_configuration)
    station_version_id = compute_station_version_id(
        observation_period=observation_period,
        source_version_id=source_version_id,
        transformation_version=transformation_version,
        configuration_fingerprint=config_fingerprint,
    )
    root = Path(station_root)
    version_path = station_version_path(root, observation_period, station_version_id)
    current_path = station_current_pointer_path(root, observation_period)
    initial_pointer = _load_current_pointer(current_path, observation_period)
    initial_current_source_index: int | None = None
    if initial_pointer is not None:
        current_version_path = station_version_path(
            root, observation_period, initial_pointer["station_version_id"]
        )
        current_metadata = validate_station_version(
            current_version_path, manifest=manifest, raw_root=raw_root
        )
        _require_bundle_binding(
            current_metadata, observation_period, initial_pointer["station_version_id"]
        )
        initial_current_source_index, current_source = _find_source_record(
            records, current_metadata["source_version_id"]
        )
        _require_metadata_source(current_metadata, current_source)
        if current_source.dataset_id != source.dataset_id:
            raise StationSnapshotError("current station pointer belongs to a different dataset")
    staging_parent = root / "staging" / f"observation_period={observation_period}"
    staging_parent.mkdir(parents=True, exist_ok=True)
    stage_path = Path(
        tempfile.mkdtemp(prefix=f"station_version_id={station_version_id}-", dir=staging_parent)
    )
    summary = _build_stage(
        stage_path=stage_path,
        manifest=manifest,
        raw_root=raw_root,
        source_version_id=source_version_id,
    )
    metadata = _build_metadata(
        stage_path=stage_path,
        source=source,
        station_version_id=station_version_id,
        transformation_version=transformation_version,
        configuration=canonical_configuration,
        configuration_fingerprint=config_fingerprint,
        summary=summary,
    )
    _write_json(stage_path / "metadata.json", metadata)
    validated = validate_station_version(stage_path, manifest=manifest, raw_root=raw_root)
    _require_bundle_binding(validated, observation_period, station_version_id)
    _require_metadata_source(validated, source)
    version_reused = False
    if version_path.exists():
        existing = validate_station_version(
            version_path, manifest=manifest, raw_root=raw_root
        )
        _require_bundle_binding(existing, observation_period, station_version_id)
        _require_same_immutable(existing, validated)
        version_reused = True
        shutil.rmtree(stage_path)
    else:
        version_path.parent.mkdir(parents=True, exist_ok=True)
        try:
            os.replace(stage_path, version_path)
        except OSError:
            if not version_path.exists():
                raise
            existing = validate_station_version(
                version_path, manifest=manifest, raw_root=raw_root
            )
            _require_bundle_binding(existing, observation_period, station_version_id)
            _require_same_immutable(existing, validated)
            version_reused = True
            shutil.rmtree(stage_path)
    pointer = _load_current_pointer(current_path, observation_period)
    if pointer != initial_pointer:
        raise StationSnapshotError(
            "current station pointer changed during build; refusing concurrent overwrite"
        )
    if pointer is not None and pointer["station_version_id"] == station_version_id:
        return _result(STATUS_REUSED_CURRENT, validated, version_path, current_path)
    if (
        pointer is not None
        and initial_current_source_index is not None
        and initial_current_source_index > source_index
    ):
        return _result(
            STATUS_REUSED_HISTORICAL if version_reused else STATUS_BUILT_HISTORICAL,
            validated,
            version_path,
            current_path,
        )
    if before_current_publish is not None:
        before_current_publish(version_path)
    with station_current_set_lock(root):
        with _publish_lock(root, observation_period, station_version_id):
            latest = _load_current_pointer(current_path, observation_period)
            if latest != initial_pointer:
                raise StationSnapshotError(
                    "current station pointer changed before atomic publish; refusing concurrent rollback"
                )
            final_metadata = validate_station_version(
                version_path, manifest=manifest, raw_root=raw_root
            )
            _require_bundle_binding(final_metadata, observation_period, station_version_id)
            _require_metadata_source(final_metadata, source)
            _require_same_immutable(final_metadata, validated)
            _write_current_pointer(current_path, observation_period, station_version_id)
    return _result(STATUS_PUBLISHED_CURRENT, validated, version_path, current_path)


def validate_station_version(
    version_path: str | Path,
    *,
    manifest: str | Path,
    raw_root: str | Path,
) -> dict[str, object]:
    path = Path(version_path)
    metadata = _load_metadata(path / "metadata.json")
    _validate_metadata_identity(metadata)
    records = load_manifest(manifest)
    _, source = _find_source_record(records, metadata["source_version_id"])
    _require_station_source(source)
    _require_metadata_source(metadata, source)
    if metadata["source_schema_version"] != STATION_SCHEMA_VERSION:
        raise StationSnapshotError(
            "station metadata source_schema_version does not match parser contract"
        )
    _require_file_fingerprint(
        path / "observations.parquet", metadata["observations_parquet"], "observations.parquet"
    )
    _require_file_fingerprint(
        path / "quarantine.parquet", metadata["quarantine_parquet"], "quarantine.parquet"
    )
    connection = duckdb.connect()
    try:
        obs = _sql_path(path / "observations.parquet")
        qua = _sql_path(path / "quarantine.parquet")
        _require_schema(connection, obs, _OBS_SCHEMA, "observations.parquet")
        _require_schema(connection, qua, _QUARANTINE_SCHEMA, "quarantine.parquet")
        period = metadata["observation_period"]
        source_version_id = metadata["source_version_id"]
        precision = metadata["observation_precision"]
        bad_identity = connection.execute(
            f"""SELECT COUNT(*) FROM read_parquet('{obs}', hive_partitioning=false)
                WHERE source_version_id IS DISTINCT FROM ?
                   OR observation_period IS DISTINCT FROM ?
                   OR observation_precision IS DISTINCT FROM ?""",
            [source_version_id, period, precision],
        ).fetchone()[0]
        if bad_identity:
            raise StationSnapshotError("observations.parquet identity does not match metadata")
        duplicate_station = connection.execute(
            f"SELECT COUNT(*) - COUNT(DISTINCT station_no) FROM read_parquet('{obs}', hive_partitioning=false)"
        ).fetchone()[0]
        if duplicate_station:
            raise StationSnapshotError("observations.parquet contains duplicate station_no")
        malformed_observation = connection.execute(
            f"""SELECT COUNT(*) FROM read_parquet('{obs}', hive_partitioning=false)
                WHERE station_no IS NULL OR station_no = ''
                   OR observation_precision NOT IN ('DAY','MONTH')
                   OR operation_mode NOT IN ('LCD','QR','LCD+QR') AND operation_mode IS NOT NULL
                   OR source_row_count <= 0
                   OR exact_duplicate_count < 0
                   OR array_length(source_row_ids) <> source_row_count
                   OR array_length(source_row_numbers) <> source_row_count
                   OR array_length(list_distinct(source_row_ids)) <> source_row_count
                   OR array_length(list_distinct(source_row_numbers)) <> source_row_count"""
        ).fetchone()[0]
        if malformed_observation:
            raise StationSnapshotError(
                "observations.parquet contains malformed station observation provenance"
            )
        malformed_observation_issue = connection.execute(
            f"""SELECT COUNT(*)
                FROM read_parquet('{obs}', hive_partitioning=false) AS o,
                     UNNEST(o.dq_issues) AS issues(issue)
                WHERE issue.rule_id IS NULL OR issue.rule_id = ''
                   OR issue.severity IS DISTINCT FROM 'WARNING'
                   OR issue.field IS NULL OR issue.field = ''
                   OR issue.reason IS NULL OR issue.reason = ''"""
        ).fetchone()[0]
        if malformed_observation_issue:
            raise StationSnapshotError(
                "observations.parquet contains malformed or non-WARNING DQ issue"
            )
        warning_count_mismatch = connection.execute(
            f"""WITH exploded AS (
                    SELECT o.station_no, issue.rule_id
                    FROM read_parquet('{obs}', hive_partitioning=false) AS o,
                         UNNEST(o.dq_issues) AS issues(issue)
                    WHERE issue.severity = 'WARNING'
                ),
                counts AS (
                    SELECT station_no, COUNT(DISTINCT rule_id) AS warning_count
                    FROM exploded GROUP BY station_no
                )
                SELECT COUNT(*)
                FROM read_parquet('{obs}', hive_partitioning=false) AS o
                LEFT JOIN counts USING (station_no)
                WHERE o.dq_warning_count <> COALESCE(counts.warning_count, 0)"""
        ).fetchone()[0]
        if warning_count_mismatch:
            raise StationSnapshotError(
                "observations.parquet dq_warning_count does not match nested WARNING rules"
            )
        duplicate_semantic_mismatch = connection.execute(
            f"""SELECT COUNT(*)
                FROM read_parquet('{obs}', hive_partitioning=false) AS o
                WHERE (o.exact_duplicate_count > 0) IS DISTINCT FROM EXISTS (
                    SELECT 1 FROM UNNEST(o.dq_issues) AS issues(issue)
                    WHERE issue.rule_id = '{RULE_EXACT_DUPLICATE}'
                      AND issue.severity = 'WARNING'
                )"""
        ).fetchone()[0]
        if duplicate_semantic_mismatch:
            raise StationSnapshotError(
                "observations.parquet exact_duplicate_count disagrees with duplicate WARNING"
            )
        obs_count, obs_source_rows, warning_obs = connection.execute(
            f"""SELECT COUNT(*), COALESCE(SUM(source_row_count),0),
                       COUNT(*) FILTER (WHERE dq_warning_count > 0)
                FROM read_parquet('{obs}', hive_partitioning=false)"""
        ).fetchone()
        quarantined_rows, quarantine_issue_count = connection.execute(
            f"SELECT COUNT(DISTINCT source_row_id), COUNT(*) FROM read_parquet('{qua}', hive_partitioning=false)"
        ).fetchone()
        malformed_quarantine = connection.execute(
            f"""SELECT COUNT(*) FROM read_parquet('{qua}', hive_partitioning=false)
                WHERE source_row_id IS NULL OR source_row_number IS NULL
                   OR source_version_id IS DISTINCT FROM ?
                   OR observation_period IS DISTINCT FROM ?
                   OR rule_id IS NULL OR rule_id = ''
                   OR severity IS NULL OR severity NOT IN ('WARNING','QUARANTINE')
                   OR field IS NULL OR field = '' OR reason IS NULL OR reason = ''""",
            [source_version_id, period],
        ).fetchone()[0]
        if malformed_quarantine:
            raise StationSnapshotError("quarantine.parquet contains malformed issue rows")
        quarantine_without_quarantine_issue = connection.execute(
            f"""SELECT COUNT(*) FROM (
                    SELECT source_row_id
                    FROM read_parquet('{qua}', hive_partitioning=false)
                    GROUP BY source_row_id
                    HAVING COUNT(*) FILTER (WHERE severity = 'QUARANTINE') = 0
                )"""
        ).fetchone()[0]
        if quarantine_without_quarantine_issue:
            raise StationSnapshotError(
                "quarantine.parquet contains source row without a QUARANTINE issue"
            )
        contributor_total, contributor_distinct = connection.execute(
            f"""WITH observation_ids AS (
                    SELECT UNNEST(source_row_ids) AS source_row_id
                    FROM read_parquet('{obs}', hive_partitioning=false)
                ),
                quarantine_ids AS (
                    SELECT DISTINCT source_row_id
                    FROM read_parquet('{qua}', hive_partitioning=false)
                ),
                all_ids AS (
                    SELECT source_row_id FROM observation_ids
                    UNION ALL
                    SELECT source_row_id FROM quarantine_ids
                )
                SELECT COUNT(*), COUNT(DISTINCT source_row_id) FROM all_ids"""
        ).fetchone()
        row_number_total, row_number_distinct = connection.execute(
            f"""WITH observation_rows AS (
                    SELECT UNNEST(source_row_numbers) AS source_row_number
                    FROM read_parquet('{obs}', hive_partitioning=false)
                ),
                quarantine_rows AS (
                    SELECT DISTINCT source_row_number
                    FROM read_parquet('{qua}', hive_partitioning=false)
                ),
                all_rows AS (
                    SELECT source_row_number FROM observation_rows
                    UNION ALL
                    SELECT source_row_number FROM quarantine_rows
                )
                SELECT COUNT(*), COUNT(DISTINCT source_row_number) FROM all_rows"""
        ).fetchone()
        conflict_group_count = connection.execute(
            f"""SELECT COUNT(DISTINCT station_group_ref)
                FROM read_parquet('{qua}', hive_partitioning=false)
                WHERE rule_id IN ('{RULE_IDENTITY_CONFLICT}','{RULE_CAPACITY_CONFLICT}')"""
        ).fetchone()[0]
        observation_exact_group_count, observation_exact_source_row_count = connection.execute(
            f"""SELECT COUNT(*) FILTER (WHERE exact_duplicate_count > 0),
                       COALESCE(SUM(exact_duplicate_count), 0)
                FROM read_parquet('{obs}', hive_partitioning=false)"""
        ).fetchone()
        quarantine_exact_group_count, quarantine_exact_source_row_count = connection.execute(
            f"""SELECT COUNT(DISTINCT station_group_ref),
                       COUNT(DISTINCT source_row_id)
                FROM read_parquet('{qua}', hive_partitioning=false)
                WHERE rule_id = '{RULE_EXACT_DUPLICATE}'
                  AND severity = 'WARNING'"""
        ).fetchone()
        exact_group_count = (
            int(observation_exact_group_count) + int(quarantine_exact_group_count)
        )
        exact_source_row_count = (
            int(observation_exact_source_row_count)
            + int(quarantine_exact_source_row_count)
        )
    except duckdb.Error as exc:
        raise StationSnapshotError(f"cannot validate station Parquet: {exc}") from exc
    finally:
        connection.close()
    expected = {
        "observation_count": int(obs_count),
        "observation_source_row_count": int(obs_source_rows),
        "quarantined_source_row_count": int(quarantined_rows),
        "quarantine_issue_count": int(quarantine_issue_count),
        "warning_observation_count": int(warning_obs),
        "conflict_group_count": int(conflict_group_count),
        "exact_duplicate_group_count": int(exact_group_count),
        "exact_duplicate_source_row_count": int(exact_source_row_count),
    }
    for key, value in expected.items():
        if metadata[key] != value:
            raise StationSnapshotError(
                f"metadata {key}={metadata[key]!r} does not match Parquet value {value!r}"
            )
    if metadata["source_row_count"] != int(obs_source_rows) + int(quarantined_rows):
        raise StationSnapshotError("station source row reconciliation failed in durable bundle")
    if (
        contributor_total != contributor_distinct
        or contributor_total != metadata["source_row_count"]
        or row_number_total != row_number_distinct
        or row_number_total != metadata["source_row_count"]
    ):
        raise StationSnapshotError(
            "station durable provenance does not form an exact one-to-one source-row partition"
        )
    _require_durable_matches_recomputed_source(
        path=path,
        metadata=metadata,
        manifest=manifest,
        raw_root=raw_root,
    )
    return metadata


def _build_stage(
    *,
    stage_path: Path,
    manifest: str | Path,
    raw_root: str | Path,
    source_version_id: str,
) -> StationSnapshotSummary:
    observations: list[StationObservation] = []
    quarantined: list[StationSourceRow] = []
    summary = read_and_consolidate_station_snapshot(
        manifest=manifest,
        raw_root=raw_root,
        source_version_id=source_version_id,
        on_observation=observations.append,
        on_quarantined_row=quarantined.append,
    )
    obs_csv = stage_path / "observations.json.csv"
    qua_csv = stage_path / "quarantine.json.csv"
    with obs_csv.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle, lineterminator="\n")
        for observation in observations:
            payload = asdict(observation)
            payload["latitude"] = str(observation.latitude) if observation.latitude is not None else None
            payload["longitude"] = str(observation.longitude) if observation.longitude is not None else None
            payload["dq_issues"] = [asdict(issue) for issue in observation.dq_issues]
            writer.writerow((_canonical_json_text(payload),))
    with qua_csv.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle, lineterminator="\n")
        for row in quarantined:
            for issue in row.dq_issues:
                writer.writerow(
                    (
                        _canonical_json_text(
                            {
                                "source_row_id": row.source_row_id,
                                "source_version_id": row.source_version_id,
                                "source_row_number": row.source_row_number,
                                "observation_period": summary.observation_period,
                                "station_no_raw": row.station_no_raw,
                                "station_no": row.station_no,
                                "station_group_ref": row.station_no,
                                **asdict(issue),
                            }
                        ),
                    )
                )
    db = stage_path / "station-build.duckdb"
    connection = duckdb.connect(str(db))
    try:
        connection.execute("CREATE TABLE obs_json(payload VARCHAR NOT NULL)")
        connection.execute("CREATE TABLE qua_json(payload VARCHAR NOT NULL)")
        connection.execute(f"COPY obs_json FROM '{_sql_path(obs_csv)}' (FORMAT CSV, HEADER FALSE)")
        connection.execute(f"COPY qua_json FROM '{_sql_path(qua_csv)}' (FORMAT CSV, HEADER FALSE)")
        issue_schema = _ISSUE_JSON_SCHEMA.replace("'", "''")
        connection.execute(
            f"""CREATE TABLE observations AS SELECT
                json_extract_string(payload,'$.source_version_id')::VARCHAR source_version_id,
                json_extract_string(payload,'$.observation_period')::VARCHAR observation_period,
                json_extract_string(payload,'$.observation_precision')::VARCHAR observation_precision,
                json_extract_string(payload,'$.station_no')::VARCHAR station_no,
                json_extract_string(payload,'$.station_name')::VARCHAR station_name,
                json_extract_string(payload,'$.district')::VARCHAR district,
                json_extract_string(payload,'$.address')::VARCHAR address,
                json_extract_string(payload,'$.latitude')::DECIMAL(18,8) latitude,
                json_extract_string(payload,'$.longitude')::DECIMAL(18,8) longitude,
                json_extract_string(payload,'$.lcd_capacity')::INTEGER lcd_capacity,
                json_extract_string(payload,'$.qr_capacity')::INTEGER qr_capacity,
                json_extract_string(payload,'$.operation_mode')::VARCHAR operation_mode,
                from_json(json_extract(payload,'$.source_row_ids'),'["VARCHAR"]') source_row_ids,
                from_json(json_extract(payload,'$.source_row_numbers'),'["BIGINT"]') source_row_numbers,
                json_extract_string(payload,'$.source_row_count')::INTEGER source_row_count,
                json_extract_string(payload,'$.exact_duplicate_count')::INTEGER exact_duplicate_count,
                from_json(json_extract(payload,'$.installation_values'),'["VARCHAR"]') installation_values,
                from_json(json_extract(payload,'$.dq_issues'),'{issue_schema}') dq_issues,
                json_extract_string(payload,'$.dq_warning_count')::INTEGER dq_warning_count
                FROM obs_json"""
        )
        connection.execute(
            """CREATE TABLE quarantine AS SELECT
                json_extract_string(payload,'$.source_row_id')::VARCHAR source_row_id,
                json_extract_string(payload,'$.source_version_id')::VARCHAR source_version_id,
                json_extract_string(payload,'$.source_row_number')::BIGINT source_row_number,
                json_extract_string(payload,'$.observation_period')::VARCHAR observation_period,
                json_extract_string(payload,'$.station_no_raw')::VARCHAR station_no_raw,
                json_extract_string(payload,'$.station_no')::VARCHAR station_no,
                json_extract_string(payload,'$.station_group_ref')::VARCHAR station_group_ref,
                json_extract_string(payload,'$.rule_id')::VARCHAR rule_id,
                json_extract_string(payload,'$.severity')::VARCHAR severity,
                json_extract_string(payload,'$.field')::VARCHAR field,
                json_extract_string(payload,'$.reason')::VARCHAR reason
                FROM qua_json"""
        )
        connection.execute(
            f"""COPY (SELECT * FROM observations ORDER BY length(station_no), station_no)
                TO '{_sql_path(stage_path / "observations.parquet")}'
                (FORMAT PARQUET, COMPRESSION ZSTD)"""
        )
        connection.execute(
            f"""COPY (SELECT * FROM quarantine
                ORDER BY source_row_id, rule_id, severity, field, reason)
                TO '{_sql_path(stage_path / "quarantine.parquet")}'
                (FORMAT PARQUET, COMPRESSION ZSTD)"""
        )
    except duckdb.Error as exc:
        raise StationSnapshotError(f"station Parquet staging failed: {exc}") from exc
    finally:
        connection.close()
    obs_csv.unlink(missing_ok=True)
    qua_csv.unlink(missing_ok=True)
    db.unlink(missing_ok=True)
    return summary


def _require_durable_matches_recomputed_source(
    *,
    path: Path,
    metadata: dict[str, object],
    manifest: str | Path,
    raw_root: str | Path,
) -> None:
    expected_observations: list[StationObservation] = []
    expected_quarantine_rows: list[StationSourceRow] = []
    summary = read_and_consolidate_station_snapshot(
        manifest=manifest,
        raw_root=raw_root,
        source_version_id=metadata["source_version_id"],
        on_observation=expected_observations.append,
        on_quarantined_row=expected_quarantine_rows.append,
    )
    if (
        summary.source_row_count != metadata["source_row_count"]
        or summary.observation_count != metadata["observation_count"]
        or summary.quarantined_source_row_count != metadata["quarantined_source_row_count"]
    ):
        raise StationSnapshotError(
            "station metadata counts do not match recomputed immutable Raw source"
        )

    connection = duckdb.connect()
    try:
        durable_observations = connection.execute(
            f"""SELECT *
                FROM read_parquet('{_sql_path(path / "observations.parquet")}', hive_partitioning=false)
                ORDER BY length(station_no), station_no"""
        ).fetchall()
        durable_quarantine = connection.execute(
            f"""SELECT *
                FROM read_parquet('{_sql_path(path / "quarantine.parquet")}', hive_partitioning=false)"""
        ).fetchall()
    except duckdb.Error as exc:
        raise StationSnapshotError(
            f"cannot compare station bundle with immutable Raw source: {exc}"
        ) from exc
    finally:
        connection.close()

    expected_observation_signatures = [
        _expected_observation_signature(observation)
        for observation in expected_observations
    ]
    durable_observation_signatures = [
        _durable_observation_signature(row) for row in durable_observations
    ]
    if durable_observation_signatures != expected_observation_signatures:
        raise StationSnapshotError(
            "observations.parquet does not exactly match recomputed immutable Raw source"
        )

    expected_quarantine = Counter()
    for row in expected_quarantine_rows:
        for issue in row.dq_issues:
            expected_quarantine[
                (
                    row.source_row_id,
                    row.source_version_id,
                    row.source_row_number,
                    summary.observation_period,
                    row.station_no_raw,
                    row.station_no,
                    row.station_no,
                    issue.rule_id,
                    issue.severity,
                    issue.field,
                    issue.reason,
                )
            ] += 1
    durable_quarantine_counter = Counter(tuple(row) for row in durable_quarantine)
    if durable_quarantine_counter != expected_quarantine:
        raise StationSnapshotError(
            "quarantine.parquet does not exactly match recomputed immutable Raw source"
        )


def _expected_observation_signature(observation: StationObservation) -> tuple[object, ...]:
    return (
        observation.source_version_id,
        observation.observation_period,
        observation.observation_precision,
        observation.station_no,
        observation.station_name,
        observation.district,
        observation.address,
        observation.latitude,
        observation.longitude,
        observation.lcd_capacity,
        observation.qr_capacity,
        observation.operation_mode,
        list(observation.source_row_ids),
        list(observation.source_row_numbers),
        observation.source_row_count,
        observation.exact_duplicate_count,
        list(observation.installation_values),
        [
            {
                "rule_id": issue.rule_id,
                "severity": issue.severity,
                "field": issue.field,
                "reason": issue.reason,
            }
            for issue in observation.dq_issues
        ],
        observation.dq_warning_count,
    )


def _durable_observation_signature(row: tuple[object, ...]) -> tuple[object, ...]:
    durable = list(row)
    durable[12] = list(durable[12])
    durable[13] = list(durable[13])
    durable[16] = list(durable[16])
    durable[17] = [
        {
            "rule_id": issue["rule_id"],
            "severity": issue["severity"],
            "field": issue["field"],
            "reason": issue["reason"],
        }
        for issue in durable[17]
    ]
    return tuple(durable)


def _build_metadata(
    *,
    stage_path: Path,
    source: SourceRecord,
    station_version_id: str,
    transformation_version: str,
    configuration: dict[str, object],
    configuration_fingerprint: str,
    summary: StationSnapshotSummary,
) -> dict[str, object]:
    connection = duckdb.connect()
    try:
        quarantine_issue_count = connection.execute(
            f"SELECT COUNT(*) FROM read_parquet('{_sql_path(stage_path / 'quarantine.parquet')}', hive_partitioning=false)"
        ).fetchone()[0]
    finally:
        connection.close()
    obs_size, obs_sha = _fingerprint_file(stage_path / "observations.parquet")
    qua_size, qua_sha = _fingerprint_file(stage_path / "quarantine.parquet")
    return {
        "schema_version": METADATA_SCHEMA_VERSION,
        "observation_period": summary.observation_period,
        "observation_precision": summary.observation_precision,
        "source_version_id": source.source_version_id,
        "source_artifact_sha256": source.sha256,
        "source_artifact_size_bytes": source.size_bytes,
        "station_version_id": station_version_id,
        "transformation_version": transformation_version,
        "configuration": configuration,
        "configuration_fingerprint": configuration_fingerprint,
        "source_row_count": summary.source_row_count,
        "observation_count": summary.observation_count,
        "observation_source_row_count": summary.observation_source_row_count,
        "quarantined_source_row_count": summary.quarantined_source_row_count,
        "quarantine_issue_count": int(quarantine_issue_count),
        "conflict_group_count": summary.conflict_group_count,
        "exact_duplicate_group_count": summary.exact_duplicate_group_count,
        "exact_duplicate_source_row_count": summary.exact_duplicate_source_row_count,
        "warning_observation_count": summary.warning_observation_count,
        "source_schema_version": STATION_SCHEMA_VERSION,
        "observations_parquet": {"size_bytes": obs_size, "sha256": obs_sha},
        "quarantine_parquet": {"size_bytes": qua_size, "sha256": qua_sha},
    }


def _read_source_rows(
    source: SourceRecord, raw_path: Path
) -> Iterator[tuple[int, tuple[str, ...], str]]:
    suffix = Path(source.published_filename).suffix.casefold()
    if suffix == ".csv":
        yield from _read_csv_rows(raw_path)
    elif suffix == ".xlsx":
        yield from _read_xlsx_rows(raw_path)
    else:
        raise StationSnapshotError(
            f"unsupported station snapshot extension for {source.published_filename!r}"
        )


def _read_csv_rows(path: Path) -> Iterator[tuple[int, tuple[str, ...], str]]:
    try:
        data = path.read_bytes()
    except OSError as exc:
        raise StationSnapshotError(f"cannot read station CSV {path}: {exc}") from exc
    text = None
    for encoding in ("utf-8-sig", "cp949"):
        try:
            text = data.decode(encoding)
            break
        except UnicodeDecodeError:
            continue
    if text is None:
        raise StationSnapshotError("station CSV is neither UTF-8 nor CP949")
    try:
        rows = list(csv.reader(io.StringIO(text, newline=""), strict=True))
    except csv.Error as exc:
        raise StationSnapshotError(f"structural station CSV parse failure: {exc}") from exc
    yield from _rows_after_header(rows, "CSV")


def _read_xlsx_rows(path: Path) -> Iterator[tuple[int, tuple[str, ...], str]]:
    try:
        archive = zipfile.ZipFile(path, "r")
    except (zipfile.BadZipFile, OSError) as exc:
        raise StationSnapshotError(f"cannot open station XLSX {path}: {exc}") from exc
    with archive:
        names = set(archive.namelist())
        if "xl/workbook.xml" not in names or "xl/_rels/workbook.xml.rels" not in names:
            raise StationSnapshotError("station XLSX is missing workbook metadata")
        shared: list[str] = []
        if "xl/sharedStrings.xml" in names:
            try:
                shared_root = ET.fromstring(archive.read("xl/sharedStrings.xml"))
            except ET.ParseError as exc:
                raise StationSnapshotError("station XLSX sharedStrings XML is malformed") from exc
            for item in shared_root.findall("m:si", _NS):
                shared.append("".join(node.text or "" for node in item.findall(".//m:t", _NS)))
        try:
            workbook = ET.fromstring(archive.read("xl/workbook.xml"))
        except ET.ParseError as exc:
            raise StationSnapshotError("station XLSX workbook XML is malformed") from exc
        sheets = workbook.findall(".//m:sheets/m:sheet", _NS)
        if not sheets:
            raise StationSnapshotError("station XLSX has no worksheet")
        relationship_id = sheets[0].get(
            "{http://schemas.openxmlformats.org/officeDocument/2006/relationships}id"
        )
        try:
            rels = ET.fromstring(archive.read("xl/_rels/workbook.xml.rels"))
        except ET.ParseError as exc:
            raise StationSnapshotError("station XLSX relationship XML is malformed") from exc
        target = next(
            (rel.get("Target") for rel in rels if rel.get("Id") == relationship_id),
            None,
        )
        if not target:
            raise StationSnapshotError("cannot resolve first station XLSX worksheet")
        sheet_path = target.lstrip("/")
        if not sheet_path.startswith("xl/"):
            sheet_path = "xl/" + sheet_path
        sheet_path = str(Path(sheet_path).as_posix())
        if sheet_path not in names:
            raise StationSnapshotError(f"station XLSX worksheet {sheet_path!r} is missing")
        try:
            root = ET.fromstring(archive.read(sheet_path))
        except ET.ParseError as exc:
            raise StationSnapshotError("station XLSX worksheet XML is malformed") from exc
        rows: list[list[str]] = []
        seen_sheet_rows: set[int] = set()
        for row in root.findall(".//m:sheetData/m:row", _NS):
            row_ref = row.get("r")
            if row_ref is None or not row_ref.isdigit() or int(row_ref) < 1:
                raise StationSnapshotError("station XLSX row has invalid or missing row coordinate")
            row_number = int(row_ref)
            if row_number in seen_sheet_rows:
                raise StationSnapshotError(
                    f"station XLSX contains duplicate worksheet row {row_number}"
                )
            seen_sheet_rows.add(row_number)
            if rows and row_number <= len(rows):
                raise StationSnapshotError(
                    "station XLSX worksheet rows are not in strictly increasing order"
                )
            while len(rows) < row_number - 1:
                rows.append([""] * 10)
            values = [""] * 10
            seen_columns: set[int] = set()
            for cell in row.findall("m:c", _NS):
                ref = cell.get("r", "")
                column, cell_row = _xlsx_cell_coordinates(ref)
                if cell_row != row_number:
                    raise StationSnapshotError(
                        f"station XLSX cell {ref!r} does not belong to worksheet row {row_number}"
                    )
                if column in seen_columns:
                    raise StationSnapshotError(
                        f"station XLSX contains duplicate cell/column reference {ref!r}"
                    )
                seen_columns.add(column)
                if cell.find("m:f", _NS) is not None:
                    raise StationSnapshotError(f"station XLSX contains formula cell {ref}")
                value = _xlsx_cell_text(cell, shared)
                if column >= 10:
                    if value.strip():
                        raise StationSnapshotError(
                            f"station XLSX contains non-empty column beyond J at {ref}"
                        )
                else:
                    values[column] = value
            rows.append(values)
        yield from _rows_after_header(rows, "XLSX")


def _rows_after_header(
    rows: list[list[str]] | list[tuple[str, ...]], source_format: str
) -> Iterator[tuple[int, tuple[str, ...], str]]:
    if len(rows) < 5:
        raise StationSnapshotError("station source is shorter than the five-row header")
    for index, expected in enumerate(_HEADER):
        actual = tuple(_normalize_header(value) for value in rows[index])
        if len(actual) != 10 or actual != expected:
            raise StationSnapshotError(
                f"unsupported station header row {index + 1}: expected {expected!r}, got {actual!r}"
            )
    for source_row_number, row in enumerate(rows[5:], 1):
        if len(row) != 10:
            raise StationSnapshotError(
                f"station data row {source_row_number} has {len(row)} columns; expected 10"
            )
        values = tuple("" if value is None else str(value) for value in row)
        if not any(value.strip() for value in values):
            continue
        yield source_row_number, values, source_format


def _canonicalize_source_row(
    source: SourceRecord,
    source_row_number: int,
    values: tuple[str, ...],
    source_format: str,
) -> StationSourceRow:
    issues: list[DqIssue] = []
    station_no_raw = _none_if_blank(values[0])
    station_no = _canonical_station_no(station_no_raw)
    if station_no is None:
        issues.append(
            DqIssue(
                RULE_STATION_NO_INVALID,
                SEVERITY_QUARANTINE,
                "station_no",
                "station number is missing or is not ASCII decimal digits",
            )
        )
    station_name = _text(values[1], "station_name", issues)
    district = _text(values[2], "district", issues)
    address = _text(values[3], "address", issues)
    latitude = _decimal_field(values[4], "latitude", -90, 90, issues)
    longitude = _decimal_field(values[5], "longitude", -180, 180, issues)
    installation_value = _installation_value(values[6], source_format)
    lcd_capacity = _capacity(values[7], "lcd_capacity", issues)
    qr_capacity = _capacity(values[8], "qr_capacity", issues)
    operation_mode_raw = _none_if_blank(values[9])
    operation_mode = operation_mode_raw.strip().upper() if operation_mode_raw else None
    if operation_mode not in (None, "LCD", "QR"):
        issues.append(
            DqIssue(
                RULE_OPERATION_MODE_INVALID,
                SEVERITY_WARNING,
                "operation_mode",
                f"unrecognized operation mode {operation_mode_raw!r}",
            )
        )
        operation_mode = None
    if operation_mode == "LCD" and qr_capacity is not None:
        issues.append(
            DqIssue(
                RULE_MODE_CAPACITY_MISMATCH,
                SEVERITY_WARNING,
                "operation_mode",
                "LCD row also contains QR capacity",
            )
        )
    if operation_mode == "QR" and lcd_capacity is not None:
        issues.append(
            DqIssue(
                RULE_MODE_CAPACITY_MISMATCH,
                SEVERITY_WARNING,
                "operation_mode",
                "QR row also contains LCD capacity",
            )
        )
    source_row_id = compute_raw_row_id(
        source.source_version_id, source.published_filename, source_row_number
    )
    return StationSourceRow(
        source_row_id=source_row_id,
        source_version_id=source.source_version_id,
        source_row_number=source_row_number,
        row_content_sha256=compute_row_content_sha256(values),
        station_no_raw=station_no_raw,
        station_no=station_no,
        station_name=station_name,
        district=district,
        address=address,
        latitude=latitude,
        longitude=longitude,
        installation_value=installation_value,
        lcd_capacity=lcd_capacity,
        qr_capacity=qr_capacity,
        operation_mode_raw=operation_mode_raw,
        operation_mode=operation_mode,
        dq_issues=tuple(issues),
        row_quarantined=any(issue.severity == SEVERITY_QUARANTINE for issue in issues),
    )


def _consolidate_group(
    source: SourceRecord,
    observation_period: str,
    precision: str,
    station_no: str,
    rows: list[StationSourceRow],
) -> tuple[StationObservation | None, list[StationSourceRow], int]:
    duplicate_row_ids: set[str] = set()
    seen_content: set[str] = set()
    for row in sorted(rows, key=lambda item: item.source_row_number):
        if row.row_content_sha256 in seen_content:
            duplicate_row_ids.add(row.source_row_id)
        else:
            seen_content.add(row.row_content_sha256)
    duplicate_count = len(duplicate_row_ids)
    conflict_fields: list[str] = []
    values_by_field: dict[str, object | None] = {}
    for field in ("station_name", "district", "address", "latitude", "longitude"):
        values = {getattr(row, field) for row in rows if getattr(row, field) is not None}
        if len(values) > 1:
            conflict_fields.append(field)
        values_by_field[field] = next(iter(values)) if values else None
    for field in ("lcd_capacity", "qr_capacity"):
        values = {getattr(row, field) for row in rows if getattr(row, field) is not None}
        if len(values) > 1:
            conflict_fields.append(field)
        values_by_field[field] = next(iter(values)) if values else None
    if conflict_fields:
        rule = (
            RULE_CAPACITY_CONFLICT
            if all(field in {"lcd_capacity", "qr_capacity"} for field in conflict_fields)
            else RULE_IDENTITY_CONFLICT
        )
        quarantined: list[StationSourceRow] = []
        for row in rows:
            conflict_issue = DqIssue(
                rule,
                SEVERITY_QUARANTINE,
                ",".join(conflict_fields),
                f"same-snapshot station {station_no} has conflicting non-null values",
            )
            issues = [*row.dq_issues, conflict_issue]
            if row.source_row_id in duplicate_row_ids:
                issues.append(
                    DqIssue(
                        RULE_EXACT_DUPLICATE,
                        SEVERITY_WARNING,
                        "source_row",
                        "exact duplicate source row occurs inside a conflicting station group",
                    )
                )
            quarantined.append(
                StationSourceRow(
                    source_row_id=row.source_row_id,
                    source_version_id=row.source_version_id,
                    source_row_number=row.source_row_number,
                    row_content_sha256=row.row_content_sha256,
                    station_no_raw=row.station_no_raw,
                    station_no=row.station_no,
                    station_name=row.station_name,
                    district=row.district,
                    address=row.address,
                    latitude=row.latitude,
                    longitude=row.longitude,
                    installation_value=row.installation_value,
                    lcd_capacity=row.lcd_capacity,
                    qr_capacity=row.qr_capacity,
                    operation_mode_raw=row.operation_mode_raw,
                    operation_mode=row.operation_mode,
                    dq_issues=tuple(issues),
                    row_quarantined=True,
                )
            )
        return None, quarantined, duplicate_count
    modes = {row.operation_mode for row in rows if row.operation_mode in {"LCD", "QR"}}
    if values_by_field["lcd_capacity"] is not None:
        modes.add("LCD")
    if values_by_field["qr_capacity"] is not None:
        modes.add("QR")
    operation_mode = (
        "LCD+QR"
        if modes == {"LCD", "QR"}
        else next(iter(modes))
        if modes
        else None
    )
    warnings = [
        issue for row in rows for issue in row.dq_issues if issue.severity == SEVERITY_WARNING
    ]
    if duplicate_count:
        warnings.append(
            DqIssue(
                RULE_EXACT_DUPLICATE,
                SEVERITY_WARNING,
                "source_row",
                f"{duplicate_count} exact duplicate source row(s) collapsed",
            )
        )
    issues = _dedupe_issues(warnings)
    ordered_rows = sorted(rows, key=lambda row: row.source_row_number)
    return (
        StationObservation(
            source_version_id=source.source_version_id,
            observation_period=observation_period,
            observation_precision=precision,
            station_no=station_no,
            station_name=values_by_field["station_name"],
            district=values_by_field["district"],
            address=values_by_field["address"],
            latitude=values_by_field["latitude"],
            longitude=values_by_field["longitude"],
            lcd_capacity=values_by_field["lcd_capacity"],
            qr_capacity=values_by_field["qr_capacity"],
            operation_mode=operation_mode,
            source_row_ids=tuple(row.source_row_id for row in ordered_rows),
            source_row_numbers=tuple(row.source_row_number for row in ordered_rows),
            source_row_count=len(rows),
            exact_duplicate_count=duplicate_count,
            installation_values=tuple(
                sorted({row.installation_value for row in rows if row.installation_value})
            ),
            dq_issues=issues,
            dq_warning_count=len({issue.rule_id for issue in issues}),
        ),
        [],
        duplicate_count,
    )


def _dedupe_issues(issues: list[DqIssue]) -> tuple[DqIssue, ...]:
    unique = {
        (issue.rule_id, issue.severity, issue.field, issue.reason): issue for issue in issues
    }
    return tuple(unique[key] for key in sorted(unique))


def _text(value: str, field: str, issues: list[DqIssue]) -> str | None:
    result = _none_if_blank(value)
    if result is None:
        issues.append(DqIssue(RULE_TEXT_MISSING, SEVERITY_WARNING, field, f"{field} is missing"))
        return None
    return result.strip()


def _decimal_field(
    value: str,
    field: str,
    minimum: int,
    maximum: int,
    issues: list[DqIssue],
) -> Decimal | None:
    text = _none_if_blank(value)
    if text is None:
        issues.append(
            DqIssue(RULE_COORDINATE_INVALID, SEVERITY_WARNING, field, f"{field} is missing")
        )
        return None
    try:
        parsed = Decimal(text.strip())
    except InvalidOperation:
        parsed = None
    if parsed is None or not parsed.is_finite() or not Decimal(minimum) <= parsed <= Decimal(maximum):
        issues.append(
            DqIssue(
                RULE_COORDINATE_INVALID,
                SEVERITY_WARNING,
                field,
                f"{field} is not a finite coordinate in [{minimum},{maximum}]",
            )
        )
        return None
    return parsed.quantize(Decimal("0.00000001"))


def _capacity(value: str, field: str, issues: list[DqIssue]) -> int | None:
    text = _none_if_blank(value)
    if text is None:
        return None
    try:
        decimal = Decimal(text.strip())
        parsed = int(decimal)
    except (InvalidOperation, ValueError, OverflowError):
        decimal = None
        parsed = -1
    if decimal is None or not decimal.is_finite() or parsed < 0 or decimal != Decimal(parsed):
        issues.append(
            DqIssue(
                RULE_CAPACITY_INVALID,
                SEVERITY_WARNING,
                field,
                f"{field} must be a non-negative integer",
            )
        )
        return None
    return parsed


def _installation_value(value: str, source_format: str) -> str | None:
    text = _none_if_blank(value)
    if text is None:
        return None
    text = text.strip()
    if source_format == "XLSX":
        try:
            serial = Decimal(text)
        except InvalidOperation:
            return text
        if serial.is_finite() and serial >= 1:
            return (date(1899, 12, 30) + timedelta(days=int(serial))).isoformat()
    return text


def _canonical_station_no(value: str | None) -> str | None:
    if value is None:
        return None
    stripped = value.strip()
    if _ASCII_DIGITS.fullmatch(stripped) is None:
        return None
    return stripped.lstrip("0") or "0"


def _none_if_blank(value: str) -> str | None:
    return None if value.strip() == "" else value


def _normalize_header(value: object) -> str:
    return re.sub(r"\s+", "", str(value or "").lstrip("\ufeff"))


def _xlsx_cell_coordinates(reference: str) -> tuple[int, int]:
    match = re.fullmatch(r"([A-Z]+)([1-9][0-9]*)", reference)
    if match is None:
        raise StationSnapshotError(f"invalid XLSX cell reference {reference!r}")
    index = 0
    for character in match.group(1):
        index = index * 26 + ord(character) - ord("A") + 1
    return index - 1, int(match.group(2))


def _xlsx_cell_text(cell: ET.Element, shared: list[str]) -> str:
    cell_type = cell.get("t")
    if cell_type == "inlineStr":
        inline = cell.find("m:is", _NS)
        return "" if inline is None else "".join(
            node.text or "" for node in inline.findall(".//m:t", _NS)
        )
    value = cell.find("m:v", _NS)
    if value is None or value.text is None:
        return ""
    raw = value.text
    if cell_type == "s":
        try:
            return shared[int(raw)]
        except (ValueError, IndexError) as exc:
            raise StationSnapshotError("invalid XLSX shared-string reference") from exc
    return raw


def _require_station_source(source: SourceRecord) -> None:
    if source.dataset_id != STATION_DATASET_ID:
        raise StationSnapshotError(
            f"source version {source.source_version_id} has dataset_id {source.dataset_id!r}; "
            f"expected {STATION_DATASET_ID!r}"
        )


def _verify_station_raw(source: SourceRecord, raw_root: str | Path) -> Path:
    try:
        return verify_raw_artifact(
            raw_root=raw_root,
            source_version_id=source.source_version_id,
            expected_size=source.size_bytes,
            expected_sha256=source.sha256,
        )
    except RawArtifactError as exc:
        raise StationSnapshotError(f"station Raw artifact failed verification: {exc}") from exc


def _find_source_record(
    records: list[SourceRecord], source_version_id: str
) -> tuple[int, SourceRecord]:
    for index, record in enumerate(records):
        if record.source_version_id == source_version_id:
            return index, record
    raise StationSnapshotError(f"source_version_id {source_version_id!r} is not registered")


def _require_observation_period(value: object) -> str:
    if not isinstance(value, str):
        raise StationSnapshotError("observation_period must be a string")
    if _MONTH_PERIOD.fullmatch(value):
        return value
    if _DAY_PERIOD.fullmatch(value):
        try:
            date.fromisoformat(value)
        except ValueError as exc:
            raise StationSnapshotError(f"invalid observation day {value!r}") from exc
        return value
    raise StationSnapshotError(
        f"observation_period must be YYYY-MM or YYYY-MM-DD, got {value!r}"
    )


def _require_text(value: object, name: str) -> str:
    if not isinstance(value, str) or not value:
        raise StationSnapshotError(f"{name} must be a non-empty string")
    return value


def _canonical_configuration_object(configuration: Mapping[str, object]) -> dict[str, object]:
    if not isinstance(configuration, Mapping):
        raise StationSnapshotError("configuration must be a JSON-compatible mapping")
    try:
        encoded = _canonical_json_text(dict(configuration))
        decoded = json.loads(encoded)
    except (TypeError, ValueError) as exc:
        raise StationSnapshotError(f"configuration must be JSON-compatible: {exc}") from exc
    if not isinstance(decoded, dict):
        raise StationSnapshotError("configuration must serialize to a JSON object")
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


def _fingerprint_file(path: Path) -> tuple[int, str]:
    digest = hashlib.sha256()
    size = 0
    try:
        with path.open("rb") as handle:
            while chunk := handle.read(1024 * 1024):
                size += len(chunk)
                digest.update(chunk)
    except OSError as exc:
        raise StationSnapshotError(f"cannot fingerprint {path}: {exc}") from exc
    return size, digest.hexdigest()


def _sql_path(path: Path) -> str:
    return path.resolve().as_posix().replace("'", "''")


def _require_schema(
    connection: duckdb.DuckDBPyConnection,
    path: str,
    expected: tuple[tuple[str, str], ...],
    label: str,
) -> None:
    actual = tuple(
        (row[0], row[1])
        for row in connection.execute(
            f"DESCRIBE SELECT * FROM read_parquet('{path}', hive_partitioning=false)"
        ).fetchall()
    )
    if actual != expected:
        raise StationSnapshotError(
            f"{label} schema mismatch: expected {expected!r}, got {actual!r}"
        )


def _write_json(path: Path, payload: dict[str, object]) -> None:
    with path.open("w", encoding="utf-8", newline="\n") as handle:
        handle.write(_canonical_json_text(payload) + "\n")
        handle.flush()
        os.fsync(handle.fileno())


def _load_metadata(path: Path) -> dict[str, object]:
    try:
        text = path.read_text(encoding="utf-8")
        payload = json.loads(text, object_pairs_hook=_reject_duplicate_pairs)
    except (OSError, json.JSONDecodeError) as exc:
        raise StationSnapshotError(f"cannot read station metadata {path}: {exc}") from exc
    if not isinstance(payload, dict):
        raise StationSnapshotError("station metadata must be a JSON object")
    return payload


def _validate_metadata_identity(metadata: dict[str, object]) -> None:
    required = {
        "schema_version",
        "observation_period",
        "observation_precision",
        "source_version_id",
        "source_artifact_sha256",
        "source_artifact_size_bytes",
        "station_version_id",
        "transformation_version",
        "configuration",
        "configuration_fingerprint",
        "source_row_count",
        "observation_count",
        "observation_source_row_count",
        "quarantined_source_row_count",
        "quarantine_issue_count",
        "conflict_group_count",
        "exact_duplicate_group_count",
        "exact_duplicate_source_row_count",
        "warning_observation_count",
        "source_schema_version",
        "observations_parquet",
        "quarantine_parquet",
    }
    if set(metadata) != required:
        raise StationSnapshotError("station metadata fields do not match schema")
    if metadata["schema_version"] != METADATA_SCHEMA_VERSION:
        raise StationSnapshotError("unsupported station metadata schema_version")
    period = _require_observation_period(metadata["observation_period"])
    expected_precision = PRECISION_DAY if len(period) == 10 else PRECISION_MONTH
    if metadata["observation_precision"] != expected_precision:
        raise StationSnapshotError("metadata observation_precision does not match period")
    config = _canonical_configuration_object(metadata["configuration"])
    fingerprint = compute_configuration_fingerprint(config)
    if metadata["configuration_fingerprint"] != fingerprint:
        raise StationSnapshotError("metadata configuration_fingerprint does not match configuration")
    expected_id = compute_station_version_id(
        observation_period=period,
        source_version_id=_require_text(metadata["source_version_id"], "source_version_id"),
        transformation_version=_require_text(
            metadata["transformation_version"], "transformation_version"
        ),
        configuration_fingerprint=fingerprint,
    )
    if metadata["station_version_id"] != expected_id:
        raise StationSnapshotError("metadata station_version_id does not match identity inputs")


def _require_file_fingerprint(path: Path, payload: object, label: str) -> None:
    if not isinstance(payload, dict) or set(payload) != {"size_bytes", "sha256"}:
        raise StationSnapshotError(f"metadata {label} fingerprint is malformed")
    actual_size, actual_sha = _fingerprint_file(path)
    if payload["size_bytes"] != actual_size or payload["sha256"] != actual_sha:
        raise StationSnapshotError(f"{label} fingerprint does not match metadata")


def _require_bundle_binding(
    metadata: dict[str, object], observation_period: str, station_version_id: str
) -> None:
    if metadata["observation_period"] != observation_period:
        raise StationSnapshotError("station bundle observation_period does not match path/pointer")
    if metadata["station_version_id"] != station_version_id:
        raise StationSnapshotError("station bundle version id does not match path/pointer")


def _require_metadata_source(metadata: dict[str, object], source: SourceRecord) -> None:
    if metadata["source_version_id"] != source.source_version_id:
        raise StationSnapshotError("station metadata source_version_id does not match manifest")
    if (
        metadata["source_artifact_sha256"] != source.sha256
        or metadata["source_artifact_size_bytes"] != source.size_bytes
    ):
        raise StationSnapshotError("station metadata source fingerprint does not match manifest")


def _require_same_immutable(
    existing: dict[str, object], candidate: dict[str, object]
) -> None:
    if existing != candidate:
        raise StationSnapshotError(
            "same station_version_id already exists with different immutable metadata/content"
        )


def _load_current_pointer(
    path: Path, observation_period: str
) -> dict[str, object] | None:
    try:
        text = path.read_text(encoding="utf-8")
    except FileNotFoundError:
        return None
    except OSError as exc:
        raise StationSnapshotError(f"cannot read station current pointer {path}: {exc}") from exc
    try:
        payload = json.loads(text, object_pairs_hook=_reject_duplicate_pairs)
    except json.JSONDecodeError as exc:
        raise StationSnapshotError(f"station current pointer {path} is invalid JSON: {exc}") from exc
    if not isinstance(payload, dict) or set(payload) != {
        "schema_version",
        "observation_period",
        "station_version_id",
    }:
        raise StationSnapshotError(f"station current pointer {path} has invalid fields")
    if payload["schema_version"] != CURRENT_POINTER_SCHEMA_VERSION:
        raise StationSnapshotError(f"station current pointer {path} has unsupported schema_version")
    if payload["observation_period"] != observation_period:
        raise StationSnapshotError(f"station current pointer {path} has wrong observation_period")
    if not isinstance(payload["station_version_id"], str) or _STATION_VERSION.fullmatch(
        payload["station_version_id"]
    ) is None:
        raise StationSnapshotError(f"station current pointer {path} has invalid station_version_id")
    return payload


def _write_current_pointer(
    path: Path, observation_period: str, station_version_id: str
) -> None:
    payload = {
        "schema_version": CURRENT_POINTER_SCHEMA_VERSION,
        "observation_period": observation_period,
        "station_version_id": station_version_id,
    }
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, temp_name = tempfile.mkstemp(
        dir=path.parent, prefix=f".{path.name}.", suffix=".tmp"
    )
    temp = Path(temp_name)
    try:
        with os.fdopen(fd, "w", encoding="utf-8", newline="\n") as handle:
            handle.write(_canonical_json_text(payload) + "\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temp, path)
    finally:
        temp.unlink(missing_ok=True)


@contextmanager
def station_current_set_lock(station_root: str | Path):
    """Freeze mutation of the complete station current-pointer set.

    Every station current-pointer write must hold this lock. Cross-snapshot
    consumers can therefore take the same lock, re-read the full set, and
    publish lineage-bound outputs without a post-check station-current race.
    Per PRD §25, the mutation/runtime contract is one WSL2 Linux runtime;
    simultaneous Windows-native + WSL writers on shared NTFS are out of scope.
    """
    lock_path = Path(station_root) / "locks" / "current-set.lock"
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
            raise StationSnapshotError(
                f"another process holds station current-set lock {lock_path}"
            ) from exc
        yield
    finally:
        try:
            _unlock(handle)
        finally:
            handle.close()


@contextmanager
def _publish_lock(root: Path, period: str, station_version_id: str):
    lock_path = root / "locks" / f"observation_period={period}.lock"
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
            raise StationSnapshotError(
                f"another same-period station publish holds lock {lock_path}"
            ) from exc
        handle.seek(1)
        handle.truncate()
        handle.write((station_version_id + "\n").encode("utf-8"))
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


def _result(
    status: str,
    metadata: dict[str, object],
    version_path: Path,
    current_path: Path,
) -> StationPublishResult:
    return StationPublishResult(
        status=status,
        observation_period=metadata["observation_period"],
        source_version_id=metadata["source_version_id"],
        station_version_id=metadata["station_version_id"],
        version_path=version_path,
        current_pointer_path=current_path,
        source_row_count=metadata["source_row_count"],
        observation_count=metadata["observation_count"],
        quarantined_source_row_count=metadata["quarantined_source_row_count"],
        quarantine_issue_count=metadata["quarantine_issue_count"],
    )


def _reject_duplicate_pairs(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise StationSnapshotError(f"duplicate JSON key {key!r} is not allowed")
        result[key] = value
    return result
