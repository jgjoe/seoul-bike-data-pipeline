"""DuckDB-backed month-level logical-trip DQ and trusted eligibility.

The input boundary is one current ``source_version_id`` plus one rental
``source_month``.  All source members that contribute rows to that month are
canonicalized, staged into a scratch DuckDB database, classified as exact
duplicates or logical conflicts, and reconciled before any final row is exposed
to a caller.  Durable Clean Parquet publication is intentionally a later stage.
"""

from __future__ import annotations

import csv
import hashlib
import json
import re
import tempfile
from dataclasses import dataclass, fields
from datetime import datetime
from decimal import Decimal
from pathlib import Path
from typing import Callable

import duckdb

from .canonical_dq import (
    SEVERITY_QUARANTINE,
    SEVERITY_WARNING,
    CanonicalRow,
    DqIssue,
    canonicalize_csv_source_unit,
)
from .errors import GroupDqError
from .manifest import SourceRecord, load_manifest
from .zip_members import load_member_inventory, member_inventory_path
from .zip_months import months_for_member_name

RULE_LOGICAL_TRIP_CONFLICT = "logical_trip_conflict"
RULE_EXACT_DUPLICATE_GROUP = "exact_duplicate_group"

_MONTH_PATTERN = re.compile(r"20\d{2}-(?:0[1-9]|1[0-2])")
_YEAR_PATTERN = re.compile(r"20\d{2}")
_BATCH_SIZE = 1_000
_NULL_TOKEN = r"\N"


@dataclass(frozen=True)
class ClassifiedCanonicalRow(CanonicalRow):
    logical_trip_key: str | None
    logical_trip_conflict: bool
    exact_duplicate_group: bool
    trusted_fact_eligible: bool


@dataclass(frozen=True)
class GroupDqSummary:
    source_version_id: str
    source_month: str
    clean_row_count: int
    trusted_fact_eligible_count: int
    quarantined_row_count: int
    logical_conflict_group_count: int
    exact_duplicate_group_count: int
    logical_conflict_row_count: int
    exact_duplicate_row_count: int


def compute_logical_trip_key(bike_id: str | None, rent_at: datetime | None) -> str | None:
    """Return the D-003 logical-trip identity, or ``None`` when it is unknowable."""
    if bike_id is None or rent_at is None:
        return None
    payload = json.dumps(
        [bike_id, rent_at.strftime("%Y-%m-%d %H:%M:%S")],
        ensure_ascii=False,
        separators=(",", ":"),
    ).encode("utf-8")
    return f"ltk1_{hashlib.sha256(payload).hexdigest()}"


def classify_registered_source_month(
    *,
    manifest: str | Path,
    raw_root: str | Path,
    source_version_id: str,
    source_month: str,
    on_row: Callable[[ClassifiedCanonicalRow], None] | None = None,
) -> GroupDqSummary:
    """Classify one current rental month and finalize trusted eligibility.

    Historical revisions are never mixed implicitly: the caller supplies exactly
    one current ``source_version_id``.  For yearly ZIPs, every inventoried member
    that can contribute to ``source_month`` is included, and range members are
    filtered by each row's validated rental month after canonicalization.
    """
    source_month = _require_source_month(source_month)
    record = _find_source_record(manifest, source_version_id)
    members = _members_for_month(
        record=record,
        raw_root=raw_root,
        source_month=source_month,
    )

    with tempfile.TemporaryDirectory(prefix="seoul-bike-group-dq-") as temp_dir:
        stage_path = Path(temp_dir) / "canonical-stage.csv"
        database_path = Path(temp_dir) / "group-dq.duckdb"
        connection: duckdb.DuckDBPyConnection | None = None
        try:
            row_ordinal = 0
            with stage_path.open("w", encoding="utf-8", newline="") as stage_handle:
                writer = csv.writer(stage_handle, lineterminator="\n")

                def stage(row: CanonicalRow) -> None:
                    nonlocal row_ordinal
                    if row.source_version_id != source_version_id:
                        raise GroupDqError(
                            "group DQ cannot mix source revisions: "
                            f"expected {source_version_id}, got {row.source_version_id}"
                        )
                    if row.source_month != source_month:
                        return
                    row_ordinal += 1
                    logical_trip_key = compute_logical_trip_key(row.bike_id, row.rent_at)
                    writer.writerow(
                        (
                            row_ordinal,
                            row.source_row_id,
                            row.row_content_sha256,
                            logical_trip_key if logical_trip_key is not None else _NULL_TOKEN,
                            row.row_quarantined,
                            _serialize_canonical_row(row),
                        )
                    )

                for member_name in members:
                    canonicalize_csv_source_unit(
                        manifest=manifest,
                        raw_root=raw_root,
                        source_version_id=source_version_id,
                        member_name=member_name,
                        on_row=stage,
                    )

            if row_ordinal == 0:
                raise GroupDqError(
                    f"source month {source_month} produced zero canonical rows for {source_version_id}"
                )
            connection = duckdb.connect(
                str(database_path), config={"memory_limit": "2GB"}
            )
            _create_stage(connection)
            _copy_stage(connection, stage_path)
            _require_unique_physical_rows(connection)
            _build_group_status(connection)
            summary = _build_summary(
                connection,
                source_version_id=source_version_id,
                source_month=source_month,
            )
            _emit_final_rows(connection, on_row=on_row)
            return summary
        except GroupDqError:
            raise
        except duckdb.Error as exc:
            raise GroupDqError(f"DuckDB group-DQ execution failed: {exc}") from exc
        finally:
            if connection is not None:
                try:
                    connection.close()
                except duckdb.Error:
                    pass


def _create_stage(connection: duckdb.DuckDBPyConnection) -> None:
    connection.execute(
        """
        CREATE TABLE canonical_stage (
            row_ordinal BIGINT NOT NULL,
            source_row_id VARCHAR NOT NULL,
            row_content_sha256 VARCHAR NOT NULL,
            logical_trip_key VARCHAR,
            row_quarantined BOOLEAN NOT NULL,
            row_payload VARCHAR NOT NULL
        )
        """
    )


def _copy_stage(connection: duckdb.DuckDBPyConnection, path: Path) -> None:
    sql_path = path.resolve().as_posix().replace("'", "''")
    connection.execute(
        f"COPY canonical_stage FROM '{sql_path}' "
        "(FORMAT CSV, HEADER FALSE, NULL '\\N')"
    )


def _require_unique_physical_rows(connection: duckdb.DuckDBPyConnection) -> None:
    row_count, unique_count = connection.execute(
        "SELECT COUNT(*), COUNT(DISTINCT source_row_id) FROM canonical_stage"
    ).fetchone()
    if row_count != unique_count:
        raise GroupDqError(
            "group DQ staging contains duplicate source_row_id values; physical row identity is not unique"
        )


def _build_group_status(connection: duckdb.DuckDBPyConnection) -> None:
    connection.execute(
        """
        CREATE TEMP TABLE logical_group_status AS
        SELECT
            logical_trip_key,
            COUNT(*) AS row_count,
            COUNT(DISTINCT row_content_sha256) AS content_variant_count,
            COUNT(*) > 1 AND COUNT(DISTINCT row_content_sha256) > 1
                AS logical_trip_conflict,
            COUNT(*) > 1 AND COUNT(DISTINCT row_content_sha256) = 1
                AS exact_duplicate_group
        FROM canonical_stage
        WHERE logical_trip_key IS NOT NULL
        GROUP BY logical_trip_key
        HAVING COUNT(*) > 1
        """
    )


def _build_summary(
    connection: duckdb.DuckDBPyConnection,
    *,
    source_version_id: str,
    source_month: str,
) -> GroupDqSummary:
    clean_row_count, quarantined_row_count, trusted_count = connection.execute(
        """
        WITH classified AS (
            SELECT
                s.row_quarantined
                OR COALESCE(g.logical_trip_conflict, FALSE)
                OR COALESCE(g.exact_duplicate_group, FALSE) AS final_quarantined
            FROM canonical_stage AS s
            LEFT JOIN logical_group_status AS g USING (logical_trip_key)
        )
        SELECT
            COUNT(*),
            COUNT(*) FILTER (WHERE final_quarantined),
            COUNT(*) FILTER (WHERE NOT final_quarantined)
        FROM classified
        """
    ).fetchone()
    (
        logical_conflict_group_count,
        exact_duplicate_group_count,
        logical_conflict_row_count,
        exact_duplicate_row_count,
    ) = connection.execute(
        """
        SELECT
            COUNT(*) FILTER (WHERE logical_trip_conflict),
            COUNT(*) FILTER (WHERE exact_duplicate_group),
            COALESCE(SUM(row_count) FILTER (WHERE logical_trip_conflict), 0),
            COALESCE(SUM(row_count) FILTER (WHERE exact_duplicate_group), 0)
        FROM logical_group_status
        """
    ).fetchone()

    if clean_row_count != trusted_count + quarantined_row_count:
        raise GroupDqError(
            "group DQ row reconciliation failed: "
            f"clean={clean_row_count}, trusted={trusted_count}, quarantined={quarantined_row_count}"
        )
    return GroupDqSummary(
        source_version_id=source_version_id,
        source_month=source_month,
        clean_row_count=int(clean_row_count),
        trusted_fact_eligible_count=int(trusted_count),
        quarantined_row_count=int(quarantined_row_count),
        logical_conflict_group_count=int(logical_conflict_group_count),
        exact_duplicate_group_count=int(exact_duplicate_group_count),
        logical_conflict_row_count=int(logical_conflict_row_count),
        exact_duplicate_row_count=int(exact_duplicate_row_count),
    )


def _emit_final_rows(
    connection: duckdb.DuckDBPyConnection,
    *,
    on_row: Callable[[ClassifiedCanonicalRow], None] | None,
) -> None:
    if on_row is None:
        return
    cursor = connection.execute(
        """
        SELECT
            s.row_payload,
            s.logical_trip_key,
            COALESCE(g.logical_trip_conflict, FALSE),
            COALESCE(g.exact_duplicate_group, FALSE)
        FROM canonical_stage AS s
        LEFT JOIN logical_group_status AS g USING (logical_trip_key)
        """
    )
    while rows := cursor.fetchmany(_BATCH_SIZE):
        for row_payload, logical_trip_key, logical_trip_conflict, exact_duplicate_group in rows:
            canonical = _deserialize_canonical_row(row_payload)
            on_row(
                _classify_row(
                    canonical,
                    logical_trip_key=logical_trip_key,
                    logical_trip_conflict=bool(logical_trip_conflict),
                    exact_duplicate_group=bool(exact_duplicate_group),
                )
            )


def _classify_row(
    row: CanonicalRow,
    *,
    logical_trip_key: str | None,
    logical_trip_conflict: bool,
    exact_duplicate_group: bool,
) -> ClassifiedCanonicalRow:
    issues = list(row.dq_issues)
    if logical_trip_conflict:
        issues.append(
            DqIssue(
                rule_id=RULE_LOGICAL_TRIP_CONFLICT,
                severity=SEVERITY_QUARANTINE,
                field="logical_trip_key",
                reason=(
                    f"logical trip group {logical_trip_key} contains conflicting source row content"
                ),
            )
        )
    elif exact_duplicate_group:
        issues.append(
            DqIssue(
                rule_id=RULE_EXACT_DUPLICATE_GROUP,
                severity=SEVERITY_QUARANTINE,
                field="logical_trip_key",
                reason=f"logical trip group {logical_trip_key} contains exact duplicate source rows",
            )
        )
    final_issues = tuple(issues)
    warning_count = len(
        {issue.rule_id for issue in final_issues if issue.severity == SEVERITY_WARNING}
    )
    row_quarantined = any(
        issue.severity == SEVERITY_QUARANTINE for issue in final_issues
    )
    base = {field.name: getattr(row, field.name) for field in fields(CanonicalRow)}
    base.update(
        dq_issues=final_issues,
        dq_warning_count=warning_count,
        row_quarantined=row_quarantined,
    )
    return ClassifiedCanonicalRow(
        **base,
        logical_trip_key=logical_trip_key,
        logical_trip_conflict=logical_trip_conflict,
        exact_duplicate_group=exact_duplicate_group,
        trusted_fact_eligible=not row_quarantined,
    )


def _serialize_canonical_row(row: CanonicalRow) -> str:
    payload: dict[str, object] = {}
    for field in fields(CanonicalRow):
        value = getattr(row, field.name)
        if isinstance(value, datetime):
            payload[field.name] = value.isoformat(sep=" ")
        elif isinstance(value, Decimal):
            payload[field.name] = str(value)
        elif field.name == "dq_issues":
            payload[field.name] = [
                {
                    "rule_id": issue.rule_id,
                    "severity": issue.severity,
                    "field": issue.field,
                    "reason": issue.reason,
                }
                for issue in value
            ]
        else:
            payload[field.name] = value
    return json.dumps(payload, ensure_ascii=False, separators=(",", ":"))


def _deserialize_canonical_row(payload: str) -> CanonicalRow:
    raw = json.loads(payload)
    for field_name in ("rent_at", "return_at"):
        if raw[field_name] is not None:
            raw[field_name] = datetime.fromisoformat(raw[field_name])
    if raw["distance_m"] is not None:
        raw["distance_m"] = Decimal(raw["distance_m"])
    raw["dq_issues"] = tuple(DqIssue(**issue) for issue in raw["dq_issues"])
    return CanonicalRow(**raw)


def _find_source_record(manifest: str | Path, source_version_id: str) -> SourceRecord:
    record = next(
        (
            candidate
            for candidate in load_manifest(manifest)
            if candidate.source_version_id == source_version_id
        ),
        None,
    )
    if record is None:
        raise GroupDqError(
            f"source_version_id {source_version_id!r} is not registered in manifest {manifest}"
        )
    return record


def _members_for_month(
    *, record: SourceRecord, raw_root: str | Path, source_month: str
) -> tuple[str | None, ...]:
    if record.logical_period == source_month:
        return (None,)
    if _YEAR_PATTERN.fullmatch(record.logical_period) is None:
        raise GroupDqError(
            f"source version {record.source_version_id} logical_period {record.logical_period!r} "
            f"cannot build rental month {source_month}"
        )
    if not source_month.startswith(f"{record.logical_period}-"):
        raise GroupDqError(
            f"source month {source_month} is outside yearly source period {record.logical_period}"
        )
    inventory_path = member_inventory_path(raw_root, record.source_version_id)
    inventory = load_member_inventory(inventory_path)
    if inventory.parent_source_version_id != record.source_version_id:
        raise GroupDqError(
            f"member inventory {inventory_path} belongs to "
            f"{inventory.parent_source_version_id}, not {record.source_version_id}"
        )
    selected: list[str] = []
    for member in inventory.members:
        months = months_for_member_name(member.member_name)
        if months is not None and source_month in months:
            selected.append(member.member_name)
    if not selected:
        raise GroupDqError(
            f"yearly source {record.source_version_id} has no CSV member for {source_month}"
        )
    return tuple(sorted(selected))


def _require_source_month(value: str) -> str:
    if not isinstance(value, str) or _MONTH_PATTERN.fullmatch(value) is None:
        raise GroupDqError(f"source_month must be YYYY-MM, got {value!r}")
    return value
