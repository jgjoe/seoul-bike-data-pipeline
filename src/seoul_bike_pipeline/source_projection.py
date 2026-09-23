"""Typed source-row projection before canonical normalization and DQ policy.

This layer converts the two approved rental schema families into Python scalar
types while preserving source-facing strings such as station numbers and sex.
It deliberately does not decide warning/quarantine severity, normalize station
keys, or derive normalized categorical values.  Those are later Clean/DQ
responsibilities.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Callable

from .csv_source import RawSourceRow, read_csv_source_unit
from .errors import SourceProjectionError, ZipMonthMappingError
from .manifest import SourceRecord, load_manifest
from .zip_months import months_for_member_name

MONTH_MATCH = "MATCH"
MONTH_MISMATCH = "MISMATCH"
MONTH_UNAVAILABLE = "UNAVAILABLE"

_MONTH_PATTERN = re.compile(r"20\d{2}-(?:0[1-9]|1[0-2])")
_TIMESTAMP_FORMAT = "%Y-%m-%d %H:%M:%S"
_NULL_MARKERS = {"", r"\N"}


@dataclass(frozen=True)
class ConversionIssue:
    field: str
    raw_value: str
    reason: str


@dataclass(frozen=True)
class TypedSourceRow:
    source_row_id: str
    source_version_id: str
    member_name: str
    source_row_number: int
    row_content_sha256: str
    schema_version: str
    expected_source_months: tuple[str, ...]
    rent_month: str | None
    validated_source_month: str | None
    source_month_validation: str
    bike_id: str | None
    rent_at: datetime | None
    rent_station_no_raw: str | None
    rent_station_name: str | None
    rent_dock_no: int | None
    return_at: datetime | None
    return_station_no_raw: str | None
    return_station_name: str | None
    return_dock_no: int | None
    usage_min: int | None
    distance_m: Decimal | None
    birth_year: int | None
    sex_raw: str | None
    user_type: str | None
    rent_station_internal_id: str | None
    return_station_internal_id: str | None
    bike_type: str | None
    conversion_issues: tuple[ConversionIssue, ...]


@dataclass(frozen=True)
class ProjectionSummary:
    source_version_id: str
    member_name: str
    schema_version: str
    expected_source_months: tuple[str, ...]
    row_count: int
    rows_with_conversion_issues: int
    conversion_issue_count: int
    month_match_count: int
    month_mismatch_count: int
    month_unavailable_count: int


def project_csv_source_unit(
    *,
    manifest: str | Path,
    raw_root: str | Path,
    source_version_id: str,
    member_name: str | None = None,
    on_row: Callable[[TypedSourceRow], None] | None = None,
) -> ProjectionSummary:
    """Stream one source unit into typed source-facing rows without persistence."""
    parent = _find_parent(manifest, source_version_id)
    expected_source_months = _expected_source_months(parent, member_name)

    rows_with_conversion_issues = 0
    conversion_issue_count = 0
    month_counts = {
        MONTH_MATCH: 0,
        MONTH_MISMATCH: 0,
        MONTH_UNAVAILABLE: 0,
    }

    def project(raw_row: RawSourceRow) -> None:
        nonlocal rows_with_conversion_issues, conversion_issue_count
        typed = project_raw_source_row(raw_row, expected_source_months=expected_source_months)
        if typed.conversion_issues:
            rows_with_conversion_issues += 1
            conversion_issue_count += len(typed.conversion_issues)
        month_counts[typed.source_month_validation] += 1
        if on_row is not None:
            on_row(typed)

    read_summary = read_csv_source_unit(
        manifest=manifest,
        raw_root=raw_root,
        source_version_id=source_version_id,
        member_name=member_name,
        on_row=project,
    )
    return ProjectionSummary(
        source_version_id=read_summary.source_version_id,
        member_name=read_summary.member_name,
        schema_version=read_summary.schema_version,
        expected_source_months=expected_source_months,
        row_count=read_summary.row_count,
        rows_with_conversion_issues=rows_with_conversion_issues,
        conversion_issue_count=conversion_issue_count,
        month_match_count=month_counts[MONTH_MATCH],
        month_mismatch_count=month_counts[MONTH_MISMATCH],
        month_unavailable_count=month_counts[MONTH_UNAVAILABLE],
    )


def project_raw_source_row(
    raw_row: RawSourceRow, *, expected_source_months: tuple[str, ...]
) -> TypedSourceRow:
    """Convert one structurally valid raw source row into typed source fields."""
    if not expected_source_months:
        raise SourceProjectionError("expected_source_months must not be empty")
    values = raw_row.values
    if raw_row.schema_version == "rental_v16" and len(values) != 16:
        raise SourceProjectionError(
            f"rental_v16 row {raw_row.raw_row_id} has {len(values)} values; expected 16"
        )
    if raw_row.schema_version == "rental_v17" and len(values) != 17:
        raise SourceProjectionError(
            f"rental_v17 row {raw_row.raw_row_id} has {len(values)} values; expected 17"
        )
    if raw_row.schema_version not in {"rental_v16", "rental_v17"}:
        raise SourceProjectionError(
            f"unsupported schema_version {raw_row.schema_version!r} for typed projection"
        )

    issues: list[ConversionIssue] = []
    rent_at = _timestamp(values[1], "rent_at", issues)
    return_at = _timestamp(values[5], "return_at", issues)
    rent_dock_no = _integer(values[4], "rent_dock_no", issues)
    return_dock_no = _integer(values[8], "return_dock_no", issues)
    usage_min = _integer(values[9], "usage_min", issues)
    distance_m = _decimal(values[10], "distance_m", issues)
    birth_year = _integer(values[11], "birth_year", issues)

    rent_month = rent_at.strftime("%Y-%m") if rent_at is not None else None
    if rent_month is None:
        month_validation = MONTH_UNAVAILABLE
        validated_source_month = None
    elif rent_month in expected_source_months:
        month_validation = MONTH_MATCH
        validated_source_month = rent_month
    else:
        month_validation = MONTH_MISMATCH
        validated_source_month = None

    return TypedSourceRow(
        source_row_id=raw_row.raw_row_id,
        source_version_id=raw_row.source_version_id,
        member_name=raw_row.member_name,
        source_row_number=raw_row.source_row_number,
        row_content_sha256=raw_row.row_content_sha256,
        schema_version=raw_row.schema_version,
        expected_source_months=expected_source_months,
        rent_month=rent_month,
        validated_source_month=validated_source_month,
        source_month_validation=month_validation,
        bike_id=_nullable_text(values[0]),
        rent_at=rent_at,
        rent_station_no_raw=_nullable_text(values[2]),
        rent_station_name=_nullable_text(values[3]),
        rent_dock_no=rent_dock_no,
        return_at=return_at,
        return_station_no_raw=_nullable_text(values[6]),
        return_station_name=_nullable_text(values[7]),
        return_dock_no=return_dock_no,
        usage_min=usage_min,
        distance_m=distance_m,
        birth_year=birth_year,
        sex_raw=_nullable_text(values[12]),
        user_type=_nullable_text(values[13]),
        rent_station_internal_id=_nullable_text(values[14]),
        return_station_internal_id=_nullable_text(values[15]),
        bike_type=(
            _nullable_text(values[16]) if raw_row.schema_version == "rental_v17" else None
        ),
        conversion_issues=tuple(issues),
    )


def _find_parent(manifest: str | Path, source_version_id: str) -> SourceRecord:
    parent = next(
        (
            record
            for record in load_manifest(manifest)
            if record.source_version_id == source_version_id
        ),
        None,
    )
    if parent is None:
        raise SourceProjectionError(
            f"source_version_id {source_version_id!r} is not registered in source manifest"
        )
    return parent


def _expected_source_months(
    parent: SourceRecord, member_name: str | None
) -> tuple[str, ...]:
    if member_name is None:
        if _MONTH_PATTERN.fullmatch(parent.logical_period) is None:
            raise SourceProjectionError(
                f"standalone CSV source {parent.source_version_id} has logical_period "
                f"{parent.logical_period!r}; source-month validation requires YYYY-MM"
            )
        return (parent.logical_period,)

    try:
        months = months_for_member_name(member_name)
    except ZipMonthMappingError as exc:
        raise SourceProjectionError(str(exc)) from exc
    if not months:
        raise SourceProjectionError(
            f"ZIP member {member_name!r} is not a recognized monthly CSV processing unit"
        )
    if re.fullmatch(r"20\d{2}", parent.logical_period) is None:
        raise SourceProjectionError(
            f"ZIP source {parent.source_version_id} has logical_period {parent.logical_period!r}; "
            "source-month validation requires a YYYY yearly parent"
        )
    expected_year = int(parent.logical_period)
    outside = [month for month in months if int(month[:4]) != expected_year]
    if outside:
        raise SourceProjectionError(
            f"ZIP member {member_name!r} maps outside yearly parent "
            f"{parent.logical_period}: {outside}"
        )
    return months


def _nullable_text(value: str) -> str | None:
    return None if _is_null(value) else value


def _timestamp(
    raw_value: str, field: str, issues: list[ConversionIssue]
) -> datetime | None:
    text = raw_value.strip()
    if _is_null(raw_value):
        return None
    try:
        return datetime.strptime(text, _TIMESTAMP_FORMAT)
    except ValueError:
        issues.append(
            ConversionIssue(
                field=field,
                raw_value=raw_value,
                reason=f"expected timestamp format {_TIMESTAMP_FORMAT}",
            )
        )
        return None


def _integer(raw_value: str, field: str, issues: list[ConversionIssue]) -> int | None:
    text = raw_value.strip()
    if _is_null(raw_value):
        return None
    try:
        return int(text)
    except ValueError:
        issues.append(
            ConversionIssue(field=field, raw_value=raw_value, reason="expected integer")
        )
        return None


def _decimal(raw_value: str, field: str, issues: list[ConversionIssue]) -> Decimal | None:
    text = raw_value.strip()
    if _is_null(raw_value):
        return None
    try:
        value = Decimal(text)
    except InvalidOperation:
        issues.append(
            ConversionIssue(field=field, raw_value=raw_value, reason="expected decimal")
        )
        return None
    if not value.is_finite():
        issues.append(
            ConversionIssue(field=field, raw_value=raw_value, reason="expected finite decimal")
        )
        return None
    return value


def _is_null(raw_value: str) -> bool:
    """Recognize only null spellings observed in the supported source families."""
    return raw_value.strip() in _NULL_MARKERS
