"""Canonical row normalization and row-level DQ classification.

This is the last row-local stage before group-level logical-trip DQ.  It keeps
source-facing provenance, derives the canonical values decided in D-002, and
attaches row-level warning/quarantine issues.  It deliberately does not decide
logical-trip conflicts, exact duplicate groups, final trusted eligibility, or
persist Clean Parquet.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from pathlib import Path
from typing import Callable

from .errors import CanonicalizationError
from .source_projection import (
    MONTH_MATCH,
    MONTH_MISMATCH,
    MONTH_UNAVAILABLE,
    ConversionIssue,
    TypedSourceRow,
    project_csv_source_unit,
)

SEVERITY_WARNING = "WARNING"
SEVERITY_QUARANTINE = "QUARANTINE"

RULE_BIKE_ID_MISSING = "bike_id_missing"
RULE_RENT_AT_MISSING = "rent_at_missing"
RULE_RENT_AT_INVALID = "rent_at_invalid"
RULE_RENT_STATION_INVALID = "rent_station_invalid"
RULE_RETURN_AT_MISSING = "return_at_missing"
RULE_RETURN_AT_INVALID = "return_at_invalid"
RULE_RETURN_STATION_INVALID = "return_station_invalid"
RULE_RETURN_BEFORE_RENT = "return_before_rent"
RULE_RENT_DOCK_INVALID = "rent_dock_invalid"
RULE_RETURN_DOCK_INVALID = "return_dock_invalid"
RULE_USAGE_INVALID = "usage_min_invalid"
RULE_USAGE_NEGATIVE = "usage_min_negative"
RULE_USAGE_OVER_1440 = "usage_min_over_1440"
RULE_DISTANCE_INVALID = "distance_m_invalid"
RULE_DISTANCE_NEGATIVE = "distance_m_negative"
RULE_DISTANCE_ZERO = "distance_m_zero"
RULE_DISTANCE_EXTREME = "distance_m_extreme_positive"
RULE_BIRTH_YEAR_MISSING = "birth_year_missing"
RULE_BIRTH_YEAR_INVALID = "birth_year_invalid"
RULE_BIRTH_YEAR_IMPOSSIBLE = "birth_year_impossible"
RULE_SEX_MISSING = "sex_missing"
RULE_SEX_NONCANONICAL = "sex_noncanonical"

_ASCII_DIGITS = re.compile(r"[0-9]+")
_DISTANCE_EXTREME_M = 100_000


@dataclass(frozen=True)
class DqIssue:
    rule_id: str
    severity: str
    field: str
    reason: str


@dataclass(frozen=True)
class CanonicalRow:
    source_row_id: str
    source_version_id: str
    member_name: str
    source_row_number: int
    row_content_sha256: str
    source_month: str
    schema_version: str
    bike_id: str | None
    rent_at: datetime | None
    rent_station_no_raw: str | None
    rent_station_no: str | None
    rent_station_name: str | None
    rent_dock_no: int | None
    return_at: datetime | None
    return_station_no_raw: str | None
    return_station_no: str | None
    return_station_name: str | None
    return_dock_no: int | None
    usage_min: int | None
    distance_m: Decimal | None
    birth_year: int | None
    sex_raw: str | None
    sex_code: str | None
    user_type: str | None
    rent_station_internal_id: str | None
    return_station_internal_id: str | None
    bike_type: str | None
    is_completed_trip: bool
    distance_metric_eligible: bool
    dq_issues: tuple[DqIssue, ...]
    dq_warning_count: int
    row_quarantined: bool


@dataclass(frozen=True)
class CanonicalizationSummary:
    source_version_id: str
    member_name: str
    schema_version: str
    expected_source_months: tuple[str, ...]
    row_count: int
    quarantined_row_count: int
    warning_row_count: int
    warning_issue_count: int
    completed_trip_count: int
    distance_metric_eligible_count: int


def canonicalize_csv_source_unit(
    *,
    manifest: str | Path,
    raw_root: str | Path,
    source_version_id: str,
    member_name: str | None = None,
    on_row: Callable[[CanonicalRow], None] | None = None,
) -> CanonicalizationSummary:
    """Validate unit-level month gates, then stream canonical row-level DQ.

    The source unit is intentionally read twice.  The first pass proves the
    hard month gate before any canonical row is exposed to the caller.  This
    prevents partial output when a later source row reveals a month mismatch or
    an unassignable multi-month row.
    """
    preflight = project_csv_source_unit(
        manifest=manifest,
        raw_root=raw_root,
        source_version_id=source_version_id,
        member_name=member_name,
    )
    if preflight.month_mismatch_count:
        raise CanonicalizationError(
            "source/rent-month validation failed: "
            f"{preflight.month_mismatch_count} row(s) are outside expected source months "
            f"{preflight.expected_source_months}"
        )
    if len(preflight.expected_source_months) > 1 and preflight.month_unavailable_count:
        raise CanonicalizationError(
            "multi-month source unit cannot assign source_month for "
            f"{preflight.month_unavailable_count} row(s) with unavailable rent_at"
        )

    quarantined_row_count = 0
    warning_row_count = 0
    warning_issue_count = 0
    completed_trip_count = 0
    distance_metric_eligible_count = 0

    def emit(typed: TypedSourceRow) -> None:
        nonlocal quarantined_row_count
        nonlocal warning_row_count
        nonlocal warning_issue_count
        nonlocal completed_trip_count
        nonlocal distance_metric_eligible_count

        row = canonicalize_typed_source_row(typed)
        if row.row_quarantined:
            quarantined_row_count += 1
        if row.dq_warning_count:
            warning_row_count += 1
            warning_issue_count += row.dq_warning_count
        if row.is_completed_trip:
            completed_trip_count += 1
        if row.distance_metric_eligible:
            distance_metric_eligible_count += 1
        if on_row is not None:
            on_row(row)

    second_pass = project_csv_source_unit(
        manifest=manifest,
        raw_root=raw_root,
        source_version_id=source_version_id,
        member_name=member_name,
        on_row=emit,
    )
    if second_pass.row_count != preflight.row_count:
        raise CanonicalizationError(
            "source unit changed between canonicalization passes: "
            f"preflight rows={preflight.row_count}, canonical rows={second_pass.row_count}"
        )

    return CanonicalizationSummary(
        source_version_id=second_pass.source_version_id,
        member_name=second_pass.member_name,
        schema_version=second_pass.schema_version,
        expected_source_months=second_pass.expected_source_months,
        row_count=second_pass.row_count,
        quarantined_row_count=quarantined_row_count,
        warning_row_count=warning_row_count,
        warning_issue_count=warning_issue_count,
        completed_trip_count=completed_trip_count,
        distance_metric_eligible_count=distance_metric_eligible_count,
    )


def canonicalize_typed_source_row(row: TypedSourceRow) -> CanonicalRow:
    """Normalize one typed source row and attach only row-local DQ issues."""
    source_month = _source_month(row)
    issues: list[DqIssue] = []
    conversion_fields = {issue.field for issue in row.conversion_issues}
    _conversion_issues(row.conversion_issues, issues)

    bike_id = _trimmed(row.bike_id)
    if bike_id is None:
        _issue(
            issues,
            RULE_BIKE_ID_MISSING,
            SEVERITY_QUARANTINE,
            "bike_id",
            "bike_id is missing after canonical whitespace normalization",
        )

    rent_station_no = _station_no(row.rent_station_no_raw)
    if rent_station_no is None:
        _issue(
            issues,
            RULE_RENT_STATION_INVALID,
            SEVERITY_QUARANTINE,
            "rent_station_no",
            "rental station number is missing or is not ASCII decimal digits",
        )

    return_station_no = _station_no(row.return_station_no_raw)
    if return_station_no is None:
        _issue(
            issues,
            RULE_RETURN_STATION_INVALID,
            SEVERITY_WARNING,
            "return_station_no",
            "return station number is missing or is not ASCII decimal digits",
        )

    if row.rent_at is None and "rent_at" not in conversion_fields:
        _issue(
            issues,
            RULE_RENT_AT_MISSING,
            SEVERITY_QUARANTINE,
            "rent_at",
            "rent_at is missing",
        )
    if row.return_at is None and "return_at" not in conversion_fields:
        _issue(
            issues,
            RULE_RETURN_AT_MISSING,
            SEVERITY_WARNING,
            "return_at",
            "return_at is missing",
        )
    if row.rent_at is not None and row.return_at is not None and row.return_at < row.rent_at:
        _issue(
            issues,
            RULE_RETURN_BEFORE_RENT,
            SEVERITY_QUARANTINE,
            "return_at",
            "return_at is earlier than rent_at",
        )

    if row.usage_min is not None:
        if row.usage_min < 0:
            _issue(
                issues,
                RULE_USAGE_NEGATIVE,
                SEVERITY_QUARANTINE,
                "usage_min",
                "usage_min is negative",
            )
        elif row.usage_min > 1440:
            _issue(
                issues,
                RULE_USAGE_OVER_1440,
                SEVERITY_WARNING,
                "usage_min",
                "usage_min exceeds 1440 minutes",
            )

    distance_metric_eligible = False
    if row.distance_m is not None:
        if row.distance_m < 0:
            _issue(
                issues,
                RULE_DISTANCE_NEGATIVE,
                SEVERITY_QUARANTINE,
                "distance_m",
                "distance_m is negative",
            )
        elif row.distance_m == 0:
            _issue(
                issues,
                RULE_DISTANCE_ZERO,
                SEVERITY_WARNING,
                "distance_m",
                "distance_m is zero and is not distance-metric eligible",
            )
        elif row.distance_m > _DISTANCE_EXTREME_M:
            _issue(
                issues,
                RULE_DISTANCE_EXTREME,
                SEVERITY_WARNING,
                "distance_m",
                "distance_m exceeds the v1 100 km monitoring threshold",
            )
        else:
            distance_metric_eligible = True

    if row.birth_year is None:
        if "birth_year" not in conversion_fields:
            _issue(
                issues,
                RULE_BIRTH_YEAR_MISSING,
                SEVERITY_WARNING,
                "birth_year",
                "birth_year is missing",
            )
    elif row.birth_year <= 0 or (
        row.rent_at is not None and row.birth_year > row.rent_at.year
    ):
        _issue(
            issues,
            RULE_BIRTH_YEAR_IMPOSSIBLE,
            SEVERITY_WARNING,
            "birth_year",
            "birth_year is <= 0 or, when rent_at is available, later than the rental year",
        )

    sex_code = _sex_code(row.sex_raw, issues)
    is_completed_trip = (
        row.rent_at is not None
        and row.return_at is not None
        and return_station_no is not None
        and row.return_at >= row.rent_at
    )
    warning_rule_ids = {
        issue.rule_id for issue in issues if issue.severity == SEVERITY_WARNING
    }
    row_quarantined = any(issue.severity == SEVERITY_QUARANTINE for issue in issues)

    return CanonicalRow(
        source_row_id=row.source_row_id,
        source_version_id=row.source_version_id,
        member_name=row.member_name,
        source_row_number=row.source_row_number,
        row_content_sha256=row.row_content_sha256,
        source_month=source_month,
        schema_version=row.schema_version,
        bike_id=bike_id,
        rent_at=row.rent_at,
        rent_station_no_raw=row.rent_station_no_raw,
        rent_station_no=rent_station_no,
        rent_station_name=row.rent_station_name,
        rent_dock_no=row.rent_dock_no,
        return_at=row.return_at,
        return_station_no_raw=row.return_station_no_raw,
        return_station_no=return_station_no,
        return_station_name=row.return_station_name,
        return_dock_no=row.return_dock_no,
        usage_min=row.usage_min,
        distance_m=row.distance_m,
        birth_year=row.birth_year,
        sex_raw=row.sex_raw,
        sex_code=sex_code,
        user_type=row.user_type,
        rent_station_internal_id=row.rent_station_internal_id,
        return_station_internal_id=row.return_station_internal_id,
        bike_type=row.bike_type,
        is_completed_trip=is_completed_trip,
        distance_metric_eligible=distance_metric_eligible,
        dq_issues=tuple(issues),
        dq_warning_count=len(warning_rule_ids),
        row_quarantined=row_quarantined,
    )


def _source_month(row: TypedSourceRow) -> str:
    if row.source_month_validation == MONTH_MATCH:
        if row.validated_source_month is None:
            raise CanonicalizationError(
                f"row {row.source_row_id} reports MATCH without validated_source_month"
            )
        return row.validated_source_month
    if row.source_month_validation == MONTH_MISMATCH:
        raise CanonicalizationError(
            f"row {row.source_row_id} has source/rent-month mismatch"
        )
    if row.source_month_validation == MONTH_UNAVAILABLE:
        if len(row.expected_source_months) == 1:
            return row.expected_source_months[0]
        raise CanonicalizationError(
            f"row {row.source_row_id} has unavailable rent_at in multi-month source unit"
        )
    raise CanonicalizationError(
        f"row {row.source_row_id} has unknown source_month_validation "
        f"{row.source_month_validation!r}"
    )


def _conversion_issues(
    conversion_issues: tuple[ConversionIssue, ...], issues: list[DqIssue]
) -> None:
    mapping = {
        "rent_at": (RULE_RENT_AT_INVALID, SEVERITY_QUARANTINE),
        "return_at": (RULE_RETURN_AT_INVALID, SEVERITY_WARNING),
        "rent_dock_no": (RULE_RENT_DOCK_INVALID, SEVERITY_WARNING),
        "return_dock_no": (RULE_RETURN_DOCK_INVALID, SEVERITY_WARNING),
        "usage_min": (RULE_USAGE_INVALID, SEVERITY_WARNING),
        "distance_m": (RULE_DISTANCE_INVALID, SEVERITY_WARNING),
        "birth_year": (RULE_BIRTH_YEAR_INVALID, SEVERITY_WARNING),
    }
    for conversion in conversion_issues:
        try:
            rule_id, severity = mapping[conversion.field]
        except KeyError as exc:
            raise CanonicalizationError(
                f"unsupported conversion issue field {conversion.field!r}"
            ) from exc
        _issue(
            issues,
            rule_id,
            severity,
            conversion.field,
            f"typed conversion failed: {conversion.reason}",
        )


def _station_no(raw_value: str | None) -> str | None:
    text = _trimmed(raw_value)
    if text is None or _ASCII_DIGITS.fullmatch(text) is None:
        return None
    return text.lstrip("0") or "0"


def _trimmed(raw_value: str | None) -> str | None:
    if raw_value is None:
        return None
    text = raw_value.strip()
    return text or None


def _sex_code(raw_value: str | None, issues: list[DqIssue]) -> str | None:
    if raw_value is None:
        _issue(
            issues,
            RULE_SEX_MISSING,
            SEVERITY_WARNING,
            "sex_code",
            "sex is missing",
        )
        return None
    text = raw_value.strip()
    upper = text.upper()
    if upper in {"M", "F"}:
        if raw_value not in {"M", "F"}:
            _issue(
                issues,
                RULE_SEX_NONCANONICAL,
                SEVERITY_WARNING,
                "sex_code",
                "sex representation was canonicalized by whitespace/case only",
            )
        return upper
    _issue(
        issues,
        RULE_SEX_NONCANONICAL,
        SEVERITY_WARNING,
        "sex_code",
        "sex representation is not recognized as M or F",
    )
    return None


def _issue(
    issues: list[DqIssue], rule_id: str, severity: str, field: str, reason: str
) -> None:
    issues.append(DqIssue(rule_id=rule_id, severity=severity, field=field, reason=reason))
