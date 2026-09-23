"""Versioned JSON-compatible state carried across orchestration process boundaries.

Airflow XCom is control-plane metadata, not a warehouse/source of truth.  The
payloads here intentionally contain only the captured BackfillPlan and compact
BackfillRunResult values needed to resume the exact business plan in another
task/process.  Paths and transformation configuration live in the separate
orchestration request payload.
"""

from __future__ import annotations

import re
from collections.abc import Mapping
from typing import Any

from .backfill import (
    MONTH_FAILED,
    MONTH_SKIPPED_UNCHANGED,
    MONTH_SUCCEEDED,
    RUN_FAILED,
    RUN_PARTIAL_FAILURE,
    RUN_SUCCEEDED,
    SOURCE_MISSING,
    SOURCE_NEW,
    SOURCE_REMOVED,
    SOURCE_REVISED,
    SOURCE_UNCHANGED,
    BackfillFailure,
    BackfillMonthPlan,
    BackfillMonthResult,
    BackfillPlan,
    BackfillRunResult,
    StationDayExecution,
    iter_months,
)
from .errors import BackfillError

STATE_SCHEMA_VERSION = 1

_SOURCE_VERSION = re.compile(r"sv1_[0-9a-f]{64}")
_CLEAN_VERSION = re.compile(r"cv1_[0-9a-f]{64}")
_FACT_VERSION = re.compile(r"fv1_[0-9a-f]{64}")
_DATE_VERSION = re.compile(r"dv1_[0-9a-f]{64}")
_CONFIGURATION_FINGERPRINT = re.compile(r"cfg1_[0-9a-f]{64}")
_STATION_DAY_MART_VERSION = re.compile(r"msd1_[0-9a-f]{64}")
_OD_DAY_MART_VERSION = re.compile(r"mod1_[0-9a-f]{64}")
_QUALITY_MART_VERSION = re.compile(r"msq1_[0-9a-f]{64}")

_SOURCE_CHANGES = {
    SOURCE_NEW,
    SOURCE_UNCHANGED,
    SOURCE_REVISED,
    SOURCE_REMOVED,
    SOURCE_MISSING,
}
_MONTH_STATUSES = {MONTH_SUCCEEDED, MONTH_SKIPPED_UNCHANGED, MONTH_FAILED}
_RUN_STATUSES = {RUN_SUCCEEDED, RUN_PARTIAL_FAILURE, RUN_FAILED}
_PUBLISH_STATUSES = {"PUBLISHED_CURRENT", "REUSED_CURRENT"}
_CLEAN_RESULT_STATUSES = _PUBLISH_STATUSES | {"SKIPPED_UNCHANGED", "FAILED"}
_FACT_RESULT_STATUSES = _PUBLISH_STATUSES | {"FAILED"}
_MART_RESULT_STATUSES = _PUBLISH_STATUSES | {"FAILED"}
_DATE_RESULT_STATUSES = _PUBLISH_STATUSES


def backfill_plan_to_payload(plan: BackfillPlan) -> dict[str, Any]:
    """Serialize an immutable plan to a small explicit XCom-safe payload."""
    return {
        "schema_version": STATE_SCHEMA_VERSION,
        "kind": "backfill_plan",
        "start_month": plan.start_month,
        "end_month": plan.end_month,
        "dataset_id": plan.dataset_id,
        "transformation_version": plan.transformation_version,
        "configuration_fingerprint": plan.configuration_fingerprint,
        "months": [
            {
                "source_month": month.source_month,
                "source_change": month.source_change,
                "source_version_id": month.source_version_id,
                "previous_source_version_id": month.previous_source_version_id,
                "previous_clean_version_id": month.previous_clean_version_id,
                "desired_clean_version_id": month.desired_clean_version_id,
                "previous_fact_version_id": month.previous_fact_version_id,
            }
            for month in plan.months
        ],
    }


def backfill_plan_from_payload(payload: object) -> BackfillPlan:
    """Strictly restore a captured plan; malformed/tampered state fails closed."""
    data = _require_mapping(payload, "BackfillPlan payload")
    _require_exact_keys(
        data,
        {
            "schema_version",
            "kind",
            "start_month",
            "end_month",
            "dataset_id",
            "transformation_version",
            "configuration_fingerprint",
            "months",
        },
        "BackfillPlan payload",
    )
    _require_schema_and_kind(data, "backfill_plan")
    start_month = _require_month(data["start_month"], "start_month")
    end_month = _require_month(data["end_month"], "end_month")
    if start_month > end_month:
        raise BackfillError("BackfillPlan start_month must be <= end_month")
    dataset_id = _require_text(data["dataset_id"], "dataset_id")
    transformation_version = _require_text(
        data["transformation_version"], "transformation_version"
    )
    configuration_fingerprint = _require_pattern(
        data["configuration_fingerprint"],
        _CONFIGURATION_FINGERPRINT,
        "configuration_fingerprint",
    )
    months_raw = _require_list(data["months"], "months")
    if not months_raw:
        raise BackfillError("BackfillPlan months must not be empty")

    months: list[BackfillMonthPlan] = []
    previous_month: str | None = None
    for index, raw_month in enumerate(months_raw):
        label = f"months[{index}]"
        month_data = _require_mapping(raw_month, label)
        _require_exact_keys(
            month_data,
            {
                "source_month",
                "source_change",
                "source_version_id",
                "previous_source_version_id",
                "previous_clean_version_id",
                "desired_clean_version_id",
                "previous_fact_version_id",
            },
            label,
        )
        source_month = _require_month(month_data["source_month"], f"{label}.source_month")
        if source_month < start_month or source_month > end_month:
            raise BackfillError(f"{label}.source_month lies outside plan bounds")
        if previous_month is not None and source_month <= previous_month:
            raise BackfillError("BackfillPlan months must be strictly increasing")
        previous_month = source_month
        source_change = _require_enum(
            month_data["source_change"], _SOURCE_CHANGES, f"{label}.source_change"
        )
        source_version_id = _optional_pattern(
            month_data["source_version_id"], _SOURCE_VERSION, f"{label}.source_version_id"
        )
        previous_source_version_id = _optional_pattern(
            month_data["previous_source_version_id"],
            _SOURCE_VERSION,
            f"{label}.previous_source_version_id",
        )
        previous_clean_version_id = _optional_pattern(
            month_data["previous_clean_version_id"],
            _CLEAN_VERSION,
            f"{label}.previous_clean_version_id",
        )
        desired_clean_version_id = _optional_pattern(
            month_data["desired_clean_version_id"],
            _CLEAN_VERSION,
            f"{label}.desired_clean_version_id",
        )
        previous_fact_version_id = _optional_pattern(
            month_data["previous_fact_version_id"],
            _FACT_VERSION,
            f"{label}.previous_fact_version_id",
        )
        if source_change in {SOURCE_MISSING, SOURCE_REMOVED}:
            if source_version_id is not None or desired_clean_version_id is not None:
                raise BackfillError(
                    f"{label} unavailable source must not carry desired source/Clean ids"
                )
        elif source_version_id is None or desired_clean_version_id is None:
            raise BackfillError(
                f"{label} available source must carry source/Clean version ids"
            )
        months.append(
            BackfillMonthPlan(
                source_month=source_month,
                source_change=source_change,
                source_version_id=source_version_id,
                previous_source_version_id=previous_source_version_id,
                previous_clean_version_id=previous_clean_version_id,
                desired_clean_version_id=desired_clean_version_id,
                previous_fact_version_id=previous_fact_version_id,
            )
        )
    if months[0].source_month != start_month or months[-1].source_month != end_month:
        raise BackfillError(
            "BackfillPlan bounds must match the first/last captured source month"
        )
    return BackfillPlan(
        start_month=start_month,
        end_month=end_month,
        dataset_id=dataset_id,
        transformation_version=transformation_version,
        configuration_fingerprint=configuration_fingerprint,
        months=tuple(months),
    )


def backfill_run_result_to_payload(result: BackfillRunResult) -> dict[str, Any]:
    """Serialize one attempt result for downstream retry/final-status tasks."""
    return {
        "schema_version": STATE_SCHEMA_VERSION,
        "kind": "backfill_run_result",
        "status": result.status,
        "start_month": result.start_month,
        "end_month": result.end_month,
        "date_dimension_status": result.date_dimension_status,
        "date_version_id": result.date_version_id,
        "months": [
            {
                "source_month": month.source_month,
                "source_change": month.source_change,
                "source_version_id": month.source_version_id,
                "status": month.status,
                "clean_status": month.clean_status,
                "clean_version_id": month.clean_version_id,
                "fact_status": month.fact_status,
                "fact_version_id": month.fact_version_id,
                "quality_status": month.quality_status,
                "quality_mart_version_id": month.quality_mart_version_id,
                "od_day_status": month.od_day_status,
                "od_day_mart_version_id": month.od_day_mart_version_id,
                "station_day": [
                    {
                        "service_month": item.service_month,
                        "status": item.status,
                        "mart_version_id": item.mart_version_id,
                    }
                    for item in month.station_day
                ],
                "failures": [
                    {
                        "component": failure.component,
                        "target_month": failure.target_month,
                        "error_type": failure.error_type,
                        "message": failure.message,
                    }
                    for failure in month.failures
                ],
            }
            for month in result.months
        ],
    }


def backfill_run_result_from_payload(payload: object) -> BackfillRunResult:
    """Strictly restore one run attempt result from orchestration metadata."""
    data = _require_mapping(payload, "BackfillRunResult payload")
    _require_exact_keys(
        data,
        {
            "schema_version",
            "kind",
            "status",
            "start_month",
            "end_month",
            "date_dimension_status",
            "date_version_id",
            "months",
        },
        "BackfillRunResult payload",
    )
    _require_schema_and_kind(data, "backfill_run_result")
    status = _require_enum(data["status"], _RUN_STATUSES, "status")
    start_month = _require_month(data["start_month"], "start_month")
    end_month = _require_month(data["end_month"], "end_month")
    if start_month > end_month:
        raise BackfillError("BackfillRunResult start_month must be <= end_month")
    date_dimension_status = _require_enum(
        data["date_dimension_status"],
        _DATE_RESULT_STATUSES,
        "date_dimension_status",
    )
    date_version_id = _require_pattern(
        data["date_version_id"], _DATE_VERSION, "date_version_id"
    )
    months_raw = _require_list(data["months"], "months")
    if not months_raw:
        raise BackfillError("BackfillRunResult months must not be empty")
    months: list[BackfillMonthResult] = []
    previous_month: str | None = None
    for index, raw_month in enumerate(months_raw):
        label = f"months[{index}]"
        month_data = _require_mapping(raw_month, label)
        _require_exact_keys(
            month_data,
            {
                "source_month",
                "source_change",
                "source_version_id",
                "status",
                "clean_status",
                "clean_version_id",
                "fact_status",
                "fact_version_id",
                "quality_status",
                "quality_mart_version_id",
                "od_day_status",
                "od_day_mart_version_id",
                "station_day",
                "failures",
            },
            label,
        )
        source_month = _require_month(month_data["source_month"], f"{label}.source_month")
        if source_month < start_month or source_month > end_month:
            raise BackfillError(f"{label}.source_month lies outside result bounds")
        if previous_month is not None and source_month <= previous_month:
            raise BackfillError("BackfillRunResult months must be strictly increasing")
        previous_month = source_month
        source_change = _require_enum(
            month_data["source_change"], _SOURCE_CHANGES, f"{label}.source_change"
        )
        month_status = _require_enum(
            month_data["status"], _MONTH_STATUSES, f"{label}.status"
        )
        station_day = tuple(
            _station_day_from_payload(item, f"{label}.station_day[{station_index}]")
            for station_index, item in enumerate(
                _require_list(month_data["station_day"], f"{label}.station_day")
            )
        )
        failures = tuple(
            _failure_from_payload(item, f"{label}.failures[{failure_index}]")
            for failure_index, item in enumerate(
                _require_list(month_data["failures"], f"{label}.failures")
            )
        )
        if month_status == MONTH_FAILED and not failures:
            raise BackfillError(f"{label} FAILED result must carry failure evidence")
        clean_status = _optional_enum(
            month_data["clean_status"], _CLEAN_RESULT_STATUSES, f"{label}.clean_status"
        )
        fact_status = _optional_enum(
            month_data["fact_status"], _FACT_RESULT_STATUSES, f"{label}.fact_status"
        )
        quality_status = _optional_enum(
            month_data["quality_status"], _MART_RESULT_STATUSES, f"{label}.quality_status"
        )
        od_day_status = _optional_enum(
            month_data["od_day_status"], _MART_RESULT_STATUSES, f"{label}.od_day_status"
        )
        component_statuses = (
            clean_status,
            fact_status,
            quality_status,
            od_day_status,
        )
        if month_status != MONTH_FAILED:
            if failures:
                raise BackfillError(
                    f"{label} successful/skipped result must not carry failure evidence"
                )
            if any(status == "FAILED" for status in component_statuses):
                raise BackfillError(
                    f"{label} successful/skipped result must not contain FAILED component status"
                )
            required_success_ids = (
                month_data["source_version_id"],
                month_data["clean_version_id"],
                month_data["fact_version_id"],
                month_data["quality_mart_version_id"],
                month_data["od_day_mart_version_id"],
            )
            if any(value is None for value in required_success_ids):
                raise BackfillError(
                    f"{label} successful/skipped result is missing published lineage ids"
                )
            if not station_day:
                raise BackfillError(
                    f"{label} successful/skipped result must carry station-day evidence"
                )
            if any(item.status == "FAILED" for item in station_day):
                raise BackfillError(
                    f"{label} successful/skipped result has failed station-day evidence"
                )
        months.append(
            BackfillMonthResult(
                source_month=source_month,
                source_change=source_change,
                source_version_id=_optional_pattern(
                    month_data["source_version_id"],
                    _SOURCE_VERSION,
                    f"{label}.source_version_id",
                ),
                status=month_status,
                clean_status=clean_status,
                clean_version_id=_optional_pattern(
                    month_data["clean_version_id"],
                    _CLEAN_VERSION,
                    f"{label}.clean_version_id",
                ),
                fact_status=fact_status,
                fact_version_id=_optional_pattern(
                    month_data["fact_version_id"],
                    _FACT_VERSION,
                    f"{label}.fact_version_id",
                ),
                quality_status=quality_status,
                quality_mart_version_id=_optional_pattern(
                    month_data["quality_mart_version_id"],
                    _QUALITY_MART_VERSION,
                    f"{label}.quality_mart_version_id",
                ),
                od_day_status=od_day_status,
                od_day_mart_version_id=_optional_pattern(
                    month_data["od_day_mart_version_id"],
                    _OD_DAY_MART_VERSION,
                    f"{label}.od_day_mart_version_id",
                ),
                station_day=station_day,
                failures=failures,
            )
        )
    if months[0].source_month < start_month or months[-1].source_month > end_month:
        raise BackfillError("BackfillRunResult months lie outside result bounds")
    failed_count = sum(month.status == MONTH_FAILED for month in months)
    expected_status = (
        RUN_SUCCEEDED
        if failed_count == 0
        else RUN_FAILED
        if failed_count == len(months)
        else RUN_PARTIAL_FAILURE
    )
    if status != expected_status:
        raise BackfillError(
            "BackfillRunResult status does not reconcile with month statuses"
        )
    return BackfillRunResult(
        status=status,
        start_month=start_month,
        end_month=end_month,
        date_dimension_status=date_dimension_status,
        date_version_id=date_version_id,
        months=tuple(months),
    )


def _station_day_from_payload(payload: object, label: str) -> StationDayExecution:
    data = _require_mapping(payload, label)
    _require_exact_keys(data, {"service_month", "status", "mart_version_id"}, label)
    return StationDayExecution(
        service_month=_require_month(data["service_month"], f"{label}.service_month"),
        status=_require_enum(data["status"], _MART_RESULT_STATUSES, f"{label}.status"),
        mart_version_id=_optional_pattern(
            data["mart_version_id"],
            _STATION_DAY_MART_VERSION,
            f"{label}.mart_version_id",
        ),
    )


def _failure_from_payload(payload: object, label: str) -> BackfillFailure:
    data = _require_mapping(payload, label)
    _require_exact_keys(
        data, {"component", "target_month", "error_type", "message"}, label
    )
    return BackfillFailure(
        component=_require_text(data["component"], f"{label}.component"),
        target_month=_require_month(data["target_month"], f"{label}.target_month"),
        error_type=_require_text(data["error_type"], f"{label}.error_type"),
        message=_require_text(data["message"], f"{label}.message", allow_empty=True),
    )


def _require_schema_and_kind(data: Mapping[str, object], expected_kind: str) -> None:
    if data["schema_version"] != STATE_SCHEMA_VERSION:
        raise BackfillError(
            f"unsupported orchestration state schema_version {data['schema_version']!r}"
        )
    if data["kind"] != expected_kind:
        raise BackfillError(
            f"orchestration state kind must be {expected_kind!r}, got {data['kind']!r}"
        )


def _require_mapping(value: object, label: str) -> Mapping[str, object]:
    if not isinstance(value, Mapping):
        raise BackfillError(f"{label} must be an object")
    if not all(isinstance(key, str) for key in value):
        raise BackfillError(f"{label} keys must be strings")
    return value


def _require_exact_keys(
    value: Mapping[str, object], expected: set[str], label: str
) -> None:
    if set(value) != expected:
        raise BackfillError(
            f"{label} fields must be exactly {sorted(expected)!r}, got {sorted(value)!r}"
        )


def _require_list(value: object, label: str) -> list[object]:
    if not isinstance(value, list):
        raise BackfillError(f"{label} must be an array")
    return value


def _require_text(value: object, label: str, *, allow_empty: bool = False) -> str:
    if not isinstance(value, str) or (not allow_empty and not value):
        raise BackfillError(f"{label} must be a {'string' if allow_empty else 'non-empty string'}")
    return value


def _optional_text(value: object, label: str) -> str | None:
    if value is None:
        return None
    return _require_text(value, label)


def _optional_enum(
    value: object, allowed: set[str], label: str
) -> str | None:
    if value is None:
        return None
    return _require_enum(value, allowed, label)


def _require_enum(value: object, allowed: set[str], label: str) -> str:
    text = _require_text(value, label)
    if text not in allowed:
        raise BackfillError(f"{label} has unsupported value {text!r}")
    return text


def _require_pattern(value: object, pattern: re.Pattern[str], label: str) -> str:
    text = _require_text(value, label)
    if pattern.fullmatch(text) is None:
        raise BackfillError(f"{label} has invalid identifier format")
    return text


def _optional_pattern(
    value: object, pattern: re.Pattern[str], label: str
) -> str | None:
    if value is None:
        return None
    return _require_pattern(value, pattern, label)


def _require_month(value: object, label: str) -> str:
    text = _require_text(value, label)
    # Reuse the production-scope validator rather than silently accepting a
    # process-boundary payload that the business layer itself would reject.
    iter_months(text, text)
    return text
