"""Reusable month/range backfill workflow (PRD §11-13, §22-24).

The workflow is orchestration-independent: Airflow may call it later, but
transformation and data-quality logic remain in the existing business modules.
"""

from __future__ import annotations

import json
import re
from collections.abc import Callable, Mapping
from dataclasses import dataclass, replace
from pathlib import Path

from .clean_publish import (
    STATUS_REUSED_CURRENT as CLEAN_REUSED_CURRENT,
    clean_version_path,
    compute_clean_version_id,
    compute_configuration_fingerprint as compute_clean_configuration_fingerprint,
    current_pointer_path as clean_current_pointer_path,
    publish_clean_month,
    validate_clean_version,
)
from .date_dimension import publish_date_dimension
from .errors import BackfillError
from .fact_publish import (
    STATUS_REUSED_CURRENT as FACT_REUSED_CURRENT,
    fact_current_pointer_path,
    fact_version_path,
    publish_current_fact_trip_month,
    validate_fact_trip_version,
)
from .manifest import SourceRecord, load_manifest
from .od_day_mart import (
    STATUS_REUSED_CURRENT as OD_REUSED_CURRENT,
    publish_current_od_day_mart,
)
from .raw import verify_raw_artifact
from .source_month_quality_mart import (
    STATUS_REUSED_CURRENT as QUALITY_REUSED_CURRENT,
    publish_current_source_month_quality_mart,
)
from .station_day_mart import (
    STATUS_REUSED_CURRENT as STATION_DAY_REUSED_CURRENT,
    plan_station_day_fact_revision_impact,
    publish_current_station_day_mart,
)
from .zip_months import (
    STATUS_NEW as ZIP_NEW,
    STATUS_REMOVED as ZIP_REMOVED,
    STATUS_REVISED as ZIP_REVISED,
    STATUS_UNCHANGED as ZIP_UNCHANGED,
    MonthImpact,
    plan_zip_months,
)

PRODUCTION_START_MONTH = "2020-01"
PRODUCTION_END_MONTH = "2026-06"

SOURCE_NEW = "NEW"
SOURCE_UNCHANGED = "UNCHANGED"
SOURCE_REVISED = "REVISED"
SOURCE_REMOVED = "REMOVED"
SOURCE_MISSING = "MISSING"

CLEAN_SKIP_UNCHANGED = "SKIPPED_UNCHANGED"
STEP_FAILED = "FAILED"

MONTH_SUCCEEDED = "SUCCEEDED"
MONTH_SKIPPED_UNCHANGED = "SKIPPED_UNCHANGED"
MONTH_FAILED = "FAILED"

RUN_SUCCEEDED = "SUCCEEDED"
RUN_PARTIAL_FAILURE = "PARTIAL_FAILURE"
RUN_FAILED = "FAILED"

_MONTH = re.compile(r"20\d{2}-(?:0[1-9]|1[0-2])")
_YEAR = re.compile(r"20\d{2}")
_CLEAN_VERSION = re.compile(r"cv1_[0-9a-f]{64}")
_FACT_VERSION = re.compile(r"fv1_[0-9a-f]{64}")

BeforeStepHook = Callable[[str, str, str], None]


@dataclass(frozen=True)
class BackfillMonthPlan:
    source_month: str
    source_change: str
    source_version_id: str | None
    previous_source_version_id: str | None
    previous_clean_version_id: str | None
    desired_clean_version_id: str | None
    previous_fact_version_id: str | None


@dataclass(frozen=True)
class BackfillPlan:
    start_month: str
    end_month: str
    dataset_id: str
    transformation_version: str
    configuration_fingerprint: str
    months: tuple[BackfillMonthPlan, ...]


@dataclass(frozen=True)
class BackfillFailure:
    component: str
    target_month: str
    error_type: str
    message: str


@dataclass(frozen=True)
class StationDayExecution:
    service_month: str
    status: str
    mart_version_id: str | None


@dataclass(frozen=True)
class BackfillMonthResult:
    source_month: str
    source_change: str
    source_version_id: str | None
    status: str
    clean_status: str | None
    clean_version_id: str | None
    fact_status: str | None
    fact_version_id: str | None
    quality_status: str | None
    quality_mart_version_id: str | None
    od_day_status: str | None
    od_day_mart_version_id: str | None
    station_day: tuple[StationDayExecution, ...]
    failures: tuple[BackfillFailure, ...]


@dataclass(frozen=True)
class BackfillRunResult:
    status: str
    start_month: str
    end_month: str
    date_dimension_status: str
    date_version_id: str
    months: tuple[BackfillMonthResult, ...]

    @property
    def failed_source_months(self) -> tuple[str, ...]:
        return tuple(
            result.source_month
            for result in self.months
            if result.status == MONTH_FAILED
        )


def iter_months(start_month: str, end_month: str) -> tuple[str, ...]:
    """Return an inclusive deterministic month vector inside production scope."""
    start_month = _require_production_month(start_month, "start_month")
    end_month = _require_production_month(end_month, "end_month")
    if start_month > end_month:
        raise BackfillError("start_month must be <= end_month")
    year, month = (int(part) for part in start_month.split("-"))
    end_year, end_month_number = (int(part) for part in end_month.split("-"))
    values: list[str] = []
    while (year, month) <= (end_year, end_month_number):
        values.append(f"{year:04d}-{month:02d}")
        month += 1
        if month == 13:
            year += 1
            month = 1
    return tuple(values)


def plan_backfill_range(
    *,
    manifest: str | Path,
    raw_root: str | Path,
    station_root: str | Path,
    clean_root: str | Path,
    warehouse_root: str | Path,
    start_month: str,
    end_month: str,
    transformation_version: str,
    configuration: Mapping[str, object],
    dataset_id: str = "rental_history",
) -> BackfillPlan:
    """Resolve target source lineage and capture retry-critical pre-run state."""
    months = iter_months(start_month, end_month)
    if not isinstance(dataset_id, str) or not dataset_id:
        raise BackfillError("dataset_id must be a non-empty string")
    if not isinstance(transformation_version, str) or not transformation_version:
        raise BackfillError("transformation_version must be a non-empty string")
    canonical_configuration = _canonical_configuration(configuration)
    clean_configuration_fingerprint = compute_clean_configuration_fingerprint(
        canonical_configuration
    )
    verified_source_version_ids: set[str] = set()

    def verify_source_record_once(record: SourceRecord) -> None:
        if record.source_version_id in verified_source_version_ids:
            return
        _verify_source_record(raw_root, record)
        verified_source_version_ids.add(record.source_version_id)

    records = load_manifest(manifest)
    indexed_records = [
        (index, record)
        for index, record in enumerate(records)
        if record.dataset_id == dataset_id
    ]
    by_id = {
        record.source_version_id: (index, record)
        for index, record in indexed_records
    }
    latest_monthly: dict[str, tuple[int, SourceRecord]] = {}
    latest_yearly: dict[str, tuple[int, SourceRecord]] = {}
    for index, record in indexed_records:
        if _MONTH.fullmatch(record.logical_period):
            latest_monthly[record.logical_period] = (index, record)
        elif _YEAR.fullmatch(record.logical_period):
            latest_yearly[record.logical_period] = (index, record)

    requested_years = {month[:4] for month in months}
    yearly_impacts: dict[str, dict[str, MonthImpact]] = {}
    for year, (_, record) in latest_yearly.items():
        if year not in requested_years:
            continue
        verify_source_record_once(record)
        zip_plan = plan_zip_months(
            manifest=manifest,
            raw_root=raw_root,
            current_source_version_id=record.source_version_id,
        )
        yearly_impacts[year] = {
            impact.logical_month: impact for impact in zip_plan.impacts
        }

    planned_months: list[BackfillMonthPlan] = []
    for source_month in months:
        (
            current,
            previous_fact_version_id,
        ) = _capture_current_month_lineage(
            manifest=manifest,
            raw_root=raw_root,
            station_root=station_root,
            clean_root=clean_root,
            warehouse_root=warehouse_root,
            source_month=source_month,
        )
        previous_source_version_id = (
            current[1]["source_version_id"] if current is not None else None
        )
        previous_clean_version_id = current[0] if current is not None else None

        monthly_candidate = latest_monthly.get(source_month)
        yearly_candidate = latest_yearly.get(source_month[:4])
        yearly_impact = (
            yearly_impacts.get(source_month[:4], {}).get(source_month)
            if yearly_candidate is not None
            else None
        )
        if monthly_candidate is not None and yearly_impact is not None:
            raise BackfillError(
                f"{source_month}: both monthly artifact and yearly ZIP member are "
                "registered; source precedence is ambiguous and must not be guessed"
            )

        source_change: str
        target_record: SourceRecord | None = None
        if monthly_candidate is not None:
            _, target_record = monthly_candidate
            verify_source_record_once(target_record)
            source_change = _source_change_for_direct_candidate(
                source_month=source_month,
                target_record=target_record,
                previous_source_version_id=previous_source_version_id,
                indexed_by_id=by_id,
            )
        elif yearly_candidate is not None:
            _, latest_year_record = yearly_candidate
            if previous_source_version_id is None:
                if yearly_impact is None:
                    source_change = SOURCE_MISSING
                else:
                    target_record = latest_year_record
                    source_change = SOURCE_NEW
            else:
                previous_entry = by_id.get(previous_source_version_id)
                if previous_entry is None:
                    raise BackfillError(
                        f"{source_month}: current Clean source_version_id "
                        f"{previous_source_version_id} is absent from manifest"
                    )
                _, previous_record = previous_entry
                if previous_record.source_version_id == latest_year_record.source_version_id:
                    if yearly_impact is None:
                        raise BackfillError(
                            f"{source_month}: current yearly source no longer maps the month"
                        )
                    target_record = previous_record
                    source_change = SOURCE_UNCHANGED
                elif (
                    previous_record.dataset_id == dataset_id
                    and previous_record.logical_period == source_month[:4]
                ):
                    comparison = plan_zip_months(
                        manifest=manifest,
                        raw_root=raw_root,
                        current_source_version_id=latest_year_record.source_version_id,
                        previous_source_version_id=previous_record.source_version_id,
                    )
                    impact = _impact_for_month(comparison.impacts, source_month)
                    if impact.status == ZIP_UNCHANGED:
                        target_record = previous_record
                        source_change = SOURCE_UNCHANGED
                    elif impact.status in {ZIP_NEW, ZIP_REVISED}:
                        target_record = latest_year_record
                        source_change = SOURCE_REVISED
                    elif impact.status == ZIP_REMOVED:
                        source_change = SOURCE_REMOVED
                    else:  # pragma: no cover - zip_months owns the status domain.
                        raise BackfillError(
                            f"{source_month}: unsupported ZIP impact status {impact.status!r}"
                        )
                elif yearly_impact is None:
                    source_change = SOURCE_REMOVED
                else:
                    target_record = latest_year_record
                    source_change = SOURCE_REVISED
        else:
            source_change = SOURCE_MISSING

        source_version_id = (
            target_record.source_version_id if target_record is not None else None
        )
        desired_clean_version_id: str | None = None
        if target_record is not None:
            verify_source_record_once(target_record)
            desired_clean_version_id = compute_clean_version_id(
                source_month=source_month,
                source_version_id=target_record.source_version_id,
                transformation_version=transformation_version,
                configuration_fingerprint=clean_configuration_fingerprint,
            )

        planned_months.append(
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

    return BackfillPlan(
        start_month=start_month,
        end_month=end_month,
        dataset_id=dataset_id,
        transformation_version=transformation_version,
        configuration_fingerprint=clean_configuration_fingerprint,
        months=tuple(planned_months),
    )


def retry_failed_months_plan(
    plan: BackfillPlan,
    previous_result: BackfillRunResult,
) -> BackfillPlan:
    """Return the exact original plans for source months that failed previously."""
    failed = set(previous_result.failed_source_months)
    selected = tuple(month for month in plan.months if month.source_month in failed)
    if not selected:
        raise BackfillError("previous result has no failed source months to retry")
    return BackfillPlan(
        start_month=selected[0].source_month,
        end_month=selected[-1].source_month,
        dataset_id=plan.dataset_id,
        transformation_version=plan.transformation_version,
        configuration_fingerprint=plan.configuration_fingerprint,
        months=selected,
    )


def execute_backfill_plan(
    plan: BackfillPlan,
    *,
    manifest: str | Path,
    raw_root: str | Path,
    station_root: str | Path,
    clean_root: str | Path,
    warehouse_root: str | Path,
    configuration: Mapping[str, object],
    before_step: BeforeStepHook | None = None,
) -> BackfillRunResult:
    """Execute an immutable plan while preserving successful month publications."""
    canonical_configuration = _canonical_configuration(configuration)
    execution_configuration_fingerprint = compute_clean_configuration_fingerprint(
        canonical_configuration
    )
    if execution_configuration_fingerprint != plan.configuration_fingerprint:
        raise BackfillError(
            "execution configuration does not match the captured BackfillPlan"
        )
    date_result = publish_date_dimension(
        warehouse_root=warehouse_root,
        transformation_version=plan.transformation_version,
        configuration=canonical_configuration,
    )
    results = [
        _execute_source_month(
            month_plan,
            manifest=manifest,
            raw_root=raw_root,
            station_root=station_root,
            clean_root=clean_root,
            warehouse_root=warehouse_root,
            transformation_version=plan.transformation_version,
            configuration=canonical_configuration,
            before_step=before_step,
        )
        for month_plan in plan.months
    ]
    results = _publish_station_day_phase(
        plan=plan,
        results=results,
        manifest=manifest,
        raw_root=raw_root,
        station_root=station_root,
        clean_root=clean_root,
        warehouse_root=warehouse_root,
        configuration=canonical_configuration,
        before_step=before_step,
    )
    failed_count = sum(result.status == MONTH_FAILED for result in results)
    if failed_count == 0:
        status = RUN_SUCCEEDED
    elif failed_count == len(results):
        status = RUN_FAILED
    else:
        status = RUN_PARTIAL_FAILURE
    return BackfillRunResult(
        status=status,
        start_month=plan.start_month,
        end_month=plan.end_month,
        date_dimension_status=date_result.status,
        date_version_id=date_result.date_version_id,
        months=tuple(results),
    )


def run_backfill_range(
    *,
    manifest: str | Path,
    raw_root: str | Path,
    station_root: str | Path,
    clean_root: str | Path,
    warehouse_root: str | Path,
    start_month: str,
    end_month: str,
    transformation_version: str,
    configuration: Mapping[str, object],
    dataset_id: str = "rental_history",
    before_step: BeforeStepHook | None = None,
) -> tuple[BackfillPlan, BackfillRunResult]:
    """Plan and execute one month/range backfill through the business layer."""
    plan = plan_backfill_range(
        manifest=manifest,
        raw_root=raw_root,
        station_root=station_root,
        clean_root=clean_root,
        warehouse_root=warehouse_root,
        start_month=start_month,
        end_month=end_month,
        transformation_version=transformation_version,
        configuration=configuration,
        dataset_id=dataset_id,
    )
    result = execute_backfill_plan(
        plan,
        manifest=manifest,
        raw_root=raw_root,
        station_root=station_root,
        clean_root=clean_root,
        warehouse_root=warehouse_root,
        configuration=configuration,
        before_step=before_step,
    )
    return plan, result


def _execute_source_month(
    plan: BackfillMonthPlan,
    *,
    manifest: str | Path,
    raw_root: str | Path,
    station_root: str | Path,
    clean_root: str | Path,
    warehouse_root: str | Path,
    transformation_version: str,
    configuration: Mapping[str, object],
    before_step: BeforeStepHook | None,
) -> BackfillMonthResult:
    failures: list[BackfillFailure] = []
    clean_status: str | None = None
    clean_version_id: str | None = None
    fact_status: str | None = None
    fact_version_id: str | None = None
    quality_status: str | None = None
    quality_mart_version_id: str | None = None
    od_status: str | None = None
    od_mart_version_id: str | None = None

    if plan.source_change in {SOURCE_MISSING, SOURCE_REMOVED} or plan.source_version_id is None:
        failure = BackfillFailure(
            component="source_plan",
            target_month=plan.source_month,
            error_type="SourceUnavailable",
            message=(
                "no registered source covers this production month"
                if plan.source_change == SOURCE_MISSING
                else "latest yearly source revision removed this month; previous current is preserved"
            ),
        )
        return _month_result(
            plan,
            status=MONTH_FAILED,
            failures=(failure,),
        )

    try:
        _call_hook(before_step, "clean", plan.source_month, plan.source_month)
        if _current_clean_is_desired(
            clean_root=clean_root,
            source_month=plan.source_month,
            desired_clean_version_id=plan.desired_clean_version_id,
        ):
            assert plan.desired_clean_version_id is not None
            clean_status = CLEAN_SKIP_UNCHANGED
            clean_version_id = plan.desired_clean_version_id
        else:
            clean_result = publish_clean_month(
                manifest=manifest,
                raw_root=raw_root,
                clean_root=clean_root,
                source_version_id=plan.source_version_id,
                source_month=plan.source_month,
                transformation_version=transformation_version,
                configuration=configuration,
            )
            clean_status = clean_result.status
            clean_version_id = clean_result.clean_version_id
        if clean_version_id != plan.desired_clean_version_id:
            raise BackfillError(
                f"{plan.source_month}: Clean publisher did not materialize the "
                "captured desired_clean_version_id"
            )
        _require_current_clean_version(
            clean_root=clean_root,
            source_month=plan.source_month,
            expected_clean_version_id=plan.desired_clean_version_id,
        )
    except Exception as exc:
        failures.append(_failure("clean", plan.source_month, exc))
        return _month_result(
            plan,
            status=MONTH_FAILED,
            clean_status=STEP_FAILED,
            failures=tuple(failures),
        )

    try:
        _call_hook(before_step, "fact", plan.source_month, plan.source_month)
        fact_result = publish_current_fact_trip_month(
            manifest=manifest,
            raw_root=raw_root,
            station_root=station_root,
            clean_root=clean_root,
            warehouse_root=warehouse_root,
            source_month=plan.source_month,
            transformation_version=transformation_version,
            configuration=configuration,
            expected_clean_version_id=plan.desired_clean_version_id,
            expected_source_version_id=plan.source_version_id,
        )
        fact_status = fact_result.status
        fact_version_id = fact_result.fact_version_id
        if fact_result.clean_version_id != plan.desired_clean_version_id:
            raise BackfillError(
                f"{plan.source_month}: trusted fact lineage does not match the "
                "captured desired Clean version"
            )
        if fact_result.source_version_id != plan.source_version_id:
            raise BackfillError(
                f"{plan.source_month}: trusted fact source lineage does not match "
                "the captured source version"
            )
    except Exception as exc:
        failures.append(_failure("fact", plan.source_month, exc))
        return _month_result(
            plan,
            status=MONTH_FAILED,
            clean_status=clean_status,
            clean_version_id=clean_version_id,
            fact_status=STEP_FAILED,
            failures=tuple(failures),
        )

    try:
        _call_hook(
            before_step,
            "source_month_quality",
            plan.source_month,
            plan.source_month,
        )
        _require_current_execution_lineage(
            clean_root=clean_root,
            warehouse_root=warehouse_root,
            source_month=plan.source_month,
            expected_clean_version_id=plan.desired_clean_version_id,
            expected_fact_version_id=fact_version_id,
        )
        quality_result = publish_current_source_month_quality_mart(
            manifest=manifest,
            raw_root=raw_root,
            station_root=station_root,
            clean_root=clean_root,
            warehouse_root=warehouse_root,
            source_month=plan.source_month,
            transformation_version=transformation_version,
            configuration=configuration,
        )
        quality_status = quality_result.status
        quality_mart_version_id = quality_result.mart_version_id
        _require_current_execution_lineage(
            clean_root=clean_root,
            warehouse_root=warehouse_root,
            source_month=plan.source_month,
            expected_clean_version_id=plan.desired_clean_version_id,
            expected_fact_version_id=fact_version_id,
        )
    except Exception as exc:
        quality_status = STEP_FAILED
        failures.append(_failure("source_month_quality", plan.source_month, exc))
        if not _execution_lineage_matches(
            clean_root=clean_root,
            warehouse_root=warehouse_root,
            source_month=plan.source_month,
            expected_clean_version_id=plan.desired_clean_version_id,
            expected_fact_version_id=fact_version_id,
        ):
            return _month_result(
                plan,
                status=MONTH_FAILED,
                clean_status=clean_status,
                clean_version_id=clean_version_id,
                fact_status=fact_status,
                fact_version_id=fact_version_id,
                quality_status=quality_status,
                quality_mart_version_id=quality_mart_version_id,
                failures=tuple(failures),
            )

    try:
        _call_hook(before_step, "od_day", plan.source_month, plan.source_month)
        _require_current_execution_lineage(
            clean_root=clean_root,
            warehouse_root=warehouse_root,
            source_month=plan.source_month,
            expected_clean_version_id=plan.desired_clean_version_id,
            expected_fact_version_id=fact_version_id,
        )
        od_result = publish_current_od_day_mart(
            manifest=manifest,
            raw_root=raw_root,
            station_root=station_root,
            clean_root=clean_root,
            warehouse_root=warehouse_root,
            service_month=plan.source_month,
            transformation_version=transformation_version,
            configuration=configuration,
        )
        od_status = od_result.status
        od_mart_version_id = od_result.mart_version_id
        _require_current_execution_lineage(
            clean_root=clean_root,
            warehouse_root=warehouse_root,
            source_month=plan.source_month,
            expected_clean_version_id=plan.desired_clean_version_id,
            expected_fact_version_id=fact_version_id,
        )
    except Exception as exc:
        od_status = STEP_FAILED
        failures.append(_failure("od_day", plan.source_month, exc))

    return _month_result(
        plan,
        status=MONTH_FAILED if failures else MONTH_SUCCEEDED,
        clean_status=clean_status,
        clean_version_id=clean_version_id,
        fact_status=fact_status,
        fact_version_id=fact_version_id,
        quality_status=quality_status,
        quality_mart_version_id=quality_mart_version_id,
        od_day_status=od_status,
        od_day_mart_version_id=od_mart_version_id,
        failures=tuple(failures),
    )


def _publish_station_day_phase(
    *,
    plan: BackfillPlan,
    results: list[BackfillMonthResult],
    manifest: str | Path,
    raw_root: str | Path,
    station_root: str | Path,
    clean_root: str | Path,
    warehouse_root: str | Path,
    configuration: Mapping[str, object],
    before_step: BeforeStepHook | None,
) -> list[BackfillMonthResult]:
    target_to_indexes: dict[str, list[int]] = {}
    captured_current_fact_ids: dict[int, str] = {}
    for index, (month_plan, result) in enumerate(zip(plan.months, results, strict=True)):
        if result.fact_version_id is None:
            continue
        old_fact_version_id = (
            month_plan.previous_fact_version_id or result.fact_version_id
        )
        current_fact_version_id = _load_current_fact_version_id(
            warehouse_root=warehouse_root,
            source_month=month_plan.source_month,
        )
        if current_fact_version_id is None:
            failure = BackfillFailure(
                component="station_day_plan",
                target_month=month_plan.source_month,
                error_type="FactUnavailable",
                message="current trusted fact disappeared before station-day planning",
            )
            results[index] = replace(
                result,
                status=MONTH_FAILED,
                failures=result.failures + (failure,),
            )
            continue
        captured_current_fact_ids[index] = current_fact_version_id
        candidate_new_fact_ids = [result.fact_version_id]
        if current_fact_version_id != result.fact_version_id:
            candidate_new_fact_ids.append(current_fact_version_id)
            failure = BackfillFailure(
                component="fact_superseded",
                target_month=month_plan.source_month,
                error_type="BackfillSuperseded",
                message=(
                    "planned trusted fact was superseded before station-day "
                    "reconciliation; current fact impact will be reconciled but "
                    "the source month remains failed and must be replanned"
                ),
            )
            results[index] = replace(
                result,
                status=MONTH_FAILED,
                failures=result.failures + (failure,),
            )
        try:
            affected_service_months: set[str] = set()
            for new_fact_version_id in candidate_new_fact_ids:
                impact = plan_station_day_fact_revision_impact(
                    manifest=manifest,
                    raw_root=raw_root,
                    station_root=station_root,
                    clean_root=clean_root,
                    warehouse_root=warehouse_root,
                    source_month=month_plan.source_month,
                    old_fact_version_id=old_fact_version_id,
                    new_fact_version_id=new_fact_version_id,
                )
                affected_service_months.update(impact.affected_service_months)
        except Exception as exc:
            failure = _failure("station_day_plan", month_plan.source_month, exc)
            results[index] = replace(
                result,
                status=MONTH_FAILED,
                failures=result.failures + (failure,),
            )
            continue
        for service_month in sorted(affected_service_months):
            if (
                PRODUCTION_START_MONTH
                <= service_month
                <= PRODUCTION_END_MONTH
            ):
                target_to_indexes.setdefault(service_month, []).append(index)

    for service_month in sorted(target_to_indexes):
        origin_indexes = target_to_indexes[service_month]
        try:
            first_source_month = plan.months[origin_indexes[0]].source_month
            _call_hook(
                before_step,
                "station_day",
                first_source_month,
                service_month,
            )
            station_result = publish_current_station_day_mart(
                manifest=manifest,
                raw_root=raw_root,
                station_root=station_root,
                clean_root=clean_root,
                warehouse_root=warehouse_root,
                service_month=service_month,
                transformation_version=plan.transformation_version,
                configuration=configuration,
            )
            execution = StationDayExecution(
                service_month=service_month,
                status=station_result.status,
                mart_version_id=station_result.mart_version_id,
            )
            for index in origin_indexes:
                result = results[index]
                results[index] = replace(
                    result,
                    station_day=result.station_day + (execution,),
                )
        except Exception as exc:
            failure = _failure("station_day", service_month, exc)
            execution = StationDayExecution(
                service_month=service_month,
                status=STEP_FAILED,
                mart_version_id=None,
            )
            for index in origin_indexes:
                result = results[index]
                results[index] = replace(
                    result,
                    status=MONTH_FAILED,
                    station_day=result.station_day + (execution,),
                    failures=result.failures + (failure,),
                )

    for index, captured_fact_version_id in captured_current_fact_ids.items():
        month_plan = plan.months[index]
        latest_fact_version_id = _load_current_fact_version_id(
            warehouse_root=warehouse_root,
            source_month=month_plan.source_month,
        )
        if latest_fact_version_id != captured_fact_version_id:
            result = results[index]
            failure = BackfillFailure(
                component="fact_superseded",
                target_month=month_plan.source_month,
                error_type="BackfillSuperseded",
                message=(
                    "current trusted fact changed during station-day reconciliation; "
                    "the source month must be replanned"
                ),
            )
            results[index] = replace(
                result,
                status=MONTH_FAILED,
                failures=result.failures + (failure,),
            )

    finalized: list[BackfillMonthResult] = []
    for month_plan, result in zip(plan.months, results, strict=True):
        if result.status == MONTH_FAILED:
            finalized.append(result)
            continue
        if _is_unchanged_reuse(month_plan, result):
            finalized.append(replace(result, status=MONTH_SKIPPED_UNCHANGED))
        else:
            finalized.append(result)
    return finalized


def _source_change_for_direct_candidate(
    *,
    source_month: str,
    target_record: SourceRecord,
    previous_source_version_id: str | None,
    indexed_by_id: Mapping[str, tuple[int, SourceRecord]],
) -> str:
    if previous_source_version_id is None:
        return SOURCE_NEW
    previous_entry = indexed_by_id.get(previous_source_version_id)
    if previous_entry is None:
        raise BackfillError(
            f"{source_month}: current Clean source_version_id "
            f"{previous_source_version_id} is absent from manifest"
        )
    previous_index, previous_record = previous_entry
    target_index, _ = indexed_by_id[target_record.source_version_id]
    if previous_record.source_version_id == target_record.source_version_id:
        return SOURCE_UNCHANGED
    if previous_record.dataset_id != target_record.dataset_id:
        raise BackfillError(
            f"{source_month}: current and target source datasets differ"
        )
    if previous_index > target_index:
        raise BackfillError(
            f"{source_month}: target source version is older than current Clean lineage"
        )
    return SOURCE_REVISED


def _impact_for_month(
    impacts: tuple[MonthImpact, ...], source_month: str
) -> MonthImpact:
    matching = [impact for impact in impacts if impact.logical_month == source_month]
    if len(matching) != 1:
        raise BackfillError(
            f"{source_month}: yearly revision comparison did not produce exactly one impact"
        )
    return matching[0]


def _load_current_clean_state(
    *,
    clean_root: str | Path,
    source_month: str,
) -> tuple[str, dict[str, object]] | None:
    path = clean_current_pointer_path(clean_root, source_month)
    if not path.exists():
        return None
    payload = _load_json(path, "current Clean pointer")
    if set(payload) != {"schema_version", "source_month", "clean_version_id"}:
        raise BackfillError(f"current Clean pointer {path} has invalid fields")
    if payload["schema_version"] != 1 or payload["source_month"] != source_month:
        raise BackfillError(
            f"current Clean pointer {path} has invalid version/month binding"
        )
    clean_version_id = payload["clean_version_id"]
    if (
        not isinstance(clean_version_id, str)
        or _CLEAN_VERSION.fullmatch(clean_version_id) is None
    ):
        raise BackfillError(
            f"current Clean pointer {path} has invalid clean_version_id"
        )
    metadata = validate_clean_version(
        clean_version_path(clean_root, source_month, clean_version_id)
    )
    if (
        metadata["source_month"] != source_month
        or metadata["clean_version_id"] != clean_version_id
    ):
        raise BackfillError(
            f"current Clean pointer {path} does not bind its immutable version"
        )
    return clean_version_id, metadata


def _load_current_fact_version_id(
    *,
    warehouse_root: str | Path,
    source_month: str,
) -> str | None:
    path = fact_current_pointer_path(warehouse_root, source_month)
    if not path.exists():
        return None
    payload = _load_json(path, "current trusted fact pointer")
    if set(payload) != {"schema_version", "source_month", "fact_version_id"}:
        raise BackfillError(
            f"current trusted fact pointer {path} has invalid fields"
        )
    if payload["schema_version"] != 1 or payload["source_month"] != source_month:
        raise BackfillError(
            f"current trusted fact pointer {path} has invalid version/month binding"
        )
    fact_version_id = payload["fact_version_id"]
    if (
        not isinstance(fact_version_id, str)
        or _FACT_VERSION.fullmatch(fact_version_id) is None
    ):
        raise BackfillError(
            f"current trusted fact pointer {path} has invalid fact_version_id"
        )
    return fact_version_id


def _capture_current_month_lineage(
    *,
    manifest: str | Path,
    raw_root: str | Path,
    station_root: str | Path,
    clean_root: str | Path,
    warehouse_root: str | Path,
    source_month: str,
) -> tuple[tuple[str, dict[str, object]] | None, str | None]:
    """Capture stable current Clean and previous-fact baselines for planning.

    Clean and fact are published independently, so a stable state where Clean
    has advanced and fact still points to the previous valid lineage is a
    recoverable downstream-failure state. Preserve that fact as the old
    station-day baseline. What is forbidden is silently mixing observations
    from different instants: after validating any current fact, re-read both
    mutable pointers and fail planning if either changed during capture.
    """
    clean_before = _load_current_clean_state(
        clean_root=clean_root,
        source_month=source_month,
    )
    fact_before = _load_current_fact_version_id(
        warehouse_root=warehouse_root,
        source_month=source_month,
    )

    if fact_before is not None:
        validate_fact_trip_version(
            fact_version_path(warehouse_root, source_month, fact_before),
            manifest=manifest,
            raw_root=raw_root,
            station_root=station_root,
            clean_root=clean_root,
            warehouse_root=warehouse_root,
        )

    clean_after = _load_current_clean_state(
        clean_root=clean_root,
        source_month=source_month,
    )
    fact_after = _load_current_fact_version_id(
        warehouse_root=warehouse_root,
        source_month=source_month,
    )
    if clean_after != clean_before or fact_after != fact_before:
        raise BackfillError(
            f"{source_month}: current Clean/fact lineage changed during backfill "
            "planning; retry planning"
        )
    return clean_before, fact_before


def _require_current_clean_version(
    *,
    clean_root: str | Path,
    source_month: str,
    expected_clean_version_id: str | None,
) -> None:
    if expected_clean_version_id is None:
        raise BackfillError(
            f"{source_month}: backfill plan has no desired Clean version"
        )
    current = _load_current_clean_state(
        clean_root=clean_root,
        source_month=source_month,
    )
    if current is None or current[0] != expected_clean_version_id:
        raise BackfillError(
            f"{source_month}: captured Clean target is no longer current; "
            "the backfill plan was superseded"
        )


def _require_current_execution_lineage(
    *,
    clean_root: str | Path,
    warehouse_root: str | Path,
    source_month: str,
    expected_clean_version_id: str | None,
    expected_fact_version_id: str | None,
) -> None:
    _require_current_clean_version(
        clean_root=clean_root,
        source_month=source_month,
        expected_clean_version_id=expected_clean_version_id,
    )
    if expected_fact_version_id is None:
        raise BackfillError(
            f"{source_month}: backfill execution has no expected trusted fact version"
        )
    current_fact_version_id = _load_current_fact_version_id(
        warehouse_root=warehouse_root,
        source_month=source_month,
    )
    if current_fact_version_id != expected_fact_version_id:
        raise BackfillError(
            f"{source_month}: planned trusted fact is no longer current; "
            "the backfill plan was superseded"
        )


def _execution_lineage_matches(
    *,
    clean_root: str | Path,
    warehouse_root: str | Path,
    source_month: str,
    expected_clean_version_id: str | None,
    expected_fact_version_id: str | None,
) -> bool:
    try:
        _require_current_execution_lineage(
            clean_root=clean_root,
            warehouse_root=warehouse_root,
            source_month=source_month,
            expected_clean_version_id=expected_clean_version_id,
            expected_fact_version_id=expected_fact_version_id,
        )
    except Exception:
        return False
    return True


def _current_clean_is_desired(
    *,
    clean_root: str | Path,
    source_month: str,
    desired_clean_version_id: str | None,
) -> bool:
    if desired_clean_version_id is None:
        return False
    current = _load_current_clean_state(
        clean_root=clean_root,
        source_month=source_month,
    )
    return current is not None and current[0] == desired_clean_version_id


def _verify_source_record(raw_root: str | Path, record: SourceRecord) -> None:
    verify_raw_artifact(
        raw_root=raw_root,
        source_version_id=record.source_version_id,
        expected_size=record.size_bytes,
        expected_sha256=record.sha256,
    )


def _month_result(
    plan: BackfillMonthPlan,
    *,
    status: str,
    clean_status: str | None = None,
    clean_version_id: str | None = None,
    fact_status: str | None = None,
    fact_version_id: str | None = None,
    quality_status: str | None = None,
    quality_mart_version_id: str | None = None,
    od_day_status: str | None = None,
    od_day_mart_version_id: str | None = None,
    station_day: tuple[StationDayExecution, ...] = (),
    failures: tuple[BackfillFailure, ...] = (),
) -> BackfillMonthResult:
    return BackfillMonthResult(
        source_month=plan.source_month,
        source_change=plan.source_change,
        source_version_id=plan.source_version_id,
        status=status,
        clean_status=clean_status,
        clean_version_id=clean_version_id,
        fact_status=fact_status,
        fact_version_id=fact_version_id,
        quality_status=quality_status,
        quality_mart_version_id=quality_mart_version_id,
        od_day_status=od_day_status,
        od_day_mart_version_id=od_day_mart_version_id,
        station_day=station_day,
        failures=failures,
    )


def _failure(
    component: str,
    target_month: str,
    exc: Exception,
) -> BackfillFailure:
    return BackfillFailure(
        component=component,
        target_month=target_month,
        error_type=type(exc).__name__,
        message=str(exc),
    )


def _call_hook(
    hook: BeforeStepHook | None,
    component: str,
    source_month: str,
    target_month: str,
) -> None:
    if hook is not None:
        hook(component, source_month, target_month)


def _is_unchanged_reuse(
    plan: BackfillMonthPlan,
    result: BackfillMonthResult,
) -> bool:
    if plan.source_change != SOURCE_UNCHANGED:
        return False
    if result.clean_status not in {CLEAN_SKIP_UNCHANGED, CLEAN_REUSED_CURRENT}:
        return False
    if result.fact_status != FACT_REUSED_CURRENT:
        return False
    if result.quality_status != QUALITY_REUSED_CURRENT:
        return False
    if result.od_day_status != OD_REUSED_CURRENT:
        return False
    return bool(result.station_day) and all(
        item.status == STATION_DAY_REUSED_CURRENT for item in result.station_day
    )


def _canonical_configuration(
    configuration: Mapping[str, object],
) -> dict[str, object]:
    try:
        canonical_text = json.dumps(
            configuration,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        )
        payload = json.loads(canonical_text)
    except (TypeError, ValueError) as exc:
        raise BackfillError(
            "configuration must be a JSON-compatible mapping without NaN/Infinity"
        ) from exc
    if not isinstance(payload, dict):
        raise BackfillError("configuration must be a JSON object")
    return payload


def _load_json(path: Path, label: str) -> dict[str, object]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise BackfillError(f"cannot read {label} {path}: {exc}") from exc
    if not isinstance(payload, dict):
        raise BackfillError(f"{label} {path} must be a JSON object")
    return payload


def _require_production_month(value: object, label: str) -> str:
    if not isinstance(value, str) or _MONTH.fullmatch(value) is None:
        raise BackfillError(f"{label} must be YYYY-MM")
    if value < PRODUCTION_START_MONTH or value > PRODUCTION_END_MONTH:
        raise BackfillError(
            f"{label} must be within "
            f"{PRODUCTION_START_MONTH}..{PRODUCTION_END_MONTH}"
        )
    return value
