from __future__ import annotations

import copy

import pytest

from seoul_bike_pipeline.backfill import (
    MONTH_FAILED,
    MONTH_SUCCEEDED,
    RUN_PARTIAL_FAILURE,
    SOURCE_NEW,
    SOURCE_REVISED,
    BackfillFailure,
    BackfillMonthPlan,
    BackfillMonthResult,
    BackfillPlan,
    BackfillRunResult,
    StationDayExecution,
)
from seoul_bike_pipeline.backfill_state import (
    backfill_plan_from_payload,
    backfill_plan_to_payload,
    backfill_run_result_from_payload,
    backfill_run_result_to_payload,
)
from seoul_bike_pipeline.errors import BackfillError


def _id(prefix: str, char: str) -> str:
    return prefix + char * 64


def test_backfill_plan_payload_round_trip_preserves_retry_baseline():
    plan = BackfillPlan(
        start_month="2020-01",
        end_month="2020-03",
        dataset_id="rental_history",
        transformation_version="git-abc",
        configuration_fingerprint=_id("cfg1_", "a"),
        months=(
            BackfillMonthPlan(
                source_month="2020-01",
                source_change=SOURCE_REVISED,
                source_version_id=_id("sv1_", "b"),
                previous_source_version_id=_id("sv1_", "c"),
                previous_clean_version_id=_id("cv1_", "d"),
                desired_clean_version_id=_id("cv1_", "e"),
                previous_fact_version_id=_id("fv1_", "f"),
            ),
            BackfillMonthPlan(
                source_month="2020-03",
                source_change=SOURCE_NEW,
                source_version_id=_id("sv1_", "1"),
                previous_source_version_id=None,
                previous_clean_version_id=None,
                desired_clean_version_id=_id("cv1_", "2"),
                previous_fact_version_id=None,
            ),
        ),
    )

    payload = backfill_plan_to_payload(plan)
    restored = backfill_plan_from_payload(payload)

    assert restored == plan
    assert restored.months[0].previous_fact_version_id == _id("fv1_", "f")


def test_backfill_plan_payload_rejects_tampered_state():
    plan = BackfillPlan(
        start_month="2020-01",
        end_month="2020-01",
        dataset_id="rental_history",
        transformation_version="git-abc",
        configuration_fingerprint=_id("cfg1_", "a"),
        months=(
            BackfillMonthPlan(
                source_month="2020-01",
                source_change=SOURCE_NEW,
                source_version_id=_id("sv1_", "b"),
                previous_source_version_id=None,
                previous_clean_version_id=None,
                desired_clean_version_id=_id("cv1_", "c"),
                previous_fact_version_id=None,
            ),
        ),
    )
    payload = backfill_plan_to_payload(plan)

    unexpected = copy.deepcopy(payload)
    unexpected["extra"] = True
    with pytest.raises(BackfillError, match="fields must be exactly"):
        backfill_plan_from_payload(unexpected)

    invalid_config = copy.deepcopy(payload)
    invalid_config["configuration_fingerprint"] = "cfg1_bad"
    with pytest.raises(BackfillError, match="invalid identifier format"):
        backfill_plan_from_payload(invalid_config)


def test_backfill_run_result_payload_round_trip_and_status_reconciliation():
    result = BackfillRunResult(
        status=RUN_PARTIAL_FAILURE,
        start_month="2020-01",
        end_month="2020-02",
        date_dimension_status="REUSED_CURRENT",
        date_version_id=_id("dv1_", "a"),
        months=(
            BackfillMonthResult(
                source_month="2020-01",
                source_change=SOURCE_REVISED,
                source_version_id=_id("sv1_", "b"),
                status=MONTH_SUCCEEDED,
                clean_status="PUBLISHED_CURRENT",
                clean_version_id=_id("cv1_", "c"),
                fact_status="PUBLISHED_CURRENT",
                fact_version_id=_id("fv1_", "d"),
                quality_status="PUBLISHED_CURRENT",
                quality_mart_version_id=_id("msq1_", "e"),
                od_day_status="PUBLISHED_CURRENT",
                od_day_mart_version_id=_id("mod1_", "f"),
                station_day=(
                    StationDayExecution(
                        service_month="2020-01",
                        status="PUBLISHED_CURRENT",
                        mart_version_id=_id("msd1_", "1"),
                    ),
                ),
                failures=(),
            ),
            BackfillMonthResult(
                source_month="2020-02",
                source_change=SOURCE_NEW,
                source_version_id=_id("sv1_", "2"),
                status=MONTH_FAILED,
                clean_status="FAILED",
                clean_version_id=None,
                fact_status=None,
                fact_version_id=None,
                quality_status=None,
                quality_mart_version_id=None,
                od_day_status=None,
                od_day_mart_version_id=None,
                station_day=(),
                failures=(
                    BackfillFailure(
                        component="clean",
                        target_month="2020-02",
                        error_type="RuntimeError",
                        message="injected",
                    ),
                ),
            ),
        ),
    )

    payload = backfill_run_result_to_payload(result)
    assert backfill_run_result_from_payload(payload) == result

    inconsistent = copy.deepcopy(payload)
    inconsistent["status"] = "SUCCEEDED"
    with pytest.raises(BackfillError, match="does not reconcile"):
        backfill_run_result_from_payload(inconsistent)

    empty = copy.deepcopy(payload)
    empty["status"] = "SUCCEEDED"
    empty["months"] = []
    with pytest.raises(BackfillError, match="must not be empty"):
        backfill_run_result_from_payload(empty)

    contradictory = copy.deepcopy(payload)
    contradictory["status"] = "SUCCEEDED"
    contradictory["months"] = [copy.deepcopy(contradictory["months"][0])]
    contradictory["start_month"] = "2020-01"
    contradictory["end_month"] = "2020-01"
    contradictory["months"][0]["clean_status"] = "FAILED"
    contradictory["months"][0]["failures"] = [
        {
            "component": "clean",
            "target_month": "2020-01",
            "error_type": "RuntimeError",
            "message": "tampered",
        }
    ]
    with pytest.raises(BackfillError, match="must not carry failure evidence"):
        backfill_run_result_from_payload(contradictory)

    wrong_mart_kind = copy.deepcopy(payload)
    wrong_mart_kind["months"][0]["quality_mart_version_id"] = _id("mod1_", "e")
    with pytest.raises(BackfillError, match="invalid identifier format"):
        backfill_run_result_from_payload(wrong_mart_kind)

    invalid_component_status = copy.deepcopy(payload)
    invalid_component_status["months"][0]["quality_status"] = "BANANA"
    with pytest.raises(BackfillError, match="unsupported value"):
        backfill_run_result_from_payload(invalid_component_status)
