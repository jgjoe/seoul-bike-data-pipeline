from __future__ import annotations

import copy
import json
from pathlib import Path

import pytest

import seoul_bike_pipeline.airflow_control as airflow_control_module
from seoul_bike_pipeline.airflow_control import (
    capture_plan_payload,
    execute_plan_payload,
    finalize_airflow_run_payload,
    request_from_payload,
    require_success_payload,
    resolve_backfill_request,
    retry_failed_payload,
)
from seoul_bike_pipeline.backfill import (
    RUN_PARTIAL_FAILURE,
    RUN_SUCCEEDED,
    execute_backfill_plan,
)
from seoul_bike_pipeline.backfill_state import (
    backfill_plan_from_payload,
    backfill_run_result_from_payload,
    backfill_run_result_to_payload,
)
from seoul_bike_pipeline.errors import BackfillError
from tests.test_backfill import (
    TRANSFORMATION_VERSION,
    _build_backfill_base,
    _month_row,
    _pointer_id,
    _register_rental,
)


def _params(root: Path, **overrides) -> dict[str, object]:
    values: dict[str, object] = {
        "manifest": str(root / "manifest.json"),
        "raw_root": str(root / "raw"),
        "station_root": str(root / "station"),
        "clean_root": str(root / "clean"),
        "warehouse_root": str(root / "warehouse"),
        "start_month": "",
        "end_month": "",
        "dataset_id": "rental_history",
        "transformation_version": TRANSFORMATION_VERSION,
        "configuration": {},
    }
    values.update(overrides)
    return values


def test_resolve_request_supports_scheduled_month_and_manual_range(tmp_path: Path):
    scheduled = resolve_backfill_request(
        _params(tmp_path),
        scheduled_month="2020-03",
    )
    assert scheduled["start_month"] == "2020-03"
    assert scheduled["end_month"] == "2020-03"

    manual = resolve_backfill_request(
        _params(tmp_path, start_month="2020-01", end_month="2020-04"),
        scheduled_month="2020-03",
    )
    restored = request_from_payload(manual)
    assert restored.start_month == "2020-01"
    assert restored.end_month == "2020-04"
    assert restored.manifest.is_absolute()


def test_resolve_request_rejects_ambiguous_or_unconfigured_run(tmp_path: Path):
    with pytest.raises(BackfillError, match="supplied together"):
        resolve_backfill_request(
            _params(tmp_path, start_month="2020-01"),
            scheduled_month=None,
        )
    with pytest.raises(BackfillError, match="manual DAG runs"):
        resolve_backfill_request(_params(tmp_path), scheduled_month=None)
    with pytest.raises(BackfillError, match="configured explicitly"):
        resolve_backfill_request(
            _params(tmp_path, transformation_version="UNSET"),
            scheduled_month="2020-01",
        )


def test_retry_stage_uses_original_captured_plan_for_failed_month_only(
    tmp_path: Path,
):
    _build_backfill_base(tmp_path)
    _register_rental(
        tmp_path,
        source_month="2020-05",
        rows=[_month_row("2020-05")],
    )
    _register_rental(
        tmp_path,
        source_month="2020-06",
        rows=[_month_row("2020-06")],
    )
    request_payload = resolve_backfill_request(
        _params(tmp_path, start_month="2020-05", end_month="2020-06"),
        scheduled_month=None,
    )
    plan_payload = capture_plan_payload(request_payload)
    plan = backfill_plan_from_payload(plan_payload["plan"])

    def fail_june_clean(component: str, source_month: str, target_month: str) -> None:
        del target_month
        if component == "clean" and source_month == "2020-06":
            raise RuntimeError("injected clean failure")

    request = request_from_payload(request_payload)
    initial = execute_backfill_plan(
        plan,
        manifest=request.manifest,
        raw_root=request.raw_root,
        station_root=request.station_root,
        clean_root=request.clean_root,
        warehouse_root=request.warehouse_root,
        configuration=request.configuration,
        before_step=fail_june_clean,
    )
    assert initial.status == RUN_PARTIAL_FAILURE
    assert initial.failed_source_months == ("2020-06",)
    may_clean_pointer = (
        tmp_path / "clean" / "current" / "source_month=2020-05.json"
    )
    may_clean_before = _pointer_id(may_clean_pointer, "clean_version_id")

    initial_payload = airflow_control_module._airflow_result_to_payload(
        request,
        plan_payload,
        initial,
        stage="initial",
        predecessor_binding=None,
    )
    retry_payload = retry_failed_payload(
        request_payload,
        plan_payload,
        initial_payload,
    )
    retry = backfill_run_result_from_payload(retry_payload["result"])

    assert retry.status == RUN_SUCCEEDED
    assert [month.source_month for month in retry.months] == ["2020-06"]
    assert _pointer_id(may_clean_pointer, "clean_version_id") == may_clean_before
    assert require_success_payload(retry_payload)["status"] == RUN_SUCCEEDED
    final_summary = finalize_airflow_run_payload(
        request_payload,
        plan_payload,
        initial_payload,
        retry_payload,
    )
    assert final_summary["start_month"] == "2020-05"
    assert final_summary["end_month"] == "2020-06"
    assert final_summary["retried_source_months"] == ["2020-06"]


def test_request_payload_rejects_configuration_tamper(tmp_path: Path):
    payload = resolve_backfill_request(
        _params(tmp_path, start_month="2020-01", end_month="2020-01"),
        scheduled_month=None,
    )
    tampered = json.loads(json.dumps(payload))
    tampered["configuration"] = {"bad": float("nan")}
    with pytest.raises(BackfillError, match="JSON-compatible"):
        request_from_payload(tampered)


def test_original_plan_cannot_silently_narrow_requested_range(tmp_path: Path):
    _build_backfill_base(tmp_path)
    request_payload = resolve_backfill_request(
        _params(tmp_path, start_month="2020-01", end_month="2020-04"),
        scheduled_month=None,
    )
    plan_payload = capture_plan_payload(request_payload)
    narrowed = json.loads(json.dumps(plan_payload))
    narrowed["plan"]["start_month"] = "2020-02"
    narrowed["plan"]["months"] = narrowed["plan"]["months"][1:]

    with pytest.raises(BackfillError, match="content fingerprint"):
        execute_plan_payload(request_payload, narrowed)


def test_captured_plan_cannot_be_substituted_or_reused_across_requests(
    tmp_path: Path,
):
    _build_backfill_base(tmp_path)
    _register_rental(
        tmp_path,
        source_month="2020-01",
        rows=[_month_row("2020-01", return_month="2020-03")],
        revision="v2-plan-binding",
    )
    request_payload = resolve_backfill_request(
        _params(tmp_path, start_month="2020-01", end_month="2020-01"),
        scheduled_month=None,
        request_id="manual__plan-binding-1",
    )
    plan_payload = capture_plan_payload(request_payload)
    body = plan_payload["plan"]
    assert body["months"][0]["source_change"] == "REVISED"

    substituted = copy.deepcopy(plan_payload)
    month = substituted["plan"]["months"][0]
    month["source_change"] = "UNCHANGED"
    month["source_version_id"] = month["previous_source_version_id"]
    month["desired_clean_version_id"] = month["previous_clean_version_id"]
    with pytest.raises(BackfillError, match="content fingerprint"):
        execute_plan_payload(request_payload, substituted)

    second_request = resolve_backfill_request(
        _params(tmp_path, start_month="2020-01", end_month="2020-01"),
        scheduled_month=None,
        request_id="manual__plan-binding-2",
    )
    with pytest.raises(BackfillError, match="different Airflow request"):
        execute_plan_payload(second_request, plan_payload)


def test_result_payload_must_bind_to_captured_plan_and_durable_lineage(
    tmp_path: Path,
):
    _build_backfill_base(tmp_path)
    _register_rental(
        tmp_path,
        source_month="2020-05",
        rows=[_month_row("2020-05")],
    )
    request_payload = resolve_backfill_request(
        _params(tmp_path, start_month="2020-05", end_month="2020-05"),
        scheduled_month=None,
    )
    plan_payload = capture_plan_payload(request_payload)
    valid_result = execute_plan_payload(request_payload, plan_payload)

    valid_retry = retry_failed_payload(
        request_payload,
        plan_payload,
        valid_result,
    )
    assert backfill_run_result_from_payload(valid_retry["result"]).status == RUN_SUCCEEDED
    assert (
        finalize_airflow_run_payload(
            request_payload,
            plan_payload,
            valid_result,
            valid_retry,
        )["status"]
        == RUN_SUCCEEDED
    )

    source_tampered = copy.deepcopy(valid_result)
    source_tampered["result"]["months"][0]["source_version_id"] = "sv1_" + "0" * 64
    source_tampered["binding_sha256"] = airflow_control_module._airflow_result_binding_sha256(
        source_tampered
    )
    with pytest.raises(BackfillError, match="source lineage"):
        retry_failed_payload(request_payload, plan_payload, source_tampered)
    with pytest.raises(BackfillError, match="source lineage"):
        finalize_airflow_run_payload(
            request_payload,
            plan_payload,
            source_tampered,
            valid_retry,
        )

    fact_tampered = copy.deepcopy(valid_retry)
    fact_tampered["result"]["months"][0]["fact_version_id"] = "fv1_" + "0" * 64
    fact_tampered["binding_sha256"] = airflow_control_module._airflow_result_binding_sha256(
        fact_tampered
    )
    with pytest.raises(BackfillError, match="durable metadata"):
        finalize_airflow_run_payload(
            request_payload,
            plan_payload,
            valid_result,
            fact_tampered,
        )


def test_result_payload_cannot_cross_airflow_request_boundary(tmp_path: Path):
    _build_backfill_base(tmp_path)
    params = _params(tmp_path, start_month="2020-01", end_month="2020-01")
    request_a = resolve_backfill_request(
        params,
        scheduled_month=None,
        request_id="manual__result-binding-a",
    )
    plan_a = capture_plan_payload(request_a)
    result_a = execute_plan_payload(request_a, plan_a)

    request_b = resolve_backfill_request(
        params,
        scheduled_month=None,
        request_id="manual__result-binding-b",
    )
    plan_b = capture_plan_payload(request_b)

    with pytest.raises(BackfillError, match="different request"):
        retry_failed_payload(request_b, plan_b, result_a)


def test_result_payload_cannot_drop_failed_station_day_impact_and_claim_success(
    tmp_path: Path,
):
    _build_backfill_base(tmp_path)
    _register_rental(
        tmp_path,
        source_month="2020-01",
        rows=[_month_row("2020-01", return_month="2020-03")],
        revision="v2-station-impact",
    )
    request_payload = resolve_backfill_request(
        _params(tmp_path, start_month="2020-01", end_month="2020-01"),
        scheduled_month=None,
    )
    plan_payload = capture_plan_payload(request_payload)
    plan = backfill_plan_from_payload(plan_payload["plan"])
    request = request_from_payload(request_payload)

    def fail_march_station_day(
        component: str,
        source_month: str,
        target_month: str,
    ) -> None:
        del source_month
        if component == "station_day" and target_month == "2020-03":
            raise RuntimeError("injected March station-day failure")

    initial = execute_backfill_plan(
        plan,
        manifest=request.manifest,
        raw_root=request.raw_root,
        station_root=request.station_root,
        clean_root=request.clean_root,
        warehouse_root=request.warehouse_root,
        configuration=request.configuration,
        before_step=fail_march_station_day,
    )
    assert initial.failed_source_months == ("2020-01",)
    assert tuple(item.service_month for item in initial.months[0].station_day) == (
        "2020-01",
        "2020-02",
        "2020-03",
    )

    initial_payload = airflow_control_module._airflow_result_to_payload(
        request,
        plan_payload,
        initial,
        stage="initial",
        predecessor_binding=None,
    )
    tampered = copy.deepcopy(initial_payload)
    tampered["result"]["status"] = RUN_SUCCEEDED
    tampered["result"]["months"][0]["status"] = RUN_SUCCEEDED
    tampered["result"]["months"][0]["failures"] = []
    tampered["result"]["months"][0]["station_day"] = [
        item
        for item in tampered["result"]["months"][0]["station_day"]
        if item["service_month"] != "2020-03"
    ]
    tampered["binding_sha256"] = airflow_control_module._airflow_result_binding_sha256(
        tampered
    )

    with pytest.raises(BackfillError, match="station-day coverage"):
        retry_failed_payload(request_payload, plan_payload, tampered)
