"""Airflow-independent control-plane helpers for the Slice 19 DAG.

The DAG itself owns dependency/schedule/retry visibility.  These helpers keep
path/config parsing and BackfillPlan/BackfillRunResult transport testable without
importing Airflow, while delegating all transformation/DQ/publish behavior to
the existing backfill business layer.
"""

from __future__ import annotations

import json
import hashlib
import uuid
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .backfill import (
    MONTH_FAILED,
    PRODUCTION_END_MONTH,
    PRODUCTION_START_MONTH,
    RUN_SUCCEEDED,
    execute_backfill_plan,
    iter_months,
    plan_backfill_range,
    retry_failed_months_plan,
)
from .backfill_state import (
    backfill_plan_from_payload,
    backfill_plan_to_payload,
    backfill_run_result_from_payload,
    backfill_run_result_to_payload,
)
from .clean_publish import (
    clean_version_path,
    compute_configuration_fingerprint as compute_clean_configuration_fingerprint,
)
from .date_dimension import date_version_path
from .errors import BackfillError
from .fact_publish import fact_version_path
from .od_day_mart import od_day_mart_version_path
from .source_month_quality_mart import source_month_quality_mart_version_path
from .station_day_mart import station_day_mart_version_path
from .station_day_mart import plan_station_day_fact_revision_impact

REQUEST_SCHEMA_VERSION = 1


@dataclass(frozen=True)
class AirflowBackfillRequest:
    request_id: str
    manifest: Path
    raw_root: Path
    station_root: Path
    clean_root: Path
    warehouse_root: Path
    start_month: str
    end_month: str
    dataset_id: str
    transformation_version: str
    configuration: dict[str, object]


def resolve_backfill_request(
    params: Mapping[str, object],
    *,
    scheduled_month: str | None,
    request_id: str | None = None,
) -> dict[str, Any]:
    """Validate one scheduled/manual DAG request into a JSON-compatible payload."""
    expected = {
        "manifest",
        "raw_root",
        "station_root",
        "clean_root",
        "warehouse_root",
        "start_month",
        "end_month",
        "dataset_id",
        "transformation_version",
        "configuration",
    }
    missing = sorted(expected - set(params))
    if missing:
        raise BackfillError(f"Airflow params missing required keys: {missing!r}")

    start = _require_string(params["start_month"], "start_month", allow_empty=True)
    end = _require_string(params["end_month"], "end_month", allow_empty=True)
    if bool(start) != bool(end):
        raise BackfillError("start_month and end_month must be supplied together")
    if not start:
        if scheduled_month is None:
            raise BackfillError(
                "manual DAG runs must provide start_month/end_month when no "
                "scheduled data interval is available"
            )
        start = end = scheduled_month

    transformation_version = _require_string(
        params["transformation_version"], "transformation_version"
    )
    if transformation_version == "UNSET":
        raise BackfillError(
            "transformation_version must be configured explicitly for Airflow runs"
        )
    configuration = _canonical_configuration(params["configuration"])
    request = {
        "schema_version": REQUEST_SCHEMA_VERSION,
        "kind": "airflow_backfill_request",
        "request_id": request_id or uuid.uuid4().hex,
        "manifest": str(_require_path(params["manifest"], "manifest")),
        "raw_root": str(_require_path(params["raw_root"], "raw_root")),
        "station_root": str(_require_path(params["station_root"], "station_root")),
        "clean_root": str(_require_path(params["clean_root"], "clean_root")),
        "warehouse_root": str(
            _require_path(params["warehouse_root"], "warehouse_root")
        ),
        "start_month": start,
        "end_month": end,
        "dataset_id": _require_string(params["dataset_id"], "dataset_id"),
        "transformation_version": transformation_version,
        "configuration": configuration,
    }
    # The business validator owns exact production-scope month validation.
    from .backfill import iter_months

    iter_months(start, end)
    return request


def request_from_payload(payload: object) -> AirflowBackfillRequest:
    data = _require_mapping(payload, "Airflow request payload")
    expected = {
        "schema_version",
        "kind",
        "request_id",
        "manifest",
        "raw_root",
        "station_root",
        "clean_root",
        "warehouse_root",
        "start_month",
        "end_month",
        "dataset_id",
        "transformation_version",
        "configuration",
    }
    if set(data) != expected:
        raise BackfillError(
            "Airflow request payload has unexpected/missing fields"
        )
    if data["schema_version"] != REQUEST_SCHEMA_VERSION:
        raise BackfillError("unsupported Airflow request schema_version")
    if data["kind"] != "airflow_backfill_request":
        raise BackfillError("Airflow request payload has invalid kind")
    configuration = _canonical_configuration(data["configuration"])
    request = AirflowBackfillRequest(
        request_id=_require_string(data["request_id"], "request_id"),
        manifest=_require_path(data["manifest"], "manifest"),
        raw_root=_require_path(data["raw_root"], "raw_root"),
        station_root=_require_path(data["station_root"], "station_root"),
        clean_root=_require_path(data["clean_root"], "clean_root"),
        warehouse_root=_require_path(data["warehouse_root"], "warehouse_root"),
        start_month=_require_string(data["start_month"], "start_month"),
        end_month=_require_string(data["end_month"], "end_month"),
        dataset_id=_require_string(data["dataset_id"], "dataset_id"),
        transformation_version=_require_string(
            data["transformation_version"], "transformation_version"
        ),
        configuration=configuration,
    )
    from .backfill import iter_months

    iter_months(request.start_month, request.end_month)
    return request


def capture_plan_payload(request_payload: object) -> dict[str, Any]:
    request = request_from_payload(request_payload)
    plan = plan_backfill_range(
        manifest=request.manifest,
        raw_root=request.raw_root,
        station_root=request.station_root,
        clean_root=request.clean_root,
        warehouse_root=request.warehouse_root,
        start_month=request.start_month,
        end_month=request.end_month,
        transformation_version=request.transformation_version,
        configuration=request.configuration,
        dataset_id=request.dataset_id,
    )
    plan_payload = backfill_plan_to_payload(plan)
    return {
        "schema_version": REQUEST_SCHEMA_VERSION,
        "kind": "airflow_captured_backfill_plan",
        "request_id": request.request_id,
        "plan": plan_payload,
        "binding_sha256": _captured_plan_binding_sha256(
            request.request_id,
            plan_payload,
        ),
    }


def execute_plan_payload(
    request_payload: object,
    plan_payload: object,
) -> dict[str, Any]:
    request = request_from_payload(request_payload)
    plan = _captured_plan_from_payload(request, plan_payload)
    _require_original_plan_binding(request, plan)
    result = execute_backfill_plan(
        plan,
        manifest=request.manifest,
        raw_root=request.raw_root,
        station_root=request.station_root,
        clean_root=request.clean_root,
        warehouse_root=request.warehouse_root,
        configuration=request.configuration,
    )
    return _airflow_result_to_payload(
        request,
        plan_payload,
        result,
        stage="initial",
        predecessor_binding=None,
    )


def retry_failed_payload(
    request_payload: object,
    original_plan_payload: object,
    previous_result_payload: object,
) -> dict[str, Any]:
    """Execute only the failed source months from the previous attempt."""
    request = request_from_payload(request_payload)
    original_plan = _captured_plan_from_payload(request, original_plan_payload)
    _require_original_plan_binding(request, original_plan)
    previous_result = _airflow_result_from_payload(
        request,
        original_plan_payload,
        previous_result_payload,
        expected_stage="initial",
        expected_predecessor_binding=None,
    )
    _require_result_matches_plan(
        previous_result,
        original_plan,
        request,
        "initial result",
    )
    if previous_result.status == RUN_SUCCEEDED:
        return _airflow_result_to_payload(
            request,
            original_plan_payload,
            previous_result,
            stage="retry",
            predecessor_binding=_airflow_result_binding(previous_result_payload),
        )
    retry_plan = retry_failed_months_plan(original_plan, previous_result)
    result = execute_backfill_plan(
        retry_plan,
        manifest=request.manifest,
        raw_root=request.raw_root,
        station_root=request.station_root,
        clean_root=request.clean_root,
        warehouse_root=request.warehouse_root,
        configuration=request.configuration,
    )
    return _airflow_result_to_payload(
        request,
        original_plan_payload,
        result,
        stage="retry",
        predecessor_binding=_airflow_result_binding(previous_result_payload),
    )


def require_success_payload(result_payload: object) -> dict[str, object]:
    """Return a compact success summary or raise so Airflow marks the DAG failed."""
    data = _require_mapping(result_payload, "result payload")
    if data.get("kind") == "airflow_backfill_result":
        result_payload = data.get("result")
    result = backfill_run_result_from_payload(result_payload)
    if result.status != RUN_SUCCEEDED:
        failed = ",".join(result.failed_source_months) or "-"
        raise BackfillError(
            f"backfill remained {result.status} after failed-month retry; "
            f"failed_source_months={failed}"
        )
    return {
        "status": result.status,
        "start_month": result.start_month,
        "end_month": result.end_month,
        "failed_source_months": [],
    }


def finalize_airflow_run_payload(
    request_payload: object,
    original_plan_payload: object,
    initial_result_payload: object,
    final_result_payload: object,
) -> dict[str, object]:
    """Validate attempt coverage and expose full requested-range run status."""
    request = request_from_payload(request_payload)
    original_plan = _captured_plan_from_payload(request, original_plan_payload)
    _require_original_plan_binding(request, original_plan)
    initial = _airflow_result_from_payload(
        request,
        original_plan_payload,
        initial_result_payload,
        expected_stage="initial",
        expected_predecessor_binding=None,
    )
    _require_result_matches_plan(initial, original_plan, request, "initial result")
    final = _airflow_result_from_payload(
        request,
        original_plan_payload,
        final_result_payload,
        expected_stage="retry",
        expected_predecessor_binding=_airflow_result_binding(initial_result_payload),
    )

    if initial.status == RUN_SUCCEEDED:
        expected_final_plan = original_plan
    else:
        expected_final_plan = retry_failed_months_plan(original_plan, initial)
    _require_result_matches_plan(final, expected_final_plan, request, "final result")
    if final.status != RUN_SUCCEEDED:
        failed = ",".join(final.failed_source_months) or "-"
        raise BackfillError(
            f"backfill remained {final.status} after failed-month retry; "
            f"failed_source_months={failed}"
        )
    return {
        "status": RUN_SUCCEEDED,
        "start_month": request.start_month,
        "end_month": request.end_month,
        "initial_status": initial.status,
        "retry_status": final.status,
        "retried_source_months": (
            list(initial.failed_source_months)
            if initial.status != RUN_SUCCEEDED
            else []
        ),
        "failed_source_months": [],
    }


def _require_original_plan_binding(request: AirflowBackfillRequest, plan) -> None:
    if plan.dataset_id != request.dataset_id:
        raise BackfillError("captured plan dataset_id does not match Airflow request")
    if plan.transformation_version != request.transformation_version:
        raise BackfillError(
            "captured plan transformation_version does not match Airflow request"
        )
    if plan.configuration_fingerprint != compute_clean_configuration_fingerprint(
        request.configuration
    ):
        raise BackfillError(
            "captured plan configuration does not match Airflow request"
        )
    if (
        plan.start_month != request.start_month
        or plan.end_month != request.end_month
    ):
        raise BackfillError(
            "captured original plan range does not exactly match Airflow request"
        )
    expected_months = iter_months(request.start_month, request.end_month)
    actual_months = tuple(month.source_month for month in plan.months)
    if actual_months != expected_months:
        raise BackfillError(
            "captured original plan does not cover every requested source month"
        )


def _captured_plan_from_payload(
    request: AirflowBackfillRequest,
    payload: object,
):
    data = _require_mapping(payload, "captured Airflow plan payload")
    expected = {
        "schema_version",
        "kind",
        "request_id",
        "plan",
        "binding_sha256",
    }
    if set(data) != expected:
        raise BackfillError("captured Airflow plan payload has unexpected/missing fields")
    if data["schema_version"] != REQUEST_SCHEMA_VERSION:
        raise BackfillError("unsupported captured Airflow plan schema_version")
    if data["kind"] != "airflow_captured_backfill_plan":
        raise BackfillError("captured Airflow plan payload has invalid kind")
    request_id = _require_string(data["request_id"], "captured plan request_id")
    if request_id != request.request_id:
        raise BackfillError("captured plan belongs to a different Airflow request")
    plan_payload = _require_mapping(data["plan"], "captured plan body")
    binding = _require_string(data["binding_sha256"], "captured plan binding_sha256")
    expected_binding = _captured_plan_binding_sha256(request_id, plan_payload)
    if binding != expected_binding:
        raise BackfillError("captured plan content fingerprint does not match payload")
    plan = backfill_plan_from_payload(plan_payload)
    _require_original_plan_binding(request, plan)
    return plan


def _captured_plan_binding_sha256(
    request_id: str,
    plan_payload: Mapping[str, object],
) -> str:
    try:
        encoded = json.dumps(
            {"request_id": request_id, "plan": plan_payload},
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        ).encode("utf-8")
    except (TypeError, ValueError) as exc:
        raise BackfillError("captured plan must be canonical JSON-compatible state") from exc
    return hashlib.sha256(encoded).hexdigest()


def _airflow_result_to_payload(
    request: AirflowBackfillRequest,
    captured_plan_payload: object,
    result,
    *,
    stage: str,
    predecessor_binding: str | None,
) -> dict[str, Any]:
    captured = _require_mapping(captured_plan_payload, "captured Airflow plan payload")
    plan_binding = _require_string(
        captured.get("binding_sha256"),
        "captured plan binding_sha256",
    )
    payload: dict[str, Any] = {
        "schema_version": REQUEST_SCHEMA_VERSION,
        "kind": "airflow_backfill_result",
        "request_id": request.request_id,
        "captured_plan_binding_sha256": plan_binding,
        "stage": stage,
        "predecessor_binding_sha256": predecessor_binding,
        "result": backfill_run_result_to_payload(result),
    }
    payload["binding_sha256"] = _airflow_result_binding_sha256(payload)
    return payload


def _airflow_result_from_payload(
    request: AirflowBackfillRequest,
    captured_plan_payload: object,
    payload: object,
    *,
    expected_stage: str,
    expected_predecessor_binding: str | None,
):
    data = _require_mapping(payload, "Airflow result payload")
    expected = {
        "schema_version",
        "kind",
        "request_id",
        "captured_plan_binding_sha256",
        "stage",
        "predecessor_binding_sha256",
        "result",
        "binding_sha256",
    }
    if set(data) != expected:
        raise BackfillError("Airflow result payload has unexpected/missing fields")
    if data["schema_version"] != REQUEST_SCHEMA_VERSION:
        raise BackfillError("unsupported Airflow result schema_version")
    if data["kind"] != "airflow_backfill_result":
        raise BackfillError("Airflow result payload has invalid kind")
    if data["request_id"] != request.request_id:
        raise BackfillError("Airflow result belongs to a different request")
    captured = _require_mapping(captured_plan_payload, "captured Airflow plan payload")
    if data["captured_plan_binding_sha256"] != captured.get("binding_sha256"):
        raise BackfillError("Airflow result belongs to a different captured plan")
    if data["stage"] != expected_stage:
        raise BackfillError(
            f"Airflow result stage must be {expected_stage!r}, got {data['stage']!r}"
        )
    if data["predecessor_binding_sha256"] != expected_predecessor_binding:
        raise BackfillError("Airflow result predecessor binding does not match attempt chain")
    binding = _require_string(data["binding_sha256"], "Airflow result binding_sha256")
    if binding != _airflow_result_binding_sha256(data):
        raise BackfillError("Airflow result content fingerprint does not match payload")
    return backfill_run_result_from_payload(data["result"])


def _airflow_result_binding(payload: object) -> str:
    data = _require_mapping(payload, "Airflow result payload")
    return _require_string(data.get("binding_sha256"), "Airflow result binding_sha256")


def _airflow_result_binding_sha256(payload: Mapping[str, object]) -> str:
    body = {key: value for key, value in payload.items() if key != "binding_sha256"}
    try:
        encoded = json.dumps(
            body,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        ).encode("utf-8")
    except (TypeError, ValueError) as exc:
        raise BackfillError("Airflow result must be canonical JSON-compatible state") from exc
    return hashlib.sha256(encoded).hexdigest()


def _require_result_matches_plan(
    result,
    plan,
    request: AirflowBackfillRequest,
    label: str,
) -> None:
    expected_months = tuple(month.source_month for month in plan.months)
    actual_months = tuple(month.source_month for month in result.months)
    if (
        result.start_month != plan.start_month
        or result.end_month != plan.end_month
        or actual_months != expected_months
    ):
        raise BackfillError(f"{label} does not match captured plan month coverage")

    date_metadata = _load_artifact_metadata(
        date_version_path(request.warehouse_root, result.date_version_id),
        f"{label} date dimension",
    )
    _require_metadata_binding(
        date_metadata,
        {
            "date_version_id": result.date_version_id,
            "transformation_version": plan.transformation_version,
            "configuration_fingerprint": plan.configuration_fingerprint,
        },
        f"{label} date dimension",
    )

    for month_result, month_plan in zip(result.months, plan.months, strict=True):
        month_label = f"{label} {month_result.source_month}"
        if (
            month_result.source_change != month_plan.source_change
            or month_result.source_version_id != month_plan.source_version_id
        ):
            raise BackfillError(
                f"{month_label} source lineage does not match captured plan"
            )

        if month_result.clean_version_id is not None:
            if month_result.clean_version_id != month_plan.desired_clean_version_id:
                raise BackfillError(
                    f"{month_label} Clean lineage does not match captured plan"
                )
            clean_metadata = _load_artifact_metadata(
                clean_version_path(
                    request.clean_root,
                    month_result.source_month,
                    month_result.clean_version_id,
                ),
                f"{month_label} Clean",
            )
            _require_metadata_binding(
                clean_metadata,
                {
                    "source_month": month_result.source_month,
                    "source_version_id": month_plan.source_version_id,
                    "clean_version_id": month_result.clean_version_id,
                    "transformation_version": plan.transformation_version,
                    "configuration_fingerprint": plan.configuration_fingerprint,
                },
                f"{month_label} Clean",
            )

        if month_result.fact_version_id is not None:
            if month_result.clean_version_id is None:
                raise BackfillError(
                    f"{month_label} fact lineage is missing its Clean lineage"
                )
            fact_metadata = _load_artifact_metadata(
                fact_version_path(
                    request.warehouse_root,
                    month_result.source_month,
                    month_result.fact_version_id,
                ),
                f"{month_label} trusted fact",
            )
            _require_metadata_binding(
                fact_metadata,
                {
                    "source_month": month_result.source_month,
                    "source_version_id": month_plan.source_version_id,
                    "clean_version_id": month_result.clean_version_id,
                    "fact_version_id": month_result.fact_version_id,
                    "transformation_version": plan.transformation_version,
                    "configuration_fingerprint": plan.configuration_fingerprint,
                },
                f"{month_label} trusted fact",
            )

        if month_result.quality_mart_version_id is not None:
            if month_result.clean_version_id is None or month_result.fact_version_id is None:
                raise BackfillError(
                    f"{month_label} quality mart is missing upstream lineage"
                )
            quality_metadata = _load_artifact_metadata(
                source_month_quality_mart_version_path(
                    request.warehouse_root,
                    month_result.source_month,
                    month_result.quality_mart_version_id,
                ),
                f"{month_label} source-month quality mart",
            )
            _require_metadata_binding(
                quality_metadata,
                {
                    "source_month": month_result.source_month,
                    "source_version_id": month_plan.source_version_id,
                    "clean_version_id": month_result.clean_version_id,
                    "fact_version_id": month_result.fact_version_id,
                    "mart_version_id": month_result.quality_mart_version_id,
                    "transformation_version": plan.transformation_version,
                    "configuration_fingerprint": plan.configuration_fingerprint,
                },
                f"{month_label} source-month quality mart",
            )

        if month_result.od_day_mart_version_id is not None:
            if month_result.fact_version_id is None:
                raise BackfillError(
                    f"{month_label} OD-day mart is missing trusted fact lineage"
                )
            od_metadata = _load_artifact_metadata(
                od_day_mart_version_path(
                    request.warehouse_root,
                    month_result.source_month,
                    month_result.od_day_mart_version_id,
                ),
                f"{month_label} OD-day mart",
            )
            _require_metadata_binding(
                od_metadata,
                {
                    "service_month": month_result.source_month,
                    "date_version_id": result.date_version_id,
                    "fact_version_id": month_result.fact_version_id,
                    "mart_version_id": month_result.od_day_mart_version_id,
                    "transformation_version": plan.transformation_version,
                    "configuration_fingerprint": plan.configuration_fingerprint,
                },
                f"{month_label} OD-day mart",
            )

        for station_day in month_result.station_day:
            if station_day.mart_version_id is None:
                continue
            station_metadata = _load_artifact_metadata(
                station_day_mart_version_path(
                    request.warehouse_root,
                    station_day.service_month,
                    station_day.mart_version_id,
                ),
                f"{month_label} station-day mart {station_day.service_month}",
            )
            _require_metadata_binding(
                station_metadata,
                {
                    "service_month": station_day.service_month,
                    "date_version_id": result.date_version_id,
                    "mart_version_id": station_day.mart_version_id,
                    "transformation_version": plan.transformation_version,
                    "configuration_fingerprint": plan.configuration_fingerprint,
                },
                f"{month_label} station-day mart {station_day.service_month}",
            )
            if month_result.status != MONTH_FAILED:
                _require_station_day_fact_binding(
                    station_metadata,
                    source_month=month_result.source_month,
                    fact_version_id=month_result.fact_version_id,
                    label=(
                        f"{month_label} station-day mart "
                        f"{station_day.service_month}"
                    ),
                )

        if month_result.status != MONTH_FAILED:
            expected_station_months = _expected_station_day_service_months(
                request,
                month_plan,
                month_result,
            )
            actual_station_months = tuple(
                item.service_month for item in month_result.station_day
            )
            if actual_station_months != expected_station_months:
                raise BackfillError(
                    f"{month_label} station-day coverage does not match captured "
                    "plan fact-revision impact"
                )


def _load_artifact_metadata(version_path: Path, label: str) -> Mapping[str, object]:
    path = version_path / "metadata.json"
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise BackfillError(
            f"{label} does not reference readable durable metadata: {path}"
        ) from exc
    return _require_mapping(payload, f"{label} metadata")


def _require_metadata_binding(
    metadata: Mapping[str, object],
    expected: Mapping[str, object],
    label: str,
) -> None:
    for key, expected_value in expected.items():
        if key not in metadata or metadata[key] != expected_value:
            raise BackfillError(
                f"{label} metadata does not match captured plan lineage for {key}"
            )


def _require_station_day_fact_binding(
    metadata: Mapping[str, object],
    *,
    source_month: str,
    fact_version_id: str | None,
    label: str,
) -> None:
    if fact_version_id is None:
        raise BackfillError(f"{label} is missing trusted fact lineage")
    lineage = metadata.get("fact_scan_lineage")
    if not isinstance(lineage, list) or {
        "source_month": source_month,
        "fact_version_id": fact_version_id,
    } not in lineage:
        raise BackfillError(
            f"{label} metadata does not contain the result trusted fact lineage"
        )


def _expected_station_day_service_months(
    request: AirflowBackfillRequest,
    month_plan,
    month_result,
) -> tuple[str, ...]:
    if month_result.fact_version_id is None:
        raise BackfillError(
            f"{month_result.source_month}: successful result is missing trusted fact lineage"
        )
    old_fact_version_id = (
        month_plan.previous_fact_version_id or month_result.fact_version_id
    )
    impact = plan_station_day_fact_revision_impact(
        manifest=request.manifest,
        raw_root=request.raw_root,
        station_root=request.station_root,
        clean_root=request.clean_root,
        warehouse_root=request.warehouse_root,
        source_month=month_result.source_month,
        old_fact_version_id=old_fact_version_id,
        new_fact_version_id=month_result.fact_version_id,
    )
    return tuple(
        service_month
        for service_month in impact.affected_service_months
        if PRODUCTION_START_MONTH <= service_month <= PRODUCTION_END_MONTH
    )


def _canonical_configuration(value: object) -> dict[str, object]:
    if not isinstance(value, Mapping):
        raise BackfillError("configuration must be a JSON object")
    try:
        text = json.dumps(
            value,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        )
        result = json.loads(text)
    except (TypeError, ValueError) as exc:
        raise BackfillError(
            "configuration must be JSON-compatible without NaN/Infinity"
        ) from exc
    if not isinstance(result, dict):
        raise BackfillError("configuration must be a JSON object")
    return result


def _require_mapping(value: object, label: str) -> Mapping[str, object]:
    if not isinstance(value, Mapping):
        raise BackfillError(f"{label} must be an object")
    if not all(isinstance(key, str) for key in value):
        raise BackfillError(f"{label} keys must be strings")
    return value


def _require_string(
    value: object, label: str, *, allow_empty: bool = False
) -> str:
    if not isinstance(value, str):
        raise BackfillError(f"{label} must be a string")
    if not allow_empty and not value:
        raise BackfillError(f"{label} must be a non-empty string")
    return value


def _require_path(value: object, label: str) -> Path:
    text = _require_string(value, label)
    return Path(text).expanduser().resolve()
