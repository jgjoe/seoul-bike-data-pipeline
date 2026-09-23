"""Airflow control plane for scheduled month processing and manual range backfills.

Business transformation/DQ/publish logic intentionally stays in
``seoul_bike_pipeline.backfill``.  This DAG only resolves run parameters,
captures immutable plan state, invokes the business layer, retries the failed
source-month subset once, and exposes final run success/failure to Airflow.
"""

from __future__ import annotations

import logging
import os
from datetime import timedelta
from pathlib import Path

import pendulum
from airflow.sdk import (
    Param,
    dag,
    get_current_context,
    task,
)

from seoul_bike_pipeline.airflow_control import (
    capture_plan_payload,
    execute_plan_payload,
    finalize_airflow_run_payload,
    resolve_backfill_request,
    retry_failed_payload,
)
from seoul_bike_pipeline.backfill_state import backfill_run_result_from_payload
from seoul_bike_pipeline.airflow_timetable import BoundedCronDataIntervalTimetable

DAG_ID = "seoul_bike_monthly_backfill"

_PROJECT_ROOT = Path(
    os.environ.get(
        "SEOUL_BIKE_PROJECT_ROOT",
        str(Path(__file__).resolve().parents[1]),
    )
).expanduser()
_TRANSFORMATION_VERSION = os.environ.get(
    "SEOUL_BIKE_TRANSFORMATION_VERSION", "UNSET"
)


@dag(
    dag_id=DAG_ID,
    description=(
        "Scheduled Seoul Bike month processing plus manual arbitrary range backfill"
    ),
    schedule=BoundedCronDataIntervalTimetable(
        "0 0 1 * *",
        timezone=pendulum.timezone("Asia/Seoul"),
        last_interval_start=pendulum.datetime(2026, 6, 1, tz="Asia/Seoul"),
    ),
    start_date=pendulum.datetime(2020, 1, 1, tz="Asia/Seoul"),
    catchup=False,
    max_active_runs=1,
    tags=["seoul-bike", "data-quality", "backfill"],
    params={
        "manifest": Param(
            str(_PROJECT_ROOT / "data" / "source-manifest.json"),
            type="string",
            minLength=1,
        ),
        "raw_root": Param(
            str(_PROJECT_ROOT / "data" / "raw"),
            type="string",
            minLength=1,
        ),
        "station_root": Param(
            str(_PROJECT_ROOT / "data" / "station"),
            type="string",
            minLength=1,
        ),
        "clean_root": Param(
            str(_PROJECT_ROOT / "data" / "clean"),
            type="string",
            minLength=1,
        ),
        "warehouse_root": Param(
            str(_PROJECT_ROOT / "data" / "warehouse"),
            type="string",
            minLength=1,
        ),
        "start_month": Param(
            "",
            type="string",
            pattern=r"^$|^20\d{2}-(0[1-9]|1[0-2])$",
            description=(
                "Manual inclusive start month. Leave start/end blank for the "
                "scheduled data-interval month."
            ),
        ),
        "end_month": Param(
            "",
            type="string",
            pattern=r"^$|^20\d{2}-(0[1-9]|1[0-2])$",
            description="Manual inclusive end month; must accompany start_month.",
        ),
        "dataset_id": Param(
            "rental_history",
            type="string",
            minLength=1,
        ),
        "transformation_version": Param(
            _TRANSFORMATION_VERSION,
            type="string",
            minLength=1,
            description=(
                "Explicit business transformation version. Configure "
                "SEOUL_BIKE_TRANSFORMATION_VERSION or override this parameter."
            ),
        ),
        "configuration": Param(
            {},
            type="object",
            description="Deterministic JSON-compatible business configuration.",
        ),
    },
)
def seoul_bike_monthly_backfill():
    @task(task_id="resolve_request")
    def resolve_request_task() -> dict[str, object]:
        context = get_current_context()
        params = context["params"]
        interval_start = context.get("data_interval_start")
        scheduled_month = (
            interval_start.strftime("%Y-%m")
            if interval_start is not None
            else None
        )
        request = resolve_backfill_request(
            params,
            scheduled_month=scheduled_month,
            request_id=str(context["run_id"]),
        )
        logging.getLogger("airflow.task").info(
            "resolved backfill request range=%s..%s run_id=%s",
            request["start_month"],
            request["end_month"],
            context.get("run_id"),
        )
        return request

    @task(task_id="capture_plan", retries=1, retry_delay=timedelta(seconds=5))
    def capture_plan_task(request: dict[str, object]) -> dict[str, object]:
        return capture_plan_payload(request)

    @task(task_id="execute_initial", retries=1, retry_delay=timedelta(seconds=5))
    def execute_initial_task(
        request: dict[str, object],
        plan: dict[str, object],
    ) -> dict[str, object]:
        result = execute_plan_payload(request, plan)
        restored = backfill_run_result_from_payload(result["result"])
        logging.getLogger("airflow.task").info(
            "initial backfill status=%s failed_source_months=%s",
            restored.status,
            list(restored.failed_source_months),
        )
        return result

    @task(
        task_id="retry_failed_months",
        retries=1,
        retry_delay=timedelta(seconds=5),
    )
    def retry_failed_task(
        request: dict[str, object],
        plan: dict[str, object],
        previous_result: dict[str, object],
    ) -> dict[str, object]:
        previous = backfill_run_result_from_payload(previous_result["result"])
        logging.getLogger("airflow.task").info(
            "retry stage input status=%s failed_source_months=%s",
            previous.status,
            list(previous.failed_source_months),
        )
        result = retry_failed_payload(request, plan, previous_result)
        restored = backfill_run_result_from_payload(result["result"])
        logging.getLogger("airflow.task").info(
            "retry stage output status=%s failed_source_months=%s",
            restored.status,
            list(restored.failed_source_months),
        )
        return result

    @task(task_id="require_success")
    def require_success_task(
        request: dict[str, object],
        plan: dict[str, object],
        initial_result: dict[str, object],
        final_result: dict[str, object],
    ) -> dict[str, object]:
        summary = finalize_airflow_run_payload(
            request,
            plan,
            initial_result,
            final_result,
        )
        context = get_current_context()
        logging.getLogger("airflow.task").info(
            "backfill run succeeded range=%s..%s run_id=%s",
            summary["start_month"],
            summary["end_month"],
            context.get("run_id"),
        )
        return summary

    request = resolve_request_task()
    plan = capture_plan_task(request)
    initial = execute_initial_task(request, plan)
    retry = retry_failed_task(request, plan, initial)
    require_success_task(request, plan, initial, retry)


dag = seoul_bike_monthly_backfill()
