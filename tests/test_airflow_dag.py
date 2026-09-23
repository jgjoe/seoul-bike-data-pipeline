from __future__ import annotations

import importlib.util
import os
from pathlib import Path

import pytest

_REPO_ROOT = Path(__file__).resolve().parents[1]
os.environ.setdefault(
    "AIRFLOW__CORE__PLUGINS_FOLDER",
    str(_REPO_ROOT / "plugins"),
)

pytest.importorskip("airflow")

from airflow.timetables.base import DataInterval, TimeRestriction
from tests.test_backfill import TRANSFORMATION_VERSION, _build_backfill_base


def _load_dag_module():
    dag_path = (
        Path(__file__).resolve().parents[1]
        / "dags"
        / "seoul_bike_monthly_backfill.py"
    )
    spec = importlib.util.spec_from_file_location("seoul_bike_airflow_dag", dag_path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.fixture(scope="module")
def airflow_backfill_base(tmp_path_factory) -> Path:
    root = tmp_path_factory.mktemp("airflow-backfill-base")
    _build_backfill_base(root)
    return root


def test_airflow_dag_structure_uses_explicit_control_plane_stages():
    module = _load_dag_module()
    dag = module.dag

    assert dag.dag_id == "seoul_bike_monthly_backfill"
    assert dag.catchup is False
    assert dag.max_active_runs == 1
    assert type(dag.timetable).__name__ == "BoundedCronDataIntervalTimetable"
    assert dag.end_date is None
    assert all(task.end_date is None for task in dag.tasks)
    assert set(dag.task_dict) == {
        "resolve_request",
        "capture_plan",
        "execute_initial",
        "retry_failed_months",
        "require_success",
    }
    assert dag.task_dict["capture_plan"].upstream_task_ids == {"resolve_request"}
    assert dag.task_dict["execute_initial"].upstream_task_ids == {
        "capture_plan",
        "resolve_request",
    }
    assert dag.task_dict["retry_failed_months"].upstream_task_ids == {
        "capture_plan",
        "execute_initial",
        "resolve_request",
    }
    assert dag.task_dict["require_success"].upstream_task_ids == {
        "capture_plan",
        "execute_initial",
        "resolve_request",
        "retry_failed_months",
    }
    assert dag.task_dict["execute_initial"].retries == 1
    assert dag.task_dict["retry_failed_months"].retries == 1


def test_airflow_timetable_includes_june_2026_interval_but_not_july():
    module = _load_dag_module()
    dag = module.dag
    timezone = module.pendulum.timezone("Asia/Seoul")
    restriction = TimeRestriction(
        earliest=module.pendulum.datetime(2020, 1, 1, tz=timezone),
        latest=None,
        catchup=True,
    )
    may = DataInterval(
        start=module.pendulum.datetime(2026, 5, 1, tz=timezone),
        end=module.pendulum.datetime(2026, 6, 1, tz=timezone),
    )
    june_info = dag.timetable.next_dagrun_info(
        last_automated_data_interval=may,
        restriction=restriction,
    )
    assert june_info is not None
    assert june_info.data_interval.start == module.pendulum.datetime(
        2026, 6, 1, tz=timezone
    )
    assert june_info.data_interval.end == module.pendulum.datetime(
        2026, 7, 1, tz=timezone
    )

    june = DataInterval(
        start=june_info.data_interval.start,
        end=june_info.data_interval.end,
    )
    assert (
        dag.timetable.next_dagrun_info(
            last_automated_data_interval=june,
            restriction=restriction,
        )
        is None
    )


@pytest.mark.skipif(
    os.environ.get("RUN_AIRFLOW_INTEGRATION") != "1",
    reason="requires initialized Airflow metadata DB in the WSL2 Airflow environment",
)
def test_airflow_dag_test_executes_same_business_layer(
    airflow_backfill_base: Path,
):
    module = _load_dag_module()
    root = airflow_backfill_base

    dag_run = module.dag.test(
        logical_date=module.pendulum.datetime(2026, 9, 22, tz="Asia/Seoul"),
        run_conf={
            "manifest": str(root / "manifest.json"),
            "raw_root": str(root / "raw"),
            "station_root": str(root / "station"),
            "clean_root": str(root / "clean"),
            "warehouse_root": str(root / "warehouse"),
            "start_month": "2020-01",
            "end_month": "2020-04",
            "dataset_id": "rental_history",
            "transformation_version": TRANSFORMATION_VERSION,
            "configuration": {},
        },
    )

    assert str(dag_run.state).lower().endswith("success")
    task_instances = dag_run.get_task_instances()
    assert {instance.task_id for instance in task_instances} == set(module.dag.task_dict)
    assert all(str(instance.state).lower().endswith("success") for instance in task_instances)


@pytest.mark.skipif(
    os.environ.get("RUN_AIRFLOW_INTEGRATION") != "1",
    reason="requires initialized Airflow metadata DB in the WSL2 Airflow environment",
)
def test_airflow_dag_test_exposes_unrecovered_month_failure(
    airflow_backfill_base: Path,
):
    module = _load_dag_module()
    root = airflow_backfill_base

    dag_run = module.dag.test(
        logical_date=module.pendulum.datetime(2026, 5, 1, tz="Asia/Seoul"),
        run_conf={
            "manifest": str(root / "manifest.json"),
            "raw_root": str(root / "raw"),
            "station_root": str(root / "station"),
            "clean_root": str(root / "clean"),
            "warehouse_root": str(root / "warehouse"),
            "start_month": "2020-05",
            "end_month": "2020-05",
            "dataset_id": "rental_history",
            "transformation_version": TRANSFORMATION_VERSION,
            "configuration": {},
        },
    )

    assert str(dag_run.state).lower().endswith("failed")
