from __future__ import annotations

import csv
import io
import json
import shutil
import zipfile
from pathlib import Path

import pytest

import seoul_bike_pipeline.backfill as backfill_module
import seoul_bike_pipeline.cli as cli
from seoul_bike_pipeline.backfill import (
    BackfillRunResult,
    MONTH_FAILED,
    MONTH_SKIPPED_UNCHANGED,
    PRODUCTION_END_MONTH,
    PRODUCTION_START_MONTH,
    RUN_PARTIAL_FAILURE,
    RUN_SUCCEEDED,
    SOURCE_NEW,
    SOURCE_REVISED,
    SOURCE_UNCHANGED,
    execute_backfill_plan,
    iter_months,
    plan_backfill_range,
    retry_failed_months_plan,
    run_backfill_range,
)
from seoul_bike_pipeline.clean_publish import publish_clean_month
from seoul_bike_pipeline.csv_source import RENTAL_V16_HEADER
from seoul_bike_pipeline.errors import BackfillError
from seoul_bike_pipeline.fact_publish import publish_current_fact_trip_month
from seoul_bike_pipeline.register import register_source
from seoul_bike_pipeline.station_day_mart import (
    station_day_mart_current_pointer_path,
)
from seoul_bike_pipeline.station_history import publish_station_history
from seoul_bike_pipeline.zip_members import inventory_registered_zip
from tests.test_station_day_mart import rental_row
from tests.test_trip_station_enrichment import (
    publish_station_snapshot_fixture,
    station_row,
)

TRANSFORMATION_VERSION = "backfill-t1"


def _csv_bytes(rows: list[tuple[str, ...]]) -> bytes:
    buffer = io.StringIO(newline="")
    writer = csv.writer(buffer, lineterminator="\r\n")
    writer.writerow(RENTAL_V16_HEADER)
    writer.writerows(rows)
    return buffer.getvalue().encode("cp949")


def _register_rental(
    root: Path,
    *,
    source_month: str,
    rows: list[tuple[str, ...]],
    revision: str = "v1",
):
    artifact = root / f"rental-{source_month}-{revision}.csv"
    artifact.write_bytes(_csv_bytes(rows))
    return register_source(
        artifact=artifact,
        manifest=root / "manifest.json",
        raw_root=root / "raw",
        dataset_id="rental_history",
        logical_period=source_month,
        published_filename=f"rental_{source_month.replace('-', '')}_{revision}.csv",
        source_url="https://example.invalid/rental",
        license="SYNTHETIC_FIXTURE",
    )


def _month_row(
    source_month: str,
    *,
    return_month: str | None = None,
) -> tuple[str, ...]:
    return_month = return_month or source_month
    return rental_row(
        bike_id=f"BIKE-{source_month}",
        rent_at=f"{source_month}-01 09:00:00",
        rent_station_no="00001",
        return_at=f"{return_month}-01 09:10:00",
        return_station_no="00002",
        usage_min="10",
        distance_m="100",
    )


def _pointer_id(path: Path, key: str) -> str:
    return json.loads(path.read_text(encoding="utf-8"))[key]


def _write_zip(path: Path, entries: list[tuple[str, bytes]]) -> None:
    with zipfile.ZipFile(path, "w") as archive:
        for name, payload in entries:
            archive.writestr(name, payload)


def _build_backfill_base(root: Path) -> None:
    publish_station_snapshot_fixture(
        root,
        period="2019-12",
        filename="공공자전거 대여소 정보(19.12월 기준).csv",
        suffix="201912",
        rows=[
            station_row("00001", "station-1"),
            station_row("00002", "station-2"),
        ],
    )
    publish_station_history(
        manifest=root / "manifest.json",
        raw_root=root / "raw",
        station_root=root / "station",
        warehouse_root=root / "warehouse",
        transformation_version=TRANSFORMATION_VERSION,
        configuration={},
    )
    _register_rental(
        root,
        source_month="2020-01",
        rows=[_month_row("2020-01", return_month="2020-02")],
    )
    for month in ("2020-02", "2020-03", "2020-04"):
        _register_rental(root, source_month=month, rows=[_month_row(month)])
    _, result = run_backfill_range(
        manifest=root / "manifest.json",
        raw_root=root / "raw",
        station_root=root / "station",
        clean_root=root / "clean",
        warehouse_root=root / "warehouse",
        start_month="2020-01",
        end_month="2020-04",
        transformation_version=TRANSFORMATION_VERSION,
        configuration={},
    )
    assert result.status == RUN_SUCCEEDED, [
        (item.source_month, item.failures) for item in result.months if item.failures
    ]


@pytest.fixture(scope="module")
def backfill_base(tmp_path_factory) -> Path:
    root = tmp_path_factory.mktemp("backfill-base")
    _build_backfill_base(root)
    return root


@pytest.fixture
def backfill_fixture(backfill_base: Path, tmp_path: Path) -> Path:
    for child in backfill_base.iterdir():
        destination = tmp_path / child.name
        if child.is_dir():
            shutil.copytree(child, destination)
        else:
            shutil.copy2(child, destination)
    return tmp_path


def test_full_core_period_month_vector_and_scope():
    months = iter_months(PRODUCTION_START_MONTH, PRODUCTION_END_MONTH)
    assert len(months) == 78
    assert months[0] == "2020-01"
    assert months[-1] == "2026-06"
    with pytest.raises(BackfillError, match="within 2020-01..2026-06"):
        iter_months("2015-01", "2020-01")
    with pytest.raises(BackfillError, match="within 2020-01..2026-06"):
        iter_months("2026-06", "2026-07")
    with pytest.raises(BackfillError, match="start_month must be <= end_month"):
        iter_months("2020-02", "2020-01")


def test_backfill_cli_calls_business_layer(monkeypatch, tmp_path: Path, capsys):
    captured = {}

    def fake_run_backfill_range(**kwargs):
        captured.update(kwargs)
        return object(), BackfillRunResult(
            status=RUN_SUCCEEDED,
            start_month="2020-01",
            end_month="2020-02",
            date_dimension_status="REUSED_CURRENT",
            date_version_id="dv1_" + "a" * 64,
            months=(),
        )

    monkeypatch.setattr(cli, "run_backfill_range", fake_run_backfill_range)
    exit_code = cli.main(
        [
            "pipeline",
            "backfill-range",
            "--manifest",
            str(tmp_path / "manifest.json"),
            "--raw-root",
            str(tmp_path / "raw"),
            "--station-root",
            str(tmp_path / "station"),
            "--clean-root",
            str(tmp_path / "clean"),
            "--warehouse-root",
            str(tmp_path / "warehouse"),
            "--start-month",
            "2020-01",
            "--end-month",
            "2020-02",
            "--transformation-version",
            "git-abc",
        ]
    )

    assert exit_code == 0
    assert captured["start_month"] == "2020-01"
    assert captured["end_month"] == "2020-02"
    assert captured["transformation_version"] == "git-abc"
    assert captured["configuration"] == {}
    assert capsys.readouterr().out.startswith(
        "BACKFILL_RUN status=SUCCEEDED range=2020-01..2020-02"
    )


def test_same_input_range_reuses_all_current_outputs(backfill_fixture: Path):
    plan = plan_backfill_range(
        manifest=backfill_fixture / "manifest.json",
        raw_root=backfill_fixture / "raw",
        station_root=backfill_fixture / "station",
        clean_root=backfill_fixture / "clean",
        warehouse_root=backfill_fixture / "warehouse",
        start_month="2020-01",
        end_month="2020-04",
        transformation_version=TRANSFORMATION_VERSION,
        configuration={},
    )
    assert {item.source_change for item in plan.months} == {SOURCE_UNCHANGED}

    result = execute_backfill_plan(
        plan,
        manifest=backfill_fixture / "manifest.json",
        raw_root=backfill_fixture / "raw",
        station_root=backfill_fixture / "station",
        clean_root=backfill_fixture / "clean",
        warehouse_root=backfill_fixture / "warehouse",
        configuration={},
    )

    assert result.status == RUN_SUCCEEDED
    assert {item.status for item in result.months} == {MONTH_SKIPPED_UNCHANGED}


def test_execution_rejects_configuration_drift_from_captured_plan(
    backfill_fixture: Path,
):
    plan = plan_backfill_range(
        manifest=backfill_fixture / "manifest.json",
        raw_root=backfill_fixture / "raw",
        station_root=backfill_fixture / "station",
        clean_root=backfill_fixture / "clean",
        warehouse_root=backfill_fixture / "warehouse",
        start_month="2020-01",
        end_month="2020-01",
        transformation_version=TRANSFORMATION_VERSION,
        configuration={},
    )

    with pytest.raises(
        BackfillError,
        match="execution configuration does not match the captured BackfillPlan",
    ):
        execute_backfill_plan(
            plan,
            manifest=backfill_fixture / "manifest.json",
            raw_root=backfill_fixture / "raw",
            station_root=backfill_fixture / "station",
            clean_root=backfill_fixture / "clean",
            warehouse_root=backfill_fixture / "warehouse",
            configuration={"drift": 1},
        )


def test_planning_rejects_clean_fact_lineage_race(
    backfill_fixture: Path,
    monkeypatch,
):
    revision = _register_rental(
        backfill_fixture,
        source_month="2020-01",
        rows=[_month_row("2020-01", return_month="2020-03")],
        revision="v2-race",
    )
    original = backfill_module._load_current_fact_version_id
    advanced = False

    def advance_between_clean_and_fact_reads(*, warehouse_root, source_month):
        nonlocal advanced
        if source_month == "2020-01" and not advanced:
            advanced = True
            publish_clean_month(
                manifest=backfill_fixture / "manifest.json",
                raw_root=backfill_fixture / "raw",
                clean_root=backfill_fixture / "clean",
                source_version_id=revision.record.source_version_id,
                source_month="2020-01",
                transformation_version=TRANSFORMATION_VERSION,
                configuration={},
            )
            publish_current_fact_trip_month(
                manifest=backfill_fixture / "manifest.json",
                raw_root=backfill_fixture / "raw",
                station_root=backfill_fixture / "station",
                clean_root=backfill_fixture / "clean",
                warehouse_root=backfill_fixture / "warehouse",
                source_month="2020-01",
                transformation_version=TRANSFORMATION_VERSION,
                configuration={},
            )
        return original(
            warehouse_root=warehouse_root,
            source_month=source_month,
        )

    monkeypatch.setattr(
        backfill_module,
        "_load_current_fact_version_id",
        advance_between_clean_and_fact_reads,
    )

    with pytest.raises(
        BackfillError,
        match="current Clean/fact lineage changed during backfill planning",
    ):
        plan_backfill_range(
            manifest=backfill_fixture / "manifest.json",
            raw_root=backfill_fixture / "raw",
            station_root=backfill_fixture / "station",
            clean_root=backfill_fixture / "clean",
            warehouse_root=backfill_fixture / "warehouse",
            start_month="2020-01",
            end_month="2020-01",
            transformation_version=TRANSFORMATION_VERSION,
            configuration={},
        )


def test_planner_recovers_when_clean_is_ahead_of_previous_fact(
    backfill_fixture: Path,
):
    revision = _register_rental(
        backfill_fixture,
        source_month="2020-01",
        rows=[_month_row("2020-01", return_month="2020-03")],
        revision="v2-clean-ahead",
    )
    old_fact_pointer = (
        backfill_fixture
        / "warehouse"
        / "fact_trip_trusted"
        / "current"
        / "source_month=2020-01.json"
    )
    old_fact_version_id = _pointer_id(old_fact_pointer, "fact_version_id")
    publish_clean_month(
        manifest=backfill_fixture / "manifest.json",
        raw_root=backfill_fixture / "raw",
        clean_root=backfill_fixture / "clean",
        source_version_id=revision.record.source_version_id,
        source_month="2020-01",
        transformation_version=TRANSFORMATION_VERSION,
        configuration={},
    )

    plan = plan_backfill_range(
        manifest=backfill_fixture / "manifest.json",
        raw_root=backfill_fixture / "raw",
        station_root=backfill_fixture / "station",
        clean_root=backfill_fixture / "clean",
        warehouse_root=backfill_fixture / "warehouse",
        start_month="2020-01",
        end_month="2020-01",
        transformation_version=TRANSFORMATION_VERSION,
        configuration={},
    )

    assert plan.months[0].source_change == SOURCE_UNCHANGED
    assert plan.months[0].previous_fact_version_id == old_fact_version_id
    result = execute_backfill_plan(
        plan,
        manifest=backfill_fixture / "manifest.json",
        raw_root=backfill_fixture / "raw",
        station_root=backfill_fixture / "station",
        clean_root=backfill_fixture / "clean",
        warehouse_root=backfill_fixture / "warehouse",
        configuration={},
    )

    assert result.status == RUN_SUCCEEDED
    assert result.months[0].fact_version_id != old_fact_version_id
    assert tuple(
        item.service_month for item in result.months[0].station_day
    ) == ("2020-01", "2020-02", "2020-03")


def test_execution_fails_when_captured_clean_target_is_superseded(
    backfill_fixture: Path,
):
    v2 = _register_rental(
        backfill_fixture,
        source_month="2020-01",
        rows=[_month_row("2020-01", return_month="2020-03")],
        revision="v2-plan",
    )
    plan = plan_backfill_range(
        manifest=backfill_fixture / "manifest.json",
        raw_root=backfill_fixture / "raw",
        station_root=backfill_fixture / "station",
        clean_root=backfill_fixture / "clean",
        warehouse_root=backfill_fixture / "warehouse",
        start_month="2020-01",
        end_month="2020-01",
        transformation_version=TRANSFORMATION_VERSION,
        configuration={},
    )
    assert plan.months[0].source_version_id == v2.record.source_version_id

    v3 = _register_rental(
        backfill_fixture,
        source_month="2020-01",
        rows=[_month_row("2020-01", return_month="2020-04")],
        revision="v3-winner",
    )
    publish_clean_month(
        manifest=backfill_fixture / "manifest.json",
        raw_root=backfill_fixture / "raw",
        clean_root=backfill_fixture / "clean",
        source_version_id=v3.record.source_version_id,
        source_month="2020-01",
        transformation_version=TRANSFORMATION_VERSION,
        configuration={},
    )
    publish_current_fact_trip_month(
        manifest=backfill_fixture / "manifest.json",
        raw_root=backfill_fixture / "raw",
        station_root=backfill_fixture / "station",
        clean_root=backfill_fixture / "clean",
        warehouse_root=backfill_fixture / "warehouse",
        source_month="2020-01",
        transformation_version=TRANSFORMATION_VERSION,
        configuration={},
    )

    result = execute_backfill_plan(
        plan,
        manifest=backfill_fixture / "manifest.json",
        raw_root=backfill_fixture / "raw",
        station_root=backfill_fixture / "station",
        clean_root=backfill_fixture / "clean",
        warehouse_root=backfill_fixture / "warehouse",
        configuration={},
    )

    assert result.status != RUN_SUCCEEDED
    assert result.failed_source_months == ("2020-01",)
    assert result.months[0].clean_status == "FAILED"
    assert result.months[0].fact_version_id is None
    assert "backfill plan was superseded" in result.months[0].failures[0].message


def test_fact_publish_rejects_clean_advance_before_side_effect(
    backfill_fixture: Path,
):
    _register_rental(
        backfill_fixture,
        source_month="2020-01",
        rows=[_month_row("2020-01", return_month="2020-03")],
        revision="v2-gap-plan",
    )
    plan = plan_backfill_range(
        manifest=backfill_fixture / "manifest.json",
        raw_root=backfill_fixture / "raw",
        station_root=backfill_fixture / "station",
        clean_root=backfill_fixture / "clean",
        warehouse_root=backfill_fixture / "warehouse",
        start_month="2020-01",
        end_month="2020-01",
        transformation_version=TRANSFORMATION_VERSION,
        configuration={},
    )
    fact_pointer = (
        backfill_fixture
        / "warehouse"
        / "fact_trip_trusted"
        / "current"
        / "source_month=2020-01.json"
    )
    fact_before = _pointer_id(fact_pointer, "fact_version_id")
    advanced = False

    def advance_clean_before_fact(
        component: str,
        source_month: str,
        target_month: str,
    ) -> None:
        nonlocal advanced
        del target_month
        if component != "fact" or source_month != "2020-01" or advanced:
            return
        advanced = True
        v3 = _register_rental(
            backfill_fixture,
            source_month="2020-01",
            rows=[_month_row("2020-01", return_month="2020-04")],
            revision="v3-gap-winner",
        )
        publish_clean_month(
            manifest=backfill_fixture / "manifest.json",
            raw_root=backfill_fixture / "raw",
            clean_root=backfill_fixture / "clean",
            source_version_id=v3.record.source_version_id,
            source_month="2020-01",
            transformation_version=TRANSFORMATION_VERSION,
            configuration={},
        )

    result = execute_backfill_plan(
        plan,
        manifest=backfill_fixture / "manifest.json",
        raw_root=backfill_fixture / "raw",
        station_root=backfill_fixture / "station",
        clean_root=backfill_fixture / "clean",
        warehouse_root=backfill_fixture / "warehouse",
        configuration={},
        before_step=advance_clean_before_fact,
    )

    assert result.status != RUN_SUCCEEDED
    assert result.failed_source_months == ("2020-01",)
    assert result.months[0].fact_status == "FAILED"
    assert result.months[0].fact_version_id is None
    assert "expected lineage" in result.months[0].failures[0].message
    assert _pointer_id(fact_pointer, "fact_version_id") == fact_before


def test_post_fact_supersession_fails_closed_and_reconciles_current_return_month(
    backfill_fixture: Path,
):
    april_pointer = station_day_mart_current_pointer_path(
        backfill_fixture / "warehouse", "2020-04"
    )
    april_before = _pointer_id(april_pointer, "mart_version_id")
    _register_rental(
        backfill_fixture,
        source_month="2020-01",
        rows=[_month_row("2020-01", return_month="2020-03")],
        revision="v2-post-fact-plan",
    )
    plan = plan_backfill_range(
        manifest=backfill_fixture / "manifest.json",
        raw_root=backfill_fixture / "raw",
        station_root=backfill_fixture / "station",
        clean_root=backfill_fixture / "clean",
        warehouse_root=backfill_fixture / "warehouse",
        start_month="2020-01",
        end_month="2020-01",
        transformation_version=TRANSFORMATION_VERSION,
        configuration={},
    )
    superseded = False

    def supersede_after_planned_fact(
        component: str,
        source_month: str,
        target_month: str,
    ) -> None:
        nonlocal superseded
        del target_month
        if (
            component != "source_month_quality"
            or source_month != "2020-01"
            or superseded
        ):
            return
        superseded = True
        v3 = _register_rental(
            backfill_fixture,
            source_month="2020-01",
            rows=[_month_row("2020-01", return_month="2020-04")],
            revision="v3-post-fact-winner",
        )
        publish_clean_month(
            manifest=backfill_fixture / "manifest.json",
            raw_root=backfill_fixture / "raw",
            clean_root=backfill_fixture / "clean",
            source_version_id=v3.record.source_version_id,
            source_month="2020-01",
            transformation_version=TRANSFORMATION_VERSION,
            configuration={},
        )
        publish_current_fact_trip_month(
            manifest=backfill_fixture / "manifest.json",
            raw_root=backfill_fixture / "raw",
            station_root=backfill_fixture / "station",
            clean_root=backfill_fixture / "clean",
            warehouse_root=backfill_fixture / "warehouse",
            source_month="2020-01",
            transformation_version=TRANSFORMATION_VERSION,
            configuration={},
        )

    result = execute_backfill_plan(
        plan,
        manifest=backfill_fixture / "manifest.json",
        raw_root=backfill_fixture / "raw",
        station_root=backfill_fixture / "station",
        clean_root=backfill_fixture / "clean",
        warehouse_root=backfill_fixture / "warehouse",
        configuration={},
        before_step=supersede_after_planned_fact,
    )

    assert result.status != RUN_SUCCEEDED
    assert result.failed_source_months == ("2020-01",)
    assert tuple(
        item.service_month for item in result.months[0].station_day
    ) == ("2020-01", "2020-02", "2020-03", "2020-04")
    assert any(
        failure.component == "fact_superseded"
        for failure in result.months[0].failures
    )
    assert _pointer_id(april_pointer, "mart_version_id") != april_before

    _, recovery = run_backfill_range(
        manifest=backfill_fixture / "manifest.json",
        raw_root=backfill_fixture / "raw",
        station_root=backfill_fixture / "station",
        clean_root=backfill_fixture / "clean",
        warehouse_root=backfill_fixture / "warehouse",
        start_month="2020-01",
        end_month="2020-01",
        transformation_version=TRANSFORMATION_VERSION,
        configuration={},
    )
    assert recovery.status == RUN_SUCCEEDED


def test_yearly_zip_parent_revision_rebuilds_only_changed_member_month(tmp_path: Path):
    jan_bytes = _csv_bytes([_month_row("2020-01")])
    feb_v1_bytes = _csv_bytes([_month_row("2020-02")])
    feb_v2_bytes = _csv_bytes(
        [
            rental_row(
                bike_id="BIKE-2020-02-REV",
                rent_at="2020-02-02 09:00:00",
                rent_station_no="00001",
                return_at="2020-02-02 09:10:00",
                return_station_no="00002",
                usage_min="10",
                distance_m="100",
            )
        ]
    )
    v1_path = tmp_path / "rental-2020-v1.zip"
    _write_zip(
        v1_path,
        [
            ("rental_202001.csv", jan_bytes),
            ("rental_202002.csv", feb_v1_bytes),
        ],
    )
    v1 = register_source(
        artifact=v1_path,
        manifest=tmp_path / "manifest.json",
        raw_root=tmp_path / "raw",
        dataset_id="rental_history",
        logical_period="2020",
        published_filename="rental_2020_v1.zip",
        source_url="https://example.invalid/rental-2020",
        license="SYNTHETIC_FIXTURE",
    )
    inventory_registered_zip(
        manifest=tmp_path / "manifest.json",
        raw_root=tmp_path / "raw",
        source_version_id=v1.record.source_version_id,
    )
    for month in ("2020-01", "2020-02"):
        publish_clean_month(
            manifest=tmp_path / "manifest.json",
            raw_root=tmp_path / "raw",
            clean_root=tmp_path / "clean",
            source_version_id=v1.record.source_version_id,
            source_month=month,
            transformation_version=TRANSFORMATION_VERSION,
            configuration={},
        )

    v2_path = tmp_path / "rental-2020-v2.zip"
    _write_zip(
        v2_path,
        [
            ("renamed_2020.01.csv", jan_bytes),
            ("rental_202002.csv", feb_v2_bytes),
        ],
    )
    v2 = register_source(
        artifact=v2_path,
        manifest=tmp_path / "manifest.json",
        raw_root=tmp_path / "raw",
        dataset_id="rental_history",
        logical_period="2020",
        published_filename="rental_2020_v2.zip",
        source_url="https://example.invalid/rental-2020",
        license="SYNTHETIC_FIXTURE",
    )
    inventory_registered_zip(
        manifest=tmp_path / "manifest.json",
        raw_root=tmp_path / "raw",
        source_version_id=v2.record.source_version_id,
    )

    plan = plan_backfill_range(
        manifest=tmp_path / "manifest.json",
        raw_root=tmp_path / "raw",
        station_root=tmp_path / "station",
        clean_root=tmp_path / "clean",
        warehouse_root=tmp_path / "warehouse",
        start_month="2020-01",
        end_month="2020-02",
        transformation_version=TRANSFORMATION_VERSION,
        configuration={},
    )

    assert plan.months[0].source_change == SOURCE_UNCHANGED
    assert plan.months[0].source_version_id == v1.record.source_version_id
    assert plan.months[1].source_change == SOURCE_REVISED
    assert plan.months[1].source_version_id == v2.record.source_version_id


def test_planner_verifies_shared_yearly_source_only_once(tmp_path: Path, monkeypatch):
    yearly_path = tmp_path / "rental-2020.zip"
    _write_zip(
        yearly_path,
        [
            ("rental_202001.csv", _csv_bytes([_month_row("2020-01")])),
            ("rental_202002.csv", _csv_bytes([_month_row("2020-02")])),
        ],
    )
    yearly = register_source(
        artifact=yearly_path,
        manifest=tmp_path / "manifest.json",
        raw_root=tmp_path / "raw",
        dataset_id="rental_history",
        logical_period="2020",
        published_filename="rental_2020.zip",
        source_url="https://example.invalid/rental-2020",
        license="SYNTHETIC_FIXTURE",
    )
    inventory_registered_zip(
        manifest=tmp_path / "manifest.json",
        raw_root=tmp_path / "raw",
        source_version_id=yearly.record.source_version_id,
    )
    verified: list[str] = []
    original = backfill_module._verify_source_record

    def record_verification(raw_root, record):
        verified.append(record.source_version_id)
        original(raw_root, record)

    monkeypatch.setattr(backfill_module, "_verify_source_record", record_verification)

    plan = plan_backfill_range(
        manifest=tmp_path / "manifest.json",
        raw_root=tmp_path / "raw",
        station_root=tmp_path / "station",
        clean_root=tmp_path / "clean",
        warehouse_root=tmp_path / "warehouse",
        start_month="2020-01",
        end_month="2020-02",
        transformation_version=TRANSFORMATION_VERSION,
        configuration={},
    )

    assert [item.source_change for item in plan.months] == [SOURCE_NEW, SOURCE_NEW]
    assert verified == [yearly.record.source_version_id]


def test_unrelated_yearly_zip_without_inventory_does_not_block_requested_range(
    tmp_path: Path,
):
    monthly = _register_rental(
        tmp_path,
        source_month="2020-01",
        rows=[_month_row("2020-01")],
    )
    unrelated_zip = tmp_path / "rental-2021.zip"
    _write_zip(
        unrelated_zip,
        [("rental_202101.csv", _csv_bytes([_month_row("2021-01")]))],
    )
    register_source(
        artifact=unrelated_zip,
        manifest=tmp_path / "manifest.json",
        raw_root=tmp_path / "raw",
        dataset_id="rental_history",
        logical_period="2021",
        published_filename="rental_2021.zip",
        source_url="https://example.invalid/rental-2021",
        license="SYNTHETIC_FIXTURE",
    )

    plan = plan_backfill_range(
        manifest=tmp_path / "manifest.json",
        raw_root=tmp_path / "raw",
        station_root=tmp_path / "station",
        clean_root=tmp_path / "clean",
        warehouse_root=tmp_path / "warehouse",
        start_month="2020-01",
        end_month="2020-01",
        transformation_version=TRANSFORMATION_VERSION,
        configuration={},
    )

    assert len(plan.months) == 1
    assert plan.months[0].source_version_id == monthly.record.source_version_id


def test_partial_clean_failure_preserves_success_and_retries_failed_month_only(
    backfill_fixture: Path,
):
    _register_rental(
        backfill_fixture,
        source_month="2020-05",
        rows=[_month_row("2020-05")],
    )
    _register_rental(
        backfill_fixture,
        source_month="2020-06",
        rows=[_month_row("2020-06")],
    )
    plan = plan_backfill_range(
        manifest=backfill_fixture / "manifest.json",
        raw_root=backfill_fixture / "raw",
        station_root=backfill_fixture / "station",
        clean_root=backfill_fixture / "clean",
        warehouse_root=backfill_fixture / "warehouse",
        start_month="2020-05",
        end_month="2020-06",
        transformation_version=TRANSFORMATION_VERSION,
        configuration={},
    )

    def fail_june_clean(component: str, source_month: str, target_month: str) -> None:
        del target_month
        if component == "clean" and source_month == "2020-06":
            raise RuntimeError("injected clean failure")

    result = execute_backfill_plan(
        plan,
        manifest=backfill_fixture / "manifest.json",
        raw_root=backfill_fixture / "raw",
        station_root=backfill_fixture / "station",
        clean_root=backfill_fixture / "clean",
        warehouse_root=backfill_fixture / "warehouse",
        configuration={},
        before_step=fail_june_clean,
    )

    assert result.status == RUN_PARTIAL_FAILURE
    assert result.failed_source_months == ("2020-06",)
    assert result.months[0].status != MONTH_FAILED
    assert result.months[1].clean_status == "FAILED"
    may_clean = _pointer_id(
        backfill_fixture / "clean" / "current" / "source_month=2020-05.json",
        "clean_version_id",
    )
    assert not (
        backfill_fixture / "clean" / "current" / "source_month=2020-06.json"
    ).exists()

    retry_plan = retry_failed_months_plan(plan, result)
    assert [item.source_month for item in retry_plan.months] == ["2020-06"]
    retry = execute_backfill_plan(
        retry_plan,
        manifest=backfill_fixture / "manifest.json",
        raw_root=backfill_fixture / "raw",
        station_root=backfill_fixture / "station",
        clean_root=backfill_fixture / "clean",
        warehouse_root=backfill_fixture / "warehouse",
        configuration={},
    )

    assert retry.status == RUN_SUCCEEDED
    assert retry.failed_source_months == ()
    assert _pointer_id(
        backfill_fixture / "clean" / "current" / "source_month=2020-05.json",
        "clean_version_id",
    ) == may_clean


def test_historical_revision_mart_failure_retry_retains_old_fact_impact(
    backfill_fixture: Path,
):
    march_pointer = station_day_mart_current_pointer_path(
        backfill_fixture / "warehouse", "2020-03"
    )
    april_pointer = station_day_mart_current_pointer_path(
        backfill_fixture / "warehouse", "2020-04"
    )
    march_before = _pointer_id(march_pointer, "mart_version_id")
    april_before = _pointer_id(april_pointer, "mart_version_id")

    _register_rental(
        backfill_fixture,
        source_month="2020-01",
        rows=[_month_row("2020-01", return_month="2020-03")],
        revision="v2",
    )
    plan = plan_backfill_range(
        manifest=backfill_fixture / "manifest.json",
        raw_root=backfill_fixture / "raw",
        station_root=backfill_fixture / "station",
        clean_root=backfill_fixture / "clean",
        warehouse_root=backfill_fixture / "warehouse",
        start_month="2020-01",
        end_month="2020-01",
        transformation_version=TRANSFORMATION_VERSION,
        configuration={},
    )
    assert plan.months[0].source_change == SOURCE_REVISED
    old_fact_version_id = plan.months[0].previous_fact_version_id
    assert old_fact_version_id is not None

    def fail_march_station_day(
        component: str, source_month: str, target_month: str
    ) -> None:
        del source_month
        if component == "station_day" and target_month == "2020-03":
            raise RuntimeError("injected mart failure")

    result = execute_backfill_plan(
        plan,
        manifest=backfill_fixture / "manifest.json",
        raw_root=backfill_fixture / "raw",
        station_root=backfill_fixture / "station",
        clean_root=backfill_fixture / "clean",
        warehouse_root=backfill_fixture / "warehouse",
        configuration={},
        before_step=fail_march_station_day,
    )
    assert result.status != RUN_SUCCEEDED
    assert result.failed_source_months == ("2020-01",)
    assert _pointer_id(march_pointer, "mart_version_id") == march_before
    assert _pointer_id(april_pointer, "mart_version_id") == april_before
    assert result.months[0].fact_version_id != old_fact_version_id

    retry_plan = retry_failed_months_plan(plan, result)
    assert retry_plan.months[0].previous_fact_version_id == old_fact_version_id
    retry = execute_backfill_plan(
        retry_plan,
        manifest=backfill_fixture / "manifest.json",
        raw_root=backfill_fixture / "raw",
        station_root=backfill_fixture / "station",
        clean_root=backfill_fixture / "clean",
        warehouse_root=backfill_fixture / "warehouse",
        configuration={},
    )

    assert retry.status == RUN_SUCCEEDED
    assert tuple(
        item.service_month for item in retry.months[0].station_day
    ) == ("2020-01", "2020-02", "2020-03")
    assert _pointer_id(march_pointer, "mart_version_id") != march_before
    assert _pointer_id(april_pointer, "mart_version_id") == april_before
