from __future__ import annotations

import csv
import hashlib
import io
import json
import shutil
from contextlib import contextmanager
from pathlib import Path

import duckdb
import pytest

import seoul_bike_pipeline.station_day_mart as station_day_mart
from seoul_bike_pipeline.clean_publish import publish_clean_month
from seoul_bike_pipeline.csv_source import RENTAL_V16_HEADER
from seoul_bike_pipeline.date_dimension import publish_date_dimension
from seoul_bike_pipeline.errors import StationDayMartError
from seoul_bike_pipeline.fact_publish import publish_current_fact_trip_month
from seoul_bike_pipeline.register import register_source
from seoul_bike_pipeline.station_day_mart import (
    STATUS_PUBLISHED_CURRENT,
    STATUS_REUSED_CURRENT,
    compute_station_day_mart_version_id,
    plan_station_day_fact_revision_impact,
    publish_current_station_day_mart,
    station_day_mart_current_pointer_path,
    validate_station_day_mart_version,
)
from seoul_bike_pipeline.station_history import publish_station_history
from tests.test_trip_station_enrichment import (
    publish_station_snapshot_fixture,
    run_cli,
    station_row,
)


def rental_row(
    *,
    bike_id: str,
    rent_at: str,
    rent_station_no: str,
    return_at: str,
    return_station_no: str,
    usage_min: str,
    distance_m: str,
) -> tuple[str, ...]:
    return (
        bike_id,
        rent_at,
        rent_station_no,
        f"rent-{rent_station_no}",
        "0",
        return_at,
        return_station_no,
        f"return-{return_station_no}" if return_station_no else "",
        "0",
        usage_min,
        distance_m,
        "1990",
        "M",
        "내국인",
        f"ST-R-{rent_station_no}",
        f"ST-X-{return_station_no}" if return_station_no else "",
    )


def publish_fact_fixture(
    root: Path,
    *,
    source_month: str,
    rows: list[tuple[str, ...]],
):
    buffer = io.StringIO(newline="")
    writer = csv.writer(buffer, lineterminator="\r\n")
    writer.writerow(RENTAL_V16_HEADER)
    writer.writerows(rows)
    artifact = root / f"rental-{source_month}.csv"
    artifact.write_bytes(buffer.getvalue().encode("cp949"))
    registered = register_source(
        artifact=artifact,
        manifest=root / "manifest.json",
        raw_root=root / "raw",
        dataset_id="rental_history",
        logical_period=source_month,
        published_filename=f"rental_{source_month.replace('-', '')}.csv",
        source_url="https://example.invalid/rental",
        license="SYNTHETIC_FIXTURE",
    )
    publish_clean_month(
        manifest=root / "manifest.json",
        raw_root=root / "raw",
        clean_root=root / "clean",
        source_version_id=registered.record.source_version_id,
        source_month=source_month,
        transformation_version="clean-t1",
        configuration={},
    )
    return publish_current_fact_trip_month(
        manifest=root / "manifest.json",
        raw_root=root / "raw",
        station_root=root / "station",
        clean_root=root / "clean",
        warehouse_root=root / "warehouse",
        source_month=source_month,
        transformation_version="fact-t1",
        configuration={},
    )


def build_base_fixture(root: Path) -> None:
    publish_station_snapshot_fixture(
        root,
        period="2019-12",
        filename="공공자전거 대여소 정보(19.12월 기준).csv",
        suffix="201912",
        rows=[
            station_row("00001", "station-1"),
            station_row("00002", "station-2"),
            station_row("00004", "station-4"),
        ],
    )
    publish_station_history(
        manifest=root / "manifest.json",
        raw_root=root / "raw",
        station_root=root / "station",
        warehouse_root=root / "warehouse",
        transformation_version="history-t1",
        configuration={},
    )
    publish_fact_fixture(
        root,
        source_month="2020-01",
        rows=[
            rental_row(
                bike_id="BIKE-A",
                rent_at="2020-01-31 23:50:00",
                rent_station_no="00001",
                return_at="2020-02-01 00:10:00",
                return_station_no="00002",
                usage_min="20",
                distance_m="100",
            ),
            rental_row(
                bike_id="BIKE-B",
                rent_at="2020-01-15 10:00:00",
                rent_station_no="00001",
                return_at="2020-01-15 10:10:00",
                return_station_no="00002",
                usage_min="10",
                distance_m="100",
            ),
            rental_row(
                bike_id="BIKE-LONG",
                rent_at="2020-01-01 00:00:00",
                rent_station_no="00001",
                return_at="2020-03-01 00:10:00",
                return_station_no="00004",
                usage_min="86400",
                distance_m="100",
            ),
        ],
    )
    publish_fact_fixture(
        root,
        source_month="2020-02",
        rows=[
            rental_row(
                bike_id="BIKE-SAME",
                rent_at="2020-02-01 01:00:00",
                rent_station_no="00002",
                return_at="2020-02-01 01:10:00",
                return_station_no="00001",
                usage_min="10",
                distance_m="0",
            ),
            rental_row(
                bike_id="BIKE-SAME",
                rent_at="2020-02-01 02:00:00",
                rent_station_no="00002",
                return_at="",
                return_station_no="",
                usage_min="20",
                distance_m="50",
            ),
            rental_row(
                bike_id="BIKE-E",
                rent_at="2020-02-01 03:00:00",
                rent_station_no="00003",
                return_at="2020-02-01 03:30:00",
                return_station_no="00004",
                usage_min="30",
                distance_m="100001",
            ),
        ],
    )
    publish_fact_fixture(
        root,
        source_month="2020-03",
        rows=[
            rental_row(
                bike_id="BIKE-MARCH",
                rent_at="2020-03-02 09:00:00",
                rent_station_no="00001",
                return_at="2020-03-02 09:05:00",
                return_station_no="00002",
                usage_min="5",
                distance_m="50",
            ),
        ],
    )
    publish_fact_fixture(
        root,
        source_month="2020-04",
        rows=[
            rental_row(
                bike_id="BIKE-APRIL",
                rent_at="2020-04-02 09:00:00",
                rent_station_no="00001",
                return_at="2020-04-02 09:05:00",
                return_station_no="00002",
                usage_min="5",
                distance_m="50",
            ),
        ],
    )
    publish_date_dimension(
        warehouse_root=root / "warehouse",
        transformation_version="date-t1",
        configuration={},
    )


@pytest.fixture(scope="module")
def base_fixture(tmp_path_factory) -> Path:
    root = tmp_path_factory.mktemp("station-day-base")
    build_base_fixture(root)
    return root


@pytest.fixture
def mart_fixture(base_fixture: Path, tmp_path: Path) -> Path:
    for child in base_fixture.iterdir():
        destination = tmp_path / child.name
        if child.is_dir():
            shutil.copytree(child, destination)
        else:
            shutil.copy2(child, destination)
    return tmp_path


def publish_mart(
    root: Path,
    *,
    service_month: str = "2020-02",
    transformation_version: str = "station-day-t1",
    **kwargs,
):
    return publish_current_station_day_mart(
        manifest=root / "manifest.json",
        raw_root=root / "raw",
        station_root=root / "station",
        clean_root=root / "clean",
        warehouse_root=root / "warehouse",
        service_month=service_month,
        transformation_version=transformation_version,
        configuration={},
        **kwargs,
    )


def rewrite_database_fingerprint(version_path: Path) -> None:
    database_path = version_path / "warehouse.duckdb"
    metadata_path = version_path / "metadata.json"
    metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
    data = database_path.read_bytes()
    metadata["warehouse_database"] = {
        "size_bytes": len(data),
        "sha256": hashlib.sha256(data).hexdigest(),
    }
    metadata_path.write_text(
        json.dumps(
            metadata,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        )
        + "\n",
        encoding="utf-8",
    )


def test_station_day_metrics_use_rental_cohort_and_actual_return_date(mart_fixture):
    first = publish_mart(mart_fixture)
    assert first.status == STATUS_PUBLISHED_CURRENT
    assert first.mart_row_count == 4
    assert first.rental_count_total == 3
    assert first.return_count_total == 3
    assert first.completed_trip_count_total == 2
    assert first.valid_distance_trip_count_total == 1
    assert first.missing_return_count_total == 1
    assert first.station_history_match_count_total == 2
    assert [month for month, _ in first.fact_lineage] == ["2020-01", "2020-02"]
    assert [month for month, _ in first.fact_scan_lineage] == [
        "2020-01",
        "2020-02",
    ]

    connection = duckdb.connect(
        str(first.version_path / "warehouse.duckdb"), read_only=True
    )
    try:
        rows = connection.execute(
            """
            SELECT *
            FROM mart_station_day
            ORDER BY service_date, station_no
            """
        ).fetchall()
    finally:
        connection.close()
    assert rows == [
        (
            station_day_mart.date(2020, 2, 1),
            "1",
            0,
            1,
            0,
            0,
            None,
            None,
            0,
            0,
            0,
        ),
        (
            station_day_mart.date(2020, 2, 1),
            "2",
            2,
            1,
            1,
            1,
            15.0,
            15.0,
            1,
            1,
            2,
        ),
        (
            station_day_mart.date(2020, 2, 1),
            "3",
            1,
            0,
            1,
            1,
            30.0,
            30.0,
            0,
            0,
            0,
        ),
        (
            station_day_mart.date(2020, 2, 1),
            "4",
            0,
            1,
            0,
            0,
            None,
            None,
            0,
            0,
            0,
        ),
    ]
    assert all(row[4] + row[9] == row[2] for row in rows)

    second = publish_mart(mart_fixture)
    assert second.status == STATUS_REUSED_CURRENT
    assert second.mart_version_id == first.mart_version_id


def test_long_completed_return_from_two_months_prior_is_counted(mart_fixture):
    result = publish_mart(mart_fixture, service_month="2020-03")
    assert [month for month, _ in result.fact_scan_lineage] == [
        "2020-01",
        "2020-02",
        "2020-03",
    ]
    assert [month for month, _ in result.fact_lineage] == ["2020-01", "2020-03"]
    assert result.rental_count_total == 1
    assert result.return_count_total == 2
    connection = duckdb.connect(
        str(result.version_path / "warehouse.duckdb"), read_only=True
    )
    try:
        assert connection.execute(
            """
            SELECT return_count
            FROM mart_station_day
            WHERE service_date = DATE '2020-03-01' AND station_no = '4'
            """
        ).fetchone() == (1,)
    finally:
        connection.close()


def test_revision_impact_uses_old_and_new_return_months_and_keeps_april_stable(
    mart_fixture,
):
    january_pointer_path = (
        mart_fixture
        / "warehouse"
        / "fact_trip_trusted"
        / "current"
        / "source_month=2020-01.json"
    )
    old_january_fact_id = json.loads(
        january_pointer_path.read_text(encoding="utf-8")
    )["fact_version_id"]

    january_before = publish_mart(mart_fixture, service_month="2020-01")
    february_before = publish_mart(mart_fixture, service_month="2020-02")
    march_before = publish_mart(mart_fixture, service_month="2020-03")
    april_before = publish_mart(mart_fixture, service_month="2020-04")
    april_pointer_bytes = april_before.current_pointer_path.read_bytes()

    revised_january = publish_fact_fixture(
        mart_fixture,
        source_month="2020-01",
        rows=[
            rental_row(
                bike_id="BIKE-A",
                rent_at="2020-01-31 23:50:00",
                rent_station_no="00001",
                return_at="2020-03-01 00:10:00",
                return_station_no="00002",
                usage_min="20",
                distance_m="100",
            ),
            rental_row(
                bike_id="BIKE-B",
                rent_at="2020-01-15 10:00:00",
                rent_station_no="00001",
                return_at="2020-01-15 10:10:00",
                return_station_no="00002",
                usage_min="10",
                distance_m="100",
            ),
            rental_row(
                bike_id="BIKE-LONG",
                rent_at="2020-01-01 00:00:00",
                rent_station_no="00001",
                return_at="2020-03-01 00:10:00",
                return_station_no="00004",
                usage_min="86400",
                distance_m="100",
            ),
        ],
    )
    assert revised_january.fact_version_id != old_january_fact_id

    impact = plan_station_day_fact_revision_impact(
        manifest=mart_fixture / "manifest.json",
        raw_root=mart_fixture / "raw",
        station_root=mart_fixture / "station",
        clean_root=mart_fixture / "clean",
        warehouse_root=mart_fixture / "warehouse",
        source_month="2020-01",
        old_fact_version_id=old_january_fact_id,
        new_fact_version_id=revised_january.fact_version_id,
    )
    assert impact.affected_service_months == (
        "2020-01",
        "2020-02",
        "2020-03",
    )
    cli_impact = run_cli(
        "warehouse",
        "plan-station-day-revision",
        "--manifest",
        str(mart_fixture / "manifest.json"),
        "--raw-root",
        str(mart_fixture / "raw"),
        "--station-root",
        str(mart_fixture / "station"),
        "--clean-root",
        str(mart_fixture / "clean"),
        "--warehouse-root",
        str(mart_fixture / "warehouse"),
        "--source-month",
        "2020-01",
        "--old-fact-version-id",
        old_january_fact_id,
        "--new-fact-version-id",
        revised_january.fact_version_id,
    )
    assert cli_impact.returncode == 0, cli_impact.stderr
    assert "affected=2020-01,2020-02,2020-03" in cli_impact.stdout

    january_after = publish_mart(mart_fixture, service_month="2020-01")
    february_after = publish_mart(mart_fixture, service_month="2020-02")
    march_after = publish_mart(mart_fixture, service_month="2020-03")
    april_after = publish_mart(mart_fixture, service_month="2020-04")

    assert january_after.mart_version_id != january_before.mart_version_id
    assert february_after.mart_version_id != february_before.mart_version_id
    assert march_after.mart_version_id != march_before.mart_version_id
    assert february_before.return_count_total == 3
    assert february_after.return_count_total == 2
    assert march_before.return_count_total == 2
    assert march_after.return_count_total == 3
    assert april_after.status == STATUS_REUSED_CURRENT
    assert april_after.mart_version_id == april_before.mart_version_id
    assert april_after.current_pointer_path.read_bytes() == april_pointer_bytes


def test_missing_prior_fact_current_is_hard_failure(mart_fixture):
    january_pointer = (
        mart_fixture
        / "warehouse"
        / "fact_trip_trusted"
        / "current"
        / "source_month=2020-01.json"
    )
    january_pointer.unlink()
    with pytest.raises(StationDayMartError, match="current trusted fact pointer"):
        publish_mart(mart_fixture)


def test_transform_failure_preserves_previous_current(mart_fixture):
    first = publish_mart(mart_fixture)
    before = first.current_pointer_path.read_bytes()
    bad_sql = mart_fixture / "broken-station-day.sql"
    bad_sql.write_text(
        "CREATE TABLE mart_station_day AS SELECT does_not_exist;\n",
        encoding="utf-8",
    )
    with pytest.raises(StationDayMartError, match="SQL build failed"):
        publish_mart(
            mart_fixture,
            transformation_version="station-day-t2",
            sql_path=bad_sql,
        )
    assert first.current_pointer_path.read_bytes() == before


def test_pre_current_failure_preserves_current_and_retry_succeeds(mart_fixture):
    first = publish_mart(mart_fixture)
    before = first.current_pointer_path.read_bytes()

    def fail_before_current(_version_path: Path) -> None:
        raise RuntimeError("injected mart failure")

    with pytest.raises(RuntimeError, match="injected mart failure"):
        publish_mart(
            mart_fixture,
            transformation_version="station-day-t2",
            before_current_publish=fail_before_current,
        )
    assert first.current_pointer_path.read_bytes() == before
    retry = publish_mart(
        mart_fixture,
        transformation_version="station-day-t2",
    )
    assert retry.status == STATUS_PUBLISHED_CURRENT


def test_fact_current_change_during_pre_current_hook_preserves_old_mart(mart_fixture):
    first = publish_mart(mart_fixture)
    before = first.current_pointer_path.read_bytes()
    fact_pointer = (
        mart_fixture
        / "warehouse"
        / "fact_trip_trusted"
        / "current"
        / "source_month=2020-01.json"
    )

    def change_fact_current(_version_path: Path) -> None:
        fact_pointer.write_text(
            json.dumps(
                {
                    "schema_version": 1,
                    "source_month": "2020-01",
                    "fact_version_id": "fv1_" + "0" * 64,
                },
                sort_keys=True,
            )
            + "\n",
            encoding="utf-8",
        )

    with pytest.raises(StationDayMartError, match="fact current pointer changed"):
        publish_mart(
            mart_fixture,
            transformation_version="station-day-t2",
            before_current_publish=change_fact_current,
        )
    assert first.current_pointer_path.read_bytes() == before


def test_concurrent_different_mart_publish_cannot_roll_back_winner(
    mart_fixture, monkeypatch
):
    real_locks = station_day_mart._publication_locks
    state = {"nested": False, "winner": None}

    @contextmanager
    def interleaving_locks(**kwargs):
        if not state["nested"]:
            state["nested"] = True
            state["winner"] = publish_mart(
                mart_fixture,
                transformation_version="station-day-t2",
            )
            state["nested"] = False
        with real_locks(**kwargs):
            yield

    monkeypatch.setattr(
        station_day_mart, "_publication_locks", interleaving_locks
    )
    with pytest.raises(StationDayMartError, match="current pointer changed during build"):
        publish_mart(
            mart_fixture,
            transformation_version="station-day-t1",
        )
    pointer = json.loads(
        station_day_mart_current_pointer_path(
            mart_fixture / "warehouse", "2020-02"
        ).read_text(encoding="utf-8")
    )
    assert pointer["mart_version_id"] == state["winner"].mart_version_id


def test_concurrent_same_version_converges_to_reused_current(
    mart_fixture, monkeypatch
):
    real_locks = station_day_mart._publication_locks
    state = {"nested": False, "winner": None}

    @contextmanager
    def interleaving_locks(**kwargs):
        if not state["nested"]:
            state["nested"] = True
            state["winner"] = publish_mart(mart_fixture)
            state["nested"] = False
        with real_locks(**kwargs):
            yield

    monkeypatch.setattr(
        station_day_mart, "_publication_locks", interleaving_locks
    )
    outer = publish_mart(mart_fixture)
    assert state["winner"].status == STATUS_PUBLISHED_CURRENT
    assert outer.status == STATUS_REUSED_CURRENT
    assert outer.mart_version_id == state["winner"].mart_version_id


def test_current_noncontributor_fact_tamper_before_lock_blocks_reuse(
    mart_fixture, monkeypatch
):
    april_before = publish_mart(mart_fixture, service_month="2020-04")
    pointer_before = april_before.current_pointer_path.read_bytes()
    revised_january = publish_fact_fixture(
        mart_fixture,
        source_month="2020-01",
        rows=[
            rental_row(
                bike_id="BIKE-A",
                rent_at="2020-01-31 23:50:00",
                rent_station_no="00001",
                return_at="2020-02-01 00:10:00",
                return_station_no="00002",
                usage_min="21",
                distance_m="100",
            ),
            rental_row(
                bike_id="BIKE-B",
                rent_at="2020-01-15 10:00:00",
                rent_station_no="00001",
                return_at="2020-01-15 10:10:00",
                return_station_no="00002",
                usage_min="10",
                distance_m="100",
            ),
            rental_row(
                bike_id="BIKE-LONG",
                rent_at="2020-01-01 00:00:00",
                rent_station_no="00001",
                return_at="2020-03-01 00:10:00",
                return_station_no="00004",
                usage_min="86400",
                distance_m="100",
            ),
        ],
    )
    database_path = revised_january.version_path / "warehouse.duckdb"
    real_locks = station_day_mart._publication_locks
    state = {"tampered": False}

    @contextmanager
    def tamper_before_real_locks(**kwargs):
        if not state["tampered"]:
            state["tampered"] = True
            connection = duckdb.connect(str(database_path))
            try:
                connection.execute("CREATE TABLE physical_noise AS SELECT 1 AS x")
                connection.execute("DROP TABLE physical_noise")
                connection.execute("CHECKPOINT")
            finally:
                connection.close()
            rewrite_database_fingerprint(revised_january.version_path)
        with real_locks(**kwargs):
            yield

    monkeypatch.setattr(
        station_day_mart, "_publication_locks", tamper_before_real_locks
    )
    with pytest.raises(StationDayMartError, match="immutable metadata changed"):
        publish_mart(mart_fixture, service_month="2020-04")
    assert april_before.current_pointer_path.read_bytes() == pointer_before


def test_mart_tamper_is_rejected_even_with_updated_fingerprint(mart_fixture):
    published = publish_mart(mart_fixture)
    database_path = published.version_path / "warehouse.duckdb"
    connection = duckdb.connect(str(database_path))
    try:
        connection.execute(
            """
            UPDATE mart_station_day
            SET rental_count = rental_count + 1
            WHERE station_no = '2'
            """
        )
        connection.execute("CHECKPOINT")
    finally:
        connection.close()
    rewrite_database_fingerprint(published.version_path)
    with pytest.raises(StationDayMartError, match="independent recomputation"):
        validate_station_day_mart_version(
            published.version_path,
            manifest=mart_fixture / "manifest.json",
            raw_root=mart_fixture / "raw",
            station_root=mart_fixture / "station",
            clean_root=mart_fixture / "clean",
            warehouse_root=mart_fixture / "warehouse",
        )


def test_material_fact_lineage_tamper_is_rejected(mart_fixture):
    published = publish_mart(mart_fixture)
    metadata = json.loads(
        (published.version_path / "metadata.json").read_text(encoding="utf-8")
    )
    metadata["fact_lineage"] = [
        item
        for item in metadata["fact_lineage"]
        if item["source_month"] == "2020-02"
    ]
    new_id = compute_station_day_mart_version_id(
        service_month=metadata["service_month"],
        date_version_id=metadata["date_version_id"],
        fact_lineage=metadata["fact_lineage"],
        transformation_version=metadata["transformation_version"],
        configuration_fingerprint=metadata["configuration_fingerprint"],
        sql_sha256=metadata["sql_sha256"],
    )
    metadata["mart_version_id"] = new_id
    fake_path = (
        published.version_path.parent / f"mart_version_id={new_id}"
    )
    fake_path.mkdir()
    (fake_path / "warehouse.duckdb").write_bytes(
        (published.version_path / "warehouse.duckdb").read_bytes()
    )
    (fake_path / "metadata.json").write_text(
        json.dumps(
            metadata,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        )
        + "\n",
        encoding="utf-8",
    )
    with pytest.raises(StationDayMartError, match="material fact_lineage"):
        validate_station_day_mart_version(
            fake_path,
            manifest=mart_fixture / "manifest.json",
            raw_root=mart_fixture / "raw",
            station_root=mart_fixture / "station",
            clean_root=mart_fixture / "clean",
            warehouse_root=mart_fixture / "warehouse",
        )


def test_small_avg_usage_tamper_is_rejected_even_with_updated_fingerprint(
    mart_fixture,
):
    published = publish_mart(mart_fixture)
    database_path = published.version_path / "warehouse.duckdb"
    connection = duckdb.connect(str(database_path))
    try:
        connection.execute(
            """
            UPDATE mart_station_day
            SET avg_usage_min = avg_usage_min + 0.00000000001
            WHERE station_no = '2'
            """
        )
        connection.execute("CHECKPOINT")
    finally:
        connection.close()
    rewrite_database_fingerprint(published.version_path)
    with pytest.raises(StationDayMartError, match="independent recomputation"):
        validate_station_day_mart_version(
            published.version_path,
            manifest=mart_fixture / "manifest.json",
            raw_root=mart_fixture / "raw",
            station_root=mart_fixture / "station",
            clean_root=mart_fixture / "clean",
            warehouse_root=mart_fixture / "warehouse",
        )


def test_nullable_usage_average_and_median_use_non_null_rental_values():
    sql_text, _ = station_day_mart._load_sql(None)
    connection = duckdb.connect()
    try:
        connection.execute(
            """
            CREATE TABLE rental_fact_input (
                bike_id VARCHAR,
                rent_at TIMESTAMP,
                rent_station_no VARCHAR,
                usage_min BIGINT,
                is_completed_trip BOOLEAN,
                distance_metric_eligible BOOLEAN,
                station_history_match BOOLEAN
            )
            """
        )
        connection.execute(
            """
            INSERT INTO rental_fact_input VALUES
                ('A', TIMESTAMP '2020-02-01 01:00:00', '1', NULL, TRUE, TRUE, TRUE),
                ('B', TIMESTAMP '2020-02-01 02:00:00', '1', 1, TRUE, TRUE, TRUE),
                ('C', TIMESTAMP '2020-02-01 03:00:00', '1', 2, TRUE, TRUE, TRUE)
            """
        )
        connection.execute(
            """
            CREATE TABLE return_event_input (
                return_at TIMESTAMP,
                return_station_no VARCHAR
            )
            """
        )
        connection.execute(sql_text)
        assert connection.execute(
            """
            SELECT avg_usage_min, median_usage_min
            FROM mart_station_day
            WHERE station_no = '1'
            """
        ).fetchone() == (1.5, 1.5)
    finally:
        connection.close()


def test_production_end_excludes_july_returns_and_rejects_july_publish(tmp_path):
    sql_text, _ = station_day_mart._load_sql(None)
    connection = duckdb.connect()
    try:
        connection.execute(
            """
            CREATE TABLE rental_fact_input (
                bike_id VARCHAR,
                rent_at TIMESTAMP,
                rent_station_no VARCHAR,
                usage_min BIGINT,
                is_completed_trip BOOLEAN,
                distance_metric_eligible BOOLEAN,
                station_history_match BOOLEAN
            )
            """
        )
        connection.execute(
            """
            INSERT INTO rental_fact_input VALUES
                ('JULY-RETURN', TIMESTAMP '2026-06-30 23:50:00', '1', 20, TRUE, TRUE, TRUE)
            """
        )
        connection.execute(
            """
            CREATE TABLE return_event_input (
                return_at TIMESTAMP,
                return_station_no VARCHAR
            )
            """
        )
        connection.execute(
            """
            INSERT INTO return_event_input VALUES
                (TIMESTAMP '2026-06-15 12:00:00', '2')
            """
        )
        connection.execute(sql_text)
        assert connection.execute(
            """
            SELECT rental_count, return_count, completed_trip_count
            FROM mart_station_day
            WHERE service_date = DATE '2026-06-30' AND station_no = '1'
            """
        ).fetchone() == (1, 0, 1)
        assert connection.execute(
            """
            SELECT return_count
            FROM mart_station_day
            WHERE service_date = DATE '2026-06-15' AND station_no = '2'
            """
        ).fetchone() == (1,)
    finally:
        connection.close()

    june_pointer = station_day_mart_current_pointer_path(
        tmp_path / "warehouse", "2026-06"
    )
    june_pointer.parent.mkdir(parents=True, exist_ok=True)
    june_pointer.write_bytes(b"sentinel-june-current")
    with pytest.raises(StationDayMartError, match="2020-01..2026-06"):
        publish_current_station_day_mart(
            manifest=tmp_path / "manifest.json",
            raw_root=tmp_path / "raw",
            station_root=tmp_path / "station",
            clean_root=tmp_path / "clean",
            warehouse_root=tmp_path / "warehouse",
            service_month="2026-07",
            transformation_version="station-day-t1",
            configuration={},
        )
    assert june_pointer.read_bytes() == b"sentinel-june-current"


def test_revision_return_month_scan_clips_post_production_returns(tmp_path):
    fact_path = tmp_path / "fact"
    fact_path.mkdir()
    connection = duckdb.connect(str(fact_path / "warehouse.duckdb"))
    try:
        connection.execute(
            """
            CREATE TABLE fact_trip_trusted (
                return_at TIMESTAMP,
                is_completed_trip BOOLEAN
            )
            """
        )
        connection.execute(
            """
            INSERT INTO fact_trip_trusted VALUES
                (TIMESTAMP '2026-06-30 23:50:00', TRUE),
                (TIMESTAMP '2026-07-01 00:10:00', TRUE)
            """
        )
        connection.execute("CHECKPOINT")
    finally:
        connection.close()
    assert station_day_mart._completed_return_service_months(fact_path) == {
        "2026-06"
    }


def test_sql_identity_normalizes_line_endings(tmp_path):
    source = Path(station_day_mart.__file__).with_name("sql") / "mart_station_day.sql"
    text = source.read_text(encoding="utf-8").replace("\r\n", "\n")
    paths = []
    for name, content in (
        ("lf.sql", text),
        ("crlf.sql", text.replace("\n", "\r\n")),
        ("cr.sql", text.replace("\n", "\r")),
    ):
        path = tmp_path / name
        path.write_bytes(content.encode("utf-8"))
        paths.append(path)
    hashes = {station_day_mart._load_sql(path)[1] for path in paths}
    assert len(hashes) == 1


def test_exact_loaded_sql_text_is_executed_after_path_mutation(
    mart_fixture, monkeypatch
):
    source = Path(station_day_mart.__file__).with_name("sql") / "mart_station_day.sql"
    sql_copy = mart_fixture / "station-day.sql"
    sql_copy.write_bytes(source.read_bytes())
    original = station_day_mart._load_sql

    def load_then_mutate(path):
        text, sha = original(path)
        Path(path).write_text("BROKEN AFTER LOAD", encoding="utf-8")
        return text, sha

    monkeypatch.setattr(station_day_mart, "_load_sql", load_then_mutate)
    result = publish_mart(mart_fixture, sql_path=sql_copy)
    assert result.status == STATUS_PUBLISHED_CURRENT


def test_active_mart_lock_blocks_and_stale_file_does_not(mart_fixture):
    root = station_day_mart.station_day_mart_root(
        mart_fixture / "warehouse"
    )
    lock_path = root / "locks" / "service_month=2020-02.lock"
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    lock_path.write_text("0stale-owner\n", encoding="utf-8")
    first = publish_mart(mart_fixture)
    assert first.status == STATUS_PUBLISHED_CURRENT
    with station_day_mart._advisory_lock(
        lock_path,
        label="test-holder",
        error_message="test lock",
    ):
        with pytest.raises(StationDayMartError, match="station-day mart"):
            publish_mart(
                mart_fixture,
                transformation_version="station-day-t2",
            )


def test_station_day_cli_calls_business_layer(mart_fixture):
    result = run_cli(
        "warehouse",
        "publish-station-day-mart",
        "--manifest",
        str(mart_fixture / "manifest.json"),
        "--raw-root",
        str(mart_fixture / "raw"),
        "--station-root",
        str(mart_fixture / "station"),
        "--clean-root",
        str(mart_fixture / "clean"),
        "--warehouse-root",
        str(mart_fixture / "warehouse"),
        "--service-month",
        "2020-02",
        "--transformation-version",
        "station-day-t1",
    )
    assert result.returncode == 0, result.stderr
    assert result.stdout.startswith("STATION_DAY_MART_PUBLISH_OK msd1_")
    assert "status=PUBLISHED_CURRENT" in result.stdout
    assert "service_month=2020-02" in result.stdout
    assert "rentals=3" in result.stdout
    assert "returns=3" in result.stdout
