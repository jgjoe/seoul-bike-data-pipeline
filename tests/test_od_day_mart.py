from __future__ import annotations

import hashlib
import json
import math
import shutil
from contextlib import contextmanager
from decimal import Decimal
from pathlib import Path

import duckdb
import pytest

import seoul_bike_pipeline.od_day_mart as od_day_mart
from seoul_bike_pipeline.errors import OdDayMartError
from seoul_bike_pipeline.od_day_mart import (
    STATUS_PUBLISHED_CURRENT,
    STATUS_REUSED_CURRENT,
    od_day_mart_current_pointer_path,
    plan_od_day_fact_revision_impact,
    publish_current_od_day_mart,
    validate_od_day_mart_version,
)
from tests.test_station_day_mart import (
    build_base_fixture,
    publish_fact_fixture,
    rental_row,
    rewrite_database_fingerprint,
)
from tests.test_trip_station_enrichment import run_cli


@pytest.fixture(scope="module")
def base_fixture(tmp_path_factory) -> Path:
    root = tmp_path_factory.mktemp("od-day-base")
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
    transformation_version: str = "od-day-t1",
    **kwargs,
):
    return publish_current_od_day_mart(
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


def test_od_day_uses_rental_date_and_completed_trips_only(mart_fixture):
    result = publish_mart(mart_fixture)
    assert result.status == STATUS_PUBLISHED_CURRENT
    assert result.trip_count_total == 2
    assert result.valid_distance_trip_count_total == 0

    connection = duckdb.connect(
        str(result.version_path / "warehouse.duckdb"), read_only=True
    )
    try:
        rows = connection.execute(
            """
            SELECT *
            FROM mart_od_day
            ORDER BY service_date, origin_station_no, destination_station_no
            """
        ).fetchall()
    finally:
        connection.close()
    assert rows == [
        (
            od_day_mart.date(2020, 2, 1),
            "2",
            "1",
            1,
            1,
            10.0,
            10.0,
            0,
            None,
            None,
        ),
        (
            od_day_mart.date(2020, 2, 1),
            "3",
            "4",
            1,
            1,
            30.0,
            30.0,
            0,
            None,
            None,
        ),
    ]
    assert all(row[0].strftime("%Y-%m") == "2020-02" for row in rows)


def test_od_metrics_aggregate_same_grain_and_distance_subset(mart_fixture):
    revised = publish_fact_fixture(
        mart_fixture,
        source_month="2020-02",
        rows=[
            rental_row(
                bike_id="BIKE-A",
                rent_at="2020-02-03 08:00:00",
                rent_station_no="00001",
                return_at="2020-02-03 08:10:00",
                return_station_no="00002",
                usage_min="10",
                distance_m="100",
            ),
            rental_row(
                bike_id="BIKE-B",
                rent_at="2020-02-03 09:00:00",
                rent_station_no="00001",
                return_at="2020-02-03 09:30:00",
                return_station_no="00002",
                usage_min="30",
                distance_m="300",
            ),
            rental_row(
                bike_id="BIKE-B",
                rent_at="2020-02-03 10:00:00",
                rent_station_no="00001",
                return_at="2020-02-03 10:20:00",
                return_station_no="00002",
                usage_min="20",
                distance_m="0",
            ),
            rental_row(
                bike_id="BIKE-D",
                rent_at="2020-02-03 10:30:00",
                rent_station_no="00001",
                return_at="2020-02-03 10:40:00",
                return_station_no="00002",
                usage_min="",
                distance_m="500",
            ),
            rental_row(
                bike_id="BIKE-C",
                rent_at="2020-02-03 11:00:00",
                rent_station_no="00001",
                return_at="",
                return_station_no="",
                usage_min="40",
                distance_m="400",
            ),
        ],
    )
    result = publish_mart(mart_fixture)
    assert result.fact_version_id == revised.fact_version_id
    connection = duckdb.connect(
        str(result.version_path / "warehouse.duckdb"), read_only=True
    )
    try:
        row = connection.execute(
            """
            SELECT *
            FROM mart_od_day
            WHERE service_date = DATE '2020-02-03'
              AND origin_station_no = '1'
              AND destination_station_no = '2'
            """
        ).fetchone()
    finally:
        connection.close()
    assert row == (
        od_day_mart.date(2020, 2, 3),
        "1",
        "2",
        4,
        3,
        20.0,
        20.0,
        3,
        300.0,
        300.0,
    )


def test_decimal_distance_average_allows_only_float_rounding_noise(tmp_path):
    fact_db = tmp_path / "fact.duckdb"
    connection = duckdb.connect(str(fact_db))
    try:
        connection.execute(
            """
            CREATE TABLE fact_trip_trusted (
                source_row_id VARCHAR,
                bike_id VARCHAR,
                rent_at TIMESTAMP,
                rent_station_no VARCHAR,
                return_station_no VARCHAR,
                usage_min BIGINT,
                distance_m DECIMAL(38,18),
                is_completed_trip BOOLEAN,
                distance_metric_eligible BOOLEAN
            )
            """
        )
        values = [
            Decimal("0.1"),
            Decimal("0.2"),
            Decimal("0.3"),
            Decimal("99999.123456789012345678"),
        ]
        for index, value in enumerate(values):
            connection.execute(
                """
                INSERT INTO fact_trip_trusted VALUES
                (?, ?, TIMESTAMP '2020-02-03 08:00:00', '1', '2', 10, ?, TRUE, TRUE)
                """,
                [f"row-{index}", f"bike-{index}", value],
            )
        connection.execute("CHECKPOINT")
    finally:
        connection.close()

    fact_path = tmp_path / "fact-version"
    fact_path.mkdir()
    fact_db.replace(fact_path / "warehouse.duckdb")
    expected = od_day_mart._expected_rows(
        service_month="2020-02", fact_path=fact_path
    )

    actual_connection = duckdb.connect()
    try:
        actual_connection.execute(
            f"ATTACH '{od_day_mart._sql_path(fact_path / 'warehouse.duckdb')}' AS source_fact (READ_ONLY)"
        )
        actual = actual_connection.execute(
            """
            SELECT
                CAST(rent_at AS DATE),
                rent_station_no,
                return_station_no,
                COUNT(*)::BIGINT,
                COUNT(DISTINCT bike_id)::BIGINT,
                AVG(usage_min)::DOUBLE,
                MEDIAN(usage_min)::DOUBLE,
                COUNT(*) FILTER (WHERE distance_metric_eligible)::BIGINT,
                AVG(distance_m) FILTER (WHERE distance_metric_eligible)::DOUBLE,
                MEDIAN(distance_m) FILTER (WHERE distance_metric_eligible)::DOUBLE
            FROM source_fact.fact_trip_trusted
            WHERE is_completed_trip
            GROUP BY 1, 2, 3
            """
        ).fetchall()
    finally:
        actual_connection.close()

    od_day_mart._require_rows_equal(actual, expected)
    expected_average = float(expected[0][8])
    ulp_shifted_average = math.nextafter(expected_average, math.inf)
    rounding_noise = [
        tuple(actual[0][:8]) + (ulp_shifted_average, actual[0][9])
    ]
    od_day_mart._require_rows_equal(rounding_noise, expected)
    tampered = [tuple(actual[0][:8]) + (actual[0][8] + 1e-9, actual[0][9])]
    with pytest.raises(OdDayMartError, match="independent recomputation"):
        od_day_mart._require_rows_equal(tampered, expected)


def test_same_input_retry_reuses_current(mart_fixture):
    first = publish_mart(mart_fixture)
    second = publish_mart(mart_fixture)
    assert second.status == STATUS_REUSED_CURRENT
    assert second.mart_version_id == first.mart_version_id
    assert second.current_pointer_path.read_bytes() == first.current_pointer_path.read_bytes()


def test_revision_impact_is_source_rental_month_only(mart_fixture):
    old = json.loads(
        (
            mart_fixture
            / "warehouse"
            / "fact_trip_trusted"
            / "current"
            / "source_month=2020-02.json"
        ).read_text(encoding="utf-8")
    )["fact_version_id"]
    new = publish_fact_fixture(
        mart_fixture,
        source_month="2020-02",
        rows=[
            rental_row(
                bike_id="BIKE-REV",
                rent_at="2020-02-01 01:00:00",
                rent_station_no="00001",
                return_at="2020-04-01 01:10:00",
                return_station_no="00002",
                usage_min="10",
                distance_m="100",
            )
        ],
    ).fact_version_id
    impact = plan_od_day_fact_revision_impact(
        manifest=mart_fixture / "manifest.json",
        raw_root=mart_fixture / "raw",
        station_root=mart_fixture / "station",
        clean_root=mart_fixture / "clean",
        warehouse_root=mart_fixture / "warehouse",
        source_month="2020-02",
        old_fact_version_id=old,
        new_fact_version_id=new,
    )
    assert impact.affected_service_months == ("2020-02",)


def test_transform_failure_preserves_previous_current(mart_fixture):
    first = publish_mart(mart_fixture)
    before = first.current_pointer_path.read_bytes()
    bad_sql = mart_fixture / "broken-od.sql"
    bad_sql.write_text(
        "CREATE TABLE mart_od_day AS SELECT does_not_exist;\n", encoding="utf-8"
    )
    with pytest.raises(OdDayMartError, match="SQL build failed"):
        publish_mart(
            mart_fixture,
            transformation_version="od-day-t2",
            sql_path=bad_sql,
        )
    assert first.current_pointer_path.read_bytes() == before


def test_pre_current_failure_preserves_current_and_retry_succeeds(mart_fixture):
    first = publish_mart(mart_fixture)
    before = first.current_pointer_path.read_bytes()

    def fail(_version_path: Path) -> None:
        raise RuntimeError("injected OD mart failure")

    with pytest.raises(RuntimeError, match="injected OD mart failure"):
        publish_mart(
            mart_fixture,
            transformation_version="od-day-t2",
            before_current_publish=fail,
        )
    assert first.current_pointer_path.read_bytes() == before
    retry = publish_mart(mart_fixture, transformation_version="od-day-t2")
    assert retry.status == STATUS_PUBLISHED_CURRENT


def test_fact_current_change_during_hook_preserves_previous_current(mart_fixture):
    first = publish_mart(mart_fixture)
    before = first.current_pointer_path.read_bytes()
    fact_pointer = (
        mart_fixture
        / "warehouse"
        / "fact_trip_trusted"
        / "current"
        / "source_month=2020-02.json"
    )

    def change_fact(_version_path: Path) -> None:
        fact_pointer.write_text(
            json.dumps(
                {
                    "schema_version": 1,
                    "source_month": "2020-02",
                    "fact_version_id": "fv1_" + "0" * 64,
                },
                sort_keys=True,
            )
            + "\n",
            encoding="utf-8",
        )

    with pytest.raises(OdDayMartError, match="fact current pointer changed"):
        publish_mart(
            mart_fixture,
            transformation_version="od-day-t2",
            before_current_publish=change_fact,
        )
    assert first.current_pointer_path.read_bytes() == before


def test_concurrent_different_publish_cannot_roll_back_winner(
    mart_fixture, monkeypatch
):
    real_locks = od_day_mart._publication_locks
    state = {"nested": False, "winner": None}

    @contextmanager
    def interleaving_locks(**kwargs):
        if not state["nested"]:
            state["nested"] = True
            state["winner"] = publish_mart(
                mart_fixture, transformation_version="od-day-t2"
            )
            state["nested"] = False
        with real_locks(**kwargs):
            yield

    monkeypatch.setattr(od_day_mart, "_publication_locks", interleaving_locks)
    with pytest.raises(OdDayMartError, match="current pointer changed during build"):
        publish_mart(mart_fixture, transformation_version="od-day-t1")
    pointer = json.loads(
        od_day_mart_current_pointer_path(
            mart_fixture / "warehouse", "2020-02"
        ).read_text(encoding="utf-8")
    )
    assert pointer["mart_version_id"] == state["winner"].mart_version_id


def test_concurrent_same_version_converges_to_reused_current(
    mart_fixture, monkeypatch
):
    real_locks = od_day_mart._publication_locks
    state = {"nested": False, "winner": None}

    @contextmanager
    def interleaving_locks(**kwargs):
        if not state["nested"]:
            state["nested"] = True
            state["winner"] = publish_mart(mart_fixture)
            state["nested"] = False
        with real_locks(**kwargs):
            yield

    monkeypatch.setattr(od_day_mart, "_publication_locks", interleaving_locks)
    outer = publish_mart(mart_fixture)
    assert state["winner"].status == STATUS_PUBLISHED_CURRENT
    assert outer.status == STATUS_REUSED_CURRENT
    assert outer.mart_version_id == state["winner"].mart_version_id


def test_fact_physical_tamper_before_final_lock_is_rejected(
    mart_fixture, monkeypatch
):
    fact_pointer = json.loads(
        (
            mart_fixture
            / "warehouse"
            / "fact_trip_trusted"
            / "current"
            / "source_month=2020-02.json"
        ).read_text(encoding="utf-8")
    )
    fact_path = (
        mart_fixture
        / "warehouse"
        / "fact_trip_trusted"
        / "versions"
        / "source_month=2020-02"
        / f"fact_version_id={fact_pointer['fact_version_id']}"
    )
    real_locks = od_day_mart._publication_locks
    state = {"tampered": False}

    @contextmanager
    def tamper_then_lock(**kwargs):
        if not state["tampered"]:
            state["tampered"] = True
            connection = duckdb.connect(str(fact_path / "warehouse.duckdb"))
            try:
                connection.execute("CREATE TABLE physical_noise AS SELECT 1 AS x")
                connection.execute("DROP TABLE physical_noise")
                connection.execute("CHECKPOINT")
            finally:
                connection.close()
            rewrite_database_fingerprint(fact_path)
        with real_locks(**kwargs):
            yield

    monkeypatch.setattr(od_day_mart, "_publication_locks", tamper_then_lock)
    with pytest.raises(OdDayMartError, match="immutable metadata changed"):
        publish_mart(mart_fixture)


def test_mart_tamper_is_rejected_even_with_updated_fingerprint(mart_fixture):
    published = publish_mart(mart_fixture)
    database_path = published.version_path / "warehouse.duckdb"
    connection = duckdb.connect(str(database_path))
    try:
        connection.execute(
            """
            UPDATE mart_od_day
            SET trip_count = trip_count + 1
            WHERE origin_station_no = '2'
            """
        )
        connection.execute("CHECKPOINT")
    finally:
        connection.close()
    rewrite_database_fingerprint(published.version_path)
    with pytest.raises(OdDayMartError, match="independent recomputation"):
        validate_od_day_mart_version(
            published.version_path,
            manifest=mart_fixture / "manifest.json",
            raw_root=mart_fixture / "raw",
            station_root=mart_fixture / "station",
            clean_root=mart_fixture / "clean",
            warehouse_root=mart_fixture / "warehouse",
        )


def test_count_preserving_grain_tamper_is_rejected(mart_fixture):
    published = publish_mart(mart_fixture)
    database_path = published.version_path / "warehouse.duckdb"
    connection = duckdb.connect(str(database_path))
    try:
        connection.execute(
            """
            UPDATE mart_od_day
            SET destination_station_no = '999'
            WHERE origin_station_no = '2'
            """
        )
        connection.execute("CHECKPOINT")
    finally:
        connection.close()
    rewrite_database_fingerprint(published.version_path)
    with pytest.raises(OdDayMartError, match="independent recomputation"):
        validate_od_day_mart_version(
            published.version_path,
            manifest=mart_fixture / "manifest.json",
            raw_root=mart_fixture / "raw",
            station_root=mart_fixture / "station",
            clean_root=mart_fixture / "clean",
            warehouse_root=mart_fixture / "warehouse",
        )


def test_metadata_fact_lineage_tamper_is_rejected(mart_fixture):
    published = publish_mart(mart_fixture)
    metadata_path = published.version_path / "metadata.json"
    metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
    metadata["fact_version_id"] = "fv1_" + "0" * 64
    metadata_path.write_text(
        json.dumps(metadata, sort_keys=True, separators=(",", ":")) + "\n",
        encoding="utf-8",
    )
    with pytest.raises(OdDayMartError):
        validate_od_day_mart_version(
            published.version_path,
            manifest=mart_fixture / "manifest.json",
            raw_root=mart_fixture / "raw",
            station_root=mart_fixture / "station",
            clean_root=mart_fixture / "clean",
            warehouse_root=mart_fixture / "warehouse",
        )


def test_sql_identity_normalizes_line_endings(tmp_path):
    source = Path(od_day_mart.__file__).with_name("sql") / "mart_od_day.sql"
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
    assert len({od_day_mart._load_sql(path)[1] for path in paths}) == 1


def test_exact_loaded_sql_text_is_executed_after_path_mutation(
    mart_fixture, monkeypatch
):
    source = Path(od_day_mart.__file__).with_name("sql") / "mart_od_day.sql"
    sql_copy = mart_fixture / "od-day.sql"
    sql_copy.write_bytes(source.read_bytes())
    original = od_day_mart._load_sql

    def load_then_mutate(path):
        text, sha = original(path)
        Path(path).write_text("BROKEN AFTER LOAD", encoding="utf-8")
        return text, sha

    monkeypatch.setattr(od_day_mart, "_load_sql", load_then_mutate)
    result = publish_mart(mart_fixture, sql_path=sql_copy)
    assert result.status == STATUS_PUBLISHED_CURRENT


def test_production_range_rejects_2015_and_post_end(mart_fixture):
    for month in ("2015-10", "2026-07"):
        with pytest.raises(OdDayMartError, match="service_month must be within"):
            publish_mart(mart_fixture, service_month=month)


def test_active_mart_lock_blocks_and_stale_file_does_not(mart_fixture):
    root = od_day_mart.od_day_mart_root(mart_fixture / "warehouse")
    lock_path = root / "locks" / "service_month=2020-02.lock"
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    lock_path.write_text("0stale-owner\n", encoding="utf-8")
    first = publish_mart(mart_fixture)
    assert first.status == STATUS_PUBLISHED_CURRENT
    with od_day_mart._advisory_lock(
        lock_path, label="test-holder", error_message="test lock"
    ):
        with pytest.raises(OdDayMartError, match="OD-day mart"):
            publish_mart(mart_fixture, transformation_version="od-day-t2")


def test_od_day_cli_calls_business_layer(mart_fixture):
    result = run_cli(
        "warehouse",
        "publish-od-day-mart",
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
        "od-day-t1",
    )
    assert result.returncode == 0, result.stderr
    assert result.stdout.startswith("OD_DAY_MART_PUBLISH_OK mod1_")
    assert "status=PUBLISHED_CURRENT" in result.stdout
    assert "service_month=2020-02" in result.stdout
    assert "trips=2" in result.stdout
