from __future__ import annotations

import hashlib
import json
import os
import subprocess
import sys
from contextlib import contextmanager
from pathlib import Path

import duckdb
import pytest

import seoul_bike_pipeline.date_dimension as date_dimension
from seoul_bike_pipeline.date_dimension import (
    DATE_COVERAGE_END,
    DATE_COVERAGE_START,
    DATE_ROW_COUNT,
    STATUS_PUBLISHED_CURRENT,
    STATUS_REUSED_CURRENT,
    date_current_pointer_path,
    publish_date_dimension,
    validate_date_dimension_version,
)
from seoul_bike_pipeline.errors import DateDimensionError


def publish_dim(
    tmp_path: Path,
    *,
    transformation_version: str = "date-t1",
    **kwargs,
):
    return publish_date_dimension(
        warehouse_root=tmp_path / "warehouse",
        transformation_version=transformation_version,
        configuration={},
        **kwargs,
    )


def update_database_fingerprint(version_path: Path) -> None:
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


def run_cli(*args: str) -> subprocess.CompletedProcess[str]:
    env = os.environ.copy()
    src = str(Path(__file__).resolve().parents[1] / "src")
    env["PYTHONPATH"] = src + os.pathsep + env.get("PYTHONPATH", "")
    return subprocess.run(
        [sys.executable, "-m", "seoul_bike_pipeline", *args],
        text=True,
        capture_output=True,
        env=env,
        check=False,
    )


def test_publish_date_dimension_has_exact_production_calendar_and_reuses(tmp_path):
    first = publish_dim(tmp_path)
    assert first.status == STATUS_PUBLISHED_CURRENT
    assert first.date_row_count == DATE_ROW_COUNT == 2373
    assert first.coverage_start == DATE_COVERAGE_START
    assert first.coverage_end == DATE_COVERAGE_END

    connection = duckdb.connect(
        str(first.version_path / "warehouse.duckdb"), read_only=True
    )
    try:
        assert connection.execute(
            "SELECT COUNT(*), MIN(date), MAX(date) FROM dim_date"
        ).fetchone() == (2373, DATE_COVERAGE_START, DATE_COVERAGE_END)
        assert connection.execute(
            """
            SELECT date, year, month, day, day_of_week, weekend_flag
            FROM dim_date
            WHERE date IN (
                DATE '2020-01-01',
                DATE '2020-02-29',
                DATE '2020-03-01',
                DATE '2024-02-29',
                DATE '2026-06-30'
            )
            ORDER BY date
            """
        ).fetchall() == [
            (DATE_COVERAGE_START, 2020, 1, 1, 3, False),
            (date_dimension.date(2020, 2, 29), 2020, 2, 29, 6, True),
            (date_dimension.date(2020, 3, 1), 2020, 3, 1, 7, True),
            (date_dimension.date(2024, 2, 29), 2024, 2, 29, 4, False),
            (DATE_COVERAGE_END, 2026, 6, 30, 2, False),
        ]
        assert connection.execute(
            "SELECT COUNT(*) FROM dim_date WHERE year < 2020"
        ).fetchone()[0] == 0
    finally:
        connection.close()

    second = publish_dim(tmp_path)
    assert second.status == STATUS_REUSED_CURRENT
    assert second.date_version_id == first.date_version_id


def test_same_logical_current_with_changed_physical_bytes_is_reused(tmp_path):
    first = publish_dim(tmp_path)
    database_path = first.version_path / "warehouse.duckdb"
    original_bytes = database_path.read_bytes()
    connection = duckdb.connect(str(database_path))
    try:
        connection.execute("CREATE TABLE transient_noise AS SELECT 1 AS x")
        connection.execute("DROP TABLE transient_noise")
        connection.execute("CHECKPOINT")
    finally:
        connection.close()
    update_database_fingerprint(first.version_path)
    assert database_path.read_bytes() != original_bytes

    second = publish_dim(tmp_path)
    assert second.status == STATUS_REUSED_CURRENT
    assert second.date_version_id == first.date_version_id


def test_transform_failure_preserves_previous_current(tmp_path):
    first = publish_dim(tmp_path)
    before = first.current_pointer_path.read_bytes()
    bad_sql = tmp_path / "broken.sql"
    bad_sql.write_text(
        "CREATE TABLE dim_date AS SELECT does_not_exist;\n",
        encoding="utf-8",
    )

    with pytest.raises(DateDimensionError, match="SQL build failed"):
        publish_dim(
            tmp_path,
            transformation_version="date-t2",
            sql_path=bad_sql,
        )
    assert first.current_pointer_path.read_bytes() == before


def test_pre_current_failure_preserves_previous_current_and_retry_succeeds(tmp_path):
    first = publish_dim(tmp_path)
    before = first.current_pointer_path.read_bytes()

    def fail_before_current(_version_path: Path) -> None:
        raise RuntimeError("injected pre-current failure")

    with pytest.raises(RuntimeError, match="injected pre-current failure"):
        publish_dim(
            tmp_path,
            transformation_version="date-t2",
            before_current_publish=fail_before_current,
        )
    assert first.current_pointer_path.read_bytes() == before

    retry = publish_dim(tmp_path, transformation_version="date-t2")
    assert retry.status == STATUS_PUBLISHED_CURRENT
    assert retry.date_version_id != first.date_version_id


def test_concurrent_different_current_cannot_roll_back_winner(tmp_path, monkeypatch):
    real_lock = date_dimension._publish_lock
    state = {"nested": False, "winner": None}

    @contextmanager
    def interleaving_lock(root, date_version_id):
        if not state["nested"]:
            state["nested"] = True
            state["winner"] = publish_dim(
                tmp_path, transformation_version="date-t2"
            )
            state["nested"] = False
        with real_lock(root, date_version_id):
            yield

    monkeypatch.setattr(date_dimension, "_publish_lock", interleaving_lock)
    with pytest.raises(DateDimensionError, match="current pointer changed during build"):
        publish_dim(tmp_path, transformation_version="date-t1")

    pointer = json.loads(
        date_current_pointer_path(tmp_path / "warehouse").read_text(encoding="utf-8")
    )
    assert pointer["date_version_id"] == state["winner"].date_version_id


def test_concurrent_same_version_converges_to_reused_current(tmp_path, monkeypatch):
    real_lock = date_dimension._publish_lock
    state = {"nested": False, "winner": None}

    @contextmanager
    def interleaving_lock(root, date_version_id):
        if not state["nested"]:
            state["nested"] = True
            state["winner"] = publish_dim(tmp_path)
            state["nested"] = False
        with real_lock(root, date_version_id):
            yield

    monkeypatch.setattr(date_dimension, "_publish_lock", interleaving_lock)
    outer = publish_dim(tmp_path)
    assert state["winner"].status == STATUS_PUBLISHED_CURRENT
    assert outer.status == STATUS_REUSED_CURRENT
    assert outer.date_version_id == state["winner"].date_version_id


def test_pointer_to_relocated_bundle_is_rejected(tmp_path):
    published = publish_dim(tmp_path)
    fake_id = "dv1_" + "0" * 64
    fake_path = (
        published.version_path.parent / f"date_version_id={fake_id}"
    )
    fake_path.mkdir()
    for name in ("warehouse.duckdb", "metadata.json"):
        (fake_path / name).write_bytes((published.version_path / name).read_bytes())
    published.current_pointer_path.write_text(
        json.dumps(
            {"schema_version": 1, "date_version_id": fake_id},
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )

    with pytest.raises(DateDimensionError, match="directory version id"):
        publish_dim(tmp_path)


def test_count_preserving_grain_tamper_is_rejected(tmp_path):
    published = publish_dim(tmp_path)
    database_path = published.version_path / "warehouse.duckdb"
    connection = duckdb.connect(str(database_path))
    try:
        connection.execute("DELETE FROM dim_date WHERE date = DATE '2020-01-02'")
        connection.execute(
            """
            INSERT INTO dim_date
            SELECT * FROM dim_date WHERE date = DATE '2020-01-03'
            """
        )
        connection.execute("CHECKPOINT")
    finally:
        connection.close()
    update_database_fingerprint(published.version_path)

    with pytest.raises(
        DateDimensionError,
        match="one row per calendar date|calendar semantics",
    ):
        validate_date_dimension_version(published.version_path)


def test_calendar_semantic_tamper_is_rejected_even_with_updated_fingerprint(tmp_path):
    published = publish_dim(tmp_path)
    database_path = published.version_path / "warehouse.duckdb"
    connection = duckdb.connect(str(database_path))
    try:
        connection.execute(
            """
            UPDATE dim_date
            SET day_of_week = 7, weekend_flag = FALSE
            WHERE date = DATE '2020-02-29'
            """
        )
        connection.execute("CHECKPOINT")
    finally:
        connection.close()
    update_database_fingerprint(published.version_path)

    with pytest.raises(DateDimensionError, match="calendar semantics"):
        validate_date_dimension_version(published.version_path)


def test_candidate_catalog_macro_cannot_shadow_independent_calendar_validation(tmp_path):
    sql_path = tmp_path / "shadow.sql"
    sql_path.write_text(
        """
        CREATE MACRO strftime(x, fmt) AS '1';
        CREATE TABLE dim_date AS
        SELECT
            CAST(calendar_date AS DATE) AS date,
            CAST(EXTRACT(YEAR FROM calendar_date) AS INTEGER) AS year,
            CAST(EXTRACT(MONTH FROM calendar_date) AS INTEGER) AS month,
            CAST(EXTRACT(DAY FROM calendar_date) AS INTEGER) AS day,
            1::INTEGER AS day_of_week,
            FALSE AS weekend_flag
        FROM generate_series(
            DATE '2020-01-01',
            DATE '2026-06-30',
            INTERVAL 1 DAY
        ) AS series(calendar_date)
        ORDER BY calendar_date;
        """,
        encoding="utf-8",
    )

    with pytest.raises(DateDimensionError, match="calendar semantics"):
        publish_dim(tmp_path, sql_path=sql_path)
    assert not date_current_pointer_path(tmp_path / "warehouse").exists()


def test_custom_sql_view_is_rejected_without_current(tmp_path):
    sql_path = tmp_path / "view.sql"
    sql_path.write_text(
        """
        CREATE VIEW dim_date AS
        SELECT
            DATE '2020-01-01' AS date,
            2020::INTEGER AS year,
            1::INTEGER AS month,
            1::INTEGER AS day,
            3::INTEGER AS day_of_week,
            FALSE AS weekend_flag;
        """,
        encoding="utf-8",
    )

    with pytest.raises(DateDimensionError, match="base table"):
        publish_dim(tmp_path, sql_path=sql_path)
    assert not date_current_pointer_path(tmp_path / "warehouse").exists()


def test_sql_identity_normalizes_line_endings(tmp_path):
    text = "CREATE TABLE dim_date AS\nSELECT 1 AS x;\n"
    paths = []
    for name, content in (
        ("lf.sql", text),
        ("crlf.sql", text.replace("\n", "\r\n")),
        ("cr.sql", text.replace("\n", "\r")),
    ):
        path = tmp_path / name
        path.write_bytes(content.encode("utf-8"))
        paths.append(path)
    hashes = {date_dimension._load_sql(path)[1] for path in paths}
    assert len(hashes) == 1


def test_sql_is_loaded_once_and_exact_hashed_text_is_executed(tmp_path, monkeypatch):
    source = Path(date_dimension.__file__).with_name("sql") / "dim_date.sql"
    sql_copy = tmp_path / "dim_date.sql"
    sql_copy.write_bytes(source.read_bytes())
    original_load_sql = date_dimension._load_sql

    def load_then_mutate(path):
        text, sha = original_load_sql(path)
        Path(path).write_text("BROKEN AFTER LOAD", encoding="utf-8")
        return text, sha

    monkeypatch.setattr(date_dimension, "_load_sql", load_then_mutate)
    result = publish_dim(tmp_path, sql_path=sql_copy)
    assert result.status == STATUS_PUBLISHED_CURRENT


def test_residual_wal_is_rejected(tmp_path):
    published = publish_dim(tmp_path)
    Path(str(published.version_path / "warehouse.duckdb") + ".wal").write_bytes(
        b"residual"
    )
    with pytest.raises(DateDimensionError, match="residual WAL"):
        validate_date_dimension_version(published.version_path)


def test_post_hook_physical_mutation_with_updated_fingerprint_preserves_current(tmp_path):
    first = publish_dim(tmp_path)
    before = first.current_pointer_path.read_bytes()

    def mutate_bundle(version_path: Path) -> None:
        database_path = version_path / "warehouse.duckdb"
        connection = duckdb.connect(str(database_path))
        try:
            connection.execute("CREATE TABLE physical_noise AS SELECT 1 AS x")
            connection.execute("CHECKPOINT")
        finally:
            connection.close()
        update_database_fingerprint(version_path)

    with pytest.raises(DateDimensionError, match="immutable bundle changed"):
        publish_dim(
            tmp_path,
            transformation_version="date-t2",
            before_current_publish=mutate_bundle,
        )
    assert first.current_pointer_path.read_bytes() == before


def test_active_lock_blocks_and_stale_lock_file_does_not(tmp_path):
    root = date_dimension.date_dimension_root(tmp_path / "warehouse")
    lock_path = root / "locks" / "current.lock"
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    lock_path.write_text("0stale-owner\n", encoding="utf-8")
    first = publish_dim(tmp_path)
    assert first.status == STATUS_PUBLISHED_CURRENT

    with date_dimension._publish_lock(root, first.date_version_id):
        with pytest.raises(DateDimensionError, match="published concurrently"):
            publish_dim(tmp_path, transformation_version="date-t2")


def test_date_dimension_cli_calls_business_layer(tmp_path):
    result = run_cli(
        "warehouse",
        "publish-date-dimension",
        "--warehouse-root",
        str(tmp_path / "warehouse"),
        "--transformation-version",
        "date-t1",
    )
    assert result.returncode == 0, result.stderr
    assert result.stdout.startswith("DATE_DIMENSION_PUBLISH_OK dv1_")
    assert "status=PUBLISHED_CURRENT" in result.stdout
    assert "coverage_start=2020-01-01" in result.stdout
    assert "coverage_end=2026-06-30" in result.stdout
    assert "date_rows=2373" in result.stdout
