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

import seoul_bike_pipeline.source_month_quality_mart as quality_mart
from seoul_bike_pipeline.clean_publish import publish_clean_month
from seoul_bike_pipeline.csv_source import RENTAL_V16_HEADER
from seoul_bike_pipeline.errors import SourceMonthQualityMartError
from seoul_bike_pipeline.register import register_source
from seoul_bike_pipeline.source_month_quality_mart import (
    STATUS_PUBLISHED_CURRENT,
    STATUS_REUSED_CURRENT,
    plan_source_month_quality_revision_impact,
    publish_current_source_month_quality_mart,
    source_month_quality_mart_current_pointer_path,
    validate_source_month_quality_mart_version,
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
    root = tmp_path_factory.mktemp("source-quality-base")
    build_base_fixture(root)
    return root


@pytest.fixture
def quality_fixture(base_fixture: Path, tmp_path: Path) -> Path:
    for child in base_fixture.iterdir():
        destination = tmp_path / child.name
        if child.is_dir():
            shutil.copytree(child, destination)
        else:
            shutil.copy2(child, destination)
    return tmp_path


def publish_quality(
    root: Path,
    *,
    source_month: str = "2020-02",
    transformation_version: str = "quality-t1",
    **kwargs,
):
    return publish_current_source_month_quality_mart(
        manifest=root / "manifest.json",
        raw_root=root / "raw",
        station_root=root / "station",
        clean_root=root / "clean",
        warehouse_root=root / "warehouse",
        source_month=source_month,
        transformation_version=transformation_version,
        configuration={},
        **kwargs,
    )


def replace_fields(row: tuple[str, ...], **updates: str) -> tuple[str, ...]:
    index = {
        "bike_id": 0,
        "return_at": 5,
        "return_station_no": 6,
        "distance_m": 10,
        "birth_year": 11,
        "sex": 12,
    }
    values = list(row)
    for key, value in updates.items():
        values[index[key]] = value
    if "return_station_no" in updates:
        values[7] = (
            f"return-{updates['return_station_no']}"
            if updates["return_station_no"]
            else ""
        )
        values[15] = (
            f"ST-X-{updates['return_station_no']}"
            if updates["return_station_no"]
            else ""
        )
    return tuple(values)


def publish_clean_only(
    root: Path,
    *,
    source_month: str,
    rows: list[tuple[str, ...]],
):
    buffer = io.StringIO(newline="")
    writer = csv.writer(buffer, lineterminator="\r\n")
    writer.writerow(RENTAL_V16_HEADER)
    writer.writerows(rows)
    artifact = root / f"clean-only-{source_month}.csv"
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
    return publish_clean_month(
        manifest=root / "manifest.json",
        raw_root=root / "raw",
        clean_root=root / "clean",
        source_version_id=registered.record.source_version_id,
        source_month=source_month,
        transformation_version="clean-only-t2",
        configuration={},
    )


def current_clean_metadata(root: Path, source_month: str) -> dict[str, object]:
    pointer = json.loads(
        (
            root
            / "clean"
            / "current"
            / f"source_month={source_month}.json"
        ).read_text(encoding="utf-8")
    )
    return json.loads(
        (current_clean_version_path(root, source_month) / "metadata.json").read_text(
            encoding="utf-8"
        )
    )


def current_clean_version_path(root: Path, source_month: str) -> Path:
    pointer = json.loads(
        (
            root
            / "clean"
            / "current"
            / f"source_month={source_month}.json"
        ).read_text(encoding="utf-8")
    )
    return (
        root
        / "clean"
        / "versions"
        / f"source_month={source_month}"
        / f"clean_version_id={pointer['clean_version_id']}"
    )


def rewrite_clean_parquet_preserving_rows(version_path: Path) -> None:
    clean_path = version_path / "clean.parquet"
    temp_path = version_path / "clean-rewrite.parquet"
    connection = duckdb.connect()
    try:
        source = str(clean_path.resolve()).replace("'", "''").replace("\\", "/")
        target = str(temp_path.resolve()).replace("'", "''").replace("\\", "/")
        connection.execute(
            f"""
            COPY (
                SELECT *
                FROM read_parquet('{source}', hive_partitioning=false)
                ORDER BY source_row_id DESC
            ) TO '{target}' (FORMAT PARQUET, COMPRESSION ZSTD)
            """
        )
    finally:
        connection.close()
    temp_path.replace(clean_path)
    data = clean_path.read_bytes()
    metadata_path = version_path / "metadata.json"
    metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
    metadata["clean_parquet"] = {
        "size_bytes": len(data),
        "sha256": hashlib.sha256(data).hexdigest(),
    }
    metadata_path.write_text(
        json.dumps(metadata, sort_keys=True, separators=(",", ":")) + "\n",
        encoding="utf-8",
    )


def test_quality_base_metrics_and_lineage(quality_fixture):
    result = publish_quality(quality_fixture)
    assert result.status == STATUS_PUBLISHED_CURRENT
    assert result.source_row_count == 3
    assert result.clean_row_count == 3
    assert result.trusted_fact_count == 3
    assert result.quarantine_count == 0

    connection = duckdb.connect(
        str(result.version_path / "warehouse.duckdb"), read_only=True
    )
    try:
        row = connection.execute(
            "SELECT * FROM mart_source_month_quality"
        ).fetchone()
    finally:
        connection.close()
    assert row[0] == "2020-02"
    assert row[1] == result.source_version_id
    assert row[2:10] == (3, 3, 3, 0, 0, 0, 1, 1)
    assert row[10] == pytest.approx(1 / 3)
    assert row[11] == 1
    assert row[12] == 0.0
    assert row[13] == 0.0
    assert row[14] == "rental_v16"
    assert row[16] == "quality-t1"
    assert row[17] == result.clean_version_id
    assert row[18] == result.fact_version_id

    clean_metadata = current_clean_metadata(quality_fixture, "2020-02")
    assert row[15] == clean_metadata["source_artifact_sha256"]


def test_d006_rates_use_all_clean_rows_and_exact_rule_semantics(quality_fixture):
    normal = rental_row(
        bike_id="Q-A",
        rent_at="2020-02-03 08:00:00",
        rent_station_no="00001",
        return_at="2020-02-03 08:10:00",
        return_station_no="00002",
        usage_min="10",
        distance_m="0",
    )
    invalid_birth = rental_row(
        bike_id="Q-B",
        rent_at="2020-02-03 09:00:00",
        rent_station_no="00001",
        return_at="2020-02-03 09:10:00",
        return_station_no="00002",
        usage_min="10",
        distance_m="100001",
    )
    impossible_birth = rental_row(
        bike_id="Q-C",
        rent_at="2020-02-03 10:00:00",
        rent_station_no="00002",
        return_at="",
        return_station_no="",
        usage_min="10",
        distance_m="100",
    )
    quarantined = rental_row(
        bike_id="Q-D",
        rent_at="2020-02-03 11:00:00",
        rent_station_no="00001",
        return_at="2020-02-03 11:10:00",
        return_station_no="00002",
        usage_min="10",
        distance_m="-1",
    )
    revised = publish_fact_fixture(
        quality_fixture,
        source_month="2020-02",
        rows=[
            replace_fields(normal, sex=""),
            replace_fields(invalid_birth, birth_year="not-a-year"),
            replace_fields(impossible_birth, birth_year="2099"),
            replace_fields(quarantined, birth_year=""),
        ],
    )
    result = publish_quality(quality_fixture)
    assert result.fact_version_id == revised.fact_version_id

    connection = duckdb.connect(
        str(result.version_path / "warehouse.duckdb"), read_only=True
    )
    try:
        row = connection.execute(
            """
            SELECT
                source_row_count,
                clean_row_count,
                trusted_fact_count,
                quarantine_count,
                incomplete_trip_count,
                station_unmatched_count,
                distance_zero_rate,
                extreme_distance_count,
                invalid_birth_year_rate,
                sex_missing_rate
            FROM mart_source_month_quality
            """
        ).fetchone()
    finally:
        connection.close()
    assert row == (4, 4, 3, 1, 1, 0, 0.25, 1, 0.5, 0.25)


def test_exact_duplicate_group_count_is_additive_metric(quality_fixture):
    duplicate = rental_row(
        bike_id="DUP",
        rent_at="2020-02-05 08:00:00",
        rent_station_no="00001",
        return_at="2020-02-05 08:10:00",
        return_station_no="00002",
        usage_min="10",
        distance_m="100",
    )
    normal = rental_row(
        bike_id="OK",
        rent_at="2020-02-05 09:00:00",
        rent_station_no="00001",
        return_at="2020-02-05 09:10:00",
        return_station_no="00002",
        usage_min="10",
        distance_m="100",
    )
    publish_fact_fixture(
        quality_fixture,
        source_month="2020-02",
        rows=[duplicate, duplicate, normal],
    )
    result = publish_quality(quality_fixture)
    connection = duckdb.connect(
        str(result.version_path / "warehouse.duckdb"), read_only=True
    )
    try:
        row = connection.execute(
            """
            SELECT
                trusted_fact_count,
                quarantine_count,
                logical_conflict_group_count,
                exact_duplicate_group_count
            FROM mart_source_month_quality
            """
        ).fetchone()
    finally:
        connection.close()
    assert row == (1, 2, 0, 1)


def test_schema_version_scalar_is_deterministic_for_mixed_known_schemas():
    assert quality_mart._schema_version_scalar(["rental_v16"]) == "rental_v16"
    assert (
        quality_mart._schema_version_scalar(
            ["rental_v17", "rental_v16", "rental_v17"]
        )
        == "mixed[rental_v16,rental_v17]"
    )


def test_same_input_retry_reuses_current(quality_fixture):
    first = publish_quality(quality_fixture)
    second = publish_quality(quality_fixture)
    assert first.status == STATUS_PUBLISHED_CURRENT
    assert second.status == STATUS_REUSED_CURRENT
    assert second.mart_version_id == first.mart_version_id
    assert second.current_pointer_path.read_bytes() == first.current_pointer_path.read_bytes()


def test_revision_impact_is_source_month_only(quality_fixture):
    old_source_version = current_clean_metadata(
        quality_fixture, "2020-02"
    )["source_version_id"]
    publish_fact_fixture(
        quality_fixture,
        source_month="2020-02",
        rows=[
            rental_row(
                bike_id="REV",
                rent_at="2020-02-08 08:00:00",
                rent_station_no="00001",
                return_at="2020-02-08 08:10:00",
                return_station_no="00002",
                usage_min="10",
                distance_m="100",
            )
        ],
    )
    new_source_version = current_clean_metadata(
        quality_fixture, "2020-02"
    )["source_version_id"]
    impact = plan_source_month_quality_revision_impact(
        manifest=quality_fixture / "manifest.json",
        raw_root=quality_fixture / "raw",
        source_month="2020-02",
        old_source_version_id=old_source_version,
        new_source_version_id=new_source_version,
    )
    assert impact.affected_source_months == ("2020-02",)


def test_revision_impact_rejects_unrelated_period_or_wrong_order(quality_fixture):
    january_source = current_clean_metadata(
        quality_fixture, "2020-01"
    )["source_version_id"]
    old_february_source = current_clean_metadata(
        quality_fixture, "2020-02"
    )["source_version_id"]
    publish_fact_fixture(
        quality_fixture,
        source_month="2020-02",
        rows=[
            rental_row(
                bike_id="REV-2",
                rent_at="2020-02-10 08:00:00",
                rent_station_no="00001",
                return_at="2020-02-10 08:10:00",
                return_station_no="00002",
                usage_min="10",
                distance_m="100",
            )
        ],
    )
    new_february_source = current_clean_metadata(
        quality_fixture, "2020-02"
    )["source_version_id"]

    with pytest.raises(SourceMonthQualityMartError, match="logical_period"):
        plan_source_month_quality_revision_impact(
            manifest=quality_fixture / "manifest.json",
            raw_root=quality_fixture / "raw",
            source_month="2020-02",
            old_source_version_id=january_source,
            new_source_version_id=new_february_source,
        )
    with pytest.raises(SourceMonthQualityMartError, match="appear before"):
        plan_source_month_quality_revision_impact(
            manifest=quality_fixture / "manifest.json",
            raw_root=quality_fixture / "raw",
            source_month="2020-02",
            old_source_version_id=new_february_source,
            new_source_version_id=old_february_source,
        )
    with pytest.raises(SourceMonthQualityMartError, match="does not cover"):
        plan_source_month_quality_revision_impact(
            manifest=quality_fixture / "manifest.json",
            raw_root=quality_fixture / "raw",
            source_month="2020-03",
            old_source_version_id=old_february_source,
            new_source_version_id=new_february_source,
        )


def test_clean_fact_current_mismatch_is_rejected(quality_fixture):
    publish_clean_only(
        quality_fixture,
        source_month="2020-02",
        rows=[
            rental_row(
                bike_id="CLEAN-NEW",
                rent_at="2020-02-09 08:00:00",
                rent_station_no="00001",
                return_at="2020-02-09 08:10:00",
                return_station_no="00002",
                usage_min="10",
                distance_m="100",
            )
        ],
    )
    with pytest.raises(
        SourceMonthQualityMartError,
        match="does not use current Clean version",
    ):
        publish_quality(quality_fixture)


def test_raw_tamper_is_rejected(quality_fixture):
    metadata = current_clean_metadata(quality_fixture, "2020-02")
    artifact = (
        quality_fixture
        / "raw"
        / "artifacts"
        / metadata["source_version_id"]
        / "artifact"
    )
    artifact.write_bytes(artifact.read_bytes() + b"tamper")
    with pytest.raises(Exception, match="checksum|fingerprint|Raw artifact"):
        publish_quality(quality_fixture)


def test_transform_failure_preserves_previous_current(quality_fixture):
    first = publish_quality(quality_fixture)
    before = first.current_pointer_path.read_bytes()
    bad_sql = quality_fixture / "broken-quality.sql"
    bad_sql.write_text(
        "CREATE TABLE mart_source_month_quality AS SELECT does_not_exist;\n",
        encoding="utf-8",
    )
    with pytest.raises(SourceMonthQualityMartError, match="SQL build failed"):
        publish_quality(
            quality_fixture,
            transformation_version="quality-t2",
            sql_path=bad_sql,
        )
    assert first.current_pointer_path.read_bytes() == before


def test_pre_current_failure_preserves_current_and_retry_succeeds(quality_fixture):
    first = publish_quality(quality_fixture)
    before = first.current_pointer_path.read_bytes()

    def fail(_version_path: Path) -> None:
        raise RuntimeError("injected quality failure")

    with pytest.raises(RuntimeError, match="injected quality failure"):
        publish_quality(
            quality_fixture,
            transformation_version="quality-t2",
            before_current_publish=fail,
        )
    assert first.current_pointer_path.read_bytes() == before
    retry = publish_quality(
        quality_fixture, transformation_version="quality-t2"
    )
    assert retry.status == STATUS_PUBLISHED_CURRENT


def test_clean_current_change_during_hook_preserves_previous_current(quality_fixture):
    first = publish_quality(quality_fixture)
    before = first.current_pointer_path.read_bytes()
    clean_pointer_path = (
        quality_fixture / "clean" / "current" / "source_month=2020-02.json"
    )

    def change_clean(_version_path: Path) -> None:
        clean_pointer_path.write_text(
            json.dumps(
                {
                    "schema_version": 1,
                    "source_month": "2020-02",
                    "clean_version_id": "cv1_" + "0" * 64,
                },
                sort_keys=True,
            )
            + "\n",
            encoding="utf-8",
        )

    with pytest.raises(
        SourceMonthQualityMartError, match="Clean current pointer changed"
    ):
        publish_quality(
            quality_fixture,
            transformation_version="quality-t2",
            before_current_publish=change_clean,
        )
    assert first.current_pointer_path.read_bytes() == before


def test_fact_current_change_during_hook_preserves_previous_current(quality_fixture):
    first = publish_quality(quality_fixture)
    before = first.current_pointer_path.read_bytes()
    fact_pointer_path = (
        quality_fixture
        / "warehouse"
        / "fact_trip_trusted"
        / "current"
        / "source_month=2020-02.json"
    )

    def change_fact(_version_path: Path) -> None:
        fact_pointer_path.write_text(
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

    with pytest.raises(
        SourceMonthQualityMartError, match="trusted fact current pointer changed"
    ):
        publish_quality(
            quality_fixture,
            transformation_version="quality-t2",
            before_current_publish=change_fact,
        )
    assert first.current_pointer_path.read_bytes() == before


def test_clean_physical_rewrite_with_refreshed_fingerprint_is_rejected(
    quality_fixture,
):
    first = publish_quality(quality_fixture)
    before = first.current_pointer_path.read_bytes()
    clean_path = current_clean_version_path(quality_fixture, "2020-02")

    def rewrite_clean(_version_path: Path) -> None:
        rewrite_clean_parquet_preserving_rows(clean_path)

    with pytest.raises(
        SourceMonthQualityMartError, match="captured Clean immutable metadata changed"
    ):
        publish_quality(
            quality_fixture,
            transformation_version="quality-t2",
            before_current_publish=rewrite_clean,
        )
    assert first.current_pointer_path.read_bytes() == before


def test_manifest_record_tamper_during_hook_is_rejected(quality_fixture):
    first = publish_quality(quality_fixture)
    before = first.current_pointer_path.read_bytes()
    manifest_path = quality_fixture / "manifest.json"
    source_version_id = current_clean_metadata(
        quality_fixture, "2020-02"
    )["source_version_id"]

    def mutate_manifest(_version_path: Path) -> None:
        payload = json.loads(manifest_path.read_text(encoding="utf-8"))
        for record in payload["records"]:
            if record["source_version_id"] == source_version_id:
                record["source_url"] = "https://example.invalid/tampered-source-url"
                break
        manifest_path.write_text(
            json.dumps(
                payload,
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
            )
            + "\n",
            encoding="utf-8",
        )

    with pytest.raises(
        SourceMonthQualityMartError, match="registered source metadata changed"
    ):
        publish_quality(
            quality_fixture,
            transformation_version="quality-t2",
            before_current_publish=mutate_manifest,
        )
    assert first.current_pointer_path.read_bytes() == before


def test_concurrent_different_publish_cannot_roll_back_winner(
    quality_fixture, monkeypatch
):
    real_locks = quality_mart._publication_locks
    state = {"nested": False, "winner": None}

    @contextmanager
    def interleaving_locks(**kwargs):
        if not state["nested"]:
            state["nested"] = True
            state["winner"] = publish_quality(
                quality_fixture, transformation_version="quality-t2"
            )
            state["nested"] = False
        with real_locks(**kwargs):
            yield

    monkeypatch.setattr(quality_mart, "_publication_locks", interleaving_locks)
    with pytest.raises(
        SourceMonthQualityMartError, match="current pointer changed during build"
    ):
        publish_quality(quality_fixture, transformation_version="quality-t1")
    pointer = json.loads(
        source_month_quality_mart_current_pointer_path(
            quality_fixture / "warehouse", "2020-02"
        ).read_text(encoding="utf-8")
    )
    assert pointer["mart_version_id"] == state["winner"].mart_version_id


def test_concurrent_same_version_converges_to_reused_current(
    quality_fixture, monkeypatch
):
    real_locks = quality_mart._publication_locks
    state = {"nested": False, "winner": None}

    @contextmanager
    def interleaving_locks(**kwargs):
        if not state["nested"]:
            state["nested"] = True
            state["winner"] = publish_quality(quality_fixture)
            state["nested"] = False
        with real_locks(**kwargs):
            yield

    monkeypatch.setattr(quality_mart, "_publication_locks", interleaving_locks)
    outer = publish_quality(quality_fixture)
    assert state["winner"].status == STATUS_PUBLISHED_CURRENT
    assert outer.status == STATUS_REUSED_CURRENT
    assert outer.mart_version_id == state["winner"].mart_version_id


def test_mart_tamper_is_rejected_even_with_updated_fingerprint(quality_fixture):
    published = publish_quality(quality_fixture)
    database_path = published.version_path / "warehouse.duckdb"
    connection = duckdb.connect(str(database_path))
    try:
        connection.execute(
            """
            UPDATE mart_source_month_quality
            SET incomplete_trip_count = incomplete_trip_count + 1
            """
        )
        connection.execute("CHECKPOINT")
    finally:
        connection.close()
    rewrite_database_fingerprint(published.version_path)
    with pytest.raises(
        SourceMonthQualityMartError, match="independent recomputation"
    ):
        validate_source_month_quality_mart_version(
            published.version_path,
            manifest=quality_fixture / "manifest.json",
            raw_root=quality_fixture / "raw",
            station_root=quality_fixture / "station",
            clean_root=quality_fixture / "clean",
            warehouse_root=quality_fixture / "warehouse",
        )


def test_rate_tamper_above_float_rounding_noise_is_rejected(quality_fixture):
    published = publish_quality(quality_fixture)
    database_path = published.version_path / "warehouse.duckdb"
    connection = duckdb.connect(str(database_path))
    try:
        connection.execute(
            """
            UPDATE mart_source_month_quality
            SET distance_zero_rate = distance_zero_rate + 1e-12
            """
        )
        connection.execute("CHECKPOINT")
    finally:
        connection.close()
    rewrite_database_fingerprint(published.version_path)
    with pytest.raises(
        SourceMonthQualityMartError, match="independent recomputation"
    ):
        validate_source_month_quality_mart_version(
            published.version_path,
            manifest=quality_fixture / "manifest.json",
            raw_root=quality_fixture / "raw",
            station_root=quality_fixture / "station",
            clean_root=quality_fixture / "clean",
            warehouse_root=quality_fixture / "warehouse",
        )


def test_metadata_lineage_tamper_is_rejected(quality_fixture):
    published = publish_quality(quality_fixture)
    metadata_path = published.version_path / "metadata.json"
    metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
    metadata["clean_version_id"] = "cv1_" + "0" * 64
    metadata_path.write_text(
        json.dumps(metadata, sort_keys=True, separators=(",", ":")) + "\n",
        encoding="utf-8",
    )
    with pytest.raises(SourceMonthQualityMartError):
        validate_source_month_quality_mart_version(
            published.version_path,
            manifest=quality_fixture / "manifest.json",
            raw_root=quality_fixture / "raw",
            station_root=quality_fixture / "station",
            clean_root=quality_fixture / "clean",
            warehouse_root=quality_fixture / "warehouse",
        )


def test_residual_wal_is_rejected(quality_fixture):
    published = publish_quality(quality_fixture)
    wal_path = Path(str(published.version_path / "warehouse.duckdb") + ".wal")
    wal_path.write_bytes(b"synthetic residual wal")
    with pytest.raises(SourceMonthQualityMartError, match="residual WAL"):
        validate_source_month_quality_mart_version(
            published.version_path,
            manifest=quality_fixture / "manifest.json",
            raw_root=quality_fixture / "raw",
            station_root=quality_fixture / "station",
            clean_root=quality_fixture / "clean",
            warehouse_root=quality_fixture / "warehouse",
        )


def test_relocated_bundle_path_is_rejected(quality_fixture):
    published = publish_quality(quality_fixture)
    relocated = (
        quality_fixture
        / "relocated"
        / "source_month=2020-03"
        / f"mart_version_id={published.mart_version_id}"
    )
    relocated.parent.mkdir(parents=True)
    shutil.copytree(published.version_path, relocated)
    with pytest.raises(SourceMonthQualityMartError, match="directory month"):
        validate_source_month_quality_mart_version(
            relocated,
            manifest=quality_fixture / "manifest.json",
            raw_root=quality_fixture / "raw",
            station_root=quality_fixture / "station",
            clean_root=quality_fixture / "clean",
            warehouse_root=quality_fixture / "warehouse",
        )


def test_sql_identity_normalizes_line_endings(tmp_path):
    source = (
        Path(quality_mart.__file__).with_name("sql")
        / "mart_source_month_quality.sql"
    )
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
    assert len({quality_mart._load_sql(path)[1] for path in paths}) == 1


def test_exact_loaded_sql_text_is_executed_after_path_mutation(
    quality_fixture, monkeypatch
):
    source = (
        Path(quality_mart.__file__).with_name("sql")
        / "mart_source_month_quality.sql"
    )
    sql_copy = quality_fixture / "quality.sql"
    sql_copy.write_bytes(source.read_bytes())
    original = quality_mart._load_sql

    def load_then_mutate(path):
        text, sha = original(path)
        Path(path).write_text("BROKEN AFTER LOAD", encoding="utf-8")
        return text, sha

    monkeypatch.setattr(quality_mart, "_load_sql", load_then_mutate)
    result = publish_quality(quality_fixture, sql_path=sql_copy)
    assert result.status == STATUS_PUBLISHED_CURRENT


def test_production_range_rejects_2015_and_post_end(quality_fixture):
    for month in ("2015-10", "2026-07"):
        with pytest.raises(
            SourceMonthQualityMartError, match="production range"
        ):
            publish_quality(quality_fixture, source_month=month)


def test_active_quality_lock_blocks_and_stale_file_does_not(quality_fixture):
    root = quality_mart.source_month_quality_mart_root(
        quality_fixture / "warehouse"
    )
    lock_path = root / "locks" / "source_month=2020-02.lock"
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    lock_path.write_text("0stale-owner\n", encoding="utf-8")
    first = publish_quality(quality_fixture)
    assert first.status == STATUS_PUBLISHED_CURRENT
    with quality_mart._advisory_lock(
        lock_path, label="test-holder", error_message="test quality lock"
    ):
        with pytest.raises(SourceMonthQualityMartError, match="source quality mart"):
            publish_quality(
                quality_fixture, transformation_version="quality-t2"
            )


def test_quality_cli_calls_business_layer(quality_fixture):
    result = run_cli(
        "warehouse",
        "publish-source-month-quality",
        "--manifest",
        str(quality_fixture / "manifest.json"),
        "--raw-root",
        str(quality_fixture / "raw"),
        "--station-root",
        str(quality_fixture / "station"),
        "--clean-root",
        str(quality_fixture / "clean"),
        "--warehouse-root",
        str(quality_fixture / "warehouse"),
        "--source-month",
        "2020-02",
        "--transformation-version",
        "quality-t1",
    )
    assert result.returncode == 0, result.stderr
    assert result.stdout.startswith("SOURCE_MONTH_QUALITY_PUBLISH_OK msq1_")
    assert "status=PUBLISHED_CURRENT" in result.stdout
    assert "source_month=2020-02" in result.stdout
    assert "source_rows=3" in result.stdout
    assert "trusted=3" in result.stdout
