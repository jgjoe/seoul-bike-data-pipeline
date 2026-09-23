from __future__ import annotations

import csv
import io
import json
import os
import shutil
import subprocess
import sys
from datetime import date
from pathlib import Path

import duckdb
import pytest
import seoul_bike_pipeline.trip_station_enrichment as trip_station_enrichment

from seoul_bike_pipeline.clean_publish import publish_clean_month
from seoul_bike_pipeline.csv_source import RENTAL_V16_HEADER
from seoul_bike_pipeline.errors import TripStationEnrichmentError
from seoul_bike_pipeline.register import register_source
from seoul_bike_pipeline.station_history import publish_station_history
from seoul_bike_pipeline.station_snapshot import publish_station_snapshot
from seoul_bike_pipeline.trip_station_enrichment import (
    RULE_STATION_HISTORY_UNMATCHED,
    evaluate_current_trip_station_enrichment,
    evaluate_trip_station_enrichment,
)

REPO_ROOT = Path(__file__).resolve().parents[1]
STATION_HEADER = [
    ["대여소\n번호", "보관소(대여소)명", "소재지(위치)", "", "", "", "설치\n시기", "설치형태", "", "운영\n방식"],
    ["", "", "", "", "", "", "", "LCD", "QR", ""],
    ["", "", "자치구", "상세주소", "위도", "경도", "", "", "", ""],
    ["", "", "", "", "", "", "", "거치\n대수", "거치\n대수", ""],
    ["", "", "", "", "", "", "", "", "", ""],
]


def station_row(station_no: str, name: str) -> list[str]:
    return [
        station_no,
        name,
        "마포구",
        f"서울특별시 마포구 {name}",
        "37.55000000",
        "126.91000000",
        "2020-01-01",
        "",
        "10",
        "QR",
    ]


def publish_station_snapshot_fixture(
    tmp_path: Path,
    *,
    period: str,
    filename: str,
    suffix: str,
    rows: list[list[str]],
) -> None:
    artifact = tmp_path / f"station-{suffix}.csv"
    with artifact.open("w", encoding="cp949", newline="") as handle:
        writer = csv.writer(handle, lineterminator="\n")
        writer.writerows(STATION_HEADER)
        writer.writerows(rows)
    registered = register_source(
        artifact=artifact,
        manifest=tmp_path / "manifest.json",
        raw_root=tmp_path / "raw",
        dataset_id="station_snapshot",
        logical_period=period,
        published_filename=filename,
        source_url="https://example.invalid/station",
        license="SYNTHETIC_FIXTURE",
    )
    publish_station_snapshot(
        manifest=tmp_path / "manifest.json",
        raw_root=tmp_path / "raw",
        station_root=tmp_path / "station",
        source_version_id=registered.record.source_version_id,
        transformation_version="station-t1",
        configuration={},
    )


def rental_row(
    *,
    bike_id: str,
    rent_at: str,
    rent_station_no: str,
    sex: str = "M",
    usage_min: str = "10",
) -> tuple[str, ...]:
    return (
        bike_id,
        rent_at,
        rent_station_no,
        f"source-{rent_station_no}",
        "0",
        "2026-01-01 12:00:00",
        "00001",
        "return",
        "0",
        usage_min,
        "100.00",
        "1990",
        sex,
        "내국인",
        "ST-R",
        "ST-X",
    )


def publish_clean_fixture(tmp_path: Path):
    rows = [
        rental_row(
            bike_id="SPB-MATCH",
            rent_at="2026-01-01 00:00:00",
            rent_station_no="00001",
        ),
        rental_row(
            bike_id="SPB-FUTURE",
            rent_at="2026-01-01 01:00:00",
            rent_station_no="00002",
        ),
        rental_row(
            bike_id="SPB-GAP",
            rent_at="2026-01-01 02:00:00",
            rent_station_no="00003",
        ),
        rental_row(
            bike_id="SPB-NEVER",
            rent_at="2026-01-01 03:00:00",
            rent_station_no="00004",
            sex="f",
        ),
        rental_row(
            bike_id="SPB-QUARANTINE",
            rent_at="2026-01-01 04:00:00",
            rent_station_no="00001",
            usage_min="-1",
        ),
    ]
    buffer = io.StringIO(newline="")
    writer = csv.writer(buffer, lineterminator="\r\n")
    writer.writerow(RENTAL_V16_HEADER)
    writer.writerows(rows)
    artifact = tmp_path / "rental.csv"
    artifact.write_bytes(buffer.getvalue().encode("cp949"))
    registered = register_source(
        artifact=artifact,
        manifest=tmp_path / "manifest.json",
        raw_root=tmp_path / "raw",
        dataset_id="rental_history",
        logical_period="2026-01",
        published_filename="rental_2601.csv",
        source_url="https://example.invalid/rental",
        license="SYNTHETIC_FIXTURE",
    )
    return publish_clean_month(
        manifest=tmp_path / "manifest.json",
        raw_root=tmp_path / "raw",
        clean_root=tmp_path / "clean",
        source_version_id=registered.record.source_version_id,
        source_month="2026-01",
        transformation_version="clean-t1",
        configuration={},
    )


def build_fixture(tmp_path: Path):
    # Station 3 is observed through 2025-06 only, so its interval closes at
    # the 2025-12 snapshot applicability boundary (2026-01-01).
    publish_station_snapshot_fixture(
        tmp_path,
        period="2025-06",
        filename="공공자전거 대여소 정보(25.6월 기준).csv",
        suffix="202506",
        rows=[station_row("00003", "gap-station")],
    )
    # Station 1 becomes applicable exactly on 2026-01-01.
    publish_station_snapshot_fixture(
        tmp_path,
        period="2025-12",
        filename="공공자전거 대여소 정보(25.12월 기준).csv",
        suffix="202512",
        rows=[station_row("00001", "matched-station")],
    )
    # Station 2 is a future-only observation for all January 2026 trips.
    publish_station_snapshot_fixture(
        tmp_path,
        period="2026-06",
        filename="공공자전거 대여소 정보(26.6월 기준).csv",
        suffix="202606",
        rows=[station_row("00002", "future-station")],
    )
    history = publish_station_history(
        manifest=tmp_path / "manifest.json",
        raw_root=tmp_path / "raw",
        station_root=tmp_path / "station",
        warehouse_root=tmp_path / "warehouse",
        transformation_version="history-t1",
        configuration={},
    )
    clean = publish_clean_fixture(tmp_path)
    return clean, history


def test_temporal_enrichment_matches_boundary_and_rejects_future_gap_and_never_seen(tmp_path):
    clean, history = build_fixture(tmp_path)
    original_clean_bytes = (clean.clean_version_path / "clean.parquet").read_bytes()
    rows: list[dict[str, object]] = []

    summary = evaluate_trip_station_enrichment(
        manifest=tmp_path / "manifest.json",
        raw_root=tmp_path / "raw",
        station_root=tmp_path / "station",
        clean_version_path=clean.clean_version_path,
        station_history_version_path=history.version_path,
        on_row=rows.append,
    )

    assert summary.trusted_candidate_count == 4
    assert summary.station_history_match_count == 1
    assert summary.station_history_unmatched_count == 3
    assert summary.station_history_warning_added_count == 3
    assert len(rows) == 4

    by_bike = {row["bike_id"]: row for row in rows}
    matched = by_bike["SPB-MATCH"]
    assert matched["station_history_match"] is True
    assert matched["matched_station_applicable_from"] == date(2026, 1, 1)
    assert matched["dq_warning_count"] == 0
    assert RULE_STATION_HISTORY_UNMATCHED not in {
        issue["rule_id"] for issue in matched["dq_issues"]
    }

    for bike_id in ("SPB-FUTURE", "SPB-GAP", "SPB-NEVER"):
        row = by_bike[bike_id]
        assert row["station_history_match"] is False
        assert row["matched_station_applicable_from"] is None
        assert RULE_STATION_HISTORY_UNMATCHED in {
            issue["rule_id"] for issue in row["dq_issues"]
        }
        assert row["trusted_fact_eligible"] is True

    never = by_bike["SPB-NEVER"]
    assert {issue["rule_id"] for issue in never["dq_issues"]} == {
        "sex_noncanonical",
        RULE_STATION_HISTORY_UNMATCHED,
    }
    assert never["dq_warning_count"] == 2
    assert "SPB-QUARANTINE" not in by_bike
    assert (clean.clean_version_path / "clean.parquet").read_bytes() == original_clean_bytes


def test_current_wrapper_uses_same_business_layer_and_reports_lineage(tmp_path):
    clean, history = build_fixture(tmp_path)

    summary = evaluate_current_trip_station_enrichment(
        manifest=tmp_path / "manifest.json",
        raw_root=tmp_path / "raw",
        station_root=tmp_path / "station",
        clean_root=tmp_path / "clean",
        warehouse_root=tmp_path / "warehouse",
        source_month="2026-01",
    )

    assert summary.clean_version_id == clean.clean_version_id
    assert summary.history_version_id == history.history_version_id
    assert summary.trusted_candidate_count == 4
    assert summary.station_history_match_count == 1


def test_current_wrapper_rejects_pointer_to_bundle_identity_mismatch(tmp_path):
    clean, history = build_fixture(tmp_path)
    clean_pointer_path = tmp_path / "clean" / "current" / "source_month=2026-01.json"
    clean_pointer = json.loads(clean_pointer_path.read_text(encoding="utf-8"))
    fake_clean_version_id = "cv1_" + "0" * 64
    fake_clean_path = (
        clean.clean_version_path.parent
        / f"clean_version_id={fake_clean_version_id}"
    )
    shutil.copytree(clean.clean_version_path, fake_clean_path)
    clean_pointer["clean_version_id"] = fake_clean_version_id
    clean_pointer_path.write_text(
        json.dumps(clean_pointer, ensure_ascii=False, sort_keys=True) + "\n",
        encoding="utf-8",
    )

    with pytest.raises(TripStationEnrichmentError, match="Clean bundle version id"):
        evaluate_current_trip_station_enrichment(
            manifest=tmp_path / "manifest.json",
            raw_root=tmp_path / "raw",
            station_root=tmp_path / "station",
            clean_root=tmp_path / "clean",
            warehouse_root=tmp_path / "warehouse",
            source_month="2026-01",
        )

    clean_pointer["clean_version_id"] = clean.clean_version_id
    clean_pointer_path.write_text(
        json.dumps(clean_pointer, ensure_ascii=False, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    history_pointer_path = tmp_path / "warehouse" / "current.json"
    history_pointer = json.loads(history_pointer_path.read_text(encoding="utf-8"))
    fake_history_version_id = "wh1_" + "0" * 64
    fake_history_path = (
        history.version_path.parent
        / f"history_version_id={fake_history_version_id}"
    )
    shutil.copytree(history.version_path, fake_history_path)
    history_pointer["history_version_id"] = fake_history_version_id
    history_pointer_path.write_text(
        json.dumps(history_pointer, ensure_ascii=False, sort_keys=True) + "\n",
        encoding="utf-8",
    )

    with pytest.raises(TripStationEnrichmentError, match="station-history bundle version id"):
        evaluate_current_trip_station_enrichment(
            manifest=tmp_path / "manifest.json",
            raw_root=tmp_path / "raw",
            station_root=tmp_path / "station",
            clean_root=tmp_path / "clean",
            warehouse_root=tmp_path / "warehouse",
            source_month="2026-01",
        )


def test_custom_sql_that_uses_future_station_is_hard_rejected(tmp_path):
    clean, history = build_fixture(tmp_path)
    sql = tmp_path / "future.sql"
    sql.write_text(
        """
        CREATE TABLE trip_station_enrichment AS
        SELECT
            clean.*,
            history.applicable_from AS matched_station_applicable_from,
            history.applicable_until AS matched_station_applicable_until
        FROM clean_input AS clean
        LEFT JOIN station_history.dim_station_history AS history
            ON history.station_no = clean.rent_station_no
        WHERE clean.trusted_fact_eligible
        QUALIFY row_number() OVER (
            PARTITION BY clean.source_row_id
            ORDER BY history.applicable_from DESC NULLS LAST
        ) = 1
        ORDER BY clean.source_row_id;
        """,
        encoding="utf-8",
    )

    with pytest.raises(TripStationEnrichmentError, match="future station-history interval"):
        evaluate_trip_station_enrichment(
            manifest=tmp_path / "manifest.json",
            raw_root=tmp_path / "raw",
            station_root=tmp_path / "station",
            clean_version_path=clean.clean_version_path,
            station_history_version_path=history.version_path,
            sql_path=sql,
        )


def test_custom_sql_that_uses_expired_gap_interval_is_hard_rejected(tmp_path):
    clean, history = build_fixture(tmp_path)
    sql = tmp_path / "expired.sql"
    sql.write_text(
        """
        CREATE TABLE trip_station_enrichment AS
        SELECT
            clean.*,
            history.applicable_from AS matched_station_applicable_from,
            history.applicable_until AS matched_station_applicable_until
        FROM clean_input AS clean
        LEFT JOIN station_history.dim_station_history AS history
            ON history.station_no = clean.rent_station_no
           AND CAST(clean.rent_at AS DATE) >= history.applicable_from
        WHERE clean.trusted_fact_eligible
        ORDER BY clean.source_row_id;
        """,
        encoding="utf-8",
    )

    with pytest.raises(TripStationEnrichmentError, match="expired station-history interval"):
        evaluate_trip_station_enrichment(
            manifest=tmp_path / "manifest.json",
            raw_root=tmp_path / "raw",
            station_root=tmp_path / "station",
            clean_version_path=clean.clean_version_path,
            station_history_version_path=history.version_path,
            sql_path=sql,
        )


def test_custom_sql_cannot_modify_clean_projection_before_callback(tmp_path):
    clean, history = build_fixture(tmp_path)
    sql = tmp_path / "modified-dq.sql"
    sql.write_text(
        """
        CREATE TABLE trip_station_enrichment AS
        SELECT
            clean.* REPLACE (
                CASE
                    WHEN clean.bike_id = 'SPB-FUTURE' THEN NULL
                    ELSE clean.dq_issues
                END AS dq_issues
            ),
            history.applicable_from AS matched_station_applicable_from,
            history.applicable_until AS matched_station_applicable_until
        FROM clean_input AS clean
        LEFT JOIN station_history.dim_station_history AS history
            ON history.station_no = clean.rent_station_no
           AND CAST(clean.rent_at AS DATE) >= history.applicable_from
           AND (
                history.applicable_until IS NULL
                OR CAST(clean.rent_at AS DATE) < history.applicable_until
           )
        WHERE clean.trusted_fact_eligible
        ORDER BY clean.source_row_id;
        """,
        encoding="utf-8",
    )
    emitted: list[dict[str, object]] = []

    with pytest.raises(TripStationEnrichmentError, match="modified the trusted Clean projection"):
        evaluate_trip_station_enrichment(
            manifest=tmp_path / "manifest.json",
            raw_root=tmp_path / "raw",
            station_root=tmp_path / "station",
            clean_version_path=clean.clean_version_path,
            station_history_version_path=history.version_path,
            sql_path=sql,
            on_row=emitted.append,
        )

    assert emitted == []


def test_custom_sql_view_is_rejected_before_callback(tmp_path):
    clean, history = build_fixture(tmp_path)
    sql = tmp_path / "view.sql"
    sql.write_text(
        """
        CREATE VIEW trip_station_enrichment AS
        SELECT
            clean.*,
            history.applicable_from AS matched_station_applicable_from,
            history.applicable_until AS matched_station_applicable_until
        FROM clean_input AS clean
        LEFT JOIN station_history.dim_station_history AS history
            ON history.station_no = clean.rent_station_no
           AND CAST(clean.rent_at AS DATE) >= history.applicable_from
           AND (
                history.applicable_until IS NULL
                OR CAST(clean.rent_at AS DATE) < history.applicable_until
           )
        WHERE clean.trusted_fact_eligible;
        """,
        encoding="utf-8",
    )
    emitted: list[dict[str, object]] = []

    with pytest.raises(TripStationEnrichmentError, match="must materialize"):
        evaluate_trip_station_enrichment(
            manifest=tmp_path / "manifest.json",
            raw_root=tmp_path / "raw",
            station_root=tmp_path / "station",
            clean_version_path=clean.clean_version_path,
            station_history_version_path=history.version_path,
            sql_path=sql,
            on_row=emitted.append,
        )

    assert emitted == []


def test_post_validation_clean_mutation_is_rejected_before_return(tmp_path, monkeypatch):
    clean, history = build_fixture(tmp_path)
    clean_parquet = clean.clean_version_path / "clean.parquet"
    original_load_sql = trip_station_enrichment._load_sql

    def mutate_clean_then_load_sql(sql_path):
        rewritten = tmp_path / "rewritten-clean.parquet"
        source_sql = clean_parquet.resolve().as_posix().replace("'", "''")
        rewritten_sql = rewritten.resolve().as_posix().replace("'", "''")
        connection = duckdb.connect()
        try:
            connection.execute(
                f"""
                COPY (
                    SELECT * REPLACE (
                        CASE
                            WHEN bike_id = 'SPB-FUTURE' THEN '1'
                            ELSE rent_station_no
                        END AS rent_station_no
                    )
                    FROM read_parquet('{source_sql}', hive_partitioning=false)
                ) TO '{rewritten_sql}' (FORMAT PARQUET)
                """
            )
        finally:
            connection.close()
        os.replace(rewritten, clean_parquet)
        return original_load_sql(sql_path)

    monkeypatch.setattr(
        trip_station_enrichment,
        "_load_sql",
        mutate_clean_then_load_sql,
    )

    with pytest.raises(TripStationEnrichmentError, match="immutable enrichment input changed"):
        evaluate_trip_station_enrichment(
            manifest=tmp_path / "manifest.json",
            raw_root=tmp_path / "raw",
            station_root=tmp_path / "station",
            clean_version_path=clean.clean_version_path,
            station_history_version_path=history.version_path,
        )


def test_missing_current_history_pointer_is_hard_failure(tmp_path):
    publish_clean_fixture(tmp_path)

    with pytest.raises(TripStationEnrichmentError, match="cannot read current station-history pointer"):
        evaluate_current_trip_station_enrichment(
            manifest=tmp_path / "manifest.json",
            raw_root=tmp_path / "raw",
            station_root=tmp_path / "station",
            clean_root=tmp_path / "clean",
            warehouse_root=tmp_path / "warehouse",
            source_month="2026-01",
        )


def run_cli(*arguments: str) -> subprocess.CompletedProcess[str]:
    environment = dict(os.environ)
    source_root = str(REPO_ROOT / "src")
    existing = environment.get("PYTHONPATH")
    environment["PYTHONPATH"] = (
        f"{source_root}{os.pathsep}{existing}" if existing else source_root
    )
    return subprocess.run(
        [sys.executable, "-m", "seoul_bike_pipeline", *arguments],
        cwd=REPO_ROOT,
        env=environment,
        text=True,
        capture_output=True,
        encoding="utf-8",
        check=False,
    )


def test_cli_calls_current_enrichment_business_layer(tmp_path):
    clean, history = build_fixture(tmp_path)

    result = run_cli(
        "warehouse",
        "evaluate-station-enrichment",
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
        "--source-month",
        "2026-01",
    )

    assert result.returncode == 0, result.stderr
    assert result.stdout.startswith("STATION_ENRICHMENT_OK source_month=2026-01")
    assert f"clean_version_id={clean.clean_version_id}" in result.stdout
    assert f"history_version_id={history.history_version_id}" in result.stdout
    assert "trusted_candidates=4 matched=1 unmatched=3 warnings_added=3" in result.stdout


def test_clean_persisted_rows_remain_unenriched_after_evaluation(tmp_path):
    clean, history = build_fixture(tmp_path)
    evaluate_trip_station_enrichment(
        manifest=tmp_path / "manifest.json",
        raw_root=tmp_path / "raw",
        station_root=tmp_path / "station",
        clean_version_path=clean.clean_version_path,
        station_history_version_path=history.version_path,
    )

    connection = duckdb.connect()
    try:
        evaluated = connection.execute(
            "SELECT COUNT(*) FROM read_parquet(?) WHERE station_history_match IS NOT NULL",
            [str(clean.clean_version_path / "clean.parquet")],
        ).fetchone()[0]
    finally:
        connection.close()
    assert evaluated == 0
