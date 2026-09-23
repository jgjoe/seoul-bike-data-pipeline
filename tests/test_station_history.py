from __future__ import annotations

import csv
import hashlib
import json
from pathlib import Path

import duckdb
import pytest

import seoul_bike_pipeline.station_history as station_history_module
from seoul_bike_pipeline.errors import StationHistoryError, StationSnapshotError
from seoul_bike_pipeline.register import register_source
from seoul_bike_pipeline.station_history import (
    STATUS_PUBLISHED_CURRENT,
    STATUS_REUSED_CURRENT,
    compute_applicable_from,
    publish_station_history,
    validate_station_history_version,
)
from seoul_bike_pipeline.station_snapshot import publish_station_snapshot


HEADER = [
    ["대여소\n번호", "보관소(대여소)명", "소재지(위치)", "", "", "", "설치\n시기", "설치형태", "", "운영\n방식"],
    ["", "", "", "", "", "", "", "LCD", "QR", ""],
    ["", "", "자치구", "상세주소", "위도", "경도", "", "", "", ""],
    ["", "", "", "", "", "", "", "거치\n대수", "거치\n대수", ""],
    ["", "", "", "", "", "", "", "", "", ""],
]


def station_row(
    station_no: str,
    *,
    name: str,
    address: str | None = None,
    qr_capacity: int = 10,
) -> list[str]:
    return [
        station_no,
        name,
        "마포구",
        address or f"서울특별시 마포구 {name}",
        "37.55000000",
        "126.91000000",
        "2020-01-01",
        "",
        str(qr_capacity),
        "QR",
    ]


def publish_snapshot(
    tmp_path: Path,
    rows: list[list[str]],
    *,
    logical_period: str,
    published_filename: str,
    suffix: str,
) -> str:
    artifact = tmp_path / f"station-{suffix}.csv"
    with artifact.open("w", encoding="cp949", newline="") as handle:
        writer = csv.writer(handle, lineterminator="\n")
        writer.writerows(HEADER)
        writer.writerows(rows)
    registered = register_source(
        artifact=artifact,
        manifest=tmp_path / "manifest.json",
        raw_root=tmp_path / "raw",
        dataset_id="station_snapshot",
        logical_period=logical_period,
        published_filename=published_filename,
        source_url="https://example.invalid/station",
        license="SYNTHETIC_FIXTURE",
    )
    result = publish_station_snapshot(
        manifest=tmp_path / "manifest.json",
        raw_root=tmp_path / "raw",
        station_root=tmp_path / "station",
        source_version_id=registered.record.source_version_id,
        transformation_version="station-t1",
        configuration={},
    )
    return result.station_version_id


def publish_four_snapshots(tmp_path: Path) -> None:
    publish_snapshot(
        tmp_path,
        [
            station_row("001", name="A"),
            station_row("002", name="X"),
        ],
        logical_period="2021-01-31",
        published_filename="공공자전거 대여소 정보(21.01.31 기준).csv",
        suffix="20210131",
    )
    publish_snapshot(
        tmp_path,
        [
            station_row("001", name="A"),
            station_row("003", name="Z"),
        ],
        logical_period="2021-06",
        published_filename="공공자전거 대여소 정보(21.06월 기준).csv",
        suffix="202106",
    )
    publish_snapshot(
        tmp_path,
        [
            station_row("001", name="B"),
            station_row("002", name="X"),
            station_row("003", name="Z"),
        ],
        logical_period="2021-12",
        published_filename="공공자전거 대여소 정보(21.12월 기준).csv",
        suffix="202112",
    )
    publish_snapshot(
        tmp_path,
        [
            station_row("001", name="B"),
            station_row("003", name="Z"),
        ],
        logical_period="2022-06",
        published_filename="공공자전거 대여소 정보(22.06월 기준).csv",
        suffix="202206",
    )


def history_kwargs(tmp_path: Path) -> dict[str, object]:
    return {
        "manifest": tmp_path / "manifest.json",
        "raw_root": tmp_path / "raw",
        "station_root": tmp_path / "station",
        "warehouse_root": tmp_path / "warehouse",
        "transformation_version": "history-t1",
        "configuration": {},
    }


def test_applicability_preserves_day_and_uses_next_month_for_month_precision():
    assert compute_applicable_from("2021-01-31", "DAY").isoformat() == "2021-01-31"
    assert compute_applicable_from("2021-06", "MONTH").isoformat() == "2021-07-01"
    assert compute_applicable_from("2021-12", "MONTH").isoformat() == "2022-01-01"


def test_sql_identity_is_independent_of_checkout_line_endings(tmp_path):
    sql_lf = tmp_path / "history-lf.sql"
    sql_crlf = tmp_path / "history-crlf.sql"
    content = "CREATE TABLE x AS\nSELECT 1 AS value;\n"
    sql_lf.write_bytes(content.encode("utf-8"))
    sql_crlf.write_bytes(content.replace("\n", "\r\n").encode("utf-8"))

    lf_text, lf_sha = station_history_module._load_sql(sql_lf)
    crlf_text, crlf_sha = station_history_module._load_sql(sql_crlf)

    assert lf_text == crlf_text == content
    assert lf_sha == crlf_sha


def test_plain_sql_history_compresses_same_state_and_preserves_observation_gap(tmp_path):
    publish_four_snapshots(tmp_path)

    result = publish_station_history(**history_kwargs(tmp_path))

    assert result.status == STATUS_PUBLISHED_CURRENT
    assert result.snapshot_count == 4
    assert result.input_observation_count == 9
    assert result.history_row_count == 5
    assert result.distinct_station_count == 3
    assert result.open_interval_count == 2

    connection = duckdb.connect(str(result.version_path / "warehouse.duckdb"), read_only=True)
    try:
        rows = connection.execute(
            """
            SELECT
                station_no,
                applicable_from,
                applicable_until,
                station_name,
                observation_periods,
                observation_count
            FROM dim_station_history
            ORDER BY station_no, applicable_from
            """
        ).fetchall()
    finally:
        connection.close()

    assert rows == [
        (
            "1",
            compute_applicable_from("2021-01-31", "DAY"),
            compute_applicable_from("2021-12", "MONTH"),
            "A",
            ["2021-01-31", "2021-06"],
            2,
        ),
        (
            "1",
            compute_applicable_from("2021-12", "MONTH"),
            None,
            "B",
            ["2021-12", "2022-06"],
            2,
        ),
        (
            "2",
            compute_applicable_from("2021-01-31", "DAY"),
            compute_applicable_from("2021-06", "MONTH"),
            "X",
            ["2021-01-31"],
            1,
        ),
        (
            "2",
            compute_applicable_from("2021-12", "MONTH"),
            compute_applicable_from("2022-06", "MONTH"),
            "X",
            ["2021-12"],
            1,
        ),
        (
            "3",
            compute_applicable_from("2021-06", "MONTH"),
            None,
            "Z",
            ["2021-06", "2021-12", "2022-06"],
            3,
        ),
    ]


def test_same_input_retry_reuses_current_history_version(tmp_path):
    publish_four_snapshots(tmp_path)
    kwargs = history_kwargs(tmp_path)

    first = publish_station_history(**kwargs)
    second = publish_station_history(**kwargs)

    assert first.status == STATUS_PUBLISHED_CURRENT
    assert second.status == STATUS_REUSED_CURRENT
    assert second.history_version_id == first.history_version_id


def test_station_revision_changes_history_version_and_current(tmp_path):
    publish_four_snapshots(tmp_path)
    kwargs = history_kwargs(tmp_path)
    first = publish_station_history(**kwargs)

    publish_snapshot(
        tmp_path,
        [
            station_row("001", name="B"),
            station_row("003", name="Z2"),
        ],
        logical_period="2022-06",
        published_filename="공공자전거 대여소 정보(22.06월 기준).csv",
        suffix="202206-revised",
    )
    second = publish_station_history(**kwargs)

    assert second.status == STATUS_PUBLISHED_CURRENT
    assert second.history_version_id != first.history_version_id
    pointer = json.loads(second.current_pointer_path.read_text(encoding="utf-8"))
    assert pointer["history_version_id"] == second.history_version_id
    assert first.version_path.exists()


def test_station_current_change_during_history_build_does_not_replace_warehouse_current(
    tmp_path,
):
    publish_four_snapshots(tmp_path)
    kwargs = history_kwargs(tmp_path)
    baseline = publish_station_history(**kwargs)

    def publish_revised_station(_version_path: Path) -> None:
        publish_snapshot(
            tmp_path,
            [
                station_row("001", name="B"),
                station_row("003", name="Z2"),
            ],
            logical_period="2022-06",
            published_filename="공공자전거 대여소 정보(22.06월 기준).csv",
            suffix="202206-race",
        )

    with pytest.raises(
        StationHistoryError, match="station current snapshot set changed"
    ):
        publish_station_history(
            **{**kwargs, "transformation_version": "history-t2"},
            before_current_publish=publish_revised_station,
        )

    pointer = json.loads(baseline.current_pointer_path.read_text(encoding="utf-8"))
    assert pointer["history_version_id"] == baseline.history_version_id


def test_bundle_tamper_before_current_publish_is_rejected(tmp_path):
    publish_four_snapshots(tmp_path)
    current = tmp_path / "warehouse" / "current.json"

    def corrupt_database(version_path: Path) -> None:
        with (version_path / "warehouse.duckdb").open("ab") as handle:
            handle.write(b"x")

    with pytest.raises(StationHistoryError, match="fingerprint"):
        publish_station_history(
            **history_kwargs(tmp_path),
            before_current_publish=corrupt_database,
        )

    assert not current.exists()


def test_station_current_set_lock_closes_post_recheck_publish_race(
    tmp_path, monkeypatch
):
    publish_four_snapshots(tmp_path)
    station_pointer = (
        tmp_path / "station" / "current" / "observation_period=2022-06.json"
    )
    before_station_pointer = station_pointer.read_text(encoding="utf-8")
    original_write = station_history_module._write_current_pointer
    attempted: dict[str, str | None] = {}

    def write_pointer_with_station_race(path: Path, history_version_id: str) -> None:
        try:
            publish_snapshot(
                tmp_path,
                [
                    station_row("001", name="B"),
                    station_row("003", name="RACE"),
                ],
                logical_period="2022-06",
                published_filename="공공자전거 대여소 정보(22.06월 기준).csv",
                suffix="post-recheck-race",
            )
        except StationSnapshotError as exc:
            attempted["error"] = str(exc)
        else:
            attempted["error"] = None
        original_write(path, history_version_id)

    monkeypatch.setattr(
        station_history_module,
        "_write_current_pointer",
        write_pointer_with_station_race,
    )
    result = publish_station_history(**history_kwargs(tmp_path))

    assert result.status == STATUS_PUBLISHED_CURRENT
    assert attempted["error"] is not None
    assert "current-set lock" in attempted["error"]
    assert station_pointer.read_text(encoding="utf-8") == before_station_pointer


def test_concurrent_same_input_publish_reuses_winner(tmp_path):
    publish_four_snapshots(tmp_path)
    kwargs = history_kwargs(tmp_path)
    nested: dict[str, object] = {}

    def publish_same_input(_version_path: Path) -> None:
        nested["result"] = publish_station_history(**kwargs)

    outer = publish_station_history(
        **kwargs,
        before_current_publish=publish_same_input,
    )

    assert nested["result"].status == STATUS_PUBLISHED_CURRENT
    assert outer.status == STATUS_REUSED_CURRENT
    assert outer.history_version_id == nested["result"].history_version_id


def test_validator_recomputes_history_independently_of_sql_output(tmp_path):
    publish_four_snapshots(tmp_path)
    result = publish_station_history(**history_kwargs(tmp_path))
    database = result.version_path / "warehouse.duckdb"

    connection = duckdb.connect(str(database))
    try:
        connection.execute(
            """
            UPDATE dim_station_history
            SET station_name = 'FAKE'
            WHERE station_no = '1' AND applicable_from = DATE '2021-01-31'
            """
        )
        connection.execute("CHECKPOINT")
    finally:
        connection.close()
    metadata_path = result.version_path / "metadata.json"
    metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
    data = database.read_bytes()
    metadata["warehouse_database"] = {
        "size_bytes": len(data),
        "sha256": hashlib.sha256(data).hexdigest(),
    }
    metadata_path.write_text(
        json.dumps(metadata, ensure_ascii=False, sort_keys=True, separators=(",", ":")) + "\n",
        encoding="utf-8",
    )

    with pytest.raises(
        StationHistoryError,
        match="does not exactly match independently recomputed",
    ):
        validate_station_history_version(
            result.version_path,
            manifest=tmp_path / "manifest.json",
            raw_root=tmp_path / "raw",
            station_root=tmp_path / "station",
        )


def test_duplicate_applicability_boundary_hard_fails(tmp_path):
    publish_snapshot(
        tmp_path,
        [station_row("001", name="A")],
        logical_period="2021-01",
        published_filename="공공자전거 대여소 정보(21.01월 기준).csv",
        suffix="202101",
    )
    publish_snapshot(
        tmp_path,
        [station_row("001", name="A")],
        logical_period="2021-02-01",
        published_filename="공공자전거 대여소 정보(21.02.01 기준).csv",
        suffix="20210201",
    )

    with pytest.raises(
        StationHistoryError, match="share applicability boundary"
    ):
        publish_station_history(**history_kwargs(tmp_path))


def test_quarantined_middle_snapshot_creates_gap_even_when_state_reappears(tmp_path):
    publish_snapshot(
        tmp_path,
        [station_row("001", name="A")],
        logical_period="2021-01-31",
        published_filename="공공자전거 대여소 정보(21.01.31 기준).csv",
        suffix="gap-a",
    )
    left = station_row("001", name="A", address="주소 1")
    right = station_row("001", name="A", address="주소 2")
    publish_snapshot(
        tmp_path,
        [left, right],
        logical_period="2021-06",
        published_filename="공공자전거 대여소 정보(21.06월 기준).csv",
        suffix="gap-conflict",
    )
    publish_snapshot(
        tmp_path,
        [station_row("001", name="A")],
        logical_period="2021-12",
        published_filename="공공자전거 대여소 정보(21.12월 기준).csv",
        suffix="gap-c",
    )

    result = publish_station_history(**history_kwargs(tmp_path))
    connection = duckdb.connect(str(result.version_path / "warehouse.duckdb"), read_only=True)
    try:
        rows = connection.execute(
            """
            SELECT applicable_from, applicable_until, observation_count
            FROM dim_station_history
            WHERE station_no = '1'
            ORDER BY applicable_from
            """
        ).fetchall()
    finally:
        connection.close()

    assert rows == [
        (
            compute_applicable_from("2021-01-31", "DAY"),
            compute_applicable_from("2021-06", "MONTH"),
            1,
        ),
        (
            compute_applicable_from("2021-12", "MONTH"),
            None,
            1,
        ),
    ]


def test_no_current_station_snapshots_hard_fails(tmp_path):
    with pytest.raises(StationHistoryError, match="does not exist"):
        publish_station_history(**history_kwargs(tmp_path))
