from __future__ import annotations

import csv
import hashlib
import json
import zipfile
from decimal import Decimal
from pathlib import Path
from xml.sax.saxutils import escape

import duckdb
import pytest

from seoul_bike_pipeline.errors import StationSnapshotError
from seoul_bike_pipeline.register import register_source
from seoul_bike_pipeline.station_snapshot import (
    PRECISION_DAY,
    PRECISION_MONTH,
    RULE_EXACT_DUPLICATE,
    RULE_IDENTITY_CONFLICT,
    STATUS_BUILT_HISTORICAL,
    STATUS_PUBLISHED_CURRENT,
    STATUS_REUSED_CURRENT,
    parse_observation_period_from_filename,
    publish_station_snapshot,
    read_and_consolidate_station_snapshot,
    validate_station_version,
)


HEADER = [
    ["대여소\n번호", "보관소(대여소)명", "소재지(위치)", "", "", "", "설치\n시기", "설치형태", "", "운영\n방식"],
    ["", "", "", "", "", "", "", "LCD", "QR", ""],
    ["", "", "자치구", "상세주소", "위도", "경도", "", "", "", ""],
    ["", "", "", "", "", "", "", "거치\n대수", "거치\n대수", ""],
    ["", "", "", "", "", "", "", "", "", ""],
]


def write_snapshot(path: Path, rows: list[list[str]]) -> None:
    with path.open("w", encoding="cp949", newline="") as handle:
        writer = csv.writer(handle, lineterminator="\n")
        writer.writerows(HEADER)
        writer.writerows(rows)


def write_xlsx_snapshot(
    path: Path, rows: list[list[str]], *, duplicate_a6: bool = False
) -> None:
    all_rows = [*HEADER, *rows]
    sheet_rows = []
    for row_number, row in enumerate(all_rows, 1):
        cells = []
        for index, value in enumerate(row):
            if value == "":
                continue
            column = chr(ord("A") + index)
            reference = f"{column}{row_number}"
            if row_number > 5 and index in {0, 4, 5, 6, 7, 8}:
                cells.append(f'<c r="{reference}"><v>{escape(value)}</v></c>')
            else:
                cells.append(
                    f'<c r="{reference}" t="inlineStr"><is><t>{escape(value)}</t></is></c>'
                )
        if duplicate_a6 and row_number == 6:
            cells.append('<c r="A6"><v>999</v></c>')
        sheet_rows.append(f'<row r="{row_number}">{"".join(cells)}</row>')
    workbook = (
        '<?xml version="1.0" encoding="UTF-8"?>'
        '<workbook xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main" '
        'xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships">'
        '<sheets><sheet name="대여소현황" sheetId="1" r:id="rId1"/></sheets></workbook>'
    )
    relationships = (
        '<?xml version="1.0" encoding="UTF-8"?>'
        '<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">'
        '<Relationship Id="rId1" '
        'Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet" '
        'Target="worksheets/sheet1.xml"/></Relationships>'
    )
    sheet = (
        '<?xml version="1.0" encoding="UTF-8"?>'
        '<worksheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main">'
        f'<sheetData>{"".join(sheet_rows)}</sheetData></worksheet>'
    )
    with zipfile.ZipFile(path, "w") as archive:
        archive.writestr("xl/workbook.xml", workbook)
        archive.writestr("xl/_rels/workbook.xml.rels", relationships)
        archive.writestr("xl/worksheets/sheet1.xml", sheet)


def register_snapshot(
    tmp_path: Path,
    rows: list[list[str]],
    *,
    period: str = "2022-06",
    filename: str = "공공자전거 대여소 정보(22.06월 기준).csv",
    suffix: str = "v1",
):
    artifact = tmp_path / f"station-{suffix}.csv"
    write_snapshot(artifact, rows)
    return register_source(
        artifact=artifact,
        manifest=tmp_path / "manifest.json",
        raw_root=tmp_path / "raw",
        dataset_id="station_snapshot",
        logical_period=period,
        published_filename=filename,
        source_url="https://data.seoul.go.kr/dataList/OA-13252/F/1/datasetView.do",
        license="공공누리 제1유형",
    )


BASE = [
    "00102",
    "망원역 1번출구 앞",
    "마포구",
    "서울특별시 마포구 월드컵로 72",
    "37.5556488",
    "126.9106293",
    "2015-09-06",
    "",
    "15",
    "QR",
]


def test_filename_precision_preserves_source_granularity():
    assert parse_observation_period_from_filename(
        "공공자전거 대여소 정보(21.01.31 기준).csv"
    ) == ("2021-01-31", PRECISION_DAY)
    assert parse_observation_period_from_filename(
        "공공자전거 대여소 정보(26.6월 기준).xlsx"
    ) == ("2026-06", PRECISION_MONTH)


def test_complementary_rows_consolidate_without_installation_date_conflict(tmp_path):
    lcd = [
        "00583",
        "청계천 생태교실 앞",
        "성동구",
        "서울특별시 성동구 마장로39길 51",
        "37.567970",
        "127.046890",
        "2016-07-06",
        "20",
        "",
        "LCD",
    ]
    qr = [
        "583",
        "청계천 생태교실 앞",
        "성동구",
        "서울특별시 성동구 마장로39길 51",
        "",
        "",
        "2020-06-30",
        "",
        "5",
        "QR",
    ]
    registered = register_snapshot(
        tmp_path,
        [lcd, qr],
        period="2021-01-31",
        filename="공공자전거 대여소 정보(21.01.31 기준).csv",
    )
    observations = []
    summary = read_and_consolidate_station_snapshot(
        manifest=tmp_path / "manifest.json",
        raw_root=tmp_path / "raw",
        source_version_id=registered.record.source_version_id,
        on_observation=observations.append,
    )

    assert summary.source_row_count == 2
    assert summary.observation_count == 1
    assert summary.quarantined_source_row_count == 0
    observation = observations[0]
    assert observation.station_no == "583"
    assert observation.lcd_capacity == 20
    assert observation.qr_capacity == 5
    assert observation.operation_mode == "LCD+QR"
    assert observation.latitude is not None
    assert observation.installation_values == ("2016-07-06", "2020-06-30")


def test_identity_conflict_quarantines_entire_station_group(tmp_path):
    left = BASE.copy()
    right = BASE.copy()
    right[3] = "서로 다른 주소"
    registered = register_snapshot(tmp_path, [left, right])
    observations = []
    quarantined = []

    summary = read_and_consolidate_station_snapshot(
        manifest=tmp_path / "manifest.json",
        raw_root=tmp_path / "raw",
        source_version_id=registered.record.source_version_id,
        on_observation=observations.append,
        on_quarantined_row=quarantined.append,
    )

    assert observations == []
    assert summary.conflict_group_count == 1
    assert summary.quarantined_source_row_count == 2
    assert len(quarantined) == 2
    assert all(
        any(issue.rule_id == RULE_IDENTITY_CONFLICT for issue in row.dq_issues)
        for row in quarantined
    )


def test_conflict_quarantine_preserves_duplicate_warning_and_count(tmp_path):
    conflict = BASE.copy()
    conflict[3] = "서로 다른 주소"
    registered = register_snapshot(tmp_path, [BASE, BASE.copy(), conflict])

    summary = read_and_consolidate_station_snapshot(
        manifest=tmp_path / "manifest.json",
        raw_root=tmp_path / "raw",
        source_version_id=registered.record.source_version_id,
    )

    assert summary.conflict_group_count == 1
    assert summary.exact_duplicate_group_count == 1
    assert summary.exact_duplicate_source_row_count == 1
    assert summary.quarantined_source_row_count == 3
    quarantined = []
    read_and_consolidate_station_snapshot(
        manifest=tmp_path / "manifest.json",
        raw_root=tmp_path / "raw",
        source_version_id=registered.record.source_version_id,
        on_quarantined_row=quarantined.append,
    )
    assert sum(
        any(issue.rule_id == RULE_EXACT_DUPLICATE for issue in row.dq_issues)
        for row in quarantined
    ) == 1


def test_exact_duplicate_collapses_with_warning_not_quarantine(tmp_path):
    registered = register_snapshot(tmp_path, [BASE, BASE.copy()])
    observations = []

    summary = read_and_consolidate_station_snapshot(
        manifest=tmp_path / "manifest.json",
        raw_root=tmp_path / "raw",
        source_version_id=registered.record.source_version_id,
        on_observation=observations.append,
    )

    assert summary.exact_duplicate_group_count == 1
    assert summary.exact_duplicate_source_row_count == 1
    assert summary.quarantined_source_row_count == 0
    assert observations[0].exact_duplicate_count == 1
    assert any(issue.rule_id == RULE_EXACT_DUPLICATE for issue in observations[0].dq_issues)


def test_invalid_station_number_quarantines_row_but_keeps_snapshot(tmp_path):
    bad = BASE.copy()
    bad[0] = "ST-102"
    good = BASE.copy()
    good[0] = "00103"
    registered = register_snapshot(tmp_path, [bad, good])

    summary = read_and_consolidate_station_snapshot(
        manifest=tmp_path / "manifest.json",
        raw_root=tmp_path / "raw",
        source_version_id=registered.record.source_version_id,
    )

    assert summary.source_row_count == 2
    assert summary.observation_count == 1
    assert summary.quarantined_source_row_count == 1


def test_unknown_header_is_snapshot_hard_failure(tmp_path):
    artifact = tmp_path / "station.csv"
    write_snapshot(artifact, [BASE])
    text = artifact.read_text(encoding="cp949")
    artifact.write_text(text.replace("대여소", "대여점", 1), encoding="cp949")
    registered = register_source(
        artifact=artifact,
        manifest=tmp_path / "manifest.json",
        raw_root=tmp_path / "raw",
        dataset_id="station_snapshot",
        logical_period="2022-06",
        published_filename="공공자전거 대여소 정보(22.06월 기준).csv",
        source_url="https://data.seoul.go.kr/dataList/OA-13252/F/1/datasetView.do",
        license="공공누리 제1유형",
    )

    with pytest.raises(StationSnapshotError, match="unsupported station header"):
        read_and_consolidate_station_snapshot(
            manifest=tmp_path / "manifest.json",
            raw_root=tmp_path / "raw",
            source_version_id=registered.record.source_version_id,
        )


def test_xlsx_snapshot_ingestion_without_spreadsheet_runtime_dependency(tmp_path):
    artifact = tmp_path / "station.xlsx"
    row = BASE.copy()
    row[0] = "102"
    row[4] = "37.550628660000001"
    row[5] = "126.91498566000001"
    row[6] = "42253"
    write_xlsx_snapshot(artifact, [row])
    registered = register_source(
        artifact=artifact,
        manifest=tmp_path / "manifest.json",
        raw_root=tmp_path / "raw",
        dataset_id="station_snapshot",
        logical_period="2026-06",
        published_filename="공공자전거 대여소 정보(26.6월 기준).xlsx",
        source_url="https://data.seoul.go.kr/dataList/OA-13252/F/1/datasetView.do",
        license="공공누리 제1유형",
    )
    observations = []

    summary = read_and_consolidate_station_snapshot(
        manifest=tmp_path / "manifest.json",
        raw_root=tmp_path / "raw",
        source_version_id=registered.record.source_version_id,
        on_observation=observations.append,
    )

    assert summary.observation_count == 1
    assert summary.observation_precision == PRECISION_MONTH
    assert observations[0].station_no == "102"
    assert observations[0].latitude == Decimal("37.55062866")
    assert observations[0].longitude == Decimal("126.91498566")
    assert observations[0].installation_values == ("2015-09-06",)


def test_xlsx_duplicate_cell_reference_is_snapshot_hard_failure(tmp_path):
    artifact = tmp_path / "station.xlsx"
    write_xlsx_snapshot(artifact, [BASE], duplicate_a6=True)
    registered = register_source(
        artifact=artifact,
        manifest=tmp_path / "manifest.json",
        raw_root=tmp_path / "raw",
        dataset_id="station_snapshot",
        logical_period="2026-06",
        published_filename="공공자전거 대여소 정보(26.6월 기준).xlsx",
        source_url="https://data.seoul.go.kr/dataList/OA-13252/F/1/datasetView.do",
        license="공공누리 제1유형",
    )

    with pytest.raises(StationSnapshotError, match="duplicate cell/column reference"):
        read_and_consolidate_station_snapshot(
            manifest=tmp_path / "manifest.json",
            raw_root=tmp_path / "raw",
            source_version_id=registered.record.source_version_id,
        )


def test_publish_is_immutable_and_same_input_retry_reuses_current(tmp_path):
    registered = register_snapshot(tmp_path, [BASE])
    kwargs = dict(
        manifest=tmp_path / "manifest.json",
        raw_root=tmp_path / "raw",
        station_root=tmp_path / "station",
        source_version_id=registered.record.source_version_id,
        transformation_version="t1",
        configuration={},
    )

    first = publish_station_snapshot(**kwargs)
    second = publish_station_snapshot(**kwargs)

    assert first.status == STATUS_PUBLISHED_CURRENT
    assert second.status == STATUS_REUSED_CURRENT
    assert first.station_version_id == second.station_version_id
    metadata = validate_station_version(
        first.version_path,
        manifest=tmp_path / "manifest.json",
        raw_root=tmp_path / "raw",
    )
    assert metadata["source_row_count"] == 1
    assert metadata["observation_count"] == 1
    assert metadata["quarantined_source_row_count"] == 0


def test_historical_revision_build_cannot_rollback_current(tmp_path):
    v1 = register_snapshot(tmp_path, [BASE], suffix="v1")
    revised = BASE.copy()
    revised[8] = "16"
    v2 = register_snapshot(tmp_path, [revised], suffix="v2")
    common = dict(
        manifest=tmp_path / "manifest.json",
        raw_root=tmp_path / "raw",
        station_root=tmp_path / "station",
        transformation_version="t1",
        configuration={},
    )

    first = publish_station_snapshot(source_version_id=v1.record.source_version_id, **common)
    current = publish_station_snapshot(source_version_id=v2.record.source_version_id, **common)
    historical = publish_station_snapshot(
        source_version_id=v1.record.source_version_id,
        transformation_version="t2",
        **{k: v for k, v in common.items() if k != "transformation_version"},
    )

    assert first.status == STATUS_PUBLISHED_CURRENT
    assert current.status == STATUS_PUBLISHED_CURRENT
    assert historical.status == STATUS_BUILT_HISTORICAL
    pointer = json.loads(current.current_pointer_path.read_text(encoding="utf-8"))
    assert pointer["station_version_id"] == current.station_version_id


def test_prepublish_failure_leaves_current_unchanged_and_retry_recovers(tmp_path):
    registered = register_snapshot(tmp_path, [BASE])
    common = dict(
        manifest=tmp_path / "manifest.json",
        raw_root=tmp_path / "raw",
        station_root=tmp_path / "station",
        source_version_id=registered.record.source_version_id,
        transformation_version="t1",
        configuration={},
    )

    def fail_after_immutable(_version_path: Path) -> None:
        raise RuntimeError("injected pre-current failure")

    with pytest.raises(RuntimeError, match="injected pre-current failure"):
        publish_station_snapshot(**common, before_current_publish=fail_after_immutable)

    current = tmp_path / "station" / "current" / "observation_period=2022-06.json"
    assert not current.exists()
    retry = publish_station_snapshot(**common)
    assert retry.status == STATUS_PUBLISHED_CURRENT
    assert current.exists()


def test_nested_newer_publish_cannot_be_rolled_back_by_outer_publish(tmp_path):
    registered = register_snapshot(tmp_path, [BASE])
    common = dict(
        manifest=tmp_path / "manifest.json",
        raw_root=tmp_path / "raw",
        station_root=tmp_path / "station",
        source_version_id=registered.record.source_version_id,
        configuration={},
    )
    nested = {}

    def publish_newer(_version_path: Path) -> None:
        nested["result"] = publish_station_snapshot(
            **common,
            transformation_version="t2",
        )

    with pytest.raises(StationSnapshotError, match="changed before atomic publish"):
        publish_station_snapshot(
            **common,
            transformation_version="t1",
            before_current_publish=publish_newer,
        )

    pointer_path = nested["result"].current_pointer_path
    pointer = json.loads(pointer_path.read_text(encoding="utf-8"))
    assert pointer["station_version_id"] == nested["result"].station_version_id


def test_bundle_tamper_before_current_publish_is_revalidated_and_rejected(tmp_path):
    registered = register_snapshot(tmp_path, [BASE])
    current_path = (
        tmp_path / "station" / "current" / "observation_period=2022-06.json"
    )

    def corrupt_immutable(version_path: Path) -> None:
        with (version_path / "observations.parquet").open("ab") as handle:
            handle.write(b"x")

    with pytest.raises(StationSnapshotError, match="fingerprint"):
        publish_station_snapshot(
            manifest=tmp_path / "manifest.json",
            raw_root=tmp_path / "raw",
            station_root=tmp_path / "station",
            source_version_id=registered.record.source_version_id,
            transformation_version="t1",
            configuration={},
            before_current_publish=corrupt_immutable,
        )

    assert not current_path.exists()


def test_durable_validation_detects_parquet_tamper(tmp_path):
    registered = register_snapshot(tmp_path, [BASE])
    result = publish_station_snapshot(
        manifest=tmp_path / "manifest.json",
        raw_root=tmp_path / "raw",
        station_root=tmp_path / "station",
        source_version_id=registered.record.source_version_id,
        transformation_version="t1",
        configuration={},
    )
    observations = result.version_path / "observations.parquet"
    connection = duckdb.connect()
    try:
        connection.execute(
            f"COPY (SELECT * REPLACE ('WRONG'::VARCHAR AS station_no) "
            f"FROM read_parquet('{observations.as_posix()}', hive_partitioning=false)) "
            f"TO '{(result.version_path / 'tampered.parquet').as_posix()}' (FORMAT PARQUET)"
        )
    finally:
        connection.close()
    (result.version_path / "tampered.parquet").replace(observations)

    with pytest.raises(StationSnapshotError, match="fingerprint"):
        validate_station_version(
            result.version_path,
            manifest=tmp_path / "manifest.json",
            raw_root=tmp_path / "raw",
        )


def test_durable_validation_recomputes_nested_warning_semantics(tmp_path):
    registered = register_snapshot(tmp_path, [BASE, BASE.copy()])
    result = publish_station_snapshot(
        manifest=tmp_path / "manifest.json",
        raw_root=tmp_path / "raw",
        station_root=tmp_path / "station",
        source_version_id=registered.record.source_version_id,
        transformation_version="t1",
        configuration={},
    )
    observations = result.version_path / "observations.parquet"
    tampered = result.version_path / "tampered.parquet"
    connection = duckdb.connect()
    try:
        connection.execute(
            f"COPY (SELECT * REPLACE (0::INTEGER AS dq_warning_count) "
            f"FROM read_parquet('{observations.as_posix()}', hive_partitioning=false)) "
            f"TO '{tampered.as_posix()}' (FORMAT PARQUET, COMPRESSION ZSTD)"
        )
    finally:
        connection.close()
    tampered.replace(observations)
    metadata_path = result.version_path / "metadata.json"
    metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
    data = observations.read_bytes()
    metadata["observations_parquet"] = {
        "size_bytes": len(data),
        "sha256": hashlib.sha256(data).hexdigest(),
    }
    metadata_path.write_text(
        json.dumps(metadata, ensure_ascii=False, sort_keys=True, separators=(",", ":")) + "\n",
        encoding="utf-8",
    )

    with pytest.raises(StationSnapshotError, match="dq_warning_count"):
        validate_station_version(
            result.version_path,
            manifest=tmp_path / "manifest.json",
            raw_root=tmp_path / "raw",
        )


def test_durable_validation_rebinds_provenance_to_immutable_raw(tmp_path):
    registered = register_snapshot(tmp_path, [BASE])
    result = publish_station_snapshot(
        manifest=tmp_path / "manifest.json",
        raw_root=tmp_path / "raw",
        station_root=tmp_path / "station",
        source_version_id=registered.record.source_version_id,
        transformation_version="t1",
        configuration={},
    )
    observations = result.version_path / "observations.parquet"
    tampered = result.version_path / "tampered.parquet"
    fake_id = "rr1_" + "a" * 64
    connection = duckdb.connect()
    try:
        connection.execute(
            f"""COPY (
                    SELECT * REPLACE (
                        ['{fake_id}']::VARCHAR[] AS source_row_ids,
                        [999]::BIGINT[] AS source_row_numbers
                    )
                    FROM read_parquet('{observations.as_posix()}', hive_partitioning=false)
                ) TO '{tampered.as_posix()}' (FORMAT PARQUET, COMPRESSION ZSTD)"""
        )
    finally:
        connection.close()
    tampered.replace(observations)
    metadata_path = result.version_path / "metadata.json"
    metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
    data = observations.read_bytes()
    metadata["observations_parquet"] = {
        "size_bytes": len(data),
        "sha256": hashlib.sha256(data).hexdigest(),
    }
    metadata_path.write_text(
        json.dumps(metadata, ensure_ascii=False, sort_keys=True, separators=(",", ":")) + "\n",
        encoding="utf-8",
    )

    with pytest.raises(
        StationSnapshotError,
        match="does not exactly match recomputed immutable Raw source",
    ):
        validate_station_version(
            result.version_path,
            manifest=tmp_path / "manifest.json",
            raw_root=tmp_path / "raw",
        )


def test_durable_validation_binds_source_fingerprint_to_manifest(tmp_path):
    registered = register_snapshot(tmp_path, [BASE])
    result = publish_station_snapshot(
        manifest=tmp_path / "manifest.json",
        raw_root=tmp_path / "raw",
        station_root=tmp_path / "station",
        source_version_id=registered.record.source_version_id,
        transformation_version="t1",
        configuration={},
    )
    metadata_path = result.version_path / "metadata.json"
    metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
    metadata["source_artifact_sha256"] = "f" * 64
    metadata["source_artifact_size_bytes"] = 999
    metadata_path.write_text(
        json.dumps(metadata, ensure_ascii=False, sort_keys=True, separators=(",", ":")) + "\n",
        encoding="utf-8",
    )

    with pytest.raises(StationSnapshotError, match="source fingerprint"):
        validate_station_version(
            result.version_path,
            manifest=tmp_path / "manifest.json",
            raw_root=tmp_path / "raw",
        )
