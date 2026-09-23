"""Streaming rental CSV ingestion and physical provenance contract."""

from __future__ import annotations

import csv
import hashlib
import io
import json
import zipfile
from pathlib import Path

import pytest

from seoul_bike_pipeline.csv_source import (
    RENTAL_V16_HEADER,
    RENTAL_V17_HEADER,
    compute_raw_row_id,
    compute_row_content_sha256,
    read_csv_source_unit,
)
from seoul_bike_pipeline.errors import CsvIngestionError
from seoul_bike_pipeline.raw import raw_artifact_path
from seoul_bike_pipeline.register import register_source
from seoul_bike_pipeline.zip_members import inventory_registered_zip

SOURCE_URL = "https://example.invalid/rental.csv"
LICENSE = "SYNTHETIC_FIXTURE"

V16_ROW_1 = (
    "SPB-65030",
    "2026-01-01 00:00:13",
    "01729",
    "도봉한신아파트 버스정류장",
    "0",
    "2026-01-01 00:02:26",
    "01729",
    "도봉한신아파트 버스정류장",
    "0",
    "2",
    "0.00",
    "",
    "M",
    "내국인",
    "ST-1461",
    "ST-1461",
)
V16_ROW_2 = (
    "SPB-00001",
    "2026-01-01 00:10:00",
    "00100",
    "쉼표, 포함 대여소",
    "1",
    "2026-01-01 00:20:00",
    "00200",
    "반납 대여소",
    "2",
    "10",
    "1234.50",
    "1990",
    "F",
    "내국인",
    "ST-1",
    "ST-2",
)
V17_ROW = V16_ROW_1 + ("LCD자전거",)


def csv_bytes(header, rows, *, encoding: str, bom: bool = False) -> bytes:
    buffer = io.StringIO(newline="")
    writer = csv.writer(buffer, lineterminator="\r\n")
    writer.writerow(header)
    writer.writerows(rows)
    text = buffer.getvalue()
    if encoding == "utf-8" and bom:
        return b"\xef\xbb\xbf" + text.encode("utf-8")
    return text.encode(encoding)


def register_direct(tmp_path: Path, payload: bytes, *, published_filename="서울특별시 공공자전거 대여이력 정보_2601.csv"):
    source = tmp_path / "download.csv"
    source.write_bytes(payload)
    result = register_source(
        artifact=source,
        manifest=tmp_path / "source-manifest.json",
        raw_root=tmp_path / "raw",
        dataset_id="rental_history",
        logical_period="2026-01",
        published_filename=published_filename,
        source_url=SOURCE_URL,
        license=LICENSE,
    )
    return result.record.source_version_id


def test_cp949_v16_stream_preserves_korean_and_generates_physical_provenance(tmp_path):
    payload = csv_bytes(RENTAL_V16_HEADER, [V16_ROW_1, V16_ROW_2], encoding="cp949")
    source_version_id = register_direct(tmp_path, payload)
    rows = []

    summary = read_csv_source_unit(
        manifest=tmp_path / "source-manifest.json",
        raw_root=tmp_path / "raw",
        source_version_id=source_version_id,
        on_row=rows.append,
    )

    assert summary.encoding == "cp949"
    assert summary.schema_version == "rental_v16"
    assert summary.row_count == 2
    assert summary.member_name == "서울특별시 공공자전거 대여이력 정보_2601.csv"
    assert rows[0].values == V16_ROW_1
    assert rows[1].values[3] == "쉼표, 포함 대여소"
    assert [row.source_row_number for row in rows] == [1, 2]
    assert len({row.raw_row_id for row in rows}) == 2
    assert rows[0].raw_row_id == compute_raw_row_id(
        source_version_id, summary.member_name, 1
    )
    assert rows[0].row_content_sha256 == compute_row_content_sha256(V16_ROW_1)


def test_cp949_v17_dispatches_same_core_header_plus_bike_type(tmp_path):
    payload = csv_bytes(RENTAL_V17_HEADER, [V17_ROW], encoding="cp949")
    source_version_id = register_direct(
        tmp_path,
        payload,
        published_filename="서울특별시 공공자전거 대여이력 정보_202001.csv",
    )
    rows = []

    summary = read_csv_source_unit(
        manifest=tmp_path / "source-manifest.json",
        raw_root=tmp_path / "raw",
        source_version_id=source_version_id,
        on_row=rows.append,
    )

    assert summary.schema_version == "rental_v17"
    assert summary.encoding == "cp949"
    assert rows[0].values[-1] == "LCD자전거"


def test_utf8_bom_known_header_is_supported_deterministically(tmp_path):
    payload = csv_bytes(RENTAL_V16_HEADER, [V16_ROW_1], encoding="utf-8", bom=True)
    source_version_id = register_direct(tmp_path, payload)

    summary = read_csv_source_unit(
        manifest=tmp_path / "source-manifest.json",
        raw_root=tmp_path / "raw",
        source_version_id=source_version_id,
    )

    assert summary.encoding == "utf-8-sig"
    assert summary.schema_version == "rental_v16"


def test_cr_only_csv_is_detected_and_parsed(tmp_path):
    buffer = io.StringIO(newline="")
    writer = csv.writer(buffer, lineterminator="\r")
    writer.writerow(RENTAL_V16_HEADER)
    writer.writerow(V16_ROW_1)
    payload = buffer.getvalue().encode("cp949")
    source_version_id = register_direct(tmp_path, payload)
    rows = []

    summary = read_csv_source_unit(
        manifest=tmp_path / "source-manifest.json",
        raw_root=tmp_path / "raw",
        source_version_id=source_version_id,
        on_row=rows.append,
    )

    assert summary.encoding == "cp949"
    assert summary.schema_version == "rental_v16"
    assert summary.row_count == 1
    assert rows[0].values == V16_ROW_1


def test_repeat_scan_produces_same_row_ids_and_content_hashes(tmp_path):
    payload = csv_bytes(RENTAL_V16_HEADER, [V16_ROW_1, V16_ROW_2], encoding="cp949")
    source_version_id = register_direct(tmp_path, payload)
    first = []
    second = []

    read_csv_source_unit(
        manifest=tmp_path / "source-manifest.json",
        raw_root=tmp_path / "raw",
        source_version_id=source_version_id,
        on_row=first.append,
    )
    read_csv_source_unit(
        manifest=tmp_path / "source-manifest.json",
        raw_root=tmp_path / "raw",
        source_version_id=source_version_id,
        on_row=second.append,
    )

    assert [(r.raw_row_id, r.row_content_sha256) for r in first] == [
        (r.raw_row_id, r.row_content_sha256) for r in second
    ]


def test_multiline_quoted_field_counts_one_logical_source_row(tmp_path):
    multiline = list(V16_ROW_1)
    multiline[3] = "첫째 줄\n둘째 줄 대여소"
    payload = csv_bytes(
        RENTAL_V16_HEADER,
        [tuple(multiline), V16_ROW_2],
        encoding="cp949",
    )
    source_version_id = register_direct(tmp_path, payload)
    rows = []

    summary = read_csv_source_unit(
        manifest=tmp_path / "source-manifest.json",
        raw_root=tmp_path / "raw",
        source_version_id=source_version_id,
        on_row=rows.append,
    )

    assert summary.row_count == 2
    assert [row.source_row_number for row in rows] == [1, 2]
    assert rows[0].values[3] == "첫째 줄\n둘째 줄 대여소"


def test_row_sink_oserror_is_not_misreported_as_source_read_failure(tmp_path):
    payload = csv_bytes(RENTAL_V16_HEADER, [V16_ROW_1], encoding="cp949")
    source_version_id = register_direct(tmp_path, payload)

    def fail_sink(row):
        raise OSError("sink failed")

    with pytest.raises(OSError, match="sink failed"):
        read_csv_source_unit(
            manifest=tmp_path / "source-manifest.json",
            raw_root=tmp_path / "raw",
            source_version_id=source_version_id,
            on_row=fail_sink,
        )


def test_unknown_header_hard_fails(tmp_path):
    header = RENTAL_V16_HEADER[:-1] + ("새로운컬럼",)
    payload = csv_bytes(header, [V16_ROW_1], encoding="cp949")
    source_version_id = register_direct(tmp_path, payload)

    with pytest.raises(CsvIngestionError, match="supported UTF-8/CP949 rental header"):
        read_csv_source_unit(
            manifest=tmp_path / "source-manifest.json",
            raw_root=tmp_path / "raw",
            source_version_id=source_version_id,
        )


def test_invalid_header_encoding_hard_fails(tmp_path):
    source_version_id = register_direct(tmp_path, b"\x81\x30\x81\x30\n1,2\n")

    with pytest.raises(CsvIngestionError, match="supported UTF-8/CP949 rental header"):
        read_csv_source_unit(
            manifest=tmp_path / "source-manifest.json",
            raw_root=tmp_path / "raw",
            source_version_id=source_version_id,
        )


def test_header_only_csv_hard_fails_zero_data_rows(tmp_path):
    payload = csv_bytes(RENTAL_V16_HEADER, [], encoding="cp949")
    source_version_id = register_direct(tmp_path, payload)

    with pytest.raises(CsvIngestionError, match="zero data rows"):
        read_csv_source_unit(
            manifest=tmp_path / "source-manifest.json",
            raw_root=tmp_path / "raw",
            source_version_id=source_version_id,
        )


def test_wrong_row_width_hard_fails(tmp_path):
    payload = csv_bytes(RENTAL_V16_HEADER, [V16_ROW_1[:-1]], encoding="cp949")
    source_version_id = register_direct(tmp_path, payload)

    with pytest.raises(CsvIngestionError, match="15 columns; expected 16"):
        read_csv_source_unit(
            manifest=tmp_path / "source-manifest.json",
            raw_root=tmp_path / "raw",
            source_version_id=source_version_id,
        )


def test_structurally_malformed_quoted_record_hard_fails(tmp_path):
    header = csv_bytes(RENTAL_V16_HEADER, [], encoding="cp949")
    payload = header + b'"unterminated,record\r\n'
    source_version_id = register_direct(tmp_path, payload)

    with pytest.raises(CsvIngestionError, match="structural CSV parse failure"):
        read_csv_source_unit(
            manifest=tmp_path / "source-manifest.json",
            raw_root=tmp_path / "raw",
            source_version_id=source_version_id,
        )


def test_direct_csv_raw_corruption_hard_fails_before_parse(tmp_path):
    payload = csv_bytes(RENTAL_V16_HEADER, [V16_ROW_1], encoding="cp949")
    source_version_id = register_direct(tmp_path, payload)
    raw = raw_artifact_path(tmp_path / "raw", source_version_id)
    raw.write_bytes(b"corrupted")

    with pytest.raises(CsvIngestionError, match="failed verification"):
        read_csv_source_unit(
            manifest=tmp_path / "source-manifest.json",
            raw_root=tmp_path / "raw",
            source_version_id=source_version_id,
        )


def test_zip_member_is_verified_then_streamed_with_exact_member_name(tmp_path):
    member_name = "2020/서울특별시 공공자전거 대여이력 정보_202001.csv"
    payload = csv_bytes(RENTAL_V17_HEADER, [V17_ROW], encoding="cp949")
    source = tmp_path / "year.zip"
    with zipfile.ZipFile(source, "w") as archive:
        archive.writestr(member_name, payload)
    registered = register_source(
        artifact=source,
        manifest=tmp_path / "source-manifest.json",
        raw_root=tmp_path / "raw",
        dataset_id="rental_history",
        logical_period="2020",
        published_filename="서울특별시 공공자전거 대여이력 정보_2020.zip",
        source_url=SOURCE_URL,
        license=LICENSE,
    )
    inventory_registered_zip(
        manifest=tmp_path / "source-manifest.json",
        raw_root=tmp_path / "raw",
        source_version_id=registered.record.source_version_id,
    )
    rows = []

    summary = read_csv_source_unit(
        manifest=tmp_path / "source-manifest.json",
        raw_root=tmp_path / "raw",
        source_version_id=registered.record.source_version_id,
        member_name=member_name,
        on_row=rows.append,
    )

    assert summary.schema_version == "rental_v17"
    assert summary.member_name == member_name
    assert rows[0].member_name == member_name
    assert rows[0].raw_row_id == compute_raw_row_id(
        registered.record.source_version_id, member_name, 1
    )


def test_zip_source_requires_explicit_member_name(tmp_path):
    source = tmp_path / "year.zip"
    with zipfile.ZipFile(source, "w") as archive:
        archive.writestr(
            "rental_202001.csv",
            csv_bytes(RENTAL_V17_HEADER, [V17_ROW], encoding="cp949"),
        )
    registered = register_source(
        artifact=source,
        manifest=tmp_path / "source-manifest.json",
        raw_root=tmp_path / "raw",
        dataset_id="rental_history",
        logical_period="2020",
        published_filename="year.zip",
        source_url=SOURCE_URL,
        license=LICENSE,
    )

    with pytest.raises(CsvIngestionError, match="--member-name is required"):
        read_csv_source_unit(
            manifest=tmp_path / "source-manifest.json",
            raw_root=tmp_path / "raw",
            source_version_id=registered.record.source_version_id,
        )


def test_unregistered_zip_member_hard_fails(tmp_path):
    member_name = "rental_202001.csv"
    source = tmp_path / "year.zip"
    with zipfile.ZipFile(source, "w") as archive:
        archive.writestr(
            member_name,
            csv_bytes(RENTAL_V17_HEADER, [V17_ROW], encoding="cp949"),
        )
    registered = register_source(
        artifact=source,
        manifest=tmp_path / "source-manifest.json",
        raw_root=tmp_path / "raw",
        dataset_id="rental_history",
        logical_period="2020",
        published_filename="year.zip",
        source_url=SOURCE_URL,
        license=LICENSE,
    )
    inventory_registered_zip(
        manifest=tmp_path / "source-manifest.json",
        raw_root=tmp_path / "raw",
        source_version_id=registered.record.source_version_id,
    )

    with pytest.raises(CsvIngestionError, match="is not registered in inventory"):
        read_csv_source_unit(
            manifest=tmp_path / "source-manifest.json",
            raw_root=tmp_path / "raw",
            source_version_id=registered.record.source_version_id,
            member_name="missing.csv",
        )


def test_zip_member_corruption_against_inventory_hard_fails(tmp_path):
    member_name = "rental_202001.csv"
    original = csv_bytes(RENTAL_V17_HEADER, [V17_ROW], encoding="cp949")
    source = tmp_path / "year.zip"
    with zipfile.ZipFile(source, "w") as archive:
        archive.writestr(member_name, original)
    registered = register_source(
        artifact=source,
        manifest=tmp_path / "source-manifest.json",
        raw_root=tmp_path / "raw",
        dataset_id="rental_history",
        logical_period="2020",
        published_filename="year.zip",
        source_url=SOURCE_URL,
        license=LICENSE,
    )
    inventory_registered_zip(
        manifest=tmp_path / "source-manifest.json",
        raw_root=tmp_path / "raw",
        source_version_id=registered.record.source_version_id,
    )
    raw = raw_artifact_path(tmp_path / "raw", registered.record.source_version_id)
    with zipfile.ZipFile(raw, "w") as archive:
        archive.writestr(member_name, original + b"changed")

    with pytest.raises(CsvIngestionError, match="parent Raw artifact.*failed verification"):
        read_csv_source_unit(
            manifest=tmp_path / "source-manifest.json",
            raw_root=tmp_path / "raw",
            source_version_id=registered.record.source_version_id,
            member_name=member_name,
        )


def test_zip_parent_corruption_in_unrelated_member_hard_fails(tmp_path):
    target_name = "rental_202001.csv"
    target = csv_bytes(RENTAL_V17_HEADER, [V17_ROW], encoding="cp949")
    source = tmp_path / "year.zip"
    with zipfile.ZipFile(source, "w") as archive:
        archive.writestr(target_name, target)
        archive.writestr("README.txt", b"original")
    registered = register_source(
        artifact=source,
        manifest=tmp_path / "source-manifest.json",
        raw_root=tmp_path / "raw",
        dataset_id="rental_history",
        logical_period="2020",
        published_filename="year.zip",
        source_url=SOURCE_URL,
        license=LICENSE,
    )
    inventory_registered_zip(
        manifest=tmp_path / "source-manifest.json",
        raw_root=tmp_path / "raw",
        source_version_id=registered.record.source_version_id,
    )
    raw = raw_artifact_path(tmp_path / "raw", registered.record.source_version_id)
    with zipfile.ZipFile(raw, "w") as archive:
        archive.writestr(target_name, target)
        archive.writestr("README.txt", b"changed but target member identical")

    with pytest.raises(CsvIngestionError, match="parent Raw artifact.*failed verification"):
        read_csv_source_unit(
            manifest=tmp_path / "source-manifest.json",
            raw_root=tmp_path / "raw",
            source_version_id=registered.record.source_version_id,
            member_name=target_name,
        )


def test_content_hash_is_encoding_independent_for_same_decoded_values():
    expected = hashlib.sha256(
        json.dumps(list(V16_ROW_1), ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    ).hexdigest()
    assert compute_row_content_sha256(V16_ROW_1) == expected
