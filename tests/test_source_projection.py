"""Typed source projection and source/rent-month validation."""

from __future__ import annotations

import csv
import io
import zipfile
from datetime import datetime
from decimal import Decimal
from pathlib import Path

import pytest

from seoul_bike_pipeline.csv_source import RENTAL_V16_HEADER, RENTAL_V17_HEADER
from seoul_bike_pipeline.errors import SourceProjectionError
from seoul_bike_pipeline.register import register_source
from seoul_bike_pipeline.source_projection import (
    MONTH_MATCH,
    MONTH_MISMATCH,
    MONTH_UNAVAILABLE,
    project_csv_source_unit,
)
from seoul_bike_pipeline.zip_members import inventory_registered_zip

SOURCE_URL = "https://example.invalid/rental.csv"
LICENSE = "SYNTHETIC_FIXTURE"


def encode_csv(header, rows, *, encoding="cp949") -> bytes:
    buffer = io.StringIO(newline="")
    writer = csv.writer(buffer, lineterminator="\r\n")
    writer.writerow(header)
    writer.writerows(rows)
    return buffer.getvalue().encode(encoding)


def v16_row(*, rent_at="2026-01-01 00:00:13", return_at="2026-01-01 00:02:26"):
    return (
        "SPB-65030",
        rent_at,
        "01729",
        "도봉한신아파트 버스정류장",
        "0",
        return_at,
        "01729",
        "도봉한신아파트 버스정류장",
        "0",
        "2",
        "1234.50",
        "",
        "M",
        "내국인",
        "ST-1461",
        "ST-1461",
    )


def register_direct(tmp_path: Path, rows, *, logical_period="2026-01") -> str:
    source = tmp_path / "source.csv"
    source.write_bytes(encode_csv(RENTAL_V16_HEADER, rows))
    result = register_source(
        artifact=source,
        manifest=tmp_path / "source-manifest.json",
        raw_root=tmp_path / "raw",
        dataset_id="rental_history",
        logical_period=logical_period,
        published_filename="서울특별시 공공자전거 대여이력 정보_2601.csv",
        source_url=SOURCE_URL,
        license=LICENSE,
    )
    return result.record.source_version_id


def test_v16_projection_types_nullable_fields_and_month_match(tmp_path):
    source_version_id = register_direct(tmp_path, [v16_row()])
    rows = []

    summary = project_csv_source_unit(
        manifest=tmp_path / "source-manifest.json",
        raw_root=tmp_path / "raw",
        source_version_id=source_version_id,
        on_row=rows.append,
    )

    row = rows[0]
    assert summary.expected_source_months == ("2026-01",)
    assert summary.month_match_count == 1
    assert summary.month_mismatch_count == summary.month_unavailable_count == 0
    assert summary.conversion_issue_count == 0
    assert row.source_month_validation == MONTH_MATCH
    assert row.validated_source_month == "2026-01"
    assert row.rent_at == datetime(2026, 1, 1, 0, 0, 13)
    assert row.return_at == datetime(2026, 1, 1, 0, 2, 26)
    assert row.rent_station_no_raw == "01729"
    assert row.rent_dock_no == 0
    assert row.usage_min == 2
    assert row.distance_m == Decimal("1234.50")
    assert row.birth_year is None
    assert row.sex_raw == "M"
    assert row.bike_type is None


def test_v17_projection_exposes_bike_type_without_categorical_normalization(tmp_path):
    source = tmp_path / "source.csv"
    source.write_bytes(encode_csv(RENTAL_V17_HEADER, [v16_row() + ("LCD자전거",)]))
    result = register_source(
        artifact=source,
        manifest=tmp_path / "source-manifest.json",
        raw_root=tmp_path / "raw",
        dataset_id="rental_history",
        logical_period="2020-01",
        published_filename="rental_202001.csv",
        source_url=SOURCE_URL,
        license=LICENSE,
    )
    rows = []

    project_csv_source_unit(
        manifest=tmp_path / "source-manifest.json",
        raw_root=tmp_path / "raw",
        source_version_id=result.record.source_version_id,
        on_row=rows.append,
    )

    assert rows[0].bike_type == "LCD자전거"
    assert rows[0].sex_raw == "M"


def test_return_month_may_cross_boundary_without_source_month_mismatch(tmp_path):
    source_version_id = register_direct(
        tmp_path,
        [v16_row(rent_at="2026-01-31 23:59:00", return_at="2026-02-01 00:05:00")],
    )
    rows = []

    summary = project_csv_source_unit(
        manifest=tmp_path / "source-manifest.json",
        raw_root=tmp_path / "raw",
        source_version_id=source_version_id,
        on_row=rows.append,
    )

    assert summary.month_match_count == 1
    assert rows[0].source_month_validation == MONTH_MATCH
    assert rows[0].return_at == datetime(2026, 2, 1, 0, 5, 0)


def test_rent_month_outside_standalone_source_month_is_reported_not_dropped(tmp_path):
    source_version_id = register_direct(
        tmp_path, [v16_row(rent_at="2026-02-01 00:00:00")]
    )
    rows = []

    summary = project_csv_source_unit(
        manifest=tmp_path / "source-manifest.json",
        raw_root=tmp_path / "raw",
        source_version_id=source_version_id,
        on_row=rows.append,
    )

    assert summary.row_count == 1
    assert summary.month_mismatch_count == 1
    assert rows[0].rent_month == "2026-02"
    assert rows[0].validated_source_month is None
    assert rows[0].source_month_validation == MONTH_MISMATCH


def test_missing_rent_at_is_month_unavailable_for_later_dq(tmp_path):
    source_version_id = register_direct(tmp_path, [v16_row(rent_at="")])
    rows = []

    summary = project_csv_source_unit(
        manifest=tmp_path / "source-manifest.json",
        raw_root=tmp_path / "raw",
        source_version_id=source_version_id,
        on_row=rows.append,
    )

    assert summary.month_unavailable_count == 1
    assert summary.conversion_issue_count == 0
    assert rows[0].rent_at is None
    assert rows[0].source_month_validation == MONTH_UNAVAILABLE


def test_invalid_nonblank_scalars_become_issues_without_dropping_row(tmp_path):
    row = list(v16_row())
    row[1] = "not-a-timestamp"
    row[5] = "also-bad"
    row[4] = "dock?"
    row[9] = "two"
    row[10] = "NaN"
    row[11] = "19xx"
    source_version_id = register_direct(tmp_path, [tuple(row)])
    projected = []

    summary = project_csv_source_unit(
        manifest=tmp_path / "source-manifest.json",
        raw_root=tmp_path / "raw",
        source_version_id=source_version_id,
        on_row=projected.append,
    )

    assert summary.row_count == 1
    assert summary.rows_with_conversion_issues == 1
    assert summary.conversion_issue_count == 6
    assert summary.month_unavailable_count == 1
    typed = projected[0]
    assert typed.rent_at is None
    assert typed.return_at is None
    assert typed.rent_dock_no is None
    assert typed.usage_min is None
    assert typed.distance_m is None
    assert typed.birth_year is None
    assert {issue.field for issue in typed.conversion_issues} == {
        "rent_at",
        "return_at",
        "rent_dock_no",
        "usage_min",
        "distance_m",
        "birth_year",
    }


def test_blank_nullable_text_normalizes_to_none_but_nonblank_text_is_not_trimmed(tmp_path):
    row = list(v16_row())
    row[6] = "   "
    row[12] = " m "
    row[13] = " "
    source_version_id = register_direct(tmp_path, [tuple(row)])
    projected = []

    project_csv_source_unit(
        manifest=tmp_path / "source-manifest.json",
        raw_root=tmp_path / "raw",
        source_version_id=source_version_id,
        on_row=projected.append,
    )

    typed = projected[0]
    assert typed.return_station_no_raw is None
    assert typed.user_type is None
    assert typed.sex_raw == " m "


def test_legacy_backslash_n_null_marker_normalizes_without_conversion_issue(tmp_path):
    row = list(v16_row())
    row[4] = r"\N"
    row[8] = r"\N"
    row[11] = r"\N"
    row[12] = r"\N"
    source_version_id = register_direct(tmp_path, [tuple(row)])
    projected = []

    summary = project_csv_source_unit(
        manifest=tmp_path / "source-manifest.json",
        raw_root=tmp_path / "raw",
        source_version_id=source_version_id,
        on_row=projected.append,
    )

    typed = projected[0]
    assert summary.conversion_issue_count == 0
    assert typed.rent_dock_no is None
    assert typed.return_dock_no is None
    assert typed.birth_year is None
    assert typed.sex_raw is None


def test_zip_range_member_accepts_each_rental_month_in_range(tmp_path):
    member_name = "공공자전거 대여이력 정보_2020.07~08.csv"
    rows = [
        v16_row(rent_at="2020-07-31 23:00:00", return_at="2020-08-01 00:05:00"),
        v16_row(rent_at="2020-08-01 01:00:00", return_at="2020-08-01 01:05:00"),
    ]
    source = tmp_path / "year.zip"
    with zipfile.ZipFile(source, "w") as archive:
        archive.writestr(member_name, encode_csv(RENTAL_V16_HEADER, rows))
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
    projected = []

    summary = project_csv_source_unit(
        manifest=tmp_path / "source-manifest.json",
        raw_root=tmp_path / "raw",
        source_version_id=registered.record.source_version_id,
        member_name=member_name,
        on_row=projected.append,
    )

    assert summary.expected_source_months == ("2020-07", "2020-08")
    assert summary.month_match_count == 2
    assert [row.validated_source_month for row in projected] == ["2020-07", "2020-08"]


def test_zip_range_member_reports_out_of_range_rental_month(tmp_path):
    member_name = "공공자전거 대여이력 정보_2020.07~08.csv"
    source = tmp_path / "year.zip"
    with zipfile.ZipFile(source, "w") as archive:
        archive.writestr(
            member_name,
            encode_csv(
                RENTAL_V16_HEADER,
                [v16_row(rent_at="2020-09-01 00:00:00", return_at="2020-09-01 00:02:00")],
            ),
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

    summary = project_csv_source_unit(
        manifest=tmp_path / "source-manifest.json",
        raw_root=tmp_path / "raw",
        source_version_id=registered.record.source_version_id,
        member_name=member_name,
    )

    assert summary.month_mismatch_count == 1


def test_standalone_csv_requires_monthly_logical_period(tmp_path):
    source_version_id = register_direct(tmp_path, [v16_row()], logical_period="2026")

    with pytest.raises(SourceProjectionError, match="requires YYYY-MM"):
        project_csv_source_unit(
            manifest=tmp_path / "source-manifest.json",
            raw_root=tmp_path / "raw",
            source_version_id=source_version_id,
        )


def test_standalone_source_month_uses_manifest_logical_period_not_filename_inference(tmp_path):
    source = tmp_path / "source.csv"
    source.write_bytes(encode_csv(RENTAL_V16_HEADER, [v16_row()]))
    result = register_source(
        artifact=source,
        manifest=tmp_path / "source-manifest.json",
        raw_root=tmp_path / "raw",
        dataset_id="rental_history",
        logical_period="2026-01",
        published_filename="서울특별시 공공자전거 대여이력 정보_2602.csv",
        source_url=SOURCE_URL,
        license=LICENSE,
    )
    projected = []

    summary = project_csv_source_unit(
        manifest=tmp_path / "source-manifest.json",
        raw_root=tmp_path / "raw",
        source_version_id=result.record.source_version_id,
        on_row=projected.append,
    )

    assert summary.expected_source_months == ("2026-01",)
    assert summary.month_match_count == 1
    assert projected[0].validated_source_month == "2026-01"


def test_2015_compatibility_short_year_member_projects_as_december(tmp_path):
    member_name = "서울특별시 공공자전거 대여이력 정보_1512.csv"
    row = v16_row(rent_at="2015-12-01 00:00:00", return_at="2015-12-01 00:02:00") + (
        "LCD자전거",
    )
    source = tmp_path / "year.zip"
    with zipfile.ZipFile(source, "w") as archive:
        archive.writestr(member_name, encode_csv(RENTAL_V17_HEADER, [row]))
    registered = register_source(
        artifact=source,
        manifest=tmp_path / "source-manifest.json",
        raw_root=tmp_path / "raw",
        dataset_id="rental_history",
        logical_period="2015",
        published_filename="서울특별시 공공자전거 대여이력 정보_2015.zip",
        source_url=SOURCE_URL,
        license=LICENSE,
    )
    inventory_registered_zip(
        manifest=tmp_path / "source-manifest.json",
        raw_root=tmp_path / "raw",
        source_version_id=registered.record.source_version_id,
    )

    summary = project_csv_source_unit(
        manifest=tmp_path / "source-manifest.json",
        raw_root=tmp_path / "raw",
        source_version_id=registered.record.source_version_id,
        member_name=member_name,
    )

    assert summary.expected_source_months == ("2015-12",)
    assert summary.month_match_count == 1


def test_business_anomaly_values_still_parse_for_later_dq_classification(tmp_path):
    row = list(v16_row())
    row[9] = "-5"
    row[10] = "100001.25"
    row[11] = "9999"
    row[12] = "f"
    source_version_id = register_direct(tmp_path, [tuple(row)])
    projected = []

    summary = project_csv_source_unit(
        manifest=tmp_path / "source-manifest.json",
        raw_root=tmp_path / "raw",
        source_version_id=source_version_id,
        on_row=projected.append,
    )

    typed = projected[0]
    assert summary.conversion_issue_count == 0
    assert typed.usage_min == -5
    assert typed.distance_m == Decimal("100001.25")
    assert typed.birth_year == 9999
    assert typed.sex_raw == "f"


def test_row_sink_exception_propagates(tmp_path):
    source_version_id = register_direct(tmp_path, [v16_row()])

    def fail(row):
        raise RuntimeError("sink failed")

    with pytest.raises(RuntimeError, match="sink failed"):
        project_csv_source_unit(
            manifest=tmp_path / "source-manifest.json",
            raw_root=tmp_path / "raw",
            source_version_id=source_version_id,
            on_row=fail,
        )
