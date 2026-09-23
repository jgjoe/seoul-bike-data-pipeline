"""Canonical normalization and row-level DQ contract (D-002)."""

from __future__ import annotations

import csv
import io
import zipfile
from decimal import Decimal
from pathlib import Path

import pytest

from seoul_bike_pipeline.canonical_dq import (
    RULE_BIRTH_YEAR_IMPOSSIBLE,
    RULE_BIRTH_YEAR_MISSING,
    RULE_BIKE_ID_MISSING,
    RULE_DISTANCE_EXTREME,
    RULE_DISTANCE_NEGATIVE,
    RULE_DISTANCE_ZERO,
    RULE_RENT_AT_INVALID,
    RULE_RENT_AT_MISSING,
    RULE_RENT_STATION_INVALID,
    RULE_RETURN_BEFORE_RENT,
    RULE_RETURN_AT_MISSING,
    RULE_RETURN_STATION_INVALID,
    RULE_SEX_MISSING,
    RULE_SEX_NONCANONICAL,
    RULE_USAGE_NEGATIVE,
    RULE_USAGE_OVER_1440,
    SEVERITY_QUARANTINE,
    SEVERITY_WARNING,
    canonicalize_csv_source_unit,
)
from seoul_bike_pipeline.csv_source import RENTAL_V16_HEADER
from seoul_bike_pipeline.errors import CanonicalizationError
from seoul_bike_pipeline.register import register_source
from seoul_bike_pipeline.zip_members import inventory_registered_zip

SOURCE_URL = "https://example.invalid/rental.csv"
LICENSE = "SYNTHETIC_FIXTURE"


def encode_csv(rows) -> bytes:
    buffer = io.StringIO(newline="")
    writer = csv.writer(buffer, lineterminator="\r\n")
    writer.writerow(RENTAL_V16_HEADER)
    writer.writerows(rows)
    return buffer.getvalue().encode("cp949")


def row(**overrides):
    values = {
        "bike_id": " SPB-1 ",
        "rent_at": "2026-01-01 00:00:00",
        "rent_station_no": " 01729 ",
        "rent_station_name": "대여소",
        "rent_dock_no": "0",
        "return_at": "2026-01-01 00:10:00",
        "return_station_no": "00002",
        "return_station_name": "반납소",
        "return_dock_no": "0",
        "usage_min": "10",
        "distance_m": "1234.50",
        "birth_year": "1990",
        "sex": "M",
        "user_type": "내국인",
        "rent_station_internal_id": "ST-1",
        "return_station_internal_id": "ST-2",
    }
    values.update(overrides)
    return tuple(values.values())


def register_direct(tmp_path: Path, rows, *, logical_period="2026-01") -> str:
    source = tmp_path / "source.csv"
    source.write_bytes(encode_csv(rows))
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


def canonical_rows(tmp_path: Path, rows):
    source_version_id = register_direct(tmp_path, rows)
    output = []
    summary = canonicalize_csv_source_unit(
        manifest=tmp_path / "source-manifest.json",
        raw_root=tmp_path / "raw",
        source_version_id=source_version_id,
        on_row=output.append,
    )
    return summary, output


def rule_ids(canonical_row):
    return {issue.rule_id for issue in canonical_row.dq_issues}


def test_canonicalizes_identity_station_and_clean_row(tmp_path):
    summary, rows = canonical_rows(tmp_path, [row()])

    actual = rows[0]
    assert actual.source_month == "2026-01"
    assert actual.bike_id == "SPB-1"
    assert actual.rent_station_no_raw == " 01729 "
    assert actual.rent_station_no == "1729"
    assert actual.return_station_no == "2"
    assert actual.sex_code == "M"
    assert actual.distance_m == Decimal("1234.50")
    assert actual.distance_metric_eligible is True
    assert actual.is_completed_trip is True
    assert actual.row_quarantined is False
    assert actual.dq_warning_count == 0
    assert actual.dq_issues == ()
    assert summary.quarantined_row_count == 0
    assert summary.warning_row_count == 0
    assert summary.completed_trip_count == 1
    assert summary.distance_metric_eligible_count == 1


def test_all_zero_station_number_normalizes_to_zero(tmp_path):
    _, rows = canonical_rows(
        tmp_path,
        [row(rent_station_no="000", return_station_no="0000")],
    )

    assert rows[0].rent_station_no == "0"
    assert rows[0].return_station_no == "0"


@pytest.mark.parametrize(
    ("raw_sex", "expected_code", "expected_rule"),
    [
        (" f ", "F", RULE_SEX_NONCANONICAL),
        ("x", None, RULE_SEX_NONCANONICAL),
        ("", None, RULE_SEX_MISSING),
    ],
)
def test_sex_code_is_m_f_or_null_with_warning(tmp_path, raw_sex, expected_code, expected_rule):
    _, rows = canonical_rows(tmp_path, [row(sex=raw_sex)])

    actual = rows[0]
    assert actual.sex_code == expected_code
    assert expected_rule in rule_ids(actual)
    assert actual.dq_warning_count == 1
    assert actual.row_quarantined is False


def test_invalid_rental_station_quarantines_without_internal_id_fallback(tmp_path):
    _, rows = canonical_rows(
        tmp_path,
        [row(rent_station_no="ST-999", return_station_no="return?", sex="f")],
    )

    actual = rows[0]
    assert actual.rent_station_no is None
    assert actual.return_station_no is None
    assert actual.rent_station_internal_id == "ST-1"
    assert actual.return_station_internal_id == "ST-2"
    assert actual.row_quarantined is True
    assert RULE_RENT_STATION_INVALID in rule_ids(actual)
    assert RULE_RETURN_STATION_INVALID in rule_ids(actual)
    assert RULE_SEX_NONCANONICAL in rule_ids(actual)
    assert actual.dq_warning_count == 2
    assert actual.is_completed_trip is False


def test_missing_rent_at_uses_single_source_month_and_quarantines(tmp_path):
    _, rows = canonical_rows(tmp_path, [row(rent_at="")])

    actual = rows[0]
    assert actual.source_month == "2026-01"
    assert actual.rent_at is None
    assert RULE_RENT_AT_MISSING in rule_ids(actual)
    assert actual.row_quarantined is True
    assert actual.is_completed_trip is False


def test_nonpositive_birth_year_warns_even_when_rent_at_is_unavailable(tmp_path):
    _, rows = canonical_rows(tmp_path, [row(rent_at="", birth_year="0")])

    actual = rows[0]
    assert RULE_RENT_AT_MISSING in rule_ids(actual)
    assert RULE_BIRTH_YEAR_IMPOSSIBLE in rule_ids(actual)
    assert actual.dq_warning_count == 1
    assert actual.row_quarantined is True


def test_invalid_rent_at_uses_single_source_month_and_conversion_quarantine(tmp_path):
    _, rows = canonical_rows(tmp_path, [row(rent_at="not-a-timestamp")])

    actual = rows[0]
    assert actual.source_month == "2026-01"
    assert RULE_RENT_AT_INVALID in rule_ids(actual)
    assert RULE_RENT_AT_MISSING not in rule_ids(actual)
    assert actual.row_quarantined is True


def test_scalar_conversion_failures_are_field_level_warnings_except_rent_at(tmp_path):
    _, rows = canonical_rows(
        tmp_path,
        [
            row(
                return_at="bad",
                rent_dock_no="dock?",
                return_dock_no="dock?",
                usage_min="many",
                distance_m="NaN",
                birth_year="19xx",
            )
        ],
    )

    actual = rows[0]
    warnings = [issue for issue in actual.dq_issues if issue.severity == SEVERITY_WARNING]
    assert {issue.field for issue in warnings} == {
        "return_at",
        "rent_dock_no",
        "return_dock_no",
        "usage_min",
        "distance_m",
        "birth_year",
    }
    assert actual.dq_warning_count == 6
    assert actual.row_quarantined is False
    assert actual.distance_metric_eligible is False
    assert actual.is_completed_trip is False


def test_business_dq_rules_preserve_warning_vs_quarantine_boundary(tmp_path):
    _, rows = canonical_rows(
        tmp_path,
        [
            row(usage_min="1441", distance_m="100001", birth_year="9999"),
            row(bike_id="", usage_min="-1", distance_m="-0.1"),
            row(distance_m="0"),
        ],
    )

    warning_only, quarantined, zero_distance = rows
    assert rule_ids(warning_only) >= {
        RULE_USAGE_OVER_1440,
        RULE_DISTANCE_EXTREME,
        RULE_BIRTH_YEAR_IMPOSSIBLE,
    }
    assert warning_only.row_quarantined is False
    assert warning_only.distance_metric_eligible is False

    assert rule_ids(quarantined) >= {
        RULE_BIKE_ID_MISSING,
        RULE_USAGE_NEGATIVE,
        RULE_DISTANCE_NEGATIVE,
    }
    assert quarantined.row_quarantined is True
    assert quarantined.distance_metric_eligible is False
    assert all(
        issue.severity == SEVERITY_QUARANTINE
        for issue in quarantined.dq_issues
        if issue.rule_id in {RULE_BIKE_ID_MISSING, RULE_USAGE_NEGATIVE, RULE_DISTANCE_NEGATIVE}
    )

    assert RULE_DISTANCE_ZERO in rule_ids(zero_distance)
    assert zero_distance.row_quarantined is False
    assert zero_distance.distance_metric_eligible is False


def test_usage_and_distance_threshold_boundaries_are_inclusive(tmp_path):
    _, rows = canonical_rows(
        tmp_path,
        [row(usage_min="1440", distance_m="100000")],
    )

    actual = rows[0]
    assert RULE_USAGE_OVER_1440 not in rule_ids(actual)
    assert RULE_DISTANCE_EXTREME not in rule_ids(actual)
    assert actual.distance_metric_eligible is True
    assert actual.row_quarantined is False


def test_return_before_rent_quarantines_and_is_not_completed(tmp_path):
    _, rows = canonical_rows(
        tmp_path,
        [row(return_at="2025-12-31 23:59:59")],
    )

    actual = rows[0]
    assert RULE_RETURN_BEFORE_RENT in rule_ids(actual)
    assert actual.row_quarantined is True
    assert actual.is_completed_trip is False


def test_missing_return_and_birth_are_warnings_without_quarantine(tmp_path):
    _, rows = canonical_rows(
        tmp_path,
        [row(return_at="", return_station_no="", birth_year="")],
    )

    actual = rows[0]
    assert rule_ids(actual) >= {
        RULE_RETURN_AT_MISSING,
        RULE_RETURN_STATION_INVALID,
        RULE_BIRTH_YEAR_MISSING,
    }
    assert actual.dq_warning_count == 3
    assert actual.row_quarantined is False
    assert actual.is_completed_trip is False


def test_source_month_mismatch_hard_fails_before_exposing_any_row(tmp_path):
    source_version_id = register_direct(
        tmp_path,
        [row(), row(rent_at="2026-02-01 00:00:00")],
    )
    output = []

    with pytest.raises(CanonicalizationError, match="source/rent-month validation failed"):
        canonicalize_csv_source_unit(
            manifest=tmp_path / "source-manifest.json",
            raw_root=tmp_path / "raw",
            source_version_id=source_version_id,
            on_row=output.append,
        )

    assert output == []


def test_multi_month_unavailable_rent_at_hard_fails_before_exposing_any_row(tmp_path):
    member_name = "공공자전거 대여이력 정보_2020.07~08.csv"
    source = tmp_path / "year.zip"
    with zipfile.ZipFile(source, "w") as archive:
        archive.writestr(
            member_name,
            encode_csv(
                [
                    row(
                        rent_at="2020-07-01 00:00:00",
                        return_at="2020-07-01 00:10:00",
                    ),
                    row(rent_at="", return_at="2020-08-01 00:10:00"),
                ]
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
    output = []

    with pytest.raises(CanonicalizationError, match="multi-month source unit"):
        canonicalize_csv_source_unit(
            manifest=tmp_path / "source-manifest.json",
            raw_root=tmp_path / "raw",
            source_version_id=registered.record.source_version_id,
            member_name=member_name,
            on_row=output.append,
        )

    assert output == []


def test_multi_month_member_assigns_each_row_to_its_rental_month(tmp_path):
    member_name = "공공자전거 대여이력 정보_2020.07~08.csv"
    source = tmp_path / "year.zip"
    with zipfile.ZipFile(source, "w") as archive:
        archive.writestr(
            member_name,
            encode_csv(
                [
                    row(
                        rent_at="2020-07-31 23:59:00",
                        return_at="2020-08-01 00:05:00",
                    ),
                    row(
                        rent_at="2020-08-01 01:00:00",
                        return_at="2020-08-01 01:05:00",
                    ),
                ]
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
    output = []

    summary = canonicalize_csv_source_unit(
        manifest=tmp_path / "source-manifest.json",
        raw_root=tmp_path / "raw",
        source_version_id=registered.record.source_version_id,
        member_name=member_name,
        on_row=output.append,
    )

    assert summary.expected_source_months == ("2020-07", "2020-08")
    assert [actual.source_month for actual in output] == ["2020-07", "2020-08"]


def test_summary_counts_rows_not_final_trusted_eligibility(tmp_path):
    summary, rows = canonical_rows(
        tmp_path,
        [
            row(),
            row(sex="f"),
            row(rent_station_no="bad"),
        ],
    )

    assert len(rows) == 3
    assert summary.row_count == 3
    assert summary.quarantined_row_count == 1
    assert summary.warning_row_count == 1
    assert summary.warning_issue_count == 1
    assert summary.completed_trip_count == 3
    assert summary.distance_metric_eligible_count == 3
