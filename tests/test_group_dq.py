"""Month-level logical-trip grouping and trusted eligibility contract (D-003)."""

from __future__ import annotations

import csv
import hashlib
import io
import json
import zipfile
from datetime import datetime
from pathlib import Path

import pytest

from seoul_bike_pipeline.csv_source import RENTAL_V16_HEADER
from seoul_bike_pipeline.errors import CanonicalizationError, GroupDqError
from seoul_bike_pipeline.group_dq import (
    RULE_EXACT_DUPLICATE_GROUP,
    RULE_LOGICAL_TRIP_CONFLICT,
    classify_registered_source_month,
    compute_logical_trip_key,
)
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
        "bike_id": "SPB-1",
        "rent_at": "2026-01-01 00:00:00",
        "rent_station_no": "00001",
        "rent_station_name": "대여소",
        "rent_dock_no": "0",
        "return_at": "2026-01-01 00:10:00",
        "return_station_no": "00002",
        "return_station_name": "반납소",
        "return_dock_no": "0",
        "usage_min": "10",
        "distance_m": "100.00",
        "birth_year": "1990",
        "sex": "M",
        "user_type": "내국인",
        "rent_station_internal_id": "ST-1",
        "return_station_internal_id": "ST-2",
    }
    values.update(overrides)
    return tuple(values.values())


def register_direct(
    tmp_path: Path,
    rows,
    *,
    logical_period="2026-01",
    filename="서울특별시 공공자전거 대여이력 정보_2601.csv",
    source_name="source.csv",
):
    source = tmp_path / source_name
    source.write_bytes(encode_csv(rows))
    return register_source(
        artifact=source,
        manifest=tmp_path / "source-manifest.json",
        raw_root=tmp_path / "raw",
        dataset_id="rental_history",
        logical_period=logical_period,
        published_filename=filename,
        source_url=SOURCE_URL,
        license=LICENSE,
    )


def classify_direct(tmp_path: Path, rows):
    registered = register_direct(tmp_path, rows)
    output = []
    summary = classify_registered_source_month(
        manifest=tmp_path / "source-manifest.json",
        raw_root=tmp_path / "raw",
        source_version_id=registered.record.source_version_id,
        source_month="2026-01",
        on_row=output.append,
    )
    return summary, output


def rule_ids(actual):
    return {issue.rule_id for issue in actual.dq_issues}


def test_logical_trip_key_is_deterministic_canonical_json_hash():
    rent_at = datetime(2026, 1, 1, 0, 0, 0)
    payload = json.dumps(
        ["SPB-1", "2026-01-01 00:00:00"],
        ensure_ascii=False,
        separators=(",", ":"),
    ).encode("utf-8")

    assert compute_logical_trip_key("SPB-1", rent_at) == (
        "ltk1_" + hashlib.sha256(payload).hexdigest()
    )
    assert compute_logical_trip_key(None, rent_at) is None
    assert compute_logical_trip_key("SPB-1", None) is None


def test_unique_warning_only_row_remains_trusted(tmp_path):
    summary, rows = classify_direct(tmp_path, [row(sex="f")])

    actual = rows[0]
    assert actual.logical_trip_key is not None
    assert actual.logical_trip_conflict is False
    assert actual.exact_duplicate_group is False
    assert actual.row_quarantined is False
    assert actual.trusted_fact_eligible is True
    assert actual.dq_warning_count == 1
    assert summary.clean_row_count == 1
    assert summary.trusted_fact_eligible_count == 1
    assert summary.quarantined_row_count == 0


def test_unique_row_level_quarantine_stays_untrusted_but_keeps_key(tmp_path):
    summary, rows = classify_direct(tmp_path, [row(usage_min="-1")])

    actual = rows[0]
    assert actual.logical_trip_key is not None
    assert actual.logical_trip_conflict is False
    assert actual.exact_duplicate_group is False
    assert actual.row_quarantined is True
    assert actual.trusted_fact_eligible is False
    assert summary.trusted_fact_eligible_count == 0
    assert summary.quarantined_row_count == 1


def test_keyless_quarantined_row_is_excluded_from_group_analysis(tmp_path):
    summary, rows = classify_direct(tmp_path, [row(bike_id="")])

    actual = rows[0]
    assert actual.logical_trip_key is None
    assert actual.logical_trip_conflict is False
    assert actual.exact_duplicate_group is False
    assert actual.trusted_fact_eligible is False
    assert RULE_LOGICAL_TRIP_CONFLICT not in rule_ids(actual)
    assert RULE_EXACT_DUPLICATE_GROUP not in rule_ids(actual)
    assert summary.logical_conflict_group_count == 0
    assert summary.exact_duplicate_group_count == 0


def test_exact_duplicate_group_quarantines_every_row_without_conflict_flag(tmp_path):
    summary, rows = classify_direct(tmp_path, [row(), row()])

    assert summary.clean_row_count == 2
    assert summary.trusted_fact_eligible_count == 0
    assert summary.quarantined_row_count == 2
    assert summary.logical_conflict_group_count == 0
    assert summary.exact_duplicate_group_count == 1
    assert summary.logical_conflict_row_count == 0
    assert summary.exact_duplicate_row_count == 2
    assert len({actual.logical_trip_key for actual in rows}) == 1
    for actual in rows:
        assert actual.logical_trip_conflict is False
        assert actual.exact_duplicate_group is True
        assert actual.trusted_fact_eligible is False
        assert RULE_EXACT_DUPLICATE_GROUP in rule_ids(actual)
        assert RULE_LOGICAL_TRIP_CONFLICT not in rule_ids(actual)


def test_different_source_content_for_same_key_conflicts_entire_group(tmp_path):
    summary, rows = classify_direct(
        tmp_path,
        [row(distance_m="100.00"), row(distance_m="200.00")],
    )

    assert summary.logical_conflict_group_count == 1
    assert summary.logical_conflict_row_count == 2
    assert summary.exact_duplicate_group_count == 0
    assert summary.exact_duplicate_row_count == 0
    assert summary.trusted_fact_eligible_count == 0
    for actual in rows:
        assert actual.logical_trip_conflict is True
        assert actual.exact_duplicate_group is False
        assert actual.trusted_fact_eligible is False
        assert RULE_LOGICAL_TRIP_CONFLICT in rule_ids(actual)


def test_conflict_takes_group_semantics_over_identical_subgroup(tmp_path):
    summary, rows = classify_direct(
        tmp_path,
        [row(), row(), row(return_at="2026-01-01 00:11:00")],
    )

    assert summary.logical_conflict_group_count == 1
    assert summary.logical_conflict_row_count == 3
    assert summary.exact_duplicate_group_count == 0
    assert all(actual.logical_trip_conflict for actual in rows)
    assert all(not actual.exact_duplicate_group for actual in rows)
    assert all(RULE_EXACT_DUPLICATE_GROUP not in rule_ids(actual) for actual in rows)


def test_row_level_quarantined_row_still_participates_in_group_conflict(tmp_path):
    summary, rows = classify_direct(
        tmp_path,
        [row(usage_min="-1"), row(usage_min="10")],
    )

    assert summary.logical_conflict_group_count == 1
    assert summary.logical_conflict_row_count == 2
    assert all(RULE_LOGICAL_TRIP_CONFLICT in rule_ids(actual) for actual in rows)
    assert all(actual.trusted_fact_eligible is False for actual in rows)


def test_grouping_spans_all_members_that_contribute_to_month(tmp_path):
    source = tmp_path / "year.zip"
    with zipfile.ZipFile(source, "w") as archive:
        archive.writestr("rental_2601_1.csv", encode_csv([row(distance_m="100.00")]))
        archive.writestr("rental_2601_2.csv", encode_csv([row(distance_m="200.00")]))
    registered = register_source(
        artifact=source,
        manifest=tmp_path / "source-manifest.json",
        raw_root=tmp_path / "raw",
        dataset_id="rental_history",
        logical_period="2026",
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

    summary = classify_registered_source_month(
        manifest=tmp_path / "source-manifest.json",
        raw_root=tmp_path / "raw",
        source_version_id=registered.record.source_version_id,
        source_month="2026-01",
        on_row=output.append,
    )

    assert summary.clean_row_count == 2
    assert summary.logical_conflict_group_count == 1
    assert summary.logical_conflict_row_count == 2
    assert {actual.member_name for actual in output} == {
        "rental_2601_1.csv",
        "rental_2601_2.csv",
    }


def test_range_member_is_filtered_to_requested_rental_month(tmp_path):
    source = tmp_path / "year.zip"
    member = "rental_2020.07~08.csv"
    with zipfile.ZipFile(source, "w") as archive:
        archive.writestr(
            member,
            encode_csv(
                [
                    row(
                        rent_at="2020-07-31 23:59:00",
                        return_at="2020-08-01 00:05:00",
                    ),
                    row(
                        bike_id="SPB-2",
                        rent_at="2020-08-01 00:01:00",
                        return_at="2020-08-01 00:05:00",
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

    summary = classify_registered_source_month(
        manifest=tmp_path / "source-manifest.json",
        raw_root=tmp_path / "raw",
        source_version_id=registered.record.source_version_id,
        source_month="2020-07",
        on_row=output.append,
    )

    assert summary.clean_row_count == 1
    assert [actual.source_month for actual in output] == ["2020-07"]


def test_historical_revisions_are_not_grouped_together(tmp_path):
    first = register_direct(tmp_path, [row(distance_m="100.00")], source_name="v1.csv")
    second = register_direct(tmp_path, [row(distance_m="200.00")], source_name="v2.csv")
    output = []

    summary = classify_registered_source_month(
        manifest=tmp_path / "source-manifest.json",
        raw_root=tmp_path / "raw",
        source_version_id=second.record.source_version_id,
        source_month="2026-01",
        on_row=output.append,
    )

    assert first.record.source_version_id != second.record.source_version_id
    assert summary.clean_row_count == 1
    assert summary.logical_conflict_group_count == 0
    assert summary.trusted_fact_eligible_count == 1
    assert output[0].source_version_id == second.record.source_version_id


def test_source_month_hard_gate_fails_before_final_rows_are_exposed(tmp_path):
    registered = register_direct(
        tmp_path,
        [row(), row(bike_id="SPB-2", rent_at="2026-02-01 00:00:00")],
    )
    output = []

    with pytest.raises(CanonicalizationError, match="source/rent-month validation failed"):
        classify_registered_source_month(
            manifest=tmp_path / "source-manifest.json",
            raw_root=tmp_path / "raw",
            source_version_id=registered.record.source_version_id,
            source_month="2026-01",
            on_row=output.append,
        )

    assert output == []


def test_rejects_month_outside_explicit_source_period_before_output(tmp_path):
    registered = register_direct(tmp_path, [row()])
    output = []

    with pytest.raises(GroupDqError, match="cannot build rental month"):
        classify_registered_source_month(
            manifest=tmp_path / "source-manifest.json",
            raw_root=tmp_path / "raw",
            source_version_id=registered.record.source_version_id,
            source_month="2026-02",
            on_row=output.append,
        )

    assert output == []
