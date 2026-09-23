"""Logical month mapping and parent ZIP revision impact planning."""

from __future__ import annotations

import zipfile
from pathlib import Path

import pytest

from seoul_bike_pipeline.errors import ZipMonthMappingError
from seoul_bike_pipeline.register import register_source
from seoul_bike_pipeline.zip_members import inventory_registered_zip
from seoul_bike_pipeline.zip_months import months_for_member_name, plan_zip_months

DATASET_ID = "rental_history"
SOURCE_URL = "https://example.invalid/seoul-bike-year.zip"
LICENSE = "SYNTHETIC_FIXTURE"


@pytest.mark.parametrize(
    ("name", "expected"),
    [
        ("공공자전거 대여이력 정보_202006.csv", ("2020-06",)),
        ("공공자전거 대여이력 정보_2020.12.csv", ("2020-12",)),
        ("서울특별시 공공자전거 대여이력 정보_1512.csv", ("2015-12",)),
        ("공공자전거 대여이력 정보_2112.csv", ("2021-12",)),
        ("서울특별시 공공자전거 대여이력 정보_22.06.csv", ("2022-06",)),
        ("서울특별시 공공자전거 대여이력 정보_2606.csv", ("2026-06",)),
        ("서울특별시 공공자전거 대여이력 정보_22081.csv", ("2022-08",)),
        ("공공자전거 대여이력 정보_2020.07~08.csv", ("2020-07", "2020-08")),
        ("nested/공공자전거 대여이력 정보_2020.09.CSV", ("2020-09",)),
    ],
)
def test_observed_filename_families_map_to_logical_months(name, expected):
    assert months_for_member_name(name) == expected


def test_non_csv_member_is_not_a_processing_unit():
    assert months_for_member_name("2020/README.txt") is None


def test_unrecognized_csv_name_hard_fails():
    with pytest.raises(ZipMonthMappingError, match="does not match"):
        months_for_member_name("2020/rental_export_final.csv")


def test_descending_month_range_hard_fails():
    with pytest.raises(ZipMonthMappingError, match="descending/cross-year"):
        months_for_member_name("공공자전거 대여이력 정보_2020.12~01.csv")


@pytest.mark.parametrize(
    "name",
    [
        "rental_202001-202002.csv",
        "rental_202001_202002.csv",
        "rental_2020-12-01.csv",
        "rental_260615.csv",
        "rental_201.csv",
    ],
)
def test_ambiguous_or_date_like_numeric_suffix_hard_fails(name):
    with pytest.raises(ZipMonthMappingError, match="multiple independent|does not match"):
        months_for_member_name(name)


def write_zip(path: Path, entries: list[tuple[str, bytes]]) -> None:
    with zipfile.ZipFile(path, "w") as archive:
        for name, payload in entries:
            archive.writestr(name, payload)


def register_and_inventory(
    tmp_path: Path,
    zip_path: Path,
    *,
    logical_period: str = "2020",
    published_filename: str = "서울특별시 공공자전거 대여이력 정보_2020.zip",
):
    result = register_source(
        artifact=zip_path,
        manifest=tmp_path / "source-manifest.json",
        raw_root=tmp_path / "raw",
        dataset_id=DATASET_ID,
        logical_period=logical_period,
        published_filename=published_filename,
        source_url=SOURCE_URL,
        license=LICENSE,
    )
    inventory_registered_zip(
        manifest=tmp_path / "source-manifest.json",
        raw_root=tmp_path / "raw",
        source_version_id=result.record.source_version_id,
    )
    return result.record.source_version_id


def plan(tmp_path: Path, current: str, previous: str | None = None):
    return plan_zip_months(
        manifest=tmp_path / "source-manifest.json",
        raw_root=tmp_path / "raw",
        current_source_version_id=current,
        previous_source_version_id=previous,
    )


def status_map(result):
    return {impact.logical_month: impact.status for impact in result.impacts}


def test_first_yearly_parent_marks_all_mapped_months_new(tmp_path):
    source = tmp_path / "year.zip"
    write_zip(
        source,
        [
            ("2020/rental_202001.csv", b"jan"),
            ("2020/rental_2020.02.csv", b"feb"),
            ("2020/README.txt", b"notes"),
        ],
    )
    current = register_and_inventory(tmp_path, source)

    result = plan(tmp_path, current)

    assert result.previous_source_version_id is None
    assert status_map(result) == {"2020-01": "NEW", "2020-02": "NEW"}


def test_revision_marks_only_changed_member_month_revised(tmp_path):
    v1 = tmp_path / "year-v1.zip"
    v2 = tmp_path / "year-v2.zip"
    write_zip(v1, [("rental_202001.csv", b"jan"), ("rental_202002.csv", b"feb-v1")])
    write_zip(v2, [("rental_202001.csv", b"jan"), ("rental_202002.csv", b"feb-v2")])
    previous = register_and_inventory(tmp_path, v1)
    current = register_and_inventory(tmp_path, v2)

    result = plan(tmp_path, current, previous)

    assert status_map(result) == {"2020-01": "UNCHANGED", "2020-02": "REVISED"}


def test_same_month_content_is_unchanged_even_when_member_name_changes(tmp_path):
    v1 = tmp_path / "year-v1.zip"
    v2 = tmp_path / "year-v2.zip"
    write_zip(v1, [("rental_202001.csv", b"same")])
    write_zip(v2, [("rental_2020.01.csv", b"same")])
    previous = register_and_inventory(tmp_path, v1)
    current = register_and_inventory(tmp_path, v2)

    result = plan(tmp_path, current, previous)

    assert status_map(result) == {"2020-01": "UNCHANGED"}


def test_range_member_change_invalidates_every_impacted_month(tmp_path):
    v1 = tmp_path / "year-v1.zip"
    v2 = tmp_path / "year-v2.zip"
    name = "공공자전거 대여이력 정보_2020.07~08.csv"
    write_zip(v1, [(name, b"jul-aug-v1")])
    write_zip(v2, [(name, b"jul-aug-v2")])
    previous = register_and_inventory(tmp_path, v1)
    current = register_and_inventory(tmp_path, v2)

    result = plan(tmp_path, current, previous)

    assert status_map(result) == {"2020-07": "REVISED", "2020-08": "REVISED"}


def test_new_and_removed_months_are_explicit(tmp_path):
    v1 = tmp_path / "year-v1.zip"
    v2 = tmp_path / "year-v2.zip"
    write_zip(v1, [("rental_202001.csv", b"jan"), ("rental_202002.csv", b"feb")])
    write_zip(v2, [("rental_202002.csv", b"feb"), ("rental_202003.csv", b"mar")])
    previous = register_and_inventory(tmp_path, v1)
    current = register_and_inventory(tmp_path, v2)

    result = plan(tmp_path, current, previous)

    assert status_map(result) == {
        "2020-01": "REMOVED",
        "2020-02": "UNCHANGED",
        "2020-03": "NEW",
    }


def test_multiple_parts_for_one_month_compare_as_content_multiset(tmp_path):
    v1 = tmp_path / "year-v1.zip"
    v2 = tmp_path / "year-v2.zip"
    write_zip(
        v1,
        [("rental_202001_1.csv", b"part-a"), ("rental_202001_2.csv", b"part-b")],
    )
    write_zip(
        v2,
        [("renamed_2020.01_9.csv", b"part-b"), ("renamed_202001_8.csv", b"part-a")],
    )
    previous = register_and_inventory(tmp_path, v1)
    current = register_and_inventory(tmp_path, v2)

    result = plan(tmp_path, current, previous)

    assert status_map(result) == {"2020-01": "UNCHANGED"}
    assert len(result.impacts[0].current_members) == 2


def test_csv_month_outside_parent_year_hard_fails(tmp_path):
    source = tmp_path / "year.zip"
    write_zip(source, [("rental_202101.csv", b"wrong-year")])
    current = register_and_inventory(tmp_path, source)

    with pytest.raises(ZipMonthMappingError, match="outside yearly parent 2020"):
        plan(tmp_path, current)


def test_unrecognized_csv_in_inventory_hard_fails_instead_of_silent_skip(tmp_path):
    source = tmp_path / "year.zip"
    write_zip(source, [("mystery.csv", b"data")])
    current = register_and_inventory(tmp_path, source)

    with pytest.raises(ZipMonthMappingError, match="does not match"):
        plan(tmp_path, current)


def test_month_planning_requires_yearly_parent(tmp_path):
    source = tmp_path / "month.zip"
    write_zip(source, [("rental_202001.csv", b"jan")])
    current = register_and_inventory(
        tmp_path,
        source,
        logical_period="2020-01",
        published_filename="rental_2020.01.zip",
    )

    with pytest.raises(ZipMonthMappingError, match="requires a YYYY yearly parent"):
        plan(tmp_path, current)


def test_previous_must_precede_current_in_manifest(tmp_path):
    v1 = tmp_path / "year-v1.zip"
    v2 = tmp_path / "year-v2.zip"
    write_zip(v1, [("rental_202001.csv", b"v1")])
    write_zip(v2, [("rental_202001.csv", b"v2")])
    older = register_and_inventory(tmp_path, v1)
    newer = register_and_inventory(tmp_path, v2)

    with pytest.raises(ZipMonthMappingError, match="must appear before current"):
        plan(tmp_path, older, newer)


def test_previous_and_current_must_share_dataset_and_year(tmp_path):
    v1 = tmp_path / "year-2020.zip"
    v2 = tmp_path / "year-2021.zip"
    write_zip(v1, [("rental_202001.csv", b"2020")])
    write_zip(v2, [("rental_202101.csv", b"2021")])
    previous = register_and_inventory(tmp_path, v1)
    current = register_and_inventory(
        tmp_path,
        v2,
        logical_period="2021",
        published_filename="서울특별시 공공자전거 대여이력 정보_2021.zip",
    )

    with pytest.raises(ZipMonthMappingError, match="same dataset_id and logical_period"):
        plan(tmp_path, current, previous)
