"""ZIP member identity/inventory contract (PRD §7, §15.1, §19)."""

from __future__ import annotations

import hashlib
import json
import warnings
import zipfile
from pathlib import Path

import pytest

import seoul_bike_pipeline.zip_members as zip_members_module
from seoul_bike_pipeline.errors import SourceRegistrationError, ZipInventoryError
from seoul_bike_pipeline.register import register_source
from seoul_bike_pipeline.zip_members import inventory_registered_zip, member_inventory_path

DATASET_ID = "rental_history"
SOURCE_URL = "https://example.invalid/seoul-bike-year.zip"
LICENSE = "SYNTHETIC_FIXTURE"

JAN_NAME = "2020/서울특별시 공공자전거 대여이력 정보_202001.csv"
FEB_NAME = "2020/서울특별시 공공자전거 대여이력 정보_202002.csv"
JAN_BYTES = b"bike_id,rent_at\nA,2020-01-01 00:00:00\n"
FEB_V1_BYTES = b"bike_id,rent_at\nB,2020-02-01 00:00:00\n"
FEB_V2_BYTES = b"bike_id,rent_at\nB,2020-02-01 00:00:00\nC,2020-02-02 00:00:00\n"


def write_zip(path: Path, entries: list[tuple[str, bytes]]) -> None:
    with zipfile.ZipFile(path, "w") as archive:
        for name, data in entries:
            info = zipfile.ZipInfo(name, date_time=(2020, 1, 1, 0, 0, 0))
            info.compress_type = zipfile.ZIP_STORED
            info.external_attr = 0o600 << 16
            archive.writestr(info, data)


def register_zip(tmp_path: Path, zip_path: Path):
    return register_source(
        artifact=zip_path,
        manifest=tmp_path / "source-manifest.json",
        raw_root=tmp_path / "raw",
        dataset_id=DATASET_ID,
        logical_period="2020",
        published_filename="서울특별시 공공자전거 대여이력 정보_2020.zip",
        source_url=SOURCE_URL,
        license=LICENSE,
    )


def inventory(tmp_path: Path, source_version_id: str):
    return inventory_registered_zip(
        manifest=tmp_path / "source-manifest.json",
        raw_root=tmp_path / "raw",
        source_version_id=source_version_id,
    )


def test_inventory_persists_parent_plus_name_size_and_sha256(tmp_path):
    source = tmp_path / "year.zip"
    write_zip(source, [(FEB_NAME, FEB_V1_BYTES), (JAN_NAME, JAN_BYTES)])
    parent = register_zip(tmp_path, source)
    manifest_before = (tmp_path / "source-manifest.json").read_bytes()

    result = inventory(tmp_path, parent.record.source_version_id)

    assert result.status == "INVENTORIED_NEW"
    assert result.path == member_inventory_path(tmp_path / "raw", parent.record.source_version_id)
    assert [member.member_name for member in result.inventory.members] == [JAN_NAME, FEB_NAME]
    assert result.inventory.members[0].member_size == len(JAN_BYTES)
    assert result.inventory.members[0].member_sha256 == hashlib.sha256(JAN_BYTES).hexdigest()
    assert result.inventory.members[1].member_sha256 == hashlib.sha256(FEB_V1_BYTES).hexdigest()
    assert (tmp_path / "source-manifest.json").read_bytes() == manifest_before

    payload = json.loads(result.path.read_text(encoding="utf-8"))
    assert payload["parent_source_version_id"] == parent.record.source_version_id
    assert sorted(payload) == ["members", "parent_source_version_id", "schema_version"]
    assert sorted(payload["members"][0]) == ["member_name", "member_sha256", "member_size"]


def test_inventory_tracks_all_regular_files_and_ignores_directory_entries(tmp_path):
    source = tmp_path / "year.zip"
    with zipfile.ZipFile(source, "w") as archive:
        archive.writestr("2020/", b"")
        archive.writestr(JAN_NAME, JAN_BYTES)
        archive.writestr("2020/README.txt", b"archive notes")
    parent = register_zip(tmp_path, source)

    result = inventory(tmp_path, parent.record.source_version_id)

    assert [member.member_name for member in result.inventory.members] == [
        "2020/README.txt",
        JAN_NAME,
    ]


def test_repeat_inventory_verifies_without_rewrite(tmp_path):
    source = tmp_path / "year.zip"
    write_zip(source, [(JAN_NAME, JAN_BYTES)])
    parent = register_zip(tmp_path, source)
    first = inventory(tmp_path, parent.record.source_version_id)
    before_bytes = first.path.read_bytes()
    before_mtime = first.path.stat().st_mtime_ns

    second = inventory(tmp_path, parent.record.source_version_id)

    assert second.status == "INVENTORY_UNCHANGED"
    assert second.inventory == first.inventory
    assert second.path.read_bytes() == before_bytes
    assert second.path.stat().st_mtime_ns == before_mtime


def test_parent_revision_can_keep_one_member_unchanged_and_change_only_one(tmp_path):
    first_zip = tmp_path / "year-v1.zip"
    second_zip = tmp_path / "year-v2.zip"
    write_zip(first_zip, [(JAN_NAME, JAN_BYTES), (FEB_NAME, FEB_V1_BYTES)])
    write_zip(second_zip, [(JAN_NAME, JAN_BYTES), (FEB_NAME, FEB_V2_BYTES)])

    first_parent = register_zip(tmp_path, first_zip)
    first_inventory = inventory(tmp_path, first_parent.record.source_version_id).inventory
    second_parent = register_zip(tmp_path, second_zip)
    second_inventory = inventory(tmp_path, second_parent.record.source_version_id).inventory

    assert first_parent.record.source_version_id != second_parent.record.source_version_id
    assert first_inventory.parent_source_version_id != second_inventory.parent_source_version_id
    assert first_inventory.members[0] == second_inventory.members[0]
    assert first_inventory.members[1].member_name == second_inventory.members[1].member_name
    assert first_inventory.members[1].member_sha256 != second_inventory.members[1].member_sha256


def test_unregistered_source_version_hard_fails(tmp_path):
    with pytest.raises(ZipInventoryError, match="is not registered"):
        inventory_registered_zip(
            manifest=tmp_path / "absent.json",
            raw_root=tmp_path / "raw",
            source_version_id="sv1_missing",
        )


def test_registered_non_zip_hard_fails_without_inventory(tmp_path):
    source = tmp_path / "not-a-zip.bin"
    source.write_bytes(b"not a zip")
    parent = register_zip(tmp_path, source)

    with pytest.raises(ZipInventoryError, match="cannot inventory ZIP"):
        inventory(tmp_path, parent.record.source_version_id)

    assert not member_inventory_path(tmp_path / "raw", parent.record.source_version_id).exists()


def test_empty_zip_hard_fails_without_inventory(tmp_path):
    source = tmp_path / "empty.zip"
    with zipfile.ZipFile(source, "w"):
        pass
    parent = register_zip(tmp_path, source)

    with pytest.raises(ZipInventoryError, match="contains no file members"):
        inventory(tmp_path, parent.record.source_version_id)

    assert not member_inventory_path(tmp_path / "raw", parent.record.source_version_id).exists()


def test_corrupted_member_crc_hard_fails(tmp_path):
    source = tmp_path / "corrupt-member.zip"
    marker = b"UNIQUE_MEMBER_PAYLOAD_123456789"
    write_zip(source, [(JAN_NAME, marker)])
    raw_bytes = source.read_bytes()
    assert raw_bytes.count(marker) == 1
    source.write_bytes(raw_bytes.replace(marker, b"X" * len(marker), 1))
    parent = register_zip(tmp_path, source)

    with pytest.raises(ZipInventoryError, match="cannot inventory ZIP"):
        inventory(tmp_path, parent.record.source_version_id)


def test_duplicate_member_name_hard_fails_as_ambiguous_provenance(tmp_path):
    source = tmp_path / "duplicates.zip"
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", UserWarning)
        write_zip(source, [(JAN_NAME, JAN_BYTES), (JAN_NAME, JAN_BYTES)])
    parent = register_zip(tmp_path, source)

    with pytest.raises(ZipInventoryError, match="duplicate member_name"):
        inventory(tmp_path, parent.record.source_version_id)


def test_corrupted_existing_inventory_is_not_auto_repaired(tmp_path):
    source = tmp_path / "year.zip"
    write_zip(source, [(JAN_NAME, JAN_BYTES)])
    parent = register_zip(tmp_path, source)
    first = inventory(tmp_path, parent.record.source_version_id)
    first.path.write_text("{", encoding="utf-8")

    with pytest.raises(ZipInventoryError, match="not valid JSON"):
        inventory(tmp_path, parent.record.source_version_id)

    assert first.path.read_text(encoding="utf-8") == "{"


def test_duplicate_json_keys_in_existing_inventory_hard_fail(tmp_path):
    source = tmp_path / "year.zip"
    write_zip(source, [(JAN_NAME, JAN_BYTES)])
    parent = register_zip(tmp_path, source)
    path = member_inventory_path(tmp_path / "raw", parent.record.source_version_id)
    path.parent.mkdir(parents=True)
    path.write_text(
        "{\n"
        '  "schema_version": 999,\n'
        '  "schema_version": 1,\n'
        f'  "parent_source_version_id": "{parent.record.source_version_id}",\n'
        '  "members": []\n'
        "}\n",
        encoding="utf-8",
    )

    with pytest.raises(ZipInventoryError, match="duplicate JSON object key 'schema_version'"):
        inventory(tmp_path, parent.record.source_version_id)


def test_duplicate_member_object_keys_in_existing_inventory_hard_fail(tmp_path):
    source = tmp_path / "year.zip"
    write_zip(source, [(JAN_NAME, JAN_BYTES)])
    parent = register_zip(tmp_path, source)
    path = member_inventory_path(tmp_path / "raw", parent.record.source_version_id)
    path.parent.mkdir(parents=True)
    member_sha = hashlib.sha256(JAN_BYTES).hexdigest()
    path.write_text(
        "{\n"
        '  "schema_version": 1,\n'
        f'  "parent_source_version_id": "{parent.record.source_version_id}",\n'
        '  "members": [{\n'
        f'    "member_name": "{JAN_NAME}",\n'
        '    "member_size": 999,\n'
        f'    "member_size": {len(JAN_BYTES)},\n'
        f'    "member_sha256": "{member_sha}"\n'
        "  }]\n"
        "}\n",
        encoding="utf-8",
    )

    with pytest.raises(ZipInventoryError, match="duplicate JSON object key 'member_size'"):
        inventory(tmp_path, parent.record.source_version_id)


def test_existing_different_inventory_is_never_overwritten(tmp_path):
    source = tmp_path / "year.zip"
    write_zip(source, [(JAN_NAME, JAN_BYTES)])
    parent = register_zip(tmp_path, source)
    path = member_inventory_path(tmp_path / "raw", parent.record.source_version_id)
    path.parent.mkdir(parents=True)
    sentinel = {
        "schema_version": 1,
        "parent_source_version_id": parent.record.source_version_id,
        "members": [
            {
                "member_name": JAN_NAME,
                "member_size": 1,
                "member_sha256": hashlib.sha256(b"x").hexdigest(),
            }
        ],
    }
    path.write_text(json.dumps(sentinel), encoding="utf-8")
    before = path.read_bytes()

    with pytest.raises(ZipInventoryError, match="does not match"):
        inventory(tmp_path, parent.record.source_version_id)

    assert path.read_bytes() == before


def test_concurrent_different_inventory_winner_is_never_overwritten(tmp_path, monkeypatch):
    source = tmp_path / "year.zip"
    write_zip(source, [(JAN_NAME, JAN_BYTES)])
    parent = register_zip(tmp_path, source)
    path = member_inventory_path(tmp_path / "raw", parent.record.source_version_id)
    winner = {
        "schema_version": 1,
        "parent_source_version_id": parent.record.source_version_id,
        "members": [
            {
                "member_name": JAN_NAME,
                "member_size": 1,
                "member_sha256": hashlib.sha256(b"x").hexdigest(),
            }
        ],
    }
    winner_bytes = (json.dumps(winner, ensure_ascii=False, indent=2) + "\n").encode("utf-8")

    def concurrent_link(source_path, destination_path):
        destination = Path(destination_path)
        destination.write_bytes(winner_bytes)
        raise FileExistsError(destination)

    monkeypatch.setattr(zip_members_module.os, "link", concurrent_link)

    with pytest.raises(ZipInventoryError, match="appeared with different content"):
        inventory(tmp_path, parent.record.source_version_id)

    assert path.read_bytes() == winner_bytes


def test_source_manifest_cannot_live_under_member_inventory_namespace(tmp_path):
    source = tmp_path / "year.zip"
    write_zip(source, [(JAN_NAME, JAN_BYTES)])
    raw_root = tmp_path / "raw"

    with pytest.raises(SourceRegistrationError, match="Raw member inventories"):
        register_source(
            artifact=source,
            manifest=raw_root / "member-inventories" / "source-manifest.json",
            raw_root=raw_root,
            dataset_id=DATASET_ID,
            logical_period="2020",
            published_filename="year.zip",
            source_url=SOURCE_URL,
            license=LICENSE,
        )

    assert not (raw_root / "artifacts").exists()


@pytest.mark.parametrize("alias", ["member-inventories.", "member-inventories "])
def test_source_manifest_rejects_windows_aliases_of_member_inventory_namespace(tmp_path, alias):
    source = tmp_path / "year.zip"
    write_zip(source, [(JAN_NAME, JAN_BYTES)])
    raw_root = tmp_path / "raw"

    with pytest.raises(SourceRegistrationError, match="Raw member inventories"):
        register_source(
            artifact=source,
            manifest=raw_root / alias / "source-manifest.json",
            raw_root=raw_root,
            dataset_id=DATASET_ID,
            logical_period="2020",
            published_filename="year.zip",
            source_url=SOURCE_URL,
            license=LICENSE,
        )

    assert not (raw_root / "artifacts").exists()
