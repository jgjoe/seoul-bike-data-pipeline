"""Source registration behavior (PRD §6, §12, §15.1).

The literal size/hash/id constants pin the checked-in fixture bytes and the
identity contract: a ``sv1_`` id is the SHA-256 of the canonical JSON of
``dataset_id``, ``logical_period``, ``published_filename`` and ``sha256``.
Changing the canonical form, the record schema, or the fixture bytes fails
these tests instead of silently re-labelling every registered source version.
"""

from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pytest

from seoul_bike_pipeline.errors import ManifestError, SourceRegistrationError
from seoul_bike_pipeline.manifest import compute_source_version_id, load_manifest
from seoul_bike_pipeline.register import register_source

FIXTURES = Path(__file__).parent / "fixtures" / "source"
ARTIFACT_V1 = FIXTURES / "rental_202001_v1.csv"
ARTIFACT_V2 = FIXTURES / "rental_202001_v2.csv"

DATASET_ID = "rental_history"
LOGICAL_PERIOD = "2020-01"
PUBLISHED_FILENAME = "서울특별시 공공자전거 대여이력 정보_202001.csv"
SOURCE_URL = "https://data.seoul.go.kr/dataList/OA-15493/A/1/datasetView.do"
LICENSE = "공공데이터 이용허락"

V1_SIZE = 687
V1_SHA256 = "7b8a935ddd9fa18ad532b6b7d945556f12bd05512d781e8bddd0298ee6f0bbe0"
V1_ID = "sv1_523e4b8078bd0fdc4c92c66b97494798a19fb402147c8d4714b331a02a229d12"
V2_SIZE = 843
V2_SHA256 = "7d3d2f86482a7636f0bcb80465b5b13747199696adef22475fcdd15522c4c00e"
V2_ID = "sv1_00e9b29d1ac2256e331ac3539bfdd394e3dd87fff546b126e94acd007201efb0"
RENAMED_FILENAME = "서울특별시 공공자전거 대여이력 정보_202001_수정.csv"
RENAMED_ID = "sv1_86e6f37a7ddcef804677cd8bb593553113dcf595c726fba4184360c2f006e37b"

PRD_SECTION_6_FIELDS = [
    "dataset_id",
    "published_filename",
    "logical_period",
    "source_modified_at",
    "observed_at",
    "size_bytes",
    "sha256",
    "source_url",
    "license",
    "source_version_id",
]

RECORD_V1 = {
    "dataset_id": DATASET_ID,
    "published_filename": PUBLISHED_FILENAME,
    "logical_period": LOGICAL_PERIOD,
    "source_modified_at": None,
    "observed_at": "2026-09-19T09:00:00Z",
    "size_bytes": V1_SIZE,
    "sha256": V1_SHA256,
    "source_url": SOURCE_URL,
    "license": LICENSE,
    "source_version_id": V1_ID,
}


def register(manifest_path: Path, artifact: Path, **overrides):
    options = {
        "raw_root": manifest_path.parent / "raw",
        "dataset_id": DATASET_ID,
        "logical_period": LOGICAL_PERIOD,
        "published_filename": PUBLISHED_FILENAME,
        "source_url": SOURCE_URL,
        "license": LICENSE,
    }
    options.update(overrides)
    return register_source(artifact=artifact, manifest=manifest_path, **options)


def manifest_payload(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def manifest_records(path: Path) -> list[dict]:
    return manifest_payload(path)["records"]


def test_fixtures_are_two_byte_different_versions_of_one_artifact(tmp_path):
    v1 = ARTIFACT_V1.read_bytes()
    v2 = ARTIFACT_V2.read_bytes()

    assert len(v1) == V1_SIZE
    assert hashlib.sha256(v1).hexdigest() == V1_SHA256
    assert len(v2) == V2_SIZE
    assert hashlib.sha256(v2).hexdigest() == V2_SHA256
    assert v1 != v2


def test_identity_is_the_canonical_json_hash_of_the_four_identity_fields():
    assert compute_source_version_id(DATASET_ID, LOGICAL_PERIOD, PUBLISHED_FILENAME, V1_SHA256) == V1_ID
    assert compute_source_version_id(DATASET_ID, LOGICAL_PERIOD, PUBLISHED_FILENAME, V2_SHA256) == V2_ID


def test_a_missing_manifest_is_an_empty_manifest(tmp_path):
    assert load_manifest(tmp_path / "absent.json") == []


def test_new_registration_writes_exactly_the_prd_section_6_record(tmp_path):
    manifest_path = tmp_path / "manifest.json"

    result = register(manifest_path, ARTIFACT_V1)

    assert result.status == "REGISTERED_NEW"
    assert result.record.source_version_id == V1_ID
    assert result.record.sha256 == V1_SHA256
    assert result.record.size_bytes == V1_SIZE
    assert result.record.dataset_id == DATASET_ID
    assert result.record.logical_period == LOGICAL_PERIOD
    assert result.record.published_filename == PUBLISHED_FILENAME
    assert result.record.source_url == SOURCE_URL
    assert result.record.license == LICENSE
    assert result.record.source_modified_at is None
    assert result.record.observed_at.endswith("Z")

    payload = manifest_payload(manifest_path)
    assert sorted(payload) == ["records", "schema_version"]
    assert payload["schema_version"] == 1
    assert len(payload["records"]) == 1
    assert sorted(payload["records"][0]) == sorted(PRD_SECTION_6_FIELDS)
    assert payload["records"][0]["sha256"] == V1_SHA256
    assert payload["records"][0]["observed_at"] == result.record.observed_at


def test_known_content_is_unchanged_and_does_not_rewrite_the_manifest(tmp_path):
    manifest_path = tmp_path / "manifest.json"
    first = register(manifest_path, ARTIFACT_V1)
    before_bytes = manifest_path.read_bytes()
    before_mtime = manifest_path.stat().st_mtime_ns

    second = register(manifest_path, ARTIFACT_V1)

    assert second.status == "KNOWN_UNCHANGED"
    assert second.record == first.record
    assert manifest_path.read_bytes() == before_bytes
    assert manifest_path.stat().st_mtime_ns == before_mtime
    assert len(manifest_records(manifest_path)) == 1


def test_path_and_observed_at_do_not_participate_in_identity(tmp_path):
    copied = tmp_path / "another download.csv"
    copied.write_bytes(ARTIFACT_V1.read_bytes())

    from_fixture = register(tmp_path / "first.json", ARTIFACT_V1)
    from_copy = register(tmp_path / "second.json", copied)

    assert from_fixture.record.source_version_id == from_copy.record.source_version_id == V1_ID
    assert from_fixture.record.sha256 == from_copy.record.sha256 == V1_SHA256
    assert from_fixture.record.published_filename == from_copy.record.published_filename == PUBLISHED_FILENAME


def test_a_known_id_is_returned_with_its_original_record(tmp_path):
    manifest_path = tmp_path / "manifest.json"
    raw_path = tmp_path / "raw" / "artifacts" / V1_ID / "artifact"
    raw_path.parent.mkdir(parents=True)
    raw_path.write_bytes(ARTIFACT_V1.read_bytes())
    manifest_path.write_text(
        json.dumps({"schema_version": 1, "records": [RECORD_V1]}, ensure_ascii=False),
        encoding="utf-8",
    )
    before = manifest_path.read_bytes()

    result = register(manifest_path, ARTIFACT_V1)

    assert result.status == "KNOWN_UNCHANGED"
    assert result.record.observed_at == "2026-09-19T09:00:00Z"
    assert manifest_path.read_bytes() == before


def test_revision_appends_a_new_version_and_keeps_the_previous_one(tmp_path):
    manifest_path = tmp_path / "manifest.json"
    first = register(manifest_path, ARTIFACT_V1)

    second = register(manifest_path, ARTIFACT_V2)

    assert second.status == "REGISTERED_REVISION"
    assert second.record.source_version_id == V2_ID
    assert second.record.sha256 == V2_SHA256
    assert second.record.size_bytes == V2_SIZE
    stored = manifest_records(manifest_path)
    assert [record["source_version_id"] for record in stored] == [V1_ID, V2_ID]
    assert [record["sha256"] for record in stored] == [V1_SHA256, V2_SHA256]
    assert stored[0] == first.record.to_json()


def test_a_changed_published_filename_is_a_revision_of_the_same_logical_period(tmp_path):
    manifest_path = tmp_path / "manifest.json"
    first = register(manifest_path, ARTIFACT_V1)

    second = register(manifest_path, ARTIFACT_V1, published_filename=RENAMED_FILENAME)

    assert second.status == "REGISTERED_REVISION"
    assert second.record.published_filename == RENAMED_FILENAME
    assert second.record.source_version_id == RENAMED_ID
    assert second.record.sha256 == V1_SHA256
    stored = manifest_records(manifest_path)
    assert [record["source_version_id"] for record in stored] == [V1_ID, RENAMED_ID]
    assert [record["published_filename"] for record in stored] == [
        PUBLISHED_FILENAME,
        RENAMED_FILENAME,
    ]
    assert stored[0] == first.record.to_json()


def test_manifest_never_contains_a_local_path(tmp_path):
    local_artifact = tmp_path / "downloads" / "202001_rental_copy.csv"
    local_artifact.parent.mkdir()
    local_artifact.write_bytes(ARTIFACT_V1.read_bytes())
    manifest_path = tmp_path / "manifest.json"

    register(manifest_path, local_artifact)

    text = manifest_path.read_text(encoding="utf-8")
    assert PUBLISHED_FILENAME in text
    assert local_artifact.name not in text
    assert str(local_artifact.parent) not in text
    assert str(tmp_path / "raw") not in text
    assert "\\" not in text
    assert "/mnt/" not in text


@pytest.mark.parametrize(
    ("given", "stored"),
    [
        ("2021-03-05", "2021-03-05T00:00:00Z"),
        ("2021-03-05T09:30:00+09:00", "2021-03-05T00:30:00Z"),
    ],
)
def test_source_modified_at_is_stored_in_utc(tmp_path, given, stored):
    result = register(tmp_path / "manifest.json", ARTIFACT_V1, source_modified_at=given)

    assert result.record.source_modified_at == stored


def test_missing_artifact_hard_fails_without_writing_a_manifest(tmp_path):
    manifest_path = tmp_path / "manifest.json"

    with pytest.raises(SourceRegistrationError):
        register(manifest_path, tmp_path / "absent.csv")

    assert not manifest_path.exists()


def test_blank_metadata_hard_fails(tmp_path):
    with pytest.raises(SourceRegistrationError):
        register(tmp_path / "manifest.json", ARTIFACT_V1, dataset_id="   ")


BAD_MANIFESTS = [
    ("invalid JSON", "{"),
    ("top level array", "[]"),
    ("missing schema version", json.dumps({"records": []})),
    ("unknown schema version", json.dumps({"schema_version": 2, "records": []})),
    ("float schema version", json.dumps({"schema_version": 1.0, "records": []})),
    ("boolean schema version", json.dumps({"schema_version": True, "records": []})),
    ("records not an array", json.dumps({"schema_version": 1, "records": {}})),
    ("unknown top level field", json.dumps({"schema_version": 1, "records": [], "extra": 1})),
    (
        "record missing fields",
        json.dumps({"schema_version": 1, "records": [{"dataset_id": DATASET_ID}]}),
    ),
    (
        "record with an unknown field",
        json.dumps({"schema_version": 1, "records": [{**RECORD_V1, "local_path": "data/raw/2020-01.csv"}]}, ensure_ascii=False),
    ),
    (
        "record with a non-integer size",
        json.dumps({"schema_version": 1, "records": [{**RECORD_V1, "size_bytes": str(V1_SIZE)}]}, ensure_ascii=False),
    ),
    (
        "record with an uppercase hash",
        json.dumps({"schema_version": 1, "records": [{**RECORD_V1, "sha256": V1_SHA256.upper()}]}, ensure_ascii=False),
    ),
    (
        "record whose id does not match its content",
        json.dumps({"schema_version": 1, "records": [{**RECORD_V1, "source_version_id": V2_ID}]}, ensure_ascii=False),
    ),
]


@pytest.mark.parametrize(("label", "text"), BAD_MANIFESTS, ids=[label for label, _ in BAD_MANIFESTS])
def test_bad_manifest_shape_hard_fails_and_leaves_the_file_alone(tmp_path, label, text):
    manifest_path = tmp_path / "manifest.json"
    manifest_path.write_text(text, encoding="utf-8")
    before = manifest_path.read_bytes()

    with pytest.raises(ManifestError):
        register(manifest_path, ARTIFACT_V1)

    assert manifest_path.read_bytes() == before
    assert [path.name for path in tmp_path.iterdir()] == ["manifest.json"]
