"""Immutable Raw artifact contract for source registration (PRD §15.1, §27)."""

from __future__ import annotations

import hashlib
from pathlib import Path

import pytest

import seoul_bike_pipeline.raw as raw_module
import seoul_bike_pipeline.register as register_module
from seoul_bike_pipeline.errors import ManifestError, RawArtifactError, SourceRegistrationError
from seoul_bike_pipeline.raw import raw_artifact_path
from seoul_bike_pipeline.register import register_source

FIXTURES = Path(__file__).parent / "fixtures" / "source"
ARTIFACT_V1 = FIXTURES / "rental_202001_v1.csv"
ARTIFACT_V2 = FIXTURES / "rental_202001_v2.csv"

V1_SHA256 = "7b8a935ddd9fa18ad532b6b7d945556f12bd05512d781e8bddd0298ee6f0bbe0"
V1_ID = "sv1_523e4b8078bd0fdc4c92c66b97494798a19fb402147c8d4714b331a02a229d12"
V2_SHA256 = "7d3d2f86482a7636f0bcb80465b5b13747199696adef22475fcdd15522c4c00e"
V2_ID = "sv1_00e9b29d1ac2256e331ac3539bfdd394e3dd87fff546b126e94acd007201efb0"


def register(tmp_path: Path, artifact: Path):
    return register_source(
        artifact=artifact,
        manifest=tmp_path / "manifest.json",
        raw_root=tmp_path / "raw",
        dataset_id="rental_history",
        logical_period="2020-01",
        published_filename="서울특별시 공공자전거 대여이력 정보_202001.csv",
        source_url="https://data.seoul.go.kr/dataList/OA-15493/A/1/datasetView.do",
        license="공공데이터 이용허락",
    )


def raw_path(tmp_path: Path, source_version_id: str) -> Path:
    return raw_artifact_path(tmp_path / "raw", source_version_id)


def test_new_registration_preserves_exact_bytes_at_deterministic_path(tmp_path):
    result = register(tmp_path, ARTIFACT_V1)
    stored = raw_path(tmp_path, V1_ID)

    assert result.status == "REGISTERED_NEW"
    assert stored == tmp_path / "raw" / "artifacts" / V1_ID / "artifact"
    assert stored.read_bytes() == ARTIFACT_V1.read_bytes()
    assert hashlib.sha256(stored.read_bytes()).hexdigest() == V1_SHA256


def test_unchanged_registration_verifies_without_rewriting_raw_or_manifest(tmp_path):
    register(tmp_path, ARTIFACT_V1)
    manifest = tmp_path / "manifest.json"
    stored = raw_path(tmp_path, V1_ID)
    manifest_before = manifest.read_bytes()
    manifest_mtime = manifest.stat().st_mtime_ns
    raw_before = stored.read_bytes()
    raw_mtime = stored.stat().st_mtime_ns

    result = register(tmp_path, ARTIFACT_V1)

    assert result.status == "KNOWN_UNCHANGED"
    assert manifest.read_bytes() == manifest_before
    assert manifest.stat().st_mtime_ns == manifest_mtime
    assert stored.read_bytes() == raw_before
    assert stored.stat().st_mtime_ns == raw_mtime


def test_revision_keeps_previous_raw_object_and_adds_new_one(tmp_path):
    first = register(tmp_path, ARTIFACT_V1)
    v1_path = raw_path(tmp_path, V1_ID)
    v1_before = v1_path.read_bytes()

    second = register(tmp_path, ARTIFACT_V2)

    assert first.status == "REGISTERED_NEW"
    assert second.status == "REGISTERED_REVISION"
    assert v1_path.read_bytes() == v1_before == ARTIFACT_V1.read_bytes()
    assert raw_path(tmp_path, V2_ID).read_bytes() == ARTIFACT_V2.read_bytes()
    assert hashlib.sha256(v1_path.read_bytes()).hexdigest() == V1_SHA256
    assert hashlib.sha256(raw_path(tmp_path, V2_ID).read_bytes()).hexdigest() == V2_SHA256


def test_known_manifest_with_missing_raw_object_hard_fails(tmp_path):
    register(tmp_path, ARTIFACT_V1)
    manifest = tmp_path / "manifest.json"
    manifest_before = manifest.read_bytes()
    stored = raw_path(tmp_path, V1_ID)
    stored.unlink()

    with pytest.raises(RawArtifactError, match="missing"):
        register(tmp_path, ARTIFACT_V1)

    assert manifest.read_bytes() == manifest_before
    assert not stored.exists()


def test_corrupted_raw_object_hard_fails_without_auto_repair(tmp_path):
    register(tmp_path, ARTIFACT_V1)
    manifest = tmp_path / "manifest.json"
    manifest_before = manifest.read_bytes()
    stored = raw_path(tmp_path, V1_ID)
    corrupted = b"externally-corrupted-raw-object"
    stored.write_bytes(corrupted)

    with pytest.raises(RawArtifactError, match="checksum verification"):
        register(tmp_path, ARTIFACT_V1)

    assert manifest.read_bytes() == manifest_before
    assert stored.read_bytes() == corrupted


def test_copy_mismatch_never_exposes_final_raw_or_manifest(tmp_path, monkeypatch):
    def mismatching_copy(source_path: Path, temp_path: Path):
        temp_path.write_bytes(b"partial-or-changed")
        return len(b"partial-or-changed"), hashlib.sha256(b"partial-or-changed").hexdigest()

    monkeypatch.setattr(raw_module, "_copy_and_fingerprint", mismatching_copy)

    with pytest.raises(RawArtifactError, match="changed while preserving"):
        register(tmp_path, ARTIFACT_V1)

    assert not raw_path(tmp_path, V1_ID).exists()
    assert not (tmp_path / "manifest.json").exists()
    assert list((tmp_path / "raw" / "artifacts" / V1_ID).glob(".artifact.*.tmp")) == []


def test_unwritable_raw_root_shape_hard_fails_before_manifest_publish(tmp_path):
    raw_root = tmp_path / "raw"
    raw_root.write_text("not-a-directory", encoding="utf-8")

    with pytest.raises(RawArtifactError, match="cannot create Raw artifact directory"):
        register_source(
            artifact=ARTIFACT_V1,
            manifest=tmp_path / "manifest.json",
            raw_root=raw_root,
            dataset_id="rental_history",
            logical_period="2020-01",
            published_filename="서울특별시 공공자전거 대여이력 정보_202001.csv",
            source_url="https://data.seoul.go.kr/dataList/OA-15493/A/1/datasetView.do",
            license="공공데이터 이용허락",
        )

    assert raw_root.read_text(encoding="utf-8") == "not-a-directory"
    assert not (tmp_path / "manifest.json").exists()


def test_manifest_cannot_collide_with_reserved_raw_artifact_namespace(tmp_path):
    raw_root = tmp_path / "raw"
    colliding_manifest = raw_artifact_path(raw_root, V1_ID)

    with pytest.raises(SourceRegistrationError, match="outside the reserved Raw artifacts"):
        register_source(
            artifact=ARTIFACT_V1,
            manifest=colliding_manifest,
            raw_root=raw_root,
            dataset_id="rental_history",
            logical_period="2020-01",
            published_filename="서울특별시 공공자전거 대여이력 정보_202001.csv",
            source_url="https://data.seoul.go.kr/dataList/OA-15493/A/1/datasetView.do",
            license="공공데이터 이용허락",
        )

    assert not colliding_manifest.exists()


@pytest.mark.parametrize("alias", ["artifacts.", "artifacts "])
def test_manifest_rejects_windows_aliases_of_raw_artifact_namespace(tmp_path, alias):
    raw_root = tmp_path / "raw"
    aliased_manifest = raw_root / alias / V1_ID / "artifact"

    with pytest.raises(SourceRegistrationError, match="outside the reserved Raw artifacts"):
        register_source(
            artifact=ARTIFACT_V1,
            manifest=aliased_manifest,
            raw_root=raw_root,
            dataset_id="rental_history",
            logical_period="2020-01",
            published_filename="서울특별시 공공자전거 대여이력 정보_202001.csv",
            source_url="https://data.seoul.go.kr/dataList/OA-15493/A/1/datasetView.do",
            license="공공데이터 이용허락",
        )

    assert not raw_artifact_path(raw_root, V1_ID).exists()


def test_manifest_failure_can_leave_verified_orphan_raw_for_safe_retry(tmp_path, monkeypatch):
    def fail_manifest(*args, **kwargs):
        raise ManifestError("injected manifest write failure")

    monkeypatch.setattr(register_module, "write_manifest", fail_manifest)

    with pytest.raises(ManifestError, match="injected"):
        register(tmp_path, ARTIFACT_V1)

    stored = raw_path(tmp_path, V1_ID)
    assert stored.read_bytes() == ARTIFACT_V1.read_bytes()
    assert not (tmp_path / "manifest.json").exists()

    monkeypatch.undo()
    result = register(tmp_path, ARTIFACT_V1)
    assert result.status == "REGISTERED_NEW"
    assert stored.read_bytes() == ARTIFACT_V1.read_bytes()
