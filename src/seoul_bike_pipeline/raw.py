"""Immutable Raw artifact preservation (PRD §15.1, §27).

Raw storage is deliberately content-preserving and filesystem-local.  The
manifest keeps provenance metadata; the Raw layer keeps the exact bytes under
a deterministic source-version path.  Existing Raw objects are verified and
never repaired or overwritten silently.
"""

from __future__ import annotations

import hashlib
import os
import tempfile
from pathlib import Path

from .errors import ArtifactError, RawArtifactError

CHUNK_SIZE = 1024 * 1024


def fingerprint_artifact(path: str | Path) -> tuple[int, str]:
    """Stream a source artifact once and return ``(size_bytes, sha256)``."""
    artifact_path = Path(path)
    digest = hashlib.sha256()
    size_bytes = 0
    try:
        with artifact_path.open("rb") as handle:
            while chunk := handle.read(CHUNK_SIZE):
                size_bytes += len(chunk)
                digest.update(chunk)
    except OSError as exc:
        raise ArtifactError(f"cannot read source artifact {artifact_path}: {exc}") from exc
    return size_bytes, digest.hexdigest()


def raw_artifact_path(raw_root: str | Path, source_version_id: str) -> Path:
    """Return the deterministic physical path for one immutable Raw object."""
    return Path(raw_root) / "artifacts" / source_version_id / "artifact"


def verify_raw_artifact(
    *,
    raw_root: str | Path,
    source_version_id: str,
    expected_size: int,
    expected_sha256: str,
) -> Path:
    """Hard-fail when the registered Raw object is missing or corrupted."""
    path = raw_artifact_path(raw_root, source_version_id)
    try:
        actual_size, actual_sha256 = _fingerprint_raw(path)
    except FileNotFoundError as exc:
        raise RawArtifactError(
            f"Raw artifact for {source_version_id} is missing at {path}"
        ) from exc
    if actual_size != expected_size or actual_sha256 != expected_sha256:
        raise RawArtifactError(
            f"Raw artifact for {source_version_id} failed checksum verification at {path}: "
            f"expected size={expected_size} sha256={expected_sha256}, "
            f"got size={actual_size} sha256={actual_sha256}"
        )
    return path


def preserve_raw_artifact(
    *,
    source: str | Path,
    raw_root: str | Path,
    source_version_id: str,
    expected_size: int,
    expected_sha256: str,
) -> Path:
    """Preserve exact bytes as an immutable Raw object before manifest publish.

    A pre-existing object is verified and reused.  New bytes are copied to a
    same-directory temporary file, verified against the fingerprint that
    defined ``source_version_id``, then exposed atomically with a hard link.
    ``os.link`` gives create-if-absent semantics, so finalization never replaces
    an existing immutable object.
    """
    source_path = Path(source)
    final_path = raw_artifact_path(raw_root, source_version_id)

    if final_path.exists():
        return verify_raw_artifact(
            raw_root=raw_root,
            source_version_id=source_version_id,
            expected_size=expected_size,
            expected_sha256=expected_sha256,
        )

    try:
        final_path.parent.mkdir(parents=True, exist_ok=True)
    except OSError as exc:
        raise RawArtifactError(
            f"cannot create Raw artifact directory {final_path.parent}: {exc}"
        ) from exc

    temp_path: Path | None = None
    try:
        descriptor, temp_name = tempfile.mkstemp(
            dir=final_path.parent, prefix=".artifact.", suffix=".tmp"
        )
    except OSError as exc:
        raise RawArtifactError(
            f"cannot create temporary Raw artifact in {final_path.parent}: {exc}"
        ) from exc
    temp_path = Path(temp_name)
    try:
        try:
            os.close(descriptor)
        except OSError as exc:
            raise RawArtifactError(
                f"cannot close temporary Raw artifact {temp_path}: {exc}"
            ) from exc

        actual_size, actual_sha256 = _copy_and_fingerprint(source_path, temp_path)
        if actual_size != expected_size or actual_sha256 != expected_sha256:
            raise RawArtifactError(
                f"source artifact changed while preserving {source_version_id}: "
                f"expected size={expected_size} sha256={expected_sha256}, "
                f"copied size={actual_size} sha256={actual_sha256}"
            )

        try:
            os.link(temp_path, final_path)
        except FileExistsError:
            # Another writer may have completed the same immutable version.
            # Concurrency control is outside this slice, but never overwrite:
            # verify the winner instead.
            return verify_raw_artifact(
                raw_root=raw_root,
                source_version_id=source_version_id,
                expected_size=expected_size,
                expected_sha256=expected_sha256,
            )
        except OSError as exc:
            raise RawArtifactError(
                f"cannot finalize immutable Raw artifact {final_path}: {exc}"
            ) from exc
        return final_path
    finally:
        _remove_temp(temp_path)


def _fingerprint_raw(path: Path) -> tuple[int, str]:
    digest = hashlib.sha256()
    size_bytes = 0
    try:
        with path.open("rb") as handle:
            while chunk := handle.read(CHUNK_SIZE):
                size_bytes += len(chunk)
                digest.update(chunk)
    except FileNotFoundError:
        raise
    except OSError as exc:
        raise RawArtifactError(f"cannot read Raw artifact {path}: {exc}") from exc
    return size_bytes, digest.hexdigest()


def _copy_and_fingerprint(source_path: Path, temp_path: Path) -> tuple[int, str]:
    digest = hashlib.sha256()
    size_bytes = 0
    try:
        source_handle = source_path.open("rb")
    except OSError as exc:
        raise ArtifactError(f"cannot read source artifact {source_path}: {exc}") from exc
    try:
        with source_handle, temp_path.open("wb") as target:
            while chunk := source_handle.read(CHUNK_SIZE):
                target.write(chunk)
                size_bytes += len(chunk)
                digest.update(chunk)
            target.flush()
            os.fsync(target.fileno())
    except OSError as exc:
        raise RawArtifactError(
            f"cannot preserve source artifact {source_path} into Raw storage: {exc}"
        ) from exc
    return size_bytes, digest.hexdigest()


def _remove_temp(temp_path: Path | None) -> None:
    if temp_path is None:
        return
    try:
        temp_path.unlink()
    except FileNotFoundError:
        pass
    except OSError:
        # The primary failure is more useful than a best-effort cleanup error.
        pass
