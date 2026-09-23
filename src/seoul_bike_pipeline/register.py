"""Register one source artifact as an immutable source version (PRD §6, §12, §15.1).

The artifact is fingerprinted, preserved byte-for-byte in immutable Raw
storage, then registered in the source manifest. Registration reports one of
exactly three statuses:

``REGISTERED_NEW``
    the first source version of this ``(dataset_id, logical_period)``.
``KNOWN_UNCHANGED``
    this exact content is already registered; the original record is returned
    and the manifest is not rewritten.
``REGISTERED_REVISION``
    the same ``(dataset_id, logical_period)`` is already registered under a
    different ``source_version_id`` (changed bytes or a changed published
    filename); the new version is appended and the previous one is preserved
    (PRD §12).
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from .errors import SourceRegistrationError
from .manifest import SourceRecord, compute_source_version_id, load_manifest, write_manifest
from .path_safety import is_same_or_descendant, same_existing_file
from .raw import fingerprint_artifact, preserve_raw_artifact, verify_raw_artifact
from .timestamps import to_utc, utc_now

STATUS_NEW = "REGISTERED_NEW"
STATUS_UNCHANGED = "KNOWN_UNCHANGED"
STATUS_REVISION = "REGISTERED_REVISION"


@dataclass(frozen=True)
class RegistrationResult:
    """Outcome of one registration attempt."""

    status: str
    record: SourceRecord


def register_source(
    *,
    artifact: str | Path,
    manifest: str | Path,
    raw_root: str | Path,
    dataset_id: str,
    logical_period: str,
    published_filename: str,
    source_url: str,
    license: str,
    source_modified_at: str | None = None,
) -> RegistrationResult:
    """Register `artifact` in the manifest at `manifest`."""
    dataset_id = _require_text(dataset_id, "dataset_id")
    logical_period = _require_text(logical_period, "logical_period")
    published_filename = _require_text(published_filename, "published_filename")
    source_url = _require_text(source_url, "source_url")
    license = _require_text(license, "license")
    normalized_modified_at = _normalize_source_modified_at(source_modified_at)
    _require_manifest_outside_reserved_raw_namespaces(manifest, raw_root)

    artifact_path = Path(artifact)
    size_bytes, sha256 = fingerprint_artifact(artifact_path)
    source_version_id = compute_source_version_id(
        dataset_id, logical_period, published_filename, sha256
    )

    records = load_manifest(manifest)
    known = next(
        (record for record in records if record.source_version_id == source_version_id), None
    )
    if known is not None:
        verify_raw_artifact(
            raw_root=raw_root,
            source_version_id=known.source_version_id,
            expected_size=known.size_bytes,
            expected_sha256=known.sha256,
        )
        return RegistrationResult(status=STATUS_UNCHANGED, record=known)

    # The identical source_version_id already returned above; any other version
    # of this (dataset_id, logical_period) is a historical revision (PRD §12).
    revision = any(
        record.dataset_id == dataset_id and record.logical_period == logical_period
        for record in records
    )
    preserved_path = preserve_raw_artifact(
        source=artifact_path,
        raw_root=raw_root,
        source_version_id=source_version_id,
        expected_size=size_bytes,
        expected_sha256=sha256,
    )
    # Re-check after Raw publication.  On Windows/NTFS, aliases such as
    # ``artifacts.`` can resolve differently once the canonical directory
    # exists.  A second check prevents manifest publication from replacing an
    # immutable Raw object through filesystem aliasing.
    _require_manifest_outside_reserved_raw_namespaces(manifest, raw_root)
    _require_manifest_not_same_file(manifest, preserved_path)
    record = SourceRecord(
        dataset_id=dataset_id,
        published_filename=published_filename,
        logical_period=logical_period,
        source_modified_at=normalized_modified_at,
        observed_at=utc_now(),
        size_bytes=size_bytes,
        sha256=sha256,
        source_url=source_url,
        license=license,
        source_version_id=source_version_id,
    )
    write_manifest(manifest, [*records, record])
    return RegistrationResult(status=STATUS_REVISION if revision else STATUS_NEW, record=record)


def _require_text(value: str, name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise SourceRegistrationError(f"{name} must be a non-empty string")
    return value


def _normalize_source_modified_at(value: str | None) -> str | None:
    if value is None:
        return None
    try:
        return to_utc(value)
    except ValueError as exc:
        raise SourceRegistrationError(
            f"source_modified_at is not an ISO-8601 timestamp: {value!r}"
        ) from exc


def _require_manifest_outside_reserved_raw_namespaces(
    manifest: str | Path, raw_root: str | Path
) -> None:
    """Keep the source manifest out of generated Raw namespaces."""
    reserved = (
        (Path(raw_root) / "artifacts", "Raw artifacts"),
        (Path(raw_root) / "member-inventories", "Raw member inventories"),
    )
    for namespace, label in reserved:
        if is_same_or_descendant(manifest, namespace):
            raise SourceRegistrationError(
                f"manifest path must be outside the reserved {label} namespace {namespace}"
            )


def _require_manifest_not_same_file(manifest: str | Path, raw_path: Path) -> None:
    """Reject filesystem aliases that resolve to the just-published Raw file."""
    try:
        aliases_raw = same_existing_file(manifest, raw_path)
    except OSError as exc:
        raise SourceRegistrationError(
            f"cannot validate manifest path against immutable Raw artifact {raw_path}: {exc}"
        ) from exc
    if aliases_raw:
        raise SourceRegistrationError(
            f"manifest path aliases immutable Raw artifact {raw_path}"
        )
