"""Immutable source manifest (PRD §6).

The manifest is a single JSON object:

.. code-block:: json

    {"schema_version": 1, "records": [ ... ]}

Each record holds exactly the PRD §6 provenance fields of one immutable source
version. Records are only appended: an artifact whose bytes changed becomes an
additional version and the previous version stays in the manifest (PRD §12).

``source_version_id`` is a pure function of ``dataset_id``,
``logical_period``, ``published_filename`` and ``sha256``, so where an artifact
was downloaded to and when it was observed never change its identity.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import tempfile
from collections.abc import Iterable
from dataclasses import dataclass
from pathlib import Path

from .errors import ManifestError
from .timestamps import parse_iso8601

SCHEMA_VERSION = 1
SOURCE_VERSION_PREFIX = "sv1_"
MANIFEST_FIELDS = ("schema_version", "records")
RECORD_FIELDS = (
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
)
_STRING_FIELDS = (
    "dataset_id",
    "published_filename",
    "logical_period",
    "observed_at",
    "sha256",
    "source_url",
    "license",
    "source_version_id",
)
_SHA256_PATTERN = re.compile(r"[0-9a-f]{64}")


@dataclass(frozen=True)
class SourceRecord:
    """One immutable source version (PRD §6)."""

    dataset_id: str
    published_filename: str
    logical_period: str
    source_modified_at: str | None
    observed_at: str
    size_bytes: int
    sha256: str
    source_url: str
    license: str
    source_version_id: str

    def to_json(self) -> dict[str, object]:
        return {
            "dataset_id": self.dataset_id,
            "published_filename": self.published_filename,
            "logical_period": self.logical_period,
            "source_modified_at": self.source_modified_at,
            "observed_at": self.observed_at,
            "size_bytes": self.size_bytes,
            "sha256": self.sha256,
            "source_url": self.source_url,
            "license": self.license,
            "source_version_id": self.source_version_id,
        }

    @classmethod
    def from_json(cls, payload: object, *, source: str) -> SourceRecord:
        """Parse one manifest record, rejecting anything off the PRD §6 shape."""
        if not isinstance(payload, dict):
            raise ManifestError(f"{source}: record must be a JSON object")
        keys = set(payload)
        if keys != set(RECORD_FIELDS):
            missing = ", ".join(sorted(set(RECORD_FIELDS) - keys)) or "none"
            unknown = ", ".join(sorted(keys - set(RECORD_FIELDS))) or "none"
            raise ManifestError(
                f"{source}: record fields must be exactly the PRD §6 fields "
                f"(missing: {missing}; unknown: {unknown})"
            )
        for field in _STRING_FIELDS:
            value = payload[field]
            if not isinstance(value, str) or not value:
                raise ManifestError(f"{source}: {field} must be a non-empty string")
        size_bytes = payload["size_bytes"]
        if isinstance(size_bytes, bool) or not isinstance(size_bytes, int) or size_bytes < 0:
            raise ManifestError(f"{source}: size_bytes must be a non-negative integer")
        sha256 = payload["sha256"]
        if _SHA256_PATTERN.fullmatch(sha256) is None:
            raise ManifestError(f"{source}: sha256 must be 64 lowercase hex characters")
        source_modified_at = payload["source_modified_at"]
        for field, value in (("observed_at", payload["observed_at"]), ("source_modified_at", source_modified_at)):
            if value is None:
                continue
            if not isinstance(value, str) or not value:
                raise ManifestError(f"{source}: {field} must be null or a timestamp string")
            try:
                parse_iso8601(value)
            except ValueError as exc:
                raise ManifestError(
                    f"{source}: {field} is not an ISO-8601 timestamp: {value!r}"
                ) from exc
        expected_id = compute_source_version_id(
            payload["dataset_id"], payload["logical_period"], payload["published_filename"], sha256
        )
        if payload["source_version_id"] != expected_id:
            raise ManifestError(
                f"{source}: source_version_id {payload['source_version_id']!r} does not match "
                f"the record content ({expected_id})"
            )
        return cls(
            dataset_id=payload["dataset_id"],
            published_filename=payload["published_filename"],
            logical_period=payload["logical_period"],
            source_modified_at=payload["source_modified_at"],
            observed_at=payload["observed_at"],
            size_bytes=size_bytes,
            sha256=sha256,
            source_url=payload["source_url"],
            license=payload["license"],
            source_version_id=payload["source_version_id"],
        )


def compute_source_version_id(
    dataset_id: str, logical_period: str, published_filename: str, sha256: str
) -> str:
    """``sv1_`` plus the SHA-256 of the canonical JSON of the four identity fields."""
    canonical = json.dumps(
        {
            "dataset_id": dataset_id,
            "logical_period": logical_period,
            "published_filename": published_filename,
            "sha256": sha256,
        },
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
    )
    return SOURCE_VERSION_PREFIX + hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def load_manifest(path: str | Path) -> list[SourceRecord]:
    """Read a manifest; a missing file is an empty manifest (PRD §6)."""
    manifest_path = Path(path)
    try:
        text = manifest_path.read_text(encoding="utf-8")
    except FileNotFoundError:
        return []
    except OSError as exc:
        raise ManifestError(f"cannot read manifest {manifest_path}: {exc}") from exc
    try:
        payload = json.loads(text)
    except json.JSONDecodeError as exc:
        raise ManifestError(f"manifest {manifest_path} is not valid JSON: {exc}") from exc
    if not isinstance(payload, dict):
        raise ManifestError(f"manifest {manifest_path} must be a JSON object")
    keys = set(payload)
    if keys != set(MANIFEST_FIELDS):
        missing = ", ".join(sorted(set(MANIFEST_FIELDS) - keys)) or "none"
        unknown = ", ".join(sorted(keys - set(MANIFEST_FIELDS))) or "none"
        raise ManifestError(
            f"manifest {manifest_path} must have exactly {MANIFEST_FIELDS} "
            f"(missing: {missing}; unknown: {unknown})"
        )
    schema_version = payload["schema_version"]
    if (
        isinstance(schema_version, bool)
        or not isinstance(schema_version, int)
        or schema_version != SCHEMA_VERSION
    ):
        raise ManifestError(
            f"manifest {manifest_path} has unsupported schema_version "
            f"{schema_version!r}; expected the integer {SCHEMA_VERSION}"
        )
    records = payload["records"]
    if not isinstance(records, list):
        raise ManifestError(f"manifest {manifest_path} field records must be a JSON array")
    loaded = [
        SourceRecord.from_json(item, source=f"manifest {manifest_path} records[{index}]")
        for index, item in enumerate(records)
    ]
    seen: set[str] = set()
    for record in loaded:
        if record.source_version_id in seen:
            raise ManifestError(
                f"manifest {manifest_path} contains duplicate source_version_id "
                f"{record.source_version_id}"
            )
        seen.add(record.source_version_id)
    return loaded


def write_manifest(path: str | Path, records: Iterable[SourceRecord]) -> None:
    """Replace the manifest atomically: same-directory temp file, fsync, os.replace."""
    manifest_path = Path(path)
    payload = {
        "schema_version": SCHEMA_VERSION,
        "records": [record.to_json() for record in records],
    }
    text = json.dumps(payload, ensure_ascii=False, indent=2) + "\n"
    try:
        manifest_path.parent.mkdir(parents=True, exist_ok=True)
    except OSError as exc:
        raise ManifestError(f"cannot create manifest directory {manifest_path.parent}: {exc}") from exc
    temp_path: Path | None = None
    try:
        descriptor, temp_name = tempfile.mkstemp(
            dir=manifest_path.parent, prefix=f".{manifest_path.name}.", suffix=".tmp"
        )
        temp_path = Path(temp_name)
        with os.fdopen(descriptor, "w", encoding="utf-8", newline="\n") as handle:
            handle.write(text)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temp_path, manifest_path)
    except OSError as exc:
        raise ManifestError(f"cannot write manifest {manifest_path}: {exc}") from exc
    finally:
        _remove_temp(temp_path)


def _remove_temp(temp_path: Path | None) -> None:
    """Best-effort cleanup of a failed atomic write; the write error is the report."""
    if temp_path is None:
        return
    try:
        temp_path.unlink()
    except OSError:
        pass
