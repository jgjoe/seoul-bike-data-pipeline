"""Deterministic ZIP member inventory for registered Raw source versions.

The source manifest remains the PRD §6 artifact manifest.  A yearly ZIP gets a
separate per-parent inventory containing exactly the ZIP member identity
material required by PRD §7: parent source version + member name + uncompressed
member size + member SHA-256.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import tempfile
import zipfile
from dataclasses import dataclass
from pathlib import Path

from .errors import ZipInventoryError
from .manifest import load_manifest
from .path_safety import is_same_or_descendant, same_existing_file
from .raw import verify_raw_artifact

SCHEMA_VERSION = 1
INVENTORY_FIELDS = ("schema_version", "parent_source_version_id", "members")
MEMBER_FIELDS = ("member_name", "member_size", "member_sha256")
_SHA256_PATTERN = re.compile(r"[0-9a-f]{64}")
_CHUNK_SIZE = 1024 * 1024

STATUS_NEW = "INVENTORIED_NEW"
STATUS_UNCHANGED = "INVENTORY_UNCHANGED"


@dataclass(frozen=True)
class ZipMemberRecord:
    member_name: str
    member_size: int
    member_sha256: str

    def to_json(self) -> dict[str, object]:
        return {
            "member_name": self.member_name,
            "member_size": self.member_size,
            "member_sha256": self.member_sha256,
        }


@dataclass(frozen=True)
class ZipMemberInventory:
    parent_source_version_id: str
    members: tuple[ZipMemberRecord, ...]

    def to_json(self) -> dict[str, object]:
        return {
            "schema_version": SCHEMA_VERSION,
            "parent_source_version_id": self.parent_source_version_id,
            "members": [member.to_json() for member in self.members],
        }


@dataclass(frozen=True)
class ZipInventoryResult:
    status: str
    inventory: ZipMemberInventory
    path: Path


def member_inventory_path(raw_root: str | Path, parent_source_version_id: str) -> Path:
    return Path(raw_root) / "member-inventories" / parent_source_version_id / "inventory.json"


def inventory_registered_zip(
    *, manifest: str | Path, raw_root: str | Path, source_version_id: str
) -> ZipInventoryResult:
    """Verify a registered Raw ZIP, inventory its members, and persist metadata."""
    parent = next(
        (record for record in load_manifest(manifest) if record.source_version_id == source_version_id),
        None,
    )
    if parent is None:
        raise ZipInventoryError(
            f"source_version_id {source_version_id!r} is not registered in manifest {manifest}"
        )

    raw_path = verify_raw_artifact(
        raw_root=raw_root,
        source_version_id=parent.source_version_id,
        expected_size=parent.size_bytes,
        expected_sha256=parent.sha256,
    )
    inventory = ZipMemberInventory(
        parent_source_version_id=parent.source_version_id,
        members=_read_zip_members(raw_path),
    )
    inventory_path = member_inventory_path(raw_root, parent.source_version_id)
    _require_inventory_path_separate_from_manifest(manifest, inventory_path)

    if inventory_path.exists():
        existing = load_member_inventory(inventory_path)
        if existing != inventory:
            raise ZipInventoryError(
                f"existing member inventory {inventory_path} does not match registered Raw ZIP "
                f"{parent.source_version_id}; refusing to overwrite"
            )
        return ZipInventoryResult(STATUS_UNCHANGED, existing, inventory_path)

    _write_member_inventory(inventory_path, inventory, manifest=manifest)
    return ZipInventoryResult(STATUS_NEW, inventory, inventory_path)


def load_member_inventory(path: str | Path) -> ZipMemberInventory:
    inventory_path = Path(path)
    try:
        text = inventory_path.read_text(encoding="utf-8")
    except FileNotFoundError as exc:
        raise ZipInventoryError(f"member inventory does not exist: {inventory_path}") from exc
    except OSError as exc:
        raise ZipInventoryError(f"cannot read member inventory {inventory_path}: {exc}") from exc
    try:
        payload = json.loads(
            text,
            object_pairs_hook=lambda pairs: _strict_json_object(
                pairs, source=f"member inventory {inventory_path}"
            ),
        )
    except json.JSONDecodeError as exc:
        raise ZipInventoryError(f"member inventory {inventory_path} is not valid JSON: {exc}") from exc

    if not isinstance(payload, dict) or set(payload) != set(INVENTORY_FIELDS):
        raise ZipInventoryError(
            f"member inventory {inventory_path} must contain exactly {INVENTORY_FIELDS}"
        )
    schema_version = payload["schema_version"]
    if isinstance(schema_version, bool) or not isinstance(schema_version, int) or schema_version != 1:
        raise ZipInventoryError(
            f"member inventory {inventory_path} has unsupported schema_version {schema_version!r}"
        )
    parent_source_version_id = payload["parent_source_version_id"]
    if not isinstance(parent_source_version_id, str) or not parent_source_version_id:
        raise ZipInventoryError(
            f"member inventory {inventory_path}: parent_source_version_id must be a non-empty string"
        )
    raw_members = payload["members"]
    if not isinstance(raw_members, list) or not raw_members:
        raise ZipInventoryError(
            f"member inventory {inventory_path}: members must be a non-empty array"
        )

    members: list[ZipMemberRecord] = []
    seen_names: set[str] = set()
    for index, raw_member in enumerate(raw_members):
        source = f"member inventory {inventory_path} members[{index}]"
        if not isinstance(raw_member, dict) or set(raw_member) != set(MEMBER_FIELDS):
            raise ZipInventoryError(f"{source} must contain exactly {MEMBER_FIELDS}")
        name = raw_member["member_name"]
        size = raw_member["member_size"]
        sha256 = raw_member["member_sha256"]
        if not isinstance(name, str) or not name:
            raise ZipInventoryError(f"{source}: member_name must be a non-empty string")
        if name in seen_names:
            raise ZipInventoryError(f"{source}: duplicate member_name {name!r}")
        seen_names.add(name)
        if isinstance(size, bool) or not isinstance(size, int) or size < 0:
            raise ZipInventoryError(f"{source}: member_size must be a non-negative integer")
        if not isinstance(sha256, str) or _SHA256_PATTERN.fullmatch(sha256) is None:
            raise ZipInventoryError(f"{source}: member_sha256 must be 64 lowercase hex characters")
        members.append(ZipMemberRecord(name, size, sha256))

    if members != sorted(members, key=lambda member: member.member_name):
        raise ZipInventoryError(
            f"member inventory {inventory_path}: members must be sorted by member_name"
        )
    return ZipMemberInventory(parent_source_version_id, tuple(members))


def _strict_json_object(pairs: list[tuple[str, object]], *, source: str) -> dict[str, object]:
    """Build a JSON object while rejecting duplicate keys at every nesting level."""
    payload: dict[str, object] = {}
    for key, value in pairs:
        if key in payload:
            raise ZipInventoryError(f"{source}: duplicate JSON object key {key!r}")
        payload[key] = value
    return payload


def _read_zip_members(path: Path) -> tuple[ZipMemberRecord, ...]:
    members: list[ZipMemberRecord] = []
    seen_names: set[str] = set()
    try:
        with zipfile.ZipFile(path, "r") as archive:
            for info in archive.infolist():
                if info.is_dir():
                    continue
                if not info.filename:
                    raise ZipInventoryError(f"ZIP {path} contains an unnamed file member")
                if info.filename in seen_names:
                    raise ZipInventoryError(
                        f"ZIP {path} contains duplicate member_name {info.filename!r}; "
                        "raw row identity would be ambiguous"
                    )
                seen_names.add(info.filename)
                digest = hashlib.sha256()
                size = 0
                with archive.open(info, "r") as handle:
                    while chunk := handle.read(_CHUNK_SIZE):
                        size += len(chunk)
                        digest.update(chunk)
                if size != info.file_size:
                    raise ZipInventoryError(
                        f"ZIP member {info.filename!r} size mismatch: metadata={info.file_size}, read={size}"
                    )
                members.append(ZipMemberRecord(info.filename, size, digest.hexdigest()))
    except ZipInventoryError:
        raise
    except (zipfile.BadZipFile, zipfile.LargeZipFile, RuntimeError, NotImplementedError, OSError) as exc:
        raise ZipInventoryError(f"cannot inventory ZIP {path}: {exc}") from exc

    if not members:
        raise ZipInventoryError(f"ZIP {path} contains no file members")
    return tuple(sorted(members, key=lambda member: member.member_name))


def _write_member_inventory(
    path: Path, inventory: ZipMemberInventory, *, manifest: str | Path
) -> None:
    _require_inventory_path_separate_from_manifest(manifest, path)
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
    except OSError as exc:
        raise ZipInventoryError(f"cannot create member inventory directory {path.parent}: {exc}") from exc
    _require_inventory_path_separate_from_manifest(manifest, path)

    text = json.dumps(inventory.to_json(), ensure_ascii=False, indent=2) + "\n"
    temp_path: Path | None = None
    try:
        descriptor, temp_name = tempfile.mkstemp(
            dir=path.parent, prefix=f".{path.name}.", suffix=".tmp"
        )
        temp_path = Path(temp_name)
        with os.fdopen(descriptor, "w", encoding="utf-8", newline="\n") as handle:
            handle.write(text)
            handle.flush()
            os.fsync(handle.fileno())
        if same_existing_file(manifest, path):
            raise ZipInventoryError(
                f"member inventory path {path} aliases source manifest {manifest}; refusing to overwrite"
            )
        try:
            os.link(temp_path, path)
        except FileExistsError:
            existing = load_member_inventory(path)
            if existing != inventory:
                raise ZipInventoryError(
                    f"member inventory {path} appeared with different content; refusing to overwrite"
                )
    except ZipInventoryError:
        raise
    except OSError as exc:
        raise ZipInventoryError(f"cannot write member inventory {path}: {exc}") from exc
    finally:
        if temp_path is not None:
            try:
                temp_path.unlink()
            except OSError:
                pass


def _require_inventory_path_separate_from_manifest(
    manifest: str | Path, inventory_path: Path
) -> None:
    inventory_namespace = inventory_path.parents[1]
    if is_same_or_descendant(manifest, inventory_namespace):
        raise ZipInventoryError(
            f"source manifest {manifest} must be outside Raw member inventory namespace "
            f"{inventory_namespace}"
        )
