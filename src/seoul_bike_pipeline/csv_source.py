"""Streaming rental CSV ingestion edge with physical row provenance.

This module stops before Clean canonical transformation.  It verifies one
immutable CSV source unit (a standalone Raw CSV or one ZIP member), interprets
its source encoding, dispatches only the two PRD-approved header families, and
emits source rows without normalizing their field values.
"""

from __future__ import annotations

import csv
import hashlib
import io
import json
import zipfile
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import BinaryIO, Callable, Iterator

from .errors import CsvIngestionError, RawArtifactError
from .manifest import SourceRecord, load_manifest
from .raw import raw_artifact_path, verify_raw_artifact
from .zip_members import ZipMemberRecord, load_member_inventory, member_inventory_path

SCHEMA_RENTAL_V16 = "rental_v16"
SCHEMA_RENTAL_V17 = "rental_v17"

RENTAL_V16_HEADER = (
    "자전거번호",
    "대여일시",
    "대여 대여소번호",
    "대여 대여소명",
    "대여거치대",
    "반납일시",
    "반납대여소번호",
    "반납대여소명",
    "반납거치대",
    "이용시간(분)",
    "이용거리(M)",
    "생년",
    "성별",
    "이용자종류",
    "대여대여소ID",
    "반납대여소ID",
)
RENTAL_V17_HEADER = RENTAL_V16_HEADER + ("자전거구분",)

_SCHEMA_BY_HEADER = {
    RENTAL_V16_HEADER: SCHEMA_RENTAL_V16,
    RENTAL_V17_HEADER: SCHEMA_RENTAL_V17,
}
_ENCODING_CANDIDATES = ("utf-8-sig", "cp949")
_MAX_HEADER_BYTES = 64 * 1024
_HASH_CHUNK_SIZE = 1024 * 1024


@dataclass(frozen=True)
class RawSourceRow:
    source_version_id: str
    member_name: str
    source_row_number: int
    schema_version: str
    raw_row_id: str
    row_content_sha256: str
    values: tuple[str, ...]


@dataclass(frozen=True)
class CsvReadSummary:
    source_version_id: str
    member_name: str
    encoding: str
    schema_version: str
    row_count: int


@dataclass(frozen=True)
class _CsvSourceUnit:
    parent: SourceRecord
    raw_path: Path
    member_name: str
    member: ZipMemberRecord | None


def read_csv_source_unit(
    *,
    manifest: str | Path,
    raw_root: str | Path,
    source_version_id: str,
    member_name: str | None = None,
    on_row: Callable[[RawSourceRow], None] | None = None,
) -> CsvReadSummary:
    """Stream one rental CSV source unit and emit physical source rows.

    ``source_row_number`` is 1-based over CSV data records and excludes the
    header.  ``raw_row_id`` is a deterministic versioned hash of the PRD §8
    tuple ``(source_version_id, member_name, source_row_number)``.  The content
    hash is SHA-256 over canonical UTF-8 JSON of the decoded, unmodified field
    sequence; it is independent of CSV quoting and source text encoding.
    """
    unit = _resolve_source_unit(
        manifest=manifest,
        raw_root=raw_root,
        source_version_id=source_version_id,
        member_name=member_name,
    )
    _verify_source_unit(unit, raw_root=raw_root)
    encoding = _detect_encoding(unit)

    row_count = 0
    schema_version: str | None = None
    with _open_binary(unit) as binary_handle:
        text_handle = io.TextIOWrapper(
            binary_handle,
            encoding=encoding,
            errors="strict",
            newline="",
        )
        reader = csv.reader(text_handle, strict=True)
        try:
            header = tuple(next(reader))
        except StopIteration as exc:
            raise CsvIngestionError(
                f"CSV source unit {unit.member_name!r} is empty"
            ) from exc
        except (UnicodeDecodeError, csv.Error) as exc:
            raise CsvIngestionError(
                f"cannot parse CSV header for {unit.member_name!r}: {exc}"
            ) from exc

        schema_version = _dispatch_schema(header, unit.member_name)

        while True:
            try:
                row = next(reader)
            except StopIteration:
                break
            except (UnicodeDecodeError, csv.Error) as exc:
                raise CsvIngestionError(
                    f"structural CSV parse failure in {unit.member_name!r} after "
                    f"{row_count} data rows: {exc}"
                ) from exc

            row_count += 1
            if len(row) != len(header):
                raise CsvIngestionError(
                    f"CSV source unit {unit.member_name!r} row {row_count} has "
                    f"{len(row)} columns; expected {len(header)} for {schema_version}"
                )
            values = tuple(row)
            parsed = RawSourceRow(
                source_version_id=unit.parent.source_version_id,
                member_name=unit.member_name,
                source_row_number=row_count,
                schema_version=schema_version,
                raw_row_id=compute_raw_row_id(
                    unit.parent.source_version_id, unit.member_name, row_count
                ),
                row_content_sha256=compute_row_content_sha256(values),
                values=values,
            )
            if on_row is not None:
                on_row(parsed)

    if row_count == 0:
        raise CsvIngestionError(
            f"CSV source unit {unit.member_name!r} contains zero data rows"
        )
    assert schema_version is not None
    return CsvReadSummary(
        source_version_id=unit.parent.source_version_id,
        member_name=unit.member_name,
        encoding=encoding,
        schema_version=schema_version,
        row_count=row_count,
    )


def compute_raw_row_id(
    source_version_id: str, member_name: str, source_row_number: int
) -> str:
    """Compute deterministic PRD §8 physical row identity."""
    if source_row_number < 1:
        raise ValueError("source_row_number must be >= 1")
    payload = json.dumps(
        [source_version_id, member_name, source_row_number],
        ensure_ascii=False,
        separators=(",", ":"),
    ).encode("utf-8")
    return f"rr1_{hashlib.sha256(payload).hexdigest()}"


def compute_row_content_sha256(values: tuple[str, ...]) -> str:
    """Hash decoded source field values without canonical business normalization."""
    payload = json.dumps(
        list(values), ensure_ascii=False, separators=(",", ":")
    ).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def _resolve_source_unit(
    *,
    manifest: str | Path,
    raw_root: str | Path,
    source_version_id: str,
    member_name: str | None,
) -> _CsvSourceUnit:
    parent = next(
        (
            record
            for record in load_manifest(manifest)
            if record.source_version_id == source_version_id
        ),
        None,
    )
    if parent is None:
        raise CsvIngestionError(
            f"source_version_id {source_version_id!r} is not registered in source manifest"
        )
    raw_path = raw_artifact_path(raw_root, parent.source_version_id)

    if member_name is None:
        if not parent.published_filename.casefold().endswith(".csv"):
            raise CsvIngestionError(
                f"source version {parent.source_version_id} is not a standalone CSV; "
                "--member-name is required for ZIP sources"
            )
        return _CsvSourceUnit(
            parent=parent,
            raw_path=raw_path,
            member_name=parent.published_filename,
            member=None,
        )

    inventory_path = member_inventory_path(raw_root, parent.source_version_id)
    inventory = load_member_inventory(inventory_path)
    if inventory.parent_source_version_id != parent.source_version_id:
        raise CsvIngestionError(
            f"member inventory {inventory_path} belongs to "
            f"{inventory.parent_source_version_id!r}, not {parent.source_version_id!r}"
        )
    member = next(
        (candidate for candidate in inventory.members if candidate.member_name == member_name),
        None,
    )
    if member is None:
        raise CsvIngestionError(
            f"ZIP member {member_name!r} is not registered in inventory for "
            f"{parent.source_version_id}"
        )
    if not member.member_name.casefold().endswith(".csv"):
        raise CsvIngestionError(f"ZIP member {member.member_name!r} is not a CSV source unit")
    return _CsvSourceUnit(
        parent=parent,
        raw_path=raw_path,
        member_name=member.member_name,
        member=member,
    )


def _verify_source_unit(unit: _CsvSourceUnit, *, raw_root: str | Path) -> None:
    try:
        verify_raw_artifact(
            raw_root=raw_root,
            source_version_id=unit.parent.source_version_id,
            expected_size=unit.parent.size_bytes,
            expected_sha256=unit.parent.sha256,
        )
    except RawArtifactError as exc:
        raise CsvIngestionError(
            f"parent Raw artifact for {unit.parent.source_version_id} failed verification: {exc}"
        ) from exc

    if unit.member is None:
        return

    digest = hashlib.sha256()
    size = 0
    with _open_binary(unit) as handle:
        while chunk := handle.read(_HASH_CHUNK_SIZE):
            size += len(chunk)
            digest.update(chunk)
    actual_sha256 = digest.hexdigest()
    if size != unit.member.member_size or actual_sha256 != unit.member.member_sha256:
        raise CsvIngestionError(
            f"ZIP member {unit.member_name!r} failed inventory checksum verification: "
            f"expected size={unit.member.member_size} sha256={unit.member.member_sha256}, "
            f"got size={size} sha256={actual_sha256}"
        )


def _detect_encoding(unit: _CsvSourceUnit) -> str:
    with _open_binary(unit) as handle:
        prefix = handle.read(_MAX_HEADER_BYTES + 1)
    if not prefix:
        raise CsvIngestionError(f"CSV source unit {unit.member_name!r} is empty")

    newline_positions = [
        position for marker in (b"\r", b"\n") if (position := prefix.find(marker)) >= 0
    ]
    if newline_positions:
        header_bytes = prefix[: min(newline_positions)]
    else:
        header_bytes = prefix
    if not newline_positions and len(prefix) > _MAX_HEADER_BYTES:
        raise CsvIngestionError(
            f"CSV header for {unit.member_name!r} exceeds {_MAX_HEADER_BYTES} bytes"
        )

    for encoding in _ENCODING_CANDIDATES:
        try:
            decoded = header_bytes.decode(encoding)
        except UnicodeDecodeError:
            continue
        try:
            parsed = next(csv.reader([decoded], strict=True))
        except csv.Error as exc:
            raise CsvIngestionError(
                f"cannot structurally parse CSV header for {unit.member_name!r}: {exc}"
            ) from exc
        if tuple(parsed) in _SCHEMA_BY_HEADER:
            return encoding

    raise CsvIngestionError(
        f"CSV source unit {unit.member_name!r} does not match a supported "
        "UTF-8/CP949 rental header"
    )


def _dispatch_schema(header: tuple[str, ...], member_name: str) -> str:
    schema_version = _SCHEMA_BY_HEADER.get(header)
    if schema_version is None:
        raise CsvIngestionError(
            f"unsupported rental CSV header in {member_name!r}: "
            f"{len(header)} columns {header!r}"
        )
    return schema_version


@contextmanager
def _open_binary(unit: _CsvSourceUnit) -> Iterator[BinaryIO]:
    if unit.member is None:
        try:
            handle = unit.raw_path.open("rb")
        except OSError as exc:
            raise CsvIngestionError(
                f"cannot read standalone Raw CSV {unit.raw_path}: {exc}"
            ) from exc
        with handle:
            yield handle
        return

    try:
        archive = zipfile.ZipFile(unit.raw_path, "r")
    except (zipfile.BadZipFile, zipfile.LargeZipFile, OSError) as exc:
        raise CsvIngestionError(
            f"cannot open Raw ZIP {unit.raw_path}: {exc}"
        ) from exc

    with archive:
        try:
            info = archive.getinfo(unit.member_name)
        except KeyError as exc:
            raise CsvIngestionError(
                f"ZIP member {unit.member_name!r} is missing from Raw artifact {unit.raw_path}"
            ) from exc
        if info.file_size != unit.member.member_size:
            raise CsvIngestionError(
                f"ZIP member {unit.member_name!r} size metadata changed: "
                f"inventory={unit.member.member_size}, zip={info.file_size}"
            )
        try:
            handle = archive.open(info, "r")
        except (RuntimeError, NotImplementedError, OSError, zipfile.BadZipFile) as exc:
            raise CsvIngestionError(
                f"cannot read ZIP member {unit.member_name!r} from {unit.raw_path}: {exc}"
            ) from exc
        with handle:
            yield handle
