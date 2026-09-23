"""Map ZIP member inventories to logical rental months and revision impacts.

This layer is intentionally metadata-only.  It does not open CSV rows.  It
maps observed Seoul source filename variants to logical months, groups one or
more source members per month, and compares two immutable parent ZIP versions
to decide which month partitions are new, unchanged, revised, or removed.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path

from .errors import ZipMonthMappingError
from .manifest import SourceRecord, load_manifest
from .zip_members import ZipMemberInventory, ZipMemberRecord, load_member_inventory, member_inventory_path

STATUS_NEW = "NEW"
STATUS_UNCHANGED = "UNCHANGED"
STATUS_REVISED = "REVISED"
STATUS_REMOVED = "REMOVED"

_YEAR_PATTERN = re.compile(r"20\d{2}")
_MONTH = r"(?:0[1-9]|1[0-2])"
_SHORT_YEAR = r"(?:1[5-9]|2\d)"
_DATE_TOKEN = re.compile(
    rf"(?<!\d)(?:20\d{{2}}|{_SHORT_YEAR})[._-]?{_MONTH}(?!\d)"
)

# Observed source naming families include 202006, 2020.12, 2112, 22.06,
# 2606, the compact 22081 anomaly, and 2020.07~08.  Keep the patterns
# end-anchored so unrelated digits in the descriptive prefix are ignored.
_FULL_RANGE = re.compile(
    rf"(?<!\d)(?P<year>20\d{{2}})[._-](?P<start>{_MONTH})\s*~\s*(?P<end>{_MONTH})$"
)
_FULL_SINGLE = re.compile(
    rf"(?<!\d)(?P<year>20\d{{2}})[._-]?(?P<month>{_MONTH})(?:_\d{{1,2}})?$"
)
_SHORT_SINGLE = re.compile(
    rf"(?<!\d)(?P<year>{_SHORT_YEAR})[._-]?(?P<month>{_MONTH})(?:_\d{{1,2}})?$"
)
_SHORT_COMPACT_PART = re.compile(
    rf"(?<!\d)(?P<year>{_SHORT_YEAR})(?P<month>0[1-9]|1[0-2])(?P<part>[1-9])$"
)


@dataclass(frozen=True)
class MonthImpact:
    logical_month: str
    status: str
    current_members: tuple[ZipMemberRecord, ...]
    previous_members: tuple[ZipMemberRecord, ...]


@dataclass(frozen=True)
class ZipMonthPlan:
    current_source_version_id: str
    previous_source_version_id: str | None
    impacts: tuple[MonthImpact, ...]


def months_for_member_name(member_name: str) -> tuple[str, ...] | None:
    """Return logical months for one CSV member, or ``None`` for non-CSV files.

    A recognized range such as ``2020.07~08.csv`` maps conservatively to both
    months.  Later row parsing must split rows by rental month; this metadata
    layer only determines which logical partitions a changed source member can
    impact.
    """
    basename = member_name.replace("\\", "/").rsplit("/", 1)[-1]
    if not basename.casefold().endswith(".csv"):
        return None
    stem = basename[:-4]

    # Do not let a suffix-oriented regex silently pick the last of multiple
    # independent date tokens (for example ``202001-202002``).  Explicit
    # same-year ranges are handled below and contain only one year-bearing
    # token by contract.
    if len(_DATE_TOKEN.findall(stem)) > 1:
        raise ZipMonthMappingError(
            f"CSV member {member_name!r} contains multiple independent month tokens"
        )

    match = _FULL_RANGE.search(stem)
    if match is not None:
        year = int(match.group("year"))
        start = int(match.group("start"))
        end = int(match.group("end"))
        if end < start:
            raise ZipMonthMappingError(
                f"CSV member {member_name!r} has unsupported descending/cross-year month range"
            )
        return tuple(_logical_month(year, month) for month in range(start, end + 1))

    match = _FULL_SINGLE.search(stem)
    if match is not None:
        return (_logical_month(int(match.group("year")), int(match.group("month"))),)

    # Check the compact suffix form before the ordinary YYMM form.  The
    # official source has historically exposed names such as ``22081`` where
    # the first four digits are YYMM and the trailing digit is a part/suffix.
    match = _SHORT_COMPACT_PART.search(stem)
    if match is not None:
        return (
            _logical_month(2000 + int(match.group("year")), int(match.group("month"))),
        )

    match = _SHORT_SINGLE.search(stem)
    if match is not None:
        return (
            _logical_month(2000 + int(match.group("year")), int(match.group("month"))),
        )

    raise ZipMonthMappingError(
        f"CSV member {member_name!r} does not match a supported single-month or month-range filename"
    )


def plan_zip_months(
    *,
    manifest: str | Path,
    raw_root: str | Path,
    current_source_version_id: str,
    previous_source_version_id: str | None = None,
) -> ZipMonthPlan:
    """Compare explicit parent ZIP versions and return deterministic month impacts."""
    records = load_manifest(manifest)
    current_index, current_record = _find_source_record(
        records, current_source_version_id, role="current"
    )
    _require_yearly_parent(current_record, role="current")

    previous_record: SourceRecord | None = None
    if previous_source_version_id is not None:
        previous_index, previous_record = _find_source_record(
            records, previous_source_version_id, role="previous"
        )
        _require_yearly_parent(previous_record, role="previous")
        if previous_record.source_version_id == current_record.source_version_id:
            raise ZipMonthMappingError("current and previous source versions must be different")
        if previous_index >= current_index:
            raise ZipMonthMappingError(
                "previous source version must appear before current source version in the append-only manifest"
            )
        if (
            previous_record.dataset_id != current_record.dataset_id
            or previous_record.logical_period != current_record.logical_period
        ):
            raise ZipMonthMappingError(
                "current and previous source versions must describe the same dataset_id and logical_period"
            )

    current_inventory = _load_inventory_for_parent(raw_root, current_record.source_version_id)
    current_months = _group_members_by_month(
        current_inventory, expected_year=int(current_record.logical_period)
    )

    previous_months: dict[str, tuple[ZipMemberRecord, ...]] = {}
    if previous_record is not None:
        previous_inventory = _load_inventory_for_parent(
            raw_root, previous_record.source_version_id
        )
        previous_months = _group_members_by_month(
            previous_inventory, expected_year=int(previous_record.logical_period)
        )

    impacts: list[MonthImpact] = []
    for logical_month in sorted(set(current_months) | set(previous_months)):
        current_members = current_months.get(logical_month, ())
        previous_members = previous_months.get(logical_month, ())
        if not previous_members:
            status = STATUS_NEW
        elif not current_members:
            status = STATUS_REMOVED
        elif _content_signature(current_members) == _content_signature(previous_members):
            status = STATUS_UNCHANGED
        else:
            status = STATUS_REVISED
        impacts.append(
            MonthImpact(
                logical_month=logical_month,
                status=status,
                current_members=current_members,
                previous_members=previous_members,
            )
        )

    return ZipMonthPlan(
        current_source_version_id=current_record.source_version_id,
        previous_source_version_id=(
            previous_record.source_version_id if previous_record is not None else None
        ),
        impacts=tuple(impacts),
    )


def _find_source_record(
    records: list[SourceRecord], source_version_id: str, *, role: str
) -> tuple[int, SourceRecord]:
    for index, record in enumerate(records):
        if record.source_version_id == source_version_id:
            return index, record
    raise ZipMonthMappingError(
        f"{role} source_version_id {source_version_id!r} is not registered in the source manifest"
    )


def _require_yearly_parent(record: SourceRecord, *, role: str) -> None:
    if _YEAR_PATTERN.fullmatch(record.logical_period) is None:
        raise ZipMonthMappingError(
            f"{role} source version {record.source_version_id} has logical_period "
            f"{record.logical_period!r}; ZIP month planning requires a YYYY yearly parent"
        )


def _load_inventory_for_parent(
    raw_root: str | Path, source_version_id: str
) -> ZipMemberInventory:
    path = member_inventory_path(raw_root, source_version_id)
    inventory = load_member_inventory(path)
    if inventory.parent_source_version_id != source_version_id:
        raise ZipMonthMappingError(
            f"member inventory {path} belongs to {inventory.parent_source_version_id!r}, "
            f"not requested parent {source_version_id!r}"
        )
    return inventory


def _group_members_by_month(
    inventory: ZipMemberInventory, *, expected_year: int
) -> dict[str, tuple[ZipMemberRecord, ...]]:
    grouped: dict[str, list[ZipMemberRecord]] = {}
    for member in inventory.members:
        months = months_for_member_name(member.member_name)
        if months is None:
            continue
        for logical_month in months:
            if int(logical_month[:4]) != expected_year:
                raise ZipMonthMappingError(
                    f"member {member.member_name!r} maps to {logical_month}, outside yearly parent "
                    f"{expected_year}"
                )
            grouped.setdefault(logical_month, []).append(member)

    if not grouped:
        raise ZipMonthMappingError(
            f"member inventory for {inventory.parent_source_version_id} contains no recognized monthly CSV members"
        )
    return {
        logical_month: tuple(sorted(members, key=lambda member: member.member_name))
        for logical_month, members in sorted(grouped.items())
    }


def _content_signature(members: tuple[ZipMemberRecord, ...]) -> tuple[tuple[int, str], ...]:
    """Content-only multiset used for downstream unchanged-month detection.

    Parent source version and member name remain provenance identity, but PRD
    §7 says a month need not be reprocessed when its member content hash is
    unchanged across a parent ZIP revision.  The multiset also supports months
    represented by multiple source parts.
    """
    return tuple(sorted((member.member_size, member.member_sha256) for member in members))


def _logical_month(year: int, month: int) -> str:
    return f"{year:04d}-{month:02d}"
