"""Filesystem path comparison helpers for the Windows-host + WSL2 contract."""

from __future__ import annotations

import os
from pathlib import Path


def filesystem_contract_parts(path: str | Path) -> tuple[str, ...]:
    """Return conservative path components for cross-Windows/WSL safety checks.

    Win32 paths are normally case-insensitive and ignore trailing dots/spaces
    in path components.  Applying those rules on every runtime prevents a path
    accepted under POSIX semantics from aliasing a reserved path on the Windows
    host filesystem.
    """
    resolved = Path(path).resolve(strict=False)
    return tuple(part.rstrip(" .").casefold() for part in resolved.parts)


def is_same_or_descendant(path: str | Path, root: str | Path) -> bool:
    """Whether ``path`` is ``root`` or below it under contract path semantics."""
    path_parts = filesystem_contract_parts(path)
    root_parts = filesystem_contract_parts(root)
    return path_parts[: len(root_parts)] == root_parts


def same_existing_file(first: str | Path, second: str | Path) -> bool:
    """Return true only when two existing paths resolve to the same file."""
    try:
        return os.path.samefile(first, second)
    except FileNotFoundError:
        return False
