"""CLI contract for ``source inventory-zip``."""

from __future__ import annotations

import json
import os
import subprocess
import sys
import zipfile
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]


def run_cli(*arguments: str) -> subprocess.CompletedProcess[str]:
    environment = dict(os.environ)
    source_root = str(REPO_ROOT / "src")
    existing = environment.get("PYTHONPATH")
    environment["PYTHONPATH"] = f"{source_root}{os.pathsep}{existing}" if existing else source_root
    return subprocess.run(
        [sys.executable, "-m", "seoul_bike_pipeline", *arguments],
        cwd=REPO_ROOT,
        env=environment,
        capture_output=True,
        text=True,
        encoding="utf-8",
    )


def write_zip(path: Path) -> None:
    with zipfile.ZipFile(path, "w") as archive:
        archive.writestr("2020/rental_202001.csv", b"bike_id,rent_at\nA,2020-01-01\n")
        archive.writestr("2020/rental_202002.csv", b"bike_id,rent_at\nB,2020-02-01\n")


def test_cli_inventories_registered_zip_then_reports_unchanged(tmp_path):
    source = tmp_path / "year.zip"
    manifest = tmp_path / "source-manifest.json"
    raw_root = tmp_path / "raw"
    write_zip(source)

    registered = run_cli(
        "source",
        "register",
        str(source),
        "--manifest",
        str(manifest),
        "--raw-root",
        str(raw_root),
        "--dataset-id",
        "rental_history",
        "--logical-period",
        "2020",
        "--published-filename",
        "year.zip",
        "--source-url",
        "https://example.invalid/year.zip",
        "--license",
        "SYNTHETIC_FIXTURE",
    )
    assert registered.returncode == 0
    source_version_id = registered.stdout.strip().split()[1]

    arguments = (
        "source",
        "inventory-zip",
        "--manifest",
        str(manifest),
        "--raw-root",
        str(raw_root),
        "--source-version-id",
        source_version_id,
    )
    first = run_cli(*arguments)
    second = run_cli(*arguments)

    assert first.returncode == second.returncode == 0
    assert first.stdout == f"INVENTORIED_NEW {source_version_id} members=2\n"
    assert second.stdout == f"INVENTORY_UNCHANGED {source_version_id} members=2\n"
    assert first.stderr == second.stderr == ""

    inventory_path = raw_root / "member-inventories" / source_version_id / "inventory.json"
    payload = json.loads(inventory_path.read_text(encoding="utf-8"))
    assert [member["member_name"] for member in payload["members"]] == [
        "2020/rental_202001.csv",
        "2020/rental_202002.csv",
    ]


def test_cli_inventory_zip_requires_registered_parent(tmp_path):
    result = run_cli(
        "source",
        "inventory-zip",
        "--manifest",
        str(tmp_path / "absent.json"),
        "--raw-root",
        str(tmp_path / "raw"),
        "--source-version-id",
        "sv1_missing",
    )

    assert result.returncode == 1
    assert result.stdout == ""
    assert "is not registered" in result.stderr
