"""CLI contract for ``source plan-zip-months``."""

from __future__ import annotations

import os
import subprocess
import sys
import zipfile
from pathlib import Path

from seoul_bike_pipeline.register import register_source
from seoul_bike_pipeline.zip_members import inventory_registered_zip

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


def write_zip(path: Path, jan: bytes, feb: bytes) -> None:
    with zipfile.ZipFile(path, "w") as archive:
        archive.writestr("rental_202001.csv", jan)
        archive.writestr("rental_202002.csv", feb)


def register_inventory(tmp_path: Path, source: Path) -> str:
    result = register_source(
        artifact=source,
        manifest=tmp_path / "source-manifest.json",
        raw_root=tmp_path / "raw",
        dataset_id="rental_history",
        logical_period="2020",
        published_filename="year.zip",
        source_url="https://example.invalid/year.zip",
        license="SYNTHETIC_FIXTURE",
    )
    inventory_registered_zip(
        manifest=tmp_path / "source-manifest.json",
        raw_root=tmp_path / "raw",
        source_version_id=result.record.source_version_id,
    )
    return result.record.source_version_id


def test_cli_reports_sorted_month_impacts(tmp_path):
    v1 = tmp_path / "v1.zip"
    v2 = tmp_path / "v2.zip"
    write_zip(v1, b"jan", b"feb-v1")
    write_zip(v2, b"jan", b"feb-v2")
    previous = register_inventory(tmp_path, v1)
    current = register_inventory(tmp_path, v2)

    result = run_cli(
        "source",
        "plan-zip-months",
        "--manifest",
        str(tmp_path / "source-manifest.json"),
        "--raw-root",
        str(tmp_path / "raw"),
        "--current-source-version-id",
        current,
        "--previous-source-version-id",
        previous,
    )

    assert result.returncode == 0
    assert result.stderr == ""
    assert result.stdout.splitlines() == [
        "2020-01 UNCHANGED current_members=1 previous_members=1",
        "2020-02 REVISED current_members=1 previous_members=1",
    ]


def test_cli_first_parent_reports_new_months(tmp_path):
    source = tmp_path / "year.zip"
    write_zip(source, b"jan", b"feb")
    current = register_inventory(tmp_path, source)

    result = run_cli(
        "source",
        "plan-zip-months",
        "--manifest",
        str(tmp_path / "source-manifest.json"),
        "--raw-root",
        str(tmp_path / "raw"),
        "--current-source-version-id",
        current,
    )

    assert result.returncode == 0
    assert result.stdout.splitlines() == [
        "2020-01 NEW current_members=1 previous_members=0",
        "2020-02 NEW current_members=1 previous_members=0",
    ]
