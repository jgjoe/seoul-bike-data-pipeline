"""CLI contract for ``source publish-clean-month``."""

from __future__ import annotations

import csv
import io
import os
import subprocess
import sys
from pathlib import Path

from seoul_bike_pipeline.csv_source import RENTAL_V16_HEADER
from seoul_bike_pipeline.register import register_source

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


def payload() -> bytes:
    buffer = io.StringIO(newline="")
    writer = csv.writer(buffer, lineterminator="\r\n")
    writer.writerow(RENTAL_V16_HEADER)
    writer.writerow(
        [
            "SPB-1",
            "2026-01-01 00:00:00",
            "00001",
            "대여소",
            "0",
            "2026-01-01 00:10:00",
            "00002",
            "반납소",
            "0",
            "10",
            "100.00",
            "1990",
            "M",
            "내국인",
            "ST-1",
            "ST-2",
        ]
    )
    return buffer.getvalue().encode("cp949")


def test_cli_publishes_clean_month(tmp_path):
    source = tmp_path / "source.csv"
    source.write_bytes(payload())
    registered = register_source(
        artifact=source,
        manifest=tmp_path / "source-manifest.json",
        raw_root=tmp_path / "raw",
        dataset_id="rental_history",
        logical_period="2026-01",
        published_filename="rental_2601.csv",
        source_url="https://example.invalid/2601.csv",
        license="SYNTHETIC_FIXTURE",
    )

    result = run_cli(
        "source",
        "publish-clean-month",
        "--manifest",
        str(tmp_path / "source-manifest.json"),
        "--raw-root",
        str(tmp_path / "raw"),
        "--clean-root",
        str(tmp_path / "clean"),
        "--source-version-id",
        registered.record.source_version_id,
        "--source-month",
        "2026-01",
        "--transformation-version",
        "commit-a",
        "--configuration-json",
        '{"mode":"test"}',
    )

    assert result.returncode == 0
    assert result.stderr == ""
    assert result.stdout.startswith("CLEAN_PUBLISH_OK cv1_")
    assert "status=PUBLISHED_CURRENT" in result.stdout
    assert "clean=1" in result.stdout
    assert "trusted=1" in result.stdout
    assert "quarantined=0" in result.stdout
