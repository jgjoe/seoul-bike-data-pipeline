"""CLI contract for ``source project-csv``."""

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


def payload(rent_at: str) -> bytes:
    buffer = io.StringIO(newline="")
    writer = csv.writer(buffer, lineterminator="\r\n")
    writer.writerow(RENTAL_V16_HEADER)
    writer.writerow(
        [
            "SPB-1",
            rent_at,
            "00001",
            "테스트 대여소",
            "0",
            "2026-01-01 00:10:00",
            "00002",
            "테스트 반납소",
            "0",
            "10",
            "100.00",
            "",
            "",
            "내국인",
            "ST-1",
            "ST-2",
        ]
    )
    return buffer.getvalue().encode("cp949")


def register(tmp_path: Path, rent_at: str) -> str:
    source = tmp_path / "source.csv"
    source.write_bytes(payload(rent_at))
    result = register_source(
        artifact=source,
        manifest=tmp_path / "source-manifest.json",
        raw_root=tmp_path / "raw",
        dataset_id="rental_history",
        logical_period="2026-01",
        published_filename="서울특별시 공공자전거 대여이력 정보_2601.csv",
        source_url="https://example.invalid/2601.csv",
        license="SYNTHETIC_FIXTURE",
    )
    return result.record.source_version_id


def test_cli_reports_typed_projection_summary(tmp_path):
    source_version_id = register(tmp_path, "2026-01-01 00:00:00")

    result = run_cli(
        "source",
        "project-csv",
        "--manifest",
        str(tmp_path / "source-manifest.json"),
        "--raw-root",
        str(tmp_path / "raw"),
        "--source-version-id",
        source_version_id,
    )

    assert result.returncode == 0
    assert result.stderr == ""
    assert result.stdout.startswith(f"PROJECT_OK {source_version_id} ")
    assert "schema=rental_v16" in result.stdout
    assert "rows=1" in result.stdout
    assert "conversion_issues=0" in result.stdout
    assert "month_match=1" in result.stdout
    assert "month_mismatch=0" in result.stdout
    assert "source_months=2026-01" in result.stdout


def test_cli_reports_month_mismatch_without_structural_failure(tmp_path):
    source_version_id = register(tmp_path, "2026-02-01 00:00:00")

    result = run_cli(
        "source",
        "project-csv",
        "--manifest",
        str(tmp_path / "source-manifest.json"),
        "--raw-root",
        str(tmp_path / "raw"),
        "--source-version-id",
        source_version_id,
    )

    assert result.returncode == 0
    assert "month_match=0" in result.stdout
    assert "month_mismatch=1" in result.stdout
