"""CLI contract for ``source inspect-csv``."""

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


def cp949_v16_payload() -> bytes:
    buffer = io.StringIO(newline="")
    writer = csv.writer(buffer, lineterminator="\r\n")
    writer.writerow(RENTAL_V16_HEADER)
    writer.writerow(
        [
            "SPB-1",
            "2026-01-01 00:00:00",
            "00001",
            "테스트 대여소",
            "1",
            "2026-01-01 00:10:00",
            "00002",
            "테스트 반납소",
            "2",
            "10",
            "100.0",
            "1990",
            "M",
            "내국인",
            "ST-1",
            "ST-2",
        ]
    )
    return buffer.getvalue().encode("cp949")


def test_cli_inspects_standalone_cp949_v16_csv(tmp_path):
    source = tmp_path / "source.csv"
    source.write_bytes(cp949_v16_payload())
    registered = register_source(
        artifact=source,
        manifest=tmp_path / "source-manifest.json",
        raw_root=tmp_path / "raw",
        dataset_id="rental_history",
        logical_period="2026-01",
        published_filename="서울특별시 공공자전거 대여이력 정보_2601.csv",
        source_url="https://example.invalid/2601.csv",
        license="SYNTHETIC_FIXTURE",
    )

    result = run_cli(
        "source",
        "inspect-csv",
        "--manifest",
        str(tmp_path / "source-manifest.json"),
        "--raw-root",
        str(tmp_path / "raw"),
        "--source-version-id",
        registered.record.source_version_id,
    )

    assert result.returncode == 0
    assert result.stderr == ""
    assert result.stdout.startswith(f"CSV_OK {registered.record.source_version_id} ")
    assert "schema=rental_v16" in result.stdout
    assert "encoding=cp949" in result.stdout
    assert "rows=1" in result.stdout


def test_cli_returns_nonzero_for_unknown_schema(tmp_path):
    source = tmp_path / "source.csv"
    source.write_text("a,b\n1,2\n", encoding="utf-8")
    registered = register_source(
        artifact=source,
        manifest=tmp_path / "source-manifest.json",
        raw_root=tmp_path / "raw",
        dataset_id="rental_history",
        logical_period="2026-01",
        published_filename="unknown.csv",
        source_url="https://example.invalid/unknown.csv",
        license="SYNTHETIC_FIXTURE",
    )

    result = run_cli(
        "source",
        "inspect-csv",
        "--manifest",
        str(tmp_path / "source-manifest.json"),
        "--raw-root",
        str(tmp_path / "raw"),
        "--source-version-id",
        registered.record.source_version_id,
    )

    assert result.returncode == 1
    assert result.stdout == ""
    assert "supported UTF-8/CP949 rental header" in result.stderr
