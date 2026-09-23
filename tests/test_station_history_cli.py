from __future__ import annotations

import csv
import os
import subprocess
import sys
from pathlib import Path

from seoul_bike_pipeline.register import register_source
from seoul_bike_pipeline.station_snapshot import publish_station_snapshot

REPO_ROOT = Path(__file__).resolve().parents[1]

HEADER = [
    ["대여소\n번호", "보관소(대여소)명", "소재지(위치)", "", "", "", "설치\n시기", "설치형태", "", "운영\n방식"],
    ["", "", "", "", "", "", "", "LCD", "QR", ""],
    ["", "", "자치구", "상세주소", "위도", "경도", "", "", "", ""],
    ["", "", "", "", "", "", "", "거치\n대수", "거치\n대수", ""],
    ["", "", "", "", "", "", "", "", "", ""],
]


def run_cli(*arguments: str) -> subprocess.CompletedProcess[str]:
    environment = dict(os.environ)
    source_root = str(REPO_ROOT / "src")
    existing = environment.get("PYTHONPATH")
    environment["PYTHONPATH"] = (
        f"{source_root}{os.pathsep}{existing}" if existing else source_root
    )
    return subprocess.run(
        [sys.executable, "-m", "seoul_bike_pipeline", *arguments],
        cwd=REPO_ROOT,
        env=environment,
        capture_output=True,
        text=True,
        encoding="utf-8",
        check=False,
    )


def publish_snapshot(tmp_path: Path, period: str, filename: str, suffix: str) -> None:
    artifact = tmp_path / f"{suffix}.csv"
    with artifact.open("w", encoding="cp949", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerows(HEADER)
        writer.writerow(
            [
                "00102",
                "망원역",
                "마포구",
                "서울특별시 마포구 월드컵로",
                "37.55",
                "126.91",
                "2020-01-01",
                "",
                "15",
                "QR",
            ]
        )
    registered = register_source(
        artifact=artifact,
        manifest=tmp_path / "manifest.json",
        raw_root=tmp_path / "raw",
        dataset_id="station_snapshot",
        logical_period=period,
        published_filename=filename,
        source_url="https://example.invalid/station",
        license="SYNTHETIC_FIXTURE",
    )
    publish_station_snapshot(
        manifest=tmp_path / "manifest.json",
        raw_root=tmp_path / "raw",
        station_root=tmp_path / "station",
        source_version_id=registered.record.source_version_id,
        transformation_version="station-t1",
        configuration={},
    )


def test_publish_station_history_cli(tmp_path):
    publish_snapshot(
        tmp_path,
        "2021-01-31",
        "공공자전거 대여소 정보(21.01.31 기준).csv",
        "s1",
    )
    publish_snapshot(
        tmp_path,
        "2021-06",
        "공공자전거 대여소 정보(21.06월 기준).csv",
        "s2",
    )

    result = run_cli(
        "warehouse",
        "publish-station-history",
        "--manifest",
        str(tmp_path / "manifest.json"),
        "--raw-root",
        str(tmp_path / "raw"),
        "--station-root",
        str(tmp_path / "station"),
        "--warehouse-root",
        str(tmp_path / "warehouse"),
        "--transformation-version",
        "history-t1",
    )

    assert result.returncode == 0, result.stderr
    assert result.stdout.startswith("STATION_HISTORY_PUBLISH_OK wh1_")
    assert "status=PUBLISHED_CURRENT" in result.stdout
    assert "snapshots=2" in result.stdout
    assert "history_rows=1" in result.stdout
    assert "stations=1" in result.stdout
    assert "open_intervals=1" in result.stdout
