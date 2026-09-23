from __future__ import annotations

import csv
import os
import subprocess
import sys
from pathlib import Path

from seoul_bike_pipeline.register import register_source

REPO_ROOT = Path(__file__).resolve().parents[1]


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
        text=True,
        capture_output=True,
        encoding="utf-8",
        check=False,
    )


def test_publish_station_snapshot_cli(tmp_path):
    artifact = tmp_path / "station.csv"
    rows = [
        ["대여소\n번호", "보관소(대여소)명", "소재지(위치)", "", "", "", "설치\n시기", "설치형태", "", "운영\n방식"],
        ["", "", "", "", "", "", "", "LCD", "QR", ""],
        ["", "", "자치구", "상세주소", "위도", "경도", "", "", "", ""],
        ["", "", "", "", "", "", "", "거치\n대수", "거치\n대수", ""],
        ["", "", "", "", "", "", "", "", "", ""],
        ["00102", "망원역 1번출구 앞", "마포구", "서울특별시 마포구 월드컵로 72", "37.55", "126.91", "2015-09-06", "", "15", "QR"],
    ]
    with artifact.open("w", encoding="cp949", newline="") as handle:
        csv.writer(handle).writerows(rows)
    registered = register_source(
        artifact=artifact,
        manifest=tmp_path / "manifest.json",
        raw_root=tmp_path / "raw",
        dataset_id="station_snapshot",
        logical_period="2022-06",
        published_filename="공공자전거 대여소 정보(22.06월 기준).csv",
        source_url="https://data.seoul.go.kr/dataList/OA-13252/F/1/datasetView.do",
        license="공공누리 제1유형",
    )

    result = run_cli(
            "source",
            "publish-station-snapshot",
            "--manifest",
            str(tmp_path / "manifest.json"),
            "--raw-root",
            str(tmp_path / "raw"),
            "--station-root",
            str(tmp_path / "station"),
            "--source-version-id",
            registered.record.source_version_id,
            "--transformation-version",
            "t1",
    )

    assert result.returncode == 0, result.stderr
    assert result.stdout.startswith("STATION_PUBLISH_OK stv1_")
    assert "status=PUBLISHED_CURRENT" in result.stdout
    assert "observation_period=2022-06" in result.stdout
