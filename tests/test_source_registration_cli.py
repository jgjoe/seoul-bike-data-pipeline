"""CLI contract for the nested ``source register`` command (PRD §25).

The CLI runs as a real process (``python -m seoul_bike_pipeline``) so the
nested subcommand wiring, the printed statuses and the exit codes are covered,
not only an in-process ``main()`` call.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
ARTIFACT_V1 = REPO_ROOT / "tests" / "fixtures" / "source" / "rental_202001_v1.csv"
ARTIFACT_V2 = REPO_ROOT / "tests" / "fixtures" / "source" / "rental_202001_v2.csv"

V1_SHA256 = "7b8a935ddd9fa18ad532b6b7d945556f12bd05512d781e8bddd0298ee6f0bbe0"
V1_ID = "sv1_523e4b8078bd0fdc4c92c66b97494798a19fb402147c8d4714b331a02a229d12"
V2_SHA256 = "7d3d2f86482a7636f0bcb80465b5b13747199696adef22475fcdd15522c4c00e"
V2_ID = "sv1_00e9b29d1ac2256e331ac3539bfdd394e3dd87fff546b126e94acd007201efb0"


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


def register_arguments(manifest_path: Path, artifact: Path) -> list[str]:
    return [
        "source",
        "register",
        str(artifact),
        "--manifest",
        str(manifest_path),
        "--raw-root",
        str(manifest_path.parent / "raw"),
        "--dataset-id",
        "rental_history",
        "--logical-period",
        "2020-01",
        "--published-filename",
        "서울특별시 공공자전거 대여이력 정보_202001.csv",
        "--source-url",
        "https://data.seoul.go.kr/dataList/OA-15493/A/1/datasetView.do",
        "--license",
        "공공데이터 이용허락",
    ]


def test_cli_reports_new_then_unchanged_then_revision(tmp_path):
    manifest_path = tmp_path / "manifest.json"

    new = run_cli(*register_arguments(manifest_path, ARTIFACT_V1))
    unchanged = run_cli(*register_arguments(manifest_path, ARTIFACT_V1))
    revision = run_cli(*register_arguments(manifest_path, ARTIFACT_V2))

    assert (new.returncode, unchanged.returncode, revision.returncode) == (0, 0, 0)
    assert new.stdout == f"REGISTERED_NEW {V1_ID}\n"
    assert unchanged.stdout == f"KNOWN_UNCHANGED {V1_ID}\n"
    assert revision.stdout == f"REGISTERED_REVISION {V2_ID}\n"
    assert (new.stderr, unchanged.stderr, revision.stderr) == ("", "", "")

    stored = json.loads(manifest_path.read_text(encoding="utf-8"))["records"]
    assert [record["source_version_id"] for record in stored] == [V1_ID, V2_ID]
    assert [record["sha256"] for record in stored] == [V1_SHA256, V2_SHA256]
    assert (manifest_path.parent / "raw" / "artifacts" / V1_ID / "artifact").read_bytes() == ARTIFACT_V1.read_bytes()
    assert (manifest_path.parent / "raw" / "artifacts" / V2_ID / "artifact").read_bytes() == ARTIFACT_V2.read_bytes()


def test_cli_missing_artifact_exits_1_without_writing_a_manifest(tmp_path):
    manifest_path = tmp_path / "manifest.json"

    result = run_cli(*register_arguments(manifest_path, tmp_path / "absent.csv"))

    assert result.returncode == 1
    assert result.stdout == ""
    assert result.stderr.startswith("error:")
    assert not manifest_path.exists()


def test_cli_malformed_manifest_exits_1_and_keeps_the_file(tmp_path):
    manifest_path = tmp_path / "manifest.json"
    manifest_path.write_text("{", encoding="utf-8")

    result = run_cli(*register_arguments(manifest_path, ARTIFACT_V1))

    assert result.returncode == 1
    assert result.stdout == ""
    assert result.stderr.startswith("error:")
    assert manifest_path.read_text(encoding="utf-8") == "{"


def test_cli_requires_the_manifest_option():
    result = run_cli("source", "register", str(ARTIFACT_V1), "--dataset-id", "rental_history")

    assert result.returncode == 2
    assert result.stdout == ""
    assert "--manifest" in result.stderr


def test_cli_requires_the_raw_root_option(tmp_path):
    arguments = register_arguments(tmp_path / "manifest.json", ARTIFACT_V1)
    index = arguments.index("--raw-root")
    del arguments[index : index + 2]

    result = run_cli(*arguments)

    assert result.returncode == 2
    assert result.stdout == ""
    assert "--raw-root" in result.stderr
