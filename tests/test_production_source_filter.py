"""Exercise the pinned source selector without downloads or source registration."""

from pathlib import Path
import os
import subprocess
import sys

import pytest


pytestmark = pytest.mark.skipif(sys.platform == "win32", reason="WSL/Linux source script")
SCRIPT = Path(__file__).resolve().parents[1] / "scripts/prepare-production-sources-wsl.sh"


def runtime_script(tmp_path):
    # Exercise identical shell content on Windows-created checkouts as well.
    # New checkouts enforce LF via .gitattributes; this runtime copy also works
    # before an existing CRLF checkout is recreated.
    script = tmp_path / "prepare-production-sources-wsl.sh"
    script.write_bytes(SCRIPT.read_bytes().replace(b"\r\n", b"\n"))
    return script


def select(tmp_path, *arguments):
    destination = tmp_path / "sources"
    result = subprocess.run(
        ["bash", str(runtime_script(tmp_path)), str(destination), "git-b9de90a", *arguments, "--list-only"],
        capture_output=True, text=True,
    )
    assert not destination.exists()
    return result


def test_representative_months_select_one_pinned_year_and_first_station_snapshot(tmp_path):
    result = select(tmp_path, "--months", "2020-08", "2020-09")
    assert result.returncode == 0, result.stderr
    assert result.stdout.splitlines() == [
        "RENTAL_SELECTED logical_period=2020 bytes=1022004940 "
        "sha256=a8f3a38f2e73661c2bc0d2d01becd08712cc5226c433da9d5ff8348f9e1b3a91",
        "STATION_SELECTED logical_period=2021-01-31 bytes=206960 "
        "sha256=4adc277768ba0c4ddbf6eaa80784a48e3ed43a6e2d199b1e27e22416043314b5",
        "SOURCES_LISTED_ONLY",
    ]


def test_unfiltered_flow_keeps_all_pinned_sources(tmp_path):
    result = select(tmp_path)
    assert result.returncode == 0, result.stderr
    assert result.stdout.count("RENTAL_SELECTED ") == 12
    assert result.stdout.count("STATION_SELECTED ") == 12


@pytest.mark.parametrize("months", [
    (), ("2020-13",), ("2019-12",), ("2026-07",), ("2020-08", "2020-08"),
])
def test_invalid_filter_stops_before_download_or_directory_creation(tmp_path, months):
    result = select(tmp_path, "--months", *months)
    assert result.returncode == 2
    assert "requires at least one" in result.stderr or "invalid or duplicate" in result.stderr
    assert not result.stdout


@pytest.mark.parametrize("correct_size, error", [(False, "SIZE_MISMATCH"), (True, "SHA256_MISMATCH")])
def test_modified_source_stops_before_registration_or_station_download(tmp_path, correct_size, error):
    tools = tmp_path / "tools"
    tools.mkdir()
    curl = tools / "curl"
    curl.write_text(
        "#!/usr/bin/env python3\n"
        "import sys\n"
        "from pathlib import Path\n"
        "path=Path(sys.argv[sys.argv.index('-o')+1])\n"
        f"with path.open('wb') as handle: handle.truncate({1022004940 if correct_size else 7})\n",
        encoding="utf-8",
    )
    curl.chmod(0o755)
    result = subprocess.run(
        ["bash", str(runtime_script(tmp_path)), str(tmp_path / "sources"), "git-b9de90a",
         "--months", "2020-08", "2020-09"],
        env={**os.environ, "PATH": str(tools) + os.pathsep + os.environ["PATH"]},
        capture_output=True, text=True,
    )
    assert result.returncode == 3
    assert error in result.stderr
    assert "STATION_DOWNLOAD" not in result.stdout
    assert not (tmp_path / "sources/manifest.json").exists()
    assert len(list((tmp_path / "sources/downloads/rental").glob("2020.zip.part.*"))) == 1


def test_per_dataset_infseq_reaches_curl(tmp_path):
    """Fake transfer/hash/registration only; inspect the actual request arguments."""
    tools = tmp_path / "tools"
    tools.mkdir()
    commands = {
        "curl": (
            "import sys\n"
            "from pathlib import Path\n"
            "args=sys.argv[1:]\n"
            "dataset=next(a.split('=',1)[1] for a in args if a.startswith('infId='))\n"
            "infseq=next(a.split('=',1)[1] for a in args if a.startswith('infSeq='))\n"
            "print('REQUEST_DATASET '+dataset+' infSeq='+infseq)\n"
            "assert infseq == {'OA-15182':'1','OA-13252':'2'}[dataset]\n"
            "path=Path(args[args.index('-o')+1])\n"
            "with path.open('wb') as f: f.truncate(1022004940 if dataset=='OA-15182' else 206960)\n"
        ),
        "sha256sum": (
            "import sys\n"
            "sha='a8f3a38f2e73661c2bc0d2d01becd08712cc5226c433da9d5ff8348f9e1b3a91' "
            "if '2020.zip' in sys.argv[1] else "
            "'4adc277768ba0c4ddbf6eaa80784a48e3ed43a6e2d199b1e27e22416043314b5'\n"
            "print(sha+'  '+sys.argv[1])\n"
        ),
        "fixture-python": "print('REGISTERED_NEW sv1_fixture')\n",
    }
    for name, body in commands.items():
        path = tools / name
        path.write_text("#!/usr/bin/env python3\n" + body, encoding="utf-8")
        path.chmod(0o755)
    result = subprocess.run(
        ["bash", str(runtime_script(tmp_path)), str(tmp_path / "sources"), "git-b9de90a",
         "--months", "2020-08", "2020-09"],
        env={**os.environ, "PATH": str(tools) + os.pathsep + os.environ["PATH"],
             "PYTHON_BIN": str(tools / "fixture-python")},
        capture_output=True, text=True,
    )
    assert result.returncode == 0, result.stderr
    assert "REQUEST_DATASET OA-15182 infSeq=1" in result.stdout
    assert "REQUEST_DATASET OA-13252 infSeq=2" in result.stdout
