"""Immutable Clean Parquet publish / atomic current contract (D-004)."""

from __future__ import annotations

import csv
import io
import json
import os
import shutil
from pathlib import Path

import duckdb
import pytest

import seoul_bike_pipeline.clean_publish as clean_module
from seoul_bike_pipeline.clean_publish import (
    STATUS_BUILT_HISTORICAL,
    STATUS_PUBLISHED_CURRENT,
    STATUS_REUSED_CURRENT,
    CleanPublishError,
    compute_clean_version_id,
    compute_configuration_fingerprint,
    current_pointer_path,
    publish_clean_month,
    validate_clean_version,
)
from seoul_bike_pipeline.csv_source import RENTAL_V16_HEADER
from seoul_bike_pipeline.errors import GroupDqError
from seoul_bike_pipeline.register import register_source

SOURCE_URL = "https://example.invalid/rental.csv"
LICENSE = "SYNTHETIC_FIXTURE"


def row(**overrides):
    values = {
        "bike_id": "SPB-1",
        "rent_at": "2026-01-01 00:00:00",
        "rent_station_no": "00001",
        "rent_station_name": "대여소",
        "rent_dock_no": "0",
        "return_at": "2026-01-01 00:10:00",
        "return_station_no": "00002",
        "return_station_name": "반납소",
        "return_dock_no": "0",
        "usage_min": "10",
        "distance_m": "100.00",
        "birth_year": "1990",
        "sex": "M",
        "user_type": "내국인",
        "rent_station_internal_id": "ST-1",
        "return_station_internal_id": "ST-2",
    }
    values.update(overrides)
    return tuple(values.values())


def encode_csv(rows) -> bytes:
    buffer = io.StringIO(newline="")
    writer = csv.writer(buffer, lineterminator="\r\n")
    writer.writerow(RENTAL_V16_HEADER)
    writer.writerows(rows)
    return buffer.getvalue().encode("cp949")


def register_rows(tmp_path: Path, rows, *, source_name="source.csv"):
    source = tmp_path / source_name
    source.write_bytes(encode_csv(rows))
    return register_source(
        artifact=source,
        manifest=tmp_path / "source-manifest.json",
        raw_root=tmp_path / "raw",
        dataset_id="rental_history",
        logical_period="2026-01",
        published_filename="rental_2601.csv",
        source_url=SOURCE_URL,
        license=LICENSE,
    )


def publish(tmp_path: Path, source_version_id: str, *, transformation="t1", configuration=None, **kwargs):
    return publish_clean_month(
        manifest=tmp_path / "source-manifest.json",
        raw_root=tmp_path / "raw",
        clean_root=tmp_path / "clean",
        source_version_id=source_version_id,
        source_month="2026-01",
        transformation_version=transformation,
        configuration={} if configuration is None else configuration,
        **kwargs,
    )


def load_json(path: Path):
    return json.loads(path.read_text(encoding="utf-8"))


def test_identity_hashes_are_deterministic_and_configuration_order_independent():
    left = compute_configuration_fingerprint({"b": [2, 3], "a": 1})
    right = compute_configuration_fingerprint({"a": 1, "b": [2, 3]})

    assert left == right
    clean_id = compute_clean_version_id(
        source_month="2026-01",
        source_version_id="sv1_" + "1" * 64,
        transformation_version="abc123",
        configuration_fingerprint=left,
    )
    assert clean_id.startswith("cv1_")
    assert len(clean_id) == 68


def test_publish_writes_clean_nested_dq_quarantine_side_output_metadata_and_pointer(tmp_path):
    registered = register_rows(
        tmp_path,
        [
            row(sex="f"),
            row(
                bike_id="SPB-2",
                rent_at="2026-01-01 01:00:00",
                return_at="2026-01-01 01:10:00",
                usage_min="-1",
                sex="f",
            ),
        ],
    )

    result = publish(
        tmp_path,
        registered.record.source_version_id,
        transformation="commit-a",
        configuration={"distance_threshold_m": 100000},
    )

    assert result.status == STATUS_PUBLISHED_CURRENT
    assert result.clean_row_count == 2
    assert result.trusted_fact_eligible_count == 1
    assert result.quarantined_row_count == 1
    assert result.quarantine_issue_count == 2
    version = result.clean_version_path
    assert {p.name for p in version.iterdir()} == {
        "clean.parquet",
        "quarantine.parquet",
        "metadata.json",
    }
    metadata = validate_clean_version(version)
    assert metadata["clean_version_id"] == result.clean_version_id
    assert metadata["source_artifact_sha256"] == registered.record.sha256
    assert metadata["configuration"] == {"distance_threshold_m": 100000}

    pointer = load_json(result.current_pointer_path)
    assert pointer == {
        "schema_version": 1,
        "source_month": "2026-01",
        "clean_version_id": result.clean_version_id,
    }

    connection = duckdb.connect()
    try:
        clean_rows = connection.execute(
            """
            SELECT bike_id, station_history_match, row_quarantined,
                   trusted_fact_eligible, dq_issues
            FROM read_parquet(?)
            ORDER BY bike_id
            """,
            [str(version / "clean.parquet")],
        ).fetchall()
        quarantine = connection.execute(
            """
            SELECT rule_id, severity
            FROM read_parquet(?)
            ORDER BY rule_id
            """,
            [str(version / "quarantine.parquet")],
        ).fetchall()
    finally:
        connection.close()

    assert clean_rows[0][1] is None
    assert clean_rows[0][2:] and clean_rows[0][2] is False
    assert clean_rows[0][3] is True
    assert {issue["rule_id"] for issue in clean_rows[0][4]} == {"sex_noncanonical"}
    assert clean_rows[1][1] is None
    assert clean_rows[1][2] is True
    assert clean_rows[1][3] is False
    assert quarantine == [
        ("sex_noncanonical", "WARNING"),
        ("usage_min_negative", "QUARANTINE"),
    ]


def test_same_input_retry_rebuilds_identical_bytes_and_is_current_noop(tmp_path):
    registered = register_rows(tmp_path, [row()])

    first = publish(tmp_path, registered.record.source_version_id, transformation="commit-a")
    first_metadata_bytes = (first.clean_version_path / "metadata.json").read_bytes()
    first_clean_bytes = (first.clean_version_path / "clean.parquet").read_bytes()
    first_pointer_bytes = first.current_pointer_path.read_bytes()

    second = publish(tmp_path, registered.record.source_version_id, transformation="commit-a")

    assert second.status == STATUS_REUSED_CURRENT
    assert second.clean_version_id == first.clean_version_id
    assert (second.clean_version_path / "metadata.json").read_bytes() == first_metadata_bytes
    assert (second.clean_version_path / "clean.parquet").read_bytes() == first_clean_bytes
    assert second.current_pointer_path.read_bytes() == first_pointer_bytes
    versions = list((tmp_path / "clean" / "versions" / "source_month=2026-01").iterdir())
    assert len(versions) == 1


def test_new_transformation_same_source_creates_new_version_and_switches_current(tmp_path):
    registered = register_rows(tmp_path, [row()])
    old = publish(tmp_path, registered.record.source_version_id, transformation="commit-a")

    new = publish(tmp_path, registered.record.source_version_id, transformation="commit-b")

    assert new.status == STATUS_PUBLISHED_CURRENT
    assert new.clean_version_id != old.clean_version_id
    assert old.clean_version_path.exists()
    assert new.clean_version_path.exists()
    assert load_json(new.current_pointer_path)["clean_version_id"] == new.clean_version_id


def test_historical_source_rebuild_does_not_roll_back_newer_current(tmp_path):
    v1 = register_rows(tmp_path, [row(distance_m="100.00")], source_name="v1.csv")
    first = publish(tmp_path, v1.record.source_version_id, transformation="commit-a")
    v2 = register_rows(tmp_path, [row(distance_m="200.00")], source_name="v2.csv")
    current = publish(tmp_path, v2.record.source_version_id, transformation="commit-a")
    assert load_json(current.current_pointer_path)["clean_version_id"] == current.clean_version_id

    historical = publish(tmp_path, v1.record.source_version_id, transformation="commit-b")

    assert historical.status == STATUS_BUILT_HISTORICAL
    assert historical.clean_version_path.exists()
    assert historical.clean_version_id != first.clean_version_id
    assert load_json(current.current_pointer_path)["clean_version_id"] == current.clean_version_id


def test_pre_publish_failure_keeps_previous_current_but_preserves_new_version(tmp_path):
    registered = register_rows(tmp_path, [row()])
    first = publish(tmp_path, registered.record.source_version_id, transformation="commit-a")

    def fail_before_pointer(_version_path):
        raise RuntimeError("injected pre-publish failure")

    expected_new_id = compute_clean_version_id(
        source_month="2026-01",
        source_version_id=registered.record.source_version_id,
        transformation_version="commit-b",
        configuration_fingerprint=compute_configuration_fingerprint({}),
    )
    with pytest.raises(RuntimeError, match="injected pre-publish failure"):
        publish(
            tmp_path,
            registered.record.source_version_id,
            transformation="commit-b",
            before_current_publish=fail_before_pointer,
        )

    assert load_json(first.current_pointer_path)["clean_version_id"] == first.clean_version_id
    failed_version = (
        tmp_path
        / "clean"
        / "versions"
        / "source_month=2026-01"
        / f"clean_version_id={expected_new_id}"
    )
    assert failed_version.exists()
    validate_clean_version(failed_version)


def test_transform_failure_keeps_current_and_leaves_staging_evidence(tmp_path, monkeypatch):
    registered = register_rows(tmp_path, [row()])
    first = publish(tmp_path, registered.record.source_version_id, transformation="commit-a")

    def fail_transform(**_kwargs):
        raise GroupDqError("injected transform failure")

    monkeypatch.setattr(clean_module, "classify_registered_source_month", fail_transform)
    with pytest.raises(GroupDqError, match="injected transform failure"):
        publish(tmp_path, registered.record.source_version_id, transformation="commit-b")

    assert load_json(first.current_pointer_path)["clean_version_id"] == first.clean_version_id
    staging = tmp_path / "clean" / "staging" / "source_month=2026-01"
    assert any(staging.iterdir())


def test_corrupted_immutable_version_hard_fails_without_auto_repair(tmp_path):
    registered = register_rows(tmp_path, [row()])
    first = publish(tmp_path, registered.record.source_version_id, transformation="commit-a")
    clean_file = first.clean_version_path / "clean.parquet"
    original = clean_file.read_bytes()
    clean_file.write_bytes(original + b"corruption")

    with pytest.raises(CleanPublishError, match="fingerprint"):
        publish(tmp_path, registered.record.source_version_id, transformation="commit-a")

    assert clean_file.read_bytes() == original + b"corruption"
    assert load_json(first.current_pointer_path)["clean_version_id"] == first.clean_version_id


def test_missing_current_version_hard_fails_without_rebuilding_it(tmp_path):
    registered = register_rows(tmp_path, [row()])
    first = publish(tmp_path, registered.record.source_version_id, transformation="commit-a")
    shutil_target = first.clean_version_path
    for child in shutil_target.iterdir():
        child.unlink()
    shutil_target.rmdir()

    with pytest.raises(CleanPublishError, match="missing immutable Clean metadata"):
        publish(tmp_path, registered.record.source_version_id, transformation="commit-a")

    assert not shutil_target.exists()
    assert load_json(first.current_pointer_path)["clean_version_id"] == first.clean_version_id


def test_pointer_change_during_build_is_detected_before_publish(tmp_path):
    registered = register_rows(tmp_path, [row()])
    first = publish(tmp_path, registered.record.source_version_id, transformation="commit-a")
    original_build = clean_module._build_staging_parquet

    def build_then_change_pointer(**kwargs):
        summary = original_build(**kwargs)
        first.current_pointer_path.write_text(
            json.dumps(
                {
                    "schema_version": 1,
                    "source_month": "2026-01",
                    "clean_version_id": "cv1_" + "f" * 64,
                }
            ),
            encoding="utf-8",
        )
        return summary

    clean_module._build_staging_parquet = build_then_change_pointer
    try:
        with pytest.raises(CleanPublishError, match="changed during build"):
            publish(tmp_path, registered.record.source_version_id, transformation="commit-b")
    finally:
        clean_module._build_staging_parquet = original_build


def test_newer_nested_publish_cannot_be_rolled_back_by_older_outer_publish(tmp_path):
    v1 = register_rows(tmp_path, [row(distance_m="100.00")], source_name="v1.csv")
    v2 = register_rows(tmp_path, [row(distance_m="200.00")], source_name="v2.csv")
    nested = {}

    def publish_newer(_version_path):
        nested["result"] = publish(
            tmp_path,
            v2.record.source_version_id,
            transformation="commit-newer",
        )

    with pytest.raises(CleanPublishError, match="refusing concurrent rollback"):
        publish(
            tmp_path,
            v1.record.source_version_id,
            transformation="commit-older",
            before_current_publish=publish_newer,
        )

    newer = nested["result"]
    assert newer.status == STATUS_PUBLISHED_CURRENT
    assert load_json(newer.current_pointer_path)["clean_version_id"] == newer.clean_version_id


def test_active_same_month_publish_lock_hard_fails_without_touching_current(tmp_path):
    registered = register_rows(tmp_path, [row()])
    first = publish(tmp_path, registered.record.source_version_id, transformation="commit-a")
    lock_path = tmp_path / "clean" / "locks" / "source_month=2026-01.lock"
    with clean_module._current_publish_lock(
        tmp_path / "clean", "2026-01", first.clean_version_id
    ):
        with pytest.raises(CleanPublishError, match="holds lock"):
            publish(tmp_path, registered.record.source_version_id, transformation="commit-b")

    assert load_json(first.current_pointer_path)["clean_version_id"] == first.clean_version_id
    assert lock_path.exists()


def test_persistent_unlocked_lock_file_does_not_wedge_retry_after_crash(tmp_path):
    registered = register_rows(tmp_path, [row()])
    first = publish(tmp_path, registered.record.source_version_id, transformation="commit-a")
    lock_path = tmp_path / "clean" / "locks" / "source_month=2026-01.lock"
    assert lock_path.exists()

    second = publish(tmp_path, registered.record.source_version_id, transformation="commit-b")

    assert second.status == STATUS_PUBLISHED_CURRENT
    assert second.clean_version_id != first.clean_version_id
    assert load_json(second.current_pointer_path)["clean_version_id"] == second.clean_version_id
    assert lock_path.exists()


def test_concurrent_same_version_promotion_converges_on_identical_winner(tmp_path, monkeypatch):
    registered = register_rows(tmp_path, [row()])
    original_replace = clean_module.os.replace
    raced = {"done": False}

    def race_replace(source, destination):
        source_path = Path(source)
        destination_path = Path(destination)
        if (
            not raced["done"]
            and source_path.is_dir()
            and destination_path.name.startswith("clean_version_id=cv1_")
        ):
            raced["done"] = True
            shutil.copytree(source_path, destination_path)
            raise FileExistsError("injected same-version promotion winner")
        return original_replace(source, destination)

    monkeypatch.setattr(clean_module.os, "replace", race_replace)

    result = publish(tmp_path, registered.record.source_version_id, transformation="commit-a")

    assert raced["done"] is True
    assert result.status == STATUS_PUBLISHED_CURRENT
    validate_clean_version(result.clean_version_path)
    staging = tmp_path / "clean" / "staging" / "source_month=2026-01"
    assert list(staging.iterdir()) == []


@pytest.mark.parametrize(
    ("replacement", "expected_message"),
    [
        ("NULL::VARCHAR AS source_version_id", "outside requested month/source identity"),
        ("NULL::VARCHAR AS source_month", "outside requested month/source identity"),
        ("NULL::BOOLEAN AS trusted_fact_eligible", "trusted_fact_eligible"),
    ],
)
def test_null_tamper_cannot_bypass_publish_hard_gates(
    tmp_path, monkeypatch, replacement, expected_message
):
    registered = register_rows(tmp_path, [row(usage_min="-1")])
    original_build_metadata = clean_module._build_metadata

    def tamper_then_build_metadata(**kwargs):
        clean_path = kwargs["stage_path"] / "clean.parquet"
        replacement_path = kwargs["stage_path"] / "tampered.parquet"
        source_sql = clean_path.resolve().as_posix().replace("'", "''")
        target_sql = replacement_path.resolve().as_posix().replace("'", "''")
        connection = duckdb.connect()
        try:
            connection.execute(
                f"""
                COPY (
                    SELECT * REPLACE ({replacement})
                    FROM read_parquet('{source_sql}', hive_partitioning=false)
                ) TO '{target_sql}' (FORMAT PARQUET, COMPRESSION ZSTD)
                """
            )
        finally:
            connection.close()
        os.replace(replacement_path, clean_path)
        return original_build_metadata(**kwargs)

    monkeypatch.setattr(clean_module, "_build_metadata", tamper_then_build_metadata)
    with pytest.raises(CleanPublishError, match=expected_message):
        publish(tmp_path, registered.record.source_version_id, transformation="commit-a")

    assert not current_pointer_path(tmp_path / "clean", "2026-01").exists()


def test_current_pointer_must_match_bundle_metadata_identity(tmp_path):
    registered = register_rows(tmp_path, [row()])
    first = publish(tmp_path, registered.record.source_version_id, transformation="commit-a")
    wrong_id = "cv1_" + "f" * 64
    wrong_path = (
        tmp_path
        / "clean"
        / "versions"
        / "source_month=2026-01"
        / f"clean_version_id={wrong_id}"
    )
    shutil.copytree(first.clean_version_path, wrong_path)
    first.current_pointer_path.write_text(
        json.dumps(
            {
                "schema_version": 1,
                "source_month": "2026-01",
                "clean_version_id": wrong_id,
            }
        ),
        encoding="utf-8",
    )

    with pytest.raises(CleanPublishError, match="does not match its directory/current binding"):
        publish(tmp_path, registered.record.source_version_id, transformation="commit-b")

    assert load_json(first.current_pointer_path)["clean_version_id"] == wrong_id


@pytest.mark.parametrize(
    ("replacement", "expected_message"),
    [
        (
            "[struct_pack(rule_id := 'tampered', severity := 'QUARANTINE', "
            "field := 'bike_id', reason := 'tampered')] AS dq_issues",
            "DQ issue semantics",
        ),
        (
            "[struct_pack(rule_id := NULL::VARCHAR, severity := 'WARNING', "
            "field := 'sex_code', reason := 'tampered')] AS dq_issues",
            "missing/empty rule_id",
        ),
        (
            "[struct_pack(rule_id := 'tampered', severity := 'INVALID', "
            "field := 'sex_code', reason := 'tampered')] AS dq_issues",
            "severity outside",
        ),
        ("0::INTEGER AS dq_warning_count", "DQ issue semantics"),
    ],
)
def test_dq_semantic_tamper_cannot_publish(tmp_path, monkeypatch, replacement, expected_message):
    registered = register_rows(tmp_path, [row(sex="f")])
    original_build_metadata = clean_module._build_metadata

    def tamper_then_build_metadata(**kwargs):
        clean_path = kwargs["stage_path"] / "clean.parquet"
        replacement_path = kwargs["stage_path"] / "tampered.parquet"
        source_sql = clean_path.resolve().as_posix().replace("'", "''")
        target_sql = replacement_path.resolve().as_posix().replace("'", "''")
        connection = duckdb.connect()
        try:
            connection.execute(
                f"""
                COPY (
                    SELECT * REPLACE ({replacement})
                    FROM read_parquet('{source_sql}', hive_partitioning=false)
                ) TO '{target_sql}' (FORMAT PARQUET, COMPRESSION ZSTD)
                """
            )
        finally:
            connection.close()
        os.replace(replacement_path, clean_path)
        return original_build_metadata(**kwargs)

    monkeypatch.setattr(clean_module, "_build_metadata", tamper_then_build_metadata)
    with pytest.raises(CleanPublishError, match=expected_message):
        publish(tmp_path, registered.record.source_version_id, transformation="commit-a")

    assert not current_pointer_path(tmp_path / "clean", "2026-01").exists()


def test_clean_smallint_overflow_hard_fails_before_current_publish(tmp_path):
    registered = register_rows(tmp_path, [row(birth_year="40000")])

    with pytest.raises(CleanPublishError, match="SMALLINT"):
        publish(tmp_path, registered.record.source_version_id, transformation="commit-a")

    assert not current_pointer_path(tmp_path / "clean", "2026-01").exists()


def test_configuration_must_be_json_compatible_mapping():
    with pytest.raises(CleanPublishError, match="JSON-compatible"):
        compute_configuration_fingerprint({"bad": {1, 2, 3}})
