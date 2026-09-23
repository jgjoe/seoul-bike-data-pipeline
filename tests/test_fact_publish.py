from __future__ import annotations

import hashlib
import json
from contextlib import contextmanager
from pathlib import Path

import duckdb
import pytest

import seoul_bike_pipeline.fact_publish as fact_publish
from seoul_bike_pipeline.errors import FactTripPublishError
from seoul_bike_pipeline.fact_publish import (
    STATUS_PUBLISHED_CURRENT,
    STATUS_REUSED_CURRENT,
    fact_current_pointer_path,
    publish_current_fact_trip_month,
    validate_fact_trip_version,
)
from seoul_bike_pipeline.trip_station_enrichment import load_station_enrichment_sql
from tests.test_trip_station_enrichment import build_fixture, run_cli


def publish_fact(tmp_path: Path, *, transformation_version: str = "fact-t1", **kwargs):
    return publish_current_fact_trip_month(
        manifest=tmp_path / "manifest.json",
        raw_root=tmp_path / "raw",
        station_root=tmp_path / "station",
        clean_root=tmp_path / "clean",
        warehouse_root=tmp_path / "warehouse",
        source_month="2026-01",
        transformation_version=transformation_version,
        configuration={},
        **kwargs,
    )


def test_publish_fact_materializes_trusted_enriched_rows_and_reuses_current(tmp_path):
    clean, history = build_fixture(tmp_path)
    clean_bytes = (clean.clean_version_path / "clean.parquet").read_bytes()

    first = publish_fact(tmp_path)

    assert first.status == STATUS_PUBLISHED_CURRENT
    assert first.clean_version_id == clean.clean_version_id
    assert first.history_version_id == history.history_version_id
    assert first.fact_row_count == 4
    assert first.station_history_match_count == 1
    assert first.station_history_unmatched_count == 3
    pointer = json.loads(first.current_pointer_path.read_text(encoding="utf-8"))
    assert pointer == {
        "schema_version": 1,
        "source_month": "2026-01",
        "fact_version_id": first.fact_version_id,
    }

    connection = duckdb.connect(str(first.version_path / "warehouse.duckdb"), read_only=True)
    try:
        rows = connection.execute(
            """
            SELECT
                bike_id,
                station_history_match,
                dq_warning_count,
                list_transform(dq_issues, issue -> issue.rule_id)
            FROM fact_trip_trusted
            ORDER BY bike_id
            """
        ).fetchall()
    finally:
        connection.close()
    by_bike = {row[0]: row[1:] for row in rows}
    assert "SPB-QUARANTINE" not in by_bike
    assert by_bike["SPB-MATCH"][0] is True
    assert by_bike["SPB-FUTURE"][0] is False
    assert "station_history_unmatched" in by_bike["SPB-FUTURE"][2]
    assert by_bike["SPB-NEVER"][1] == 2
    assert (clean.clean_version_path / "clean.parquet").read_bytes() == clean_bytes

    second = publish_fact(tmp_path)
    assert second.status == STATUS_REUSED_CURRENT
    assert second.fact_version_id == first.fact_version_id


def test_pre_publish_failure_preserves_previous_current_and_retry_succeeds(tmp_path):
    build_fixture(tmp_path)
    first = publish_fact(tmp_path, transformation_version="fact-t1")
    before = first.current_pointer_path.read_bytes()

    def fail_before_current(_version_path):
        raise RuntimeError("injected pre-current failure")

    with pytest.raises(RuntimeError, match="injected pre-current failure"):
        publish_fact(
            tmp_path,
            transformation_version="fact-t2",
            before_current_publish=fail_before_current,
        )
    assert first.current_pointer_path.read_bytes() == before

    retry = publish_fact(tmp_path, transformation_version="fact-t2")
    assert retry.status == STATUS_PUBLISHED_CURRENT
    assert retry.fact_version_id != first.fact_version_id


def test_fact_sql_failure_preserves_previous_current(tmp_path):
    build_fixture(tmp_path)
    first = publish_fact(tmp_path)
    before = first.current_pointer_path.read_bytes()
    bad_sql = tmp_path / "bad-fact.sql"
    bad_sql.write_text(
        "CREATE TABLE fact_trip_trusted AS "
        "SELECT does_not_exist FROM fact_enriched_input;\n",
        encoding="utf-8",
    )

    with pytest.raises(FactTripPublishError, match="trusted fact build failed"):
        publish_fact(
            tmp_path,
            transformation_version="fact-t2",
            fact_sql_path=bad_sql,
        )
    assert first.current_pointer_path.read_bytes() == before


def test_concurrent_different_fact_publish_cannot_roll_back_winner(tmp_path, monkeypatch):
    build_fixture(tmp_path)
    real_locks = fact_publish._publication_locks
    state = {"nested": False, "winner": None}

    @contextmanager
    def interleaving_locks(**kwargs):
        if not state["nested"]:
            state["nested"] = True
            state["winner"] = publish_fact(
                tmp_path,
                transformation_version="fact-t2",
            )
            state["nested"] = False
        with real_locks(**kwargs):
            yield

    monkeypatch.setattr(fact_publish, "_publication_locks", interleaving_locks)
    with pytest.raises(FactTripPublishError, match="current pointer changed during build"):
        publish_fact(tmp_path, transformation_version="fact-t1")

    pointer = json.loads(
        fact_current_pointer_path(tmp_path / "warehouse", "2026-01").read_text(
            encoding="utf-8"
        )
    )
    assert pointer["fact_version_id"] == state["winner"].fact_version_id


def test_concurrent_same_fact_version_converges_to_reused_current(tmp_path, monkeypatch):
    build_fixture(tmp_path)
    real_locks = fact_publish._publication_locks
    state = {"nested": False, "winner": None}

    @contextmanager
    def interleaving_locks(**kwargs):
        if not state["nested"]:
            state["nested"] = True
            state["winner"] = publish_fact(tmp_path, transformation_version="fact-t1")
            state["nested"] = False
        with real_locks(**kwargs):
            yield

    monkeypatch.setattr(fact_publish, "_publication_locks", interleaving_locks)
    outer = publish_fact(tmp_path, transformation_version="fact-t1")
    assert state["winner"].status == STATUS_PUBLISHED_CURRENT
    assert outer.status == STATUS_REUSED_CURRENT
    assert outer.fact_version_id == state["winner"].fact_version_id


def test_fact_current_pointer_rejects_bundle_identity_substitution(tmp_path):
    build_fixture(tmp_path)
    published = publish_fact(tmp_path)
    fake_fact_version_id = "fv1_" + "0" * 64
    fake_path = (
        published.version_path.parent / f"fact_version_id={fake_fact_version_id}"
    )
    fake_path.mkdir(parents=True)
    for name in ("warehouse.duckdb", "metadata.json"):
        (fake_path / name).write_bytes((published.version_path / name).read_bytes())
    pointer_path = published.current_pointer_path
    pointer_path.write_text(
        json.dumps(
            {
                "schema_version": 1,
                "source_month": "2026-01",
                "fact_version_id": fake_fact_version_id,
            },
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )

    with pytest.raises(FactTripPublishError, match="version directory id|bundle version id"):
        publish_fact(tmp_path)


def test_validator_rejects_relocated_bundle_path(tmp_path):
    build_fixture(tmp_path)
    published = publish_fact(tmp_path)
    fake_fact_version_id = "fv1_" + "f" * 64
    fake_path = (
        published.version_path.parent / f"fact_version_id={fake_fact_version_id}"
    )
    fake_path.mkdir(parents=True)
    for name in ("warehouse.duckdb", "metadata.json"):
        (fake_path / name).write_bytes((published.version_path / name).read_bytes())

    with pytest.raises(FactTripPublishError, match="version directory id|bundle version id"):
        validate_fact_trip_version(
            fake_path,
            manifest=tmp_path / "manifest.json",
            raw_root=tmp_path / "raw",
            station_root=tmp_path / "station",
            clean_root=tmp_path / "clean",
            warehouse_root=tmp_path / "warehouse",
        )


def test_fact_logical_tamper_is_rejected_even_when_fingerprint_is_updated(tmp_path):
    build_fixture(tmp_path)
    published = publish_fact(tmp_path)
    database_path = published.version_path / "warehouse.duckdb"
    connection = duckdb.connect(str(database_path))
    try:
        connection.execute(
            """
            UPDATE fact_trip_trusted
            SET station_history_match = FALSE
            WHERE bike_id = 'SPB-MATCH'
            """
        )
        connection.execute("CHECKPOINT")
    finally:
        connection.close()

    metadata_path = published.version_path / "metadata.json"
    metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
    data = database_path.read_bytes()
    metadata["warehouse_database"] = {
        "size_bytes": len(data),
        "sha256": hashlib.sha256(data).hexdigest(),
    }
    metadata_path.write_text(
        json.dumps(
            metadata,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        )
        + "\n",
        encoding="utf-8",
    )

    with pytest.raises(FactTripPublishError, match="station_history_match"):
        validate_fact_trip_version(
            published.version_path,
            manifest=tmp_path / "manifest.json",
            raw_root=tmp_path / "raw",
            station_root=tmp_path / "station",
            clean_root=tmp_path / "clean",
            warehouse_root=tmp_path / "warehouse",
        )


@pytest.mark.parametrize(
    ("field_name", "field_value", "expected_error"),
    [
        ("rule_id", "NULL::VARCHAR", "missing/empty"),
        ("severity", "NULL::VARCHAR", "missing/empty"),
        ("field", "NULL::VARCHAR", "missing/empty"),
        ("reason", "NULL::VARCHAR", "missing/empty"),
        ("severity", "'INVALID'", "outside WARNING/QUARANTINE"),
    ],
)
def test_malformed_fact_dq_issue_is_rejected_even_with_updated_fingerprint(
    tmp_path,
    field_name,
    field_value,
    expected_error,
):
    build_fixture(tmp_path)
    published = publish_fact(tmp_path)
    database_path = published.version_path / "warehouse.duckdb"
    connection = duckdb.connect(str(database_path))
    try:
        connection.execute(
            f"""
            UPDATE fact_trip_trusted
            SET dq_issues = list_append(
                dq_issues,
                struct_pack(
                    rule_id := CASE WHEN '{field_name}' = 'rule_id'
                        THEN {field_value} ELSE 'tampered' END,
                    severity := CASE WHEN '{field_name}' = 'severity'
                        THEN {field_value} ELSE 'WARNING' END,
                    field := CASE WHEN '{field_name}' = 'field'
                        THEN {field_value} ELSE 'bike_id' END,
                    reason := CASE WHEN '{field_name}' = 'reason'
                        THEN {field_value} ELSE 'tampered' END
                )
            )
            WHERE bike_id = 'SPB-MATCH'
            """
        )
        connection.execute("CHECKPOINT")
    finally:
        connection.close()
    _rewrite_database_fingerprint(published.version_path)

    with pytest.raises(FactTripPublishError, match=expected_error):
        validate_fact_trip_version(
            published.version_path,
            manifest=tmp_path / "manifest.json",
            raw_root=tmp_path / "raw",
            station_root=tmp_path / "station",
            clean_root=tmp_path / "clean",
            warehouse_root=tmp_path / "warehouse",
        )


def test_fact_sql_view_is_rejected_without_current_publish(tmp_path):
    build_fixture(tmp_path)
    sql_path = tmp_path / "fact-view.sql"
    sql_path.write_text(
        "CREATE VIEW fact_trip_trusted AS SELECT * FROM fact_enriched_input;\n",
        encoding="utf-8",
    )

    with pytest.raises(FactTripPublishError, match="base table"):
        publish_fact(tmp_path, fact_sql_path=sql_path)
    assert not fact_current_pointer_path(
        tmp_path / "warehouse", "2026-01"
    ).exists()


def test_drop_one_duplicate_one_preserving_count_is_hard_rejected(tmp_path):
    build_fixture(tmp_path)
    published = publish_fact(tmp_path)
    database_path = published.version_path / "warehouse.duckdb"
    connection = duckdb.connect(str(database_path))
    try:
        connection.execute(
            "DELETE FROM fact_trip_trusted WHERE bike_id = 'SPB-FUTURE'"
        )
        connection.execute(
            """
            INSERT INTO fact_trip_trusted
            SELECT * FROM fact_trip_trusted WHERE bike_id = 'SPB-GAP'
            """
        )
        connection.execute("CHECKPOINT")
    finally:
        connection.close()
    metadata_path = published.version_path / "metadata.json"
    metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
    data = database_path.read_bytes()
    metadata["warehouse_database"] = {
        "size_bytes": len(data),
        "sha256": hashlib.sha256(data).hexdigest(),
    }
    metadata_path.write_text(
        json.dumps(
            metadata,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        )
        + "\n",
        encoding="utf-8",
    )

    with pytest.raises(FactTripPublishError, match="source_row_id values are not unique"):
        validate_fact_trip_version(
            published.version_path,
            manifest=tmp_path / "manifest.json",
            raw_root=tmp_path / "raw",
            station_root=tmp_path / "station",
            clean_root=tmp_path / "clean",
            warehouse_root=tmp_path / "warehouse",
        )


def test_sql_identity_normalizes_line_endings(tmp_path):
    fact_lf = tmp_path / "fact-lf.sql"
    fact_crlf = tmp_path / "fact-crlf.sql"
    fact_cr = tmp_path / "fact-cr.sql"
    text = "CREATE TABLE fact_trip_trusted AS\nSELECT * FROM fact_enriched_input;\n"
    fact_lf.write_bytes(text.encode("utf-8"))
    fact_crlf.write_bytes(text.replace("\n", "\r\n").encode("utf-8"))
    fact_cr.write_bytes(text.replace("\n", "\r").encode("utf-8"))

    fact_hashes = {
        fact_publish._load_sql(path)[1] for path in (fact_lf, fact_crlf, fact_cr)
    }
    assert len(fact_hashes) == 1

    enrich_lf = tmp_path / "enrich-lf.sql"
    enrich_crlf = tmp_path / "enrich-crlf.sql"
    enrich_cr = tmp_path / "enrich-cr.sql"
    enrich_text = "CREATE TABLE trip_station_enrichment AS\nSELECT 1;\n"
    enrich_lf.write_bytes(enrich_text.encode("utf-8"))
    enrich_crlf.write_bytes(enrich_text.replace("\n", "\r\n").encode("utf-8"))
    enrich_cr.write_bytes(enrich_text.replace("\n", "\r").encode("utf-8"))
    enrich_hashes = {
        load_station_enrichment_sql(path)[1]
        for path in (enrich_lf, enrich_crlf, enrich_cr)
    }
    assert len(enrich_hashes) == 1


def test_sql_text_is_loaded_once_and_exact_hashed_text_is_executed(tmp_path, monkeypatch):
    build_fixture(tmp_path)
    default_enrichment = (
        Path(fact_publish.__file__).with_name("sql")
        / "evaluate_trip_station_history.sql"
    )
    enrichment_copy = tmp_path / "enrichment.sql"
    enrichment_copy.write_bytes(default_enrichment.read_bytes())
    fact_copy = tmp_path / "fact.sql"
    fact_copy.write_bytes(
        (Path(fact_publish.__file__).with_name("sql") / "fact_trip_trusted.sql").read_bytes()
    )
    original_enrichment_loader = fact_publish.load_station_enrichment_sql
    original_fact_loader = fact_publish._load_sql

    def load_enrichment_then_mutate(path):
        text, sha = original_enrichment_loader(path)
        Path(path).write_text("BROKEN SQL AFTER IDENTITY CAPTURE", encoding="utf-8")
        return text, sha

    def load_fact_then_mutate(path):
        text, sha = original_fact_loader(path)
        Path(path).write_text("BROKEN SQL AFTER IDENTITY CAPTURE", encoding="utf-8")
        return text, sha

    monkeypatch.setattr(
        fact_publish,
        "load_station_enrichment_sql",
        load_enrichment_then_mutate,
    )
    monkeypatch.setattr(fact_publish, "_load_sql", load_fact_then_mutate)

    result = publish_fact(
        tmp_path,
        enrichment_sql_path=enrichment_copy,
        fact_sql_path=fact_copy,
    )
    assert result.status == STATUS_PUBLISHED_CURRENT


def test_stale_unlocked_fact_lock_file_does_not_block_retry(tmp_path):
    build_fixture(tmp_path)
    lock_path = (
        tmp_path
        / "warehouse"
        / "fact_trip_trusted"
        / "locks"
        / "source_month=2026-01.lock"
    )
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    lock_path.write_text("0stale-owner\n", encoding="utf-8")

    result = publish_fact(tmp_path)
    assert result.status == STATUS_PUBLISHED_CURRENT


def test_active_fact_lock_blocks_same_month_publish(tmp_path):
    build_fixture(tmp_path)
    lock_path = (
        tmp_path
        / "warehouse"
        / "fact_trip_trusted"
        / "locks"
        / "source_month=2026-01.lock"
    )
    with fact_publish._advisory_lock(
        lock_path,
        label="test-holder",
        error_message="test lock",
    ):
        with pytest.raises(FactTripPublishError, match="same-month trusted fact"):
            publish_fact(tmp_path)
    result = publish_fact(tmp_path)
    assert result.status == STATUS_PUBLISHED_CURRENT


def test_residual_wal_is_rejected(tmp_path):
    build_fixture(tmp_path)
    published = publish_fact(tmp_path)
    wal_path = Path(str(published.version_path / "warehouse.duckdb") + ".wal")
    wal_path.write_bytes(b"not-finalized")

    with pytest.raises(FactTripPublishError, match="residual WAL"):
        validate_fact_trip_version(
            published.version_path,
            manifest=tmp_path / "manifest.json",
            raw_root=tmp_path / "raw",
            station_root=tmp_path / "station",
            clean_root=tmp_path / "clean",
            warehouse_root=tmp_path / "warehouse",
        )


def test_pre_current_fact_tamper_preserves_previous_current(tmp_path):
    build_fixture(tmp_path)
    first = publish_fact(tmp_path, transformation_version="fact-t1")
    before = first.current_pointer_path.read_bytes()

    def tamper_fact(version_path):
        database_path = version_path / "warehouse.duckdb"
        connection = duckdb.connect(str(database_path))
        try:
            connection.execute(
                """
                UPDATE fact_trip_trusted
                SET station_history_match = FALSE
                WHERE bike_id = 'SPB-MATCH'
                """
            )
            connection.execute("CHECKPOINT")
        finally:
            connection.close()

    with pytest.raises(FactTripPublishError, match="fingerprint"):
        publish_fact(
            tmp_path,
            transformation_version="fact-t2",
            before_current_publish=tamper_fact,
        )
    assert first.current_pointer_path.read_bytes() == before


def test_pre_current_physical_fact_change_preserves_previous_current(tmp_path):
    build_fixture(tmp_path)
    first = publish_fact(tmp_path, transformation_version="fact-t1")
    before = first.current_pointer_path.read_bytes()

    def rewrite_same_semantics(version_path):
        database_path = version_path / "warehouse.duckdb"
        connection = duckdb.connect(str(database_path))
        try:
            connection.execute("CREATE TABLE transient_rewrite_marker(value INTEGER)")
            connection.execute("DROP TABLE transient_rewrite_marker")
            connection.execute("CHECKPOINT")
        finally:
            connection.close()
        _rewrite_database_fingerprint(version_path)

    with pytest.raises(FactTripPublishError, match="immutable trusted fact bundle changed"):
        publish_fact(
            tmp_path,
            transformation_version="fact-t2",
            before_current_publish=rewrite_same_semantics,
        )
    assert first.current_pointer_path.read_bytes() == before


def test_pre_lock_physical_fact_change_preserves_previous_current(tmp_path, monkeypatch):
    build_fixture(tmp_path)
    first = publish_fact(tmp_path, transformation_version="fact-t1")
    before = first.current_pointer_path.read_bytes()
    real_locks = fact_publish._publication_locks
    state = {"mutated": False}

    @contextmanager
    def mutate_before_real_locks(**kwargs):
        if not state["mutated"]:
            state["mutated"] = True
            version_path = fact_publish.fact_version_path(
                kwargs["warehouse_root"],
                kwargs["source_month"],
                kwargs["fact_version_id"],
            )
            database_path = version_path / "warehouse.duckdb"
            connection = duckdb.connect(str(database_path))
            try:
                connection.execute("CREATE TABLE transient_rewrite_marker(value INTEGER)")
                connection.execute("DROP TABLE transient_rewrite_marker")
                connection.execute("CHECKPOINT")
            finally:
                connection.close()
            _rewrite_database_fingerprint(version_path)
        with real_locks(**kwargs):
            yield

    monkeypatch.setattr(fact_publish, "_publication_locks", mutate_before_real_locks)
    with pytest.raises(
        FactTripPublishError,
        match="immutable trusted fact bundle changed before final publication validation",
    ):
        publish_fact(tmp_path, transformation_version="fact-t2")
    assert first.current_pointer_path.read_bytes() == before


def test_pre_current_physical_clean_change_preserves_previous_current(tmp_path):
    clean, _history = build_fixture(tmp_path)
    first = publish_fact(tmp_path, transformation_version="fact-t1")
    before = first.current_pointer_path.read_bytes()

    def rewrite_same_logical_clean(_version_path):
        clean_path = clean.clean_version_path / "clean.parquet"
        rewritten_path = clean.clean_version_path / "clean-rewritten.parquet"
        connection = duckdb.connect()
        try:
            connection.execute(
                f"""
                COPY (
                    SELECT *
                    FROM read_parquet(
                        '{clean_path.resolve().as_posix().replace("'", "''")}',
                        hive_partitioning=false
                    )
                ) TO '{rewritten_path.resolve().as_posix().replace("'", "''")}'
                (FORMAT PARQUET, COMPRESSION UNCOMPRESSED)
                """
            )
        finally:
            connection.close()
        rewritten_path.replace(clean_path)
        _rewrite_named_file_fingerprint(
            clean.clean_version_path,
            metadata_key="clean_parquet",
            filename="clean.parquet",
        )

    with pytest.raises(FactTripPublishError, match="immutable Clean metadata changed"):
        publish_fact(
            tmp_path,
            transformation_version="fact-t2",
            before_current_publish=rewrite_same_logical_clean,
        )
    assert first.current_pointer_path.read_bytes() == before


def test_pre_current_physical_history_change_preserves_previous_current(tmp_path):
    _clean, history = build_fixture(tmp_path)
    first = publish_fact(tmp_path, transformation_version="fact-t1")
    before = first.current_pointer_path.read_bytes()

    def rewrite_same_logical_history(_version_path):
        database_path = history.version_path / "warehouse.duckdb"
        connection = duckdb.connect(str(database_path))
        try:
            connection.execute("CREATE TABLE transient_rewrite_marker(value INTEGER)")
            connection.execute("DROP TABLE transient_rewrite_marker")
            connection.execute("CHECKPOINT")
        finally:
            connection.close()
        _rewrite_database_fingerprint(history.version_path)

    with pytest.raises(
        FactTripPublishError,
        match="immutable station-history metadata changed",
    ):
        publish_fact(
            tmp_path,
            transformation_version="fact-t2",
            before_current_publish=rewrite_same_logical_history,
        )
    assert first.current_pointer_path.read_bytes() == before


def _rewrite_database_fingerprint(version_path: Path) -> None:
    _rewrite_named_file_fingerprint(
        version_path,
        metadata_key="warehouse_database",
        filename="warehouse.duckdb",
    )


def _rewrite_named_file_fingerprint(
    version_path: Path,
    *,
    metadata_key: str,
    filename: str,
) -> None:
    database_path = version_path / filename
    metadata_path = version_path / "metadata.json"
    metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
    data = database_path.read_bytes()
    metadata[metadata_key] = {
        "size_bytes": len(data),
        "sha256": hashlib.sha256(data).hexdigest(),
    }
    metadata_path.write_text(
        json.dumps(
            metadata,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        )
        + "\n",
        encoding="utf-8",
    )


def test_fact_publish_cli_calls_business_layer(tmp_path):
    clean, history = build_fixture(tmp_path)
    result = run_cli(
        "warehouse",
        "publish-fact-trip-month",
        "--manifest",
        str(tmp_path / "manifest.json"),
        "--raw-root",
        str(tmp_path / "raw"),
        "--station-root",
        str(tmp_path / "station"),
        "--clean-root",
        str(tmp_path / "clean"),
        "--warehouse-root",
        str(tmp_path / "warehouse"),
        "--source-month",
        "2026-01",
        "--transformation-version",
        "fact-t1",
    )
    assert result.returncode == 0, result.stderr
    assert result.stdout.startswith("FACT_TRIP_PUBLISH_OK fv1_")
    assert "status=PUBLISHED_CURRENT" in result.stdout
    assert f"clean_version_id={clean.clean_version_id}" in result.stdout
    assert f"history_version_id={history.history_version_id}" in result.stdout
    assert "fact_rows=4 matched=1 unmatched=3" in result.stdout
