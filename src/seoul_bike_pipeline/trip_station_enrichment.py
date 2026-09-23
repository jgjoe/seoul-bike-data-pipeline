"""Evaluate trusted Clean rental rows against snapshot-observed station history.

Slice 12 is intentionally an enrichment business layer, not another durable
truth layer. It consumes immutable Clean and immutable station-history
versions, performs the temporal join in plain SQL, and exposes enriched
trusted rows to CLI/tests/future fact publication without mutating Clean.
"""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Callable

import duckdb

from .clean_publish import (
    _CLEAN_SCHEMA,
    clean_version_path,
    current_pointer_path,
    validate_clean_version,
)
from .errors import SourceRegistrationError, TripStationEnrichmentError
from .station_history import (
    history_current_pointer_path,
    history_version_path,
    validate_station_history_version,
)

RULE_STATION_HISTORY_UNMATCHED = "station_history_unmatched"
SEVERITY_WARNING = "WARNING"
_CLEAN_VERSION = re.compile(r"cv1_[0-9a-f]{64}")
_HISTORY_VERSION = re.compile(r"wh1_[0-9a-f]{64}")
_MONTH = re.compile(r"20[0-9]{2}-(?:0[1-9]|1[0-2])")
_DEFAULT_SQL_PATH = Path(__file__).with_name("sql") / "evaluate_trip_station_history.sql"


@dataclass(frozen=True)
class TripStationEnrichmentSummary:
    source_month: str
    clean_version_id: str
    history_version_id: str
    sql_sha256: str
    trusted_candidate_count: int
    station_history_match_count: int
    station_history_unmatched_count: int
    station_history_warning_added_count: int


def station_enrichment_sql_identity(
    sql_path: str | Path | None = None,
) -> str:
    """Return the canonical SHA-256 identity of the enrichment SQL."""
    return load_station_enrichment_sql(sql_path)[1]


def load_station_enrichment_sql(
    sql_path: str | Path | None = None,
) -> tuple[str, str]:
    """Load canonical SQL text once so callers can execute the exact hashed bytes."""
    return _load_sql(sql_path)


def evaluate_current_trip_station_enrichment(
    *,
    manifest: str | Path,
    raw_root: str | Path,
    station_root: str | Path,
    clean_root: str | Path,
    warehouse_root: str | Path,
    source_month: str,
    sql_path: str | Path | None = None,
) -> TripStationEnrichmentSummary:
    """Evaluate one month using the Clean/history versions current at capture time."""
    source_month = _require_month(source_month)
    clean_pointer_path = current_pointer_path(clean_root, source_month)
    clean_pointer = _load_clean_pointer(clean_pointer_path, source_month)
    history_pointer_path = history_current_pointer_path(warehouse_root)
    history_pointer = _load_history_pointer(history_pointer_path)

    clean_path = clean_version_path(
        clean_root, source_month, clean_pointer["clean_version_id"]
    )
    history_path = history_version_path(
        warehouse_root, history_pointer["history_version_id"]
    )
    summary = evaluate_trip_station_enrichment(
        manifest=manifest,
        raw_root=raw_root,
        station_root=station_root,
        clean_version_path=clean_path,
        station_history_version_path=history_path,
        sql_path=sql_path,
    )

    if summary.source_month != source_month:
        raise TripStationEnrichmentError(
            "evaluated Clean source_month does not match captured current pointer"
        )
    if summary.clean_version_id != clean_pointer["clean_version_id"]:
        raise TripStationEnrichmentError(
            "evaluated Clean version does not match captured current pointer"
        )
    if summary.history_version_id != history_pointer["history_version_id"]:
        raise TripStationEnrichmentError(
            "evaluated station-history version does not match captured current pointer"
        )

    if _load_clean_pointer(clean_pointer_path, source_month) != clean_pointer:
        raise TripStationEnrichmentError(
            "current Clean pointer changed during station enrichment evaluation"
        )
    if _load_history_pointer(history_pointer_path) != history_pointer:
        raise TripStationEnrichmentError(
            "current station-history pointer changed during station enrichment evaluation"
        )
    return summary


def evaluate_trip_station_enrichment(
    *,
    manifest: str | Path,
    raw_root: str | Path,
    station_root: str | Path,
    clean_version_path: str | Path,
    station_history_version_path: str | Path,
    sql_path: str | Path | None = None,
    sql_text: str | None = None,
    on_row: Callable[[dict[str, object]], None] | None = None,
) -> TripStationEnrichmentSummary:
    """Evaluate immutable inputs and optionally stream enriched trusted rows."""
    clean_path = Path(clean_version_path)
    history_path = Path(station_history_version_path)
    clean_metadata = validate_clean_version(clean_path)
    history_metadata = validate_station_history_version(
        history_path,
        manifest=manifest,
        raw_root=raw_root,
        station_root=station_root,
    )
    source_month = _require_month(clean_metadata["source_month"])
    clean_version_id = _require_version(
        clean_metadata["clean_version_id"], _CLEAN_VERSION, "clean_version_id"
    )
    history_version_id = _require_version(
        history_metadata["history_version_id"], _HISTORY_VERSION, "history_version_id"
    )
    _require_input_path_binding(
        clean_path=clean_path,
        history_path=history_path,
        source_month=source_month,
        clean_version_id=clean_version_id,
        history_version_id=history_version_id,
    )
    if sql_text is not None and sql_path is not None:
        raise TripStationEnrichmentError(
            "provide either sql_path or sql_text for station enrichment, not both"
        )
    if sql_text is None:
        sql_text, sql_sha256 = _load_sql(sql_path)
    else:
        sql_text, sql_sha256 = _canonicalize_sql_text(sql_text)

    connection = duckdb.connect(
        config={
            "memory_limit": "3GB",
            "threads": "2",
        }
    )
    try:
        clean_sql = _sql_path(clean_path / "clean.parquet")
        history_db_sql = _sql_path(history_path / "warehouse.duckdb")
        connection.execute(
            f"CREATE VIEW clean_input AS "
            f"SELECT * FROM read_parquet('{clean_sql}', hive_partitioning=false)"
        )
        connection.execute(f"ATTACH '{history_db_sql}' AS station_history (READ_ONLY)")
        _require_pre_enrichment_contract(connection)
        connection.execute(sql_text)
        _require_materialized_enrichment_table(connection)
        _require_enrichment_schema(connection)
        (
            candidate_count,
            unique_source_rows,
            matched_count,
            invalid_future_count,
            invalid_expired_count,
            invalid_interval_shape_count,
        ) = connection.execute(
            """
            SELECT
                COUNT(*),
                COUNT(DISTINCT source_row_id),
                COUNT(*) FILTER (WHERE matched_station_applicable_from IS NOT NULL),
                COUNT(*) FILTER (
                    WHERE matched_station_applicable_from IS NOT NULL
                      AND matched_station_applicable_from > CAST(rent_at AS DATE)
                ),
                COUNT(*) FILTER (
                    WHERE matched_station_applicable_from IS NOT NULL
                      AND matched_station_applicable_until IS NOT NULL
                      AND CAST(rent_at AS DATE) >= matched_station_applicable_until
                ),
                COUNT(*) FILTER (
                    WHERE matched_station_applicable_from IS NULL
                      AND matched_station_applicable_until IS NOT NULL
                )
            FROM trip_station_enrichment
            """
        ).fetchone()
        if candidate_count != clean_metadata["trusted_fact_eligible_count"]:
            raise TripStationEnrichmentError(
                "enrichment candidate count does not match trusted-fact-eligible Clean count"
            )
        if unique_source_rows != candidate_count:
            raise TripStationEnrichmentError(
                "station temporal join produced duplicate trusted source rows"
            )
        if invalid_future_count:
            raise TripStationEnrichmentError(
                "station temporal join used a future station-history interval"
            )
        if invalid_expired_count:
            raise TripStationEnrichmentError(
                "station temporal join used an expired station-history interval outside the half-open boundary"
            )
        if invalid_interval_shape_count:
            raise TripStationEnrichmentError(
                "station temporal join produced an invalid matched interval shape"
            )
        if _count_clean_projection_mismatches(connection):
            raise TripStationEnrichmentError(
                "station enrichment SQL modified the trusted Clean projection or source-row set"
            )
        if _count_expected_interval_mismatches(connection):
            raise TripStationEnrichmentError(
                "station temporal join does not match the applicable rental-station history interval"
            )
        unmatched_count = int(candidate_count) - int(matched_count)

        _require_inputs_unchanged(
            clean_path=clean_path,
            history_path=history_path,
            clean_metadata=clean_metadata,
            history_metadata=history_metadata,
            manifest=manifest,
            raw_root=raw_root,
            station_root=station_root,
            source_month=source_month,
            clean_version_id=clean_version_id,
            history_version_id=history_version_id,
        )

        if on_row is not None:
            cursor = connection.execute(
                "SELECT * FROM trip_station_enrichment ORDER BY source_row_id"
            )
            columns = [item[0] for item in cursor.description]
            while batch := cursor.fetchmany(1000):
                for values in batch:
                    row = dict(zip(columns, values, strict=True))
                    _apply_station_history_result(row)
                    on_row(row)
    except duckdb.Error as exc:
        raise TripStationEnrichmentError(
            f"station-history trip enrichment failed: {exc}"
        ) from exc
    finally:
        connection.close()

    return TripStationEnrichmentSummary(
        source_month=source_month,
        clean_version_id=clean_version_id,
        history_version_id=history_version_id,
        sql_sha256=sql_sha256,
        trusted_candidate_count=int(candidate_count),
        station_history_match_count=int(matched_count),
        station_history_unmatched_count=unmatched_count,
        station_history_warning_added_count=unmatched_count,
    )


def _apply_station_history_result(row: dict[str, object]) -> None:
    matched = row["matched_station_applicable_from"] is not None
    row["station_history_match"] = matched
    issues = [
        {
            "rule_id": issue["rule_id"],
            "severity": issue["severity"],
            "field": issue["field"],
            "reason": issue["reason"],
        }
        for issue in row["dq_issues"]
    ]
    if not matched:
        issues.append(
            {
                "rule_id": RULE_STATION_HISTORY_UNMATCHED,
                "severity": SEVERITY_WARNING,
                "field": "rent_station_no",
                "reason": (
                    "no applicable observed station-history interval exists at rental date; "
                    "future observations are not used"
                ),
            }
        )
    row["dq_issues"] = issues
    row["dq_warning_count"] = len(
        {
            issue["rule_id"]
            for issue in issues
            if issue["severity"] == SEVERITY_WARNING
        }
    )


def _count_clean_projection_mismatches(
    connection: duckdb.DuckDBPyConnection,
) -> int:
    comparisons = "\n                   OR ".join(
        f'enriched."{name}" IS DISTINCT FROM clean."{name}"'
        for name, _ in _CLEAN_SCHEMA
        if name != "source_row_id"
    )
    return int(
        connection.execute(
            f"""
            WITH trusted_clean AS (
                SELECT *
                FROM clean_input
                WHERE trusted_fact_eligible
            )
            SELECT COUNT(*)
            FROM trusted_clean AS clean
            FULL OUTER JOIN trip_station_enrichment AS enriched
                ON enriched.source_row_id = clean.source_row_id
            WHERE clean.source_row_id IS NULL
               OR enriched.source_row_id IS NULL
               OR {comparisons}
            """
        ).fetchone()[0]
    )


def _count_expected_interval_mismatches(
    connection: duckdb.DuckDBPyConnection,
) -> int:
    return int(
        connection.execute(
            """
            WITH expected AS (
                SELECT
                    clean.source_row_id,
                    history.applicable_from AS expected_applicable_from,
                    history.applicable_until AS expected_applicable_until
                FROM clean_input AS clean
                LEFT JOIN station_history.dim_station_history AS history
                    ON history.station_no = clean.rent_station_no
                   AND CAST(clean.rent_at AS DATE) >= history.applicable_from
                   AND (
                        history.applicable_until IS NULL
                        OR CAST(clean.rent_at AS DATE) < history.applicable_until
                   )
                WHERE clean.trusted_fact_eligible
            )
            SELECT COUNT(*)
            FROM trip_station_enrichment AS enriched
            JOIN expected USING (source_row_id)
            WHERE enriched.matched_station_applicable_from
                    IS DISTINCT FROM expected.expected_applicable_from
               OR enriched.matched_station_applicable_until
                    IS DISTINCT FROM expected.expected_applicable_until
            """
        ).fetchone()[0]
    )


def _require_inputs_unchanged(
    *,
    clean_path: Path,
    history_path: Path,
    clean_metadata: dict[str, object],
    history_metadata: dict[str, object],
    manifest: str | Path,
    raw_root: str | Path,
    station_root: str | Path,
    source_month: str,
    clean_version_id: str,
    history_version_id: str,
) -> None:
    try:
        final_clean_metadata = validate_clean_version(clean_path)
        final_history_metadata = validate_station_history_version(
            history_path,
            manifest=manifest,
            raw_root=raw_root,
            station_root=station_root,
        )
    except SourceRegistrationError as exc:
        raise TripStationEnrichmentError(
            f"immutable enrichment input changed during evaluation: {exc}"
        ) from exc
    if final_clean_metadata != clean_metadata:
        raise TripStationEnrichmentError(
            "immutable Clean metadata changed during station enrichment evaluation"
        )
    if final_history_metadata != history_metadata:
        raise TripStationEnrichmentError(
            "immutable station-history metadata changed during station enrichment evaluation"
        )
    _require_input_path_binding(
        clean_path=clean_path,
        history_path=history_path,
        source_month=source_month,
        clean_version_id=clean_version_id,
        history_version_id=history_version_id,
    )


def _require_input_path_binding(
    *,
    clean_path: Path,
    history_path: Path,
    source_month: str,
    clean_version_id: str,
    history_version_id: str,
) -> None:
    if clean_path.name != f"clean_version_id={clean_version_id}":
        raise TripStationEnrichmentError(
            "Clean bundle version id does not match its directory/current binding"
        )
    if clean_path.parent.name != f"source_month={source_month}":
        raise TripStationEnrichmentError(
            "Clean bundle source_month does not match its directory/current binding"
        )
    if history_path.name != f"history_version_id={history_version_id}":
        raise TripStationEnrichmentError(
            "station-history bundle version id does not match its directory/current binding"
        )


def _require_pre_enrichment_contract(connection: duckdb.DuckDBPyConnection) -> None:
    (
        non_null_match_count,
        preexisting_station_warning_count,
        invalid_trusted_count,
    ) = connection.execute(
        f"""
        WITH station_warning_rows AS (
            SELECT DISTINCT clean.source_row_id
            FROM clean_input AS clean,
                 UNNEST(clean.dq_issues) AS issues(issue)
            WHERE issue.rule_id = '{RULE_STATION_HISTORY_UNMATCHED}'
        )
        SELECT
            COUNT(*) FILTER (WHERE clean.station_history_match IS NOT NULL),
            COUNT(*) FILTER (WHERE warning.source_row_id IS NOT NULL),
            COUNT(*) FILTER (
                WHERE clean.trusted_fact_eligible
                  AND (
                      clean.row_quarantined
                      OR clean.rent_at IS NULL
                      OR clean.rent_station_no IS NULL
                  )
            )
        FROM clean_input AS clean
        LEFT JOIN station_warning_rows AS warning USING (source_row_id)
        """
    ).fetchone()
    if non_null_match_count:
        raise TripStationEnrichmentError(
            "Clean input already contains evaluated station_history_match values"
        )
    if preexisting_station_warning_count:
        raise TripStationEnrichmentError(
            "Clean input contains station-history unmatched warning before enrichment"
        )
    if invalid_trusted_count:
        raise TripStationEnrichmentError(
            "trusted Clean candidate lacks required rental station/date identity"
        )


def _require_enrichment_schema(connection: duckdb.DuckDBPyConnection) -> None:
    actual = tuple(
        (item[0], item[1])
        for item in connection.execute(
            "DESCRIBE SELECT * FROM trip_station_enrichment"
        ).fetchall()
    )
    expected = (
        *_CLEAN_SCHEMA,
        ("matched_station_applicable_from", "DATE"),
        ("matched_station_applicable_until", "DATE"),
    )
    if actual != expected:
        raise TripStationEnrichmentError(
            f"trip station enrichment schema mismatch: expected {expected!r}, got {actual!r}"
        )


def _require_materialized_enrichment_table(
    connection: duckdb.DuckDBPyConnection,
) -> None:
    rows = connection.execute(
        """
        SELECT table_type
        FROM information_schema.tables
        WHERE table_schema = 'main'
          AND table_name = 'trip_station_enrichment'
        """
    ).fetchall()
    if rows != [("BASE TABLE",)]:
        raise TripStationEnrichmentError(
            "station enrichment SQL must materialize trip_station_enrichment as a base table"
        )


def _load_clean_pointer(path: Path, source_month: str) -> dict[str, object]:
    payload = _load_json(path, "current Clean pointer")
    if set(payload) != {"schema_version", "source_month", "clean_version_id"}:
        raise TripStationEnrichmentError(
            f"current Clean pointer {path} has invalid fields"
        )
    if payload["schema_version"] != 1 or payload["source_month"] != source_month:
        raise TripStationEnrichmentError(
            f"current Clean pointer {path} has invalid version/month binding"
        )
    _require_version(payload["clean_version_id"], _CLEAN_VERSION, "clean_version_id")
    return payload


def _load_history_pointer(path: Path) -> dict[str, object]:
    payload = _load_json(path, "current station-history pointer")
    if set(payload) != {"schema_version", "history_version_id"}:
        raise TripStationEnrichmentError(
            f"current station-history pointer {path} has invalid fields"
        )
    if payload["schema_version"] != 1:
        raise TripStationEnrichmentError(
            f"current station-history pointer {path} has unsupported schema_version"
        )
    _require_version(
        payload["history_version_id"], _HISTORY_VERSION, "history_version_id"
    )
    return payload


def _load_json(path: Path, label: str) -> dict[str, object]:
    try:
        with path.open("r", encoding="utf-8") as handle:
            payload = json.load(handle, object_pairs_hook=_reject_duplicate_pairs)
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise TripStationEnrichmentError(
            f"cannot read {label} {path}: {exc}"
        ) from exc
    if not isinstance(payload, dict):
        raise TripStationEnrichmentError(
            f"{label} {path} must be a JSON object"
        )
    return payload


def _load_sql(sql_path: str | Path | None) -> tuple[str, str]:
    path = Path(sql_path) if sql_path is not None else _DEFAULT_SQL_PATH
    try:
        text = path.read_text(encoding="utf-8")
    except (OSError, UnicodeDecodeError) as exc:
        raise TripStationEnrichmentError(
            f"cannot read station enrichment SQL {path}: {exc}"
        ) from exc
    return _canonicalize_sql_text(text)


def _canonicalize_sql_text(text: str) -> tuple[str, str]:
    text = text.replace("\r\n", "\n").replace("\r", "\n")
    if not text.strip():
        raise TripStationEnrichmentError("station enrichment SQL is empty")
    return text, hashlib.sha256(text.encode("utf-8")).hexdigest()


def _require_month(value: object) -> str:
    if not isinstance(value, str) or _MONTH.fullmatch(value) is None:
        raise TripStationEnrichmentError(
            f"source_month must be YYYY-MM, got {value!r}"
        )
    return value


def _require_version(
    value: object, pattern: re.Pattern[str], label: str
) -> str:
    if not isinstance(value, str) or pattern.fullmatch(value) is None:
        raise TripStationEnrichmentError(
            f"{label} has invalid deterministic identity"
        )
    return value


def _reject_duplicate_pairs(
    pairs: list[tuple[str, object]],
) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise TripStationEnrichmentError(
                f"duplicate JSON key {key!r} is not allowed"
            )
        result[key] = value
    return result


def _sql_path(path: Path) -> str:
    return path.resolve().as_posix().replace("'", "''")
