"""Bounded, optional cloud publication; local immutable bundles remain canonical.

Importing this module never imports a Google SDK or discovers credentials.
The same captured plan and business functions serve CLI, Airflow and offline CI.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import date, datetime
from decimal import Decimal
import hashlib
import json
from pathlib import Path
import re
import tempfile

import duckdb

from .clean_publish import clean_version_path, current_pointer_path, validate_clean_version
from .errors import SourceRegistrationError
from .fact_publish import (
    _advisory_lock,
    fact_current_pointer_path,
    fact_version_path,
    validate_fact_trip_version,
)
from .manifest import load_manifest
from .od_day_mart import (
    od_day_mart_current_pointer_path,
    od_day_mart_version_path,
    validate_od_day_mart_version,
)
from .source_month_quality_mart import (
    source_month_quality_mart_current_pointer_path,
    source_month_quality_mart_version_path,
    validate_source_month_quality_mart_version,
)
from .station_day_mart import (
    station_day_mart_current_pointer_path,
    station_day_mart_version_path,
    validate_station_day_mart_version,
)


class CloudPublishError(SourceRegistrationError):
    """Invalid cloud scope, artifact, lineage or verification result."""


class DefinitiveLoadError(CloudPublishError):
    """A load job definitively failed; a new attempt/job ID is permitted."""


TABLES = {
    "fact_trip_trusted": ("source_month_date", ("rent_station_no", "return_station_no")),
    "mart_od_day": ("service_date", ("origin_station_no", "destination_station_no")),
    "mart_station_day": ("service_date", ("station_no",)),
    "mart_source_month_quality": ("source_month_date", ()),
}
MONTH = re.compile(r"20\d{2}-(?:0[1-9]|1[0-2])")
SHA = re.compile(r"[0-9a-f]{64}")


def canonical(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False)


def identity(value):
    return hashlib.sha256(canonical(value).encode("utf-8")).hexdigest()


def fingerprint(path):
    digest = hashlib.sha256()
    size = 0
    with Path(path).open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
            size += len(block)
    return {"size_bytes": size, "sha256": digest.hexdigest()}


def read_json(path):
    return json.loads(Path(path).read_text(encoding="utf-8"))


def write_json(path, value):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(path.name + ".tmp")
    temporary.write_text(canonical(value) + "\n", encoding="utf-8")
    temporary.replace(path)


def scalar(value):
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, Decimal):
        return str(value)
    return value


@dataclass(frozen=True)
class CloudConfig:
    project: str
    bucket: str
    dataset: str = "bike_cloud"
    location: str = "us-central1"
    maximum_bytes_billed: int = 1024**3
    maximum_upload_bytes: int = 4_000_000_000
    verification: bool = False

    def table(self, name):
        prefix = "verify_" if self.verification else ""
        return f"{self.project}.{self.dataset}.{prefix}{name}"

    @property
    def object_prefix(self):
        return "v1/verification" if self.verification else "v1"

    def validate(self):
        if type(self.verification) is not bool:
            raise CloudPublishError("verification must be boolean")
        if not re.fullmatch(r"[a-z][a-z0-9-]{4,28}[a-z0-9]", self.project):
            raise CloudPublishError("invalid project ID")
        if not re.fullmatch(r"[a-z0-9][a-z0-9._-]{1,220}[a-z0-9]", self.bucket):
            raise CloudPublishError("invalid bucket name")
        if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]{0,127}", self.dataset):
            raise CloudPublishError("invalid dataset ID")
        if self.location != "us-central1":
            raise CloudPublishError("this representative path requires us-central1")
        for value in (self.maximum_bytes_billed, self.maximum_upload_bytes):
            if type(value) is not int or value <= 0:
                raise CloudPublishError("cost limits must be positive integers")


def require_months(months):
    if not isinstance(months, (list, tuple)) or not 1 <= len(months) <= 3:
        raise CloudPublishError("select 1..3 distinct representative months")
    if len(set(months)) != len(months) or any(
        not isinstance(month, str) or not MONTH.fullmatch(month)
        or not "2020-01" <= month <= "2026-06" for month in months
    ):
        raise CloudPublishError("invalid or duplicate representative month")
    return sorted(months)


def _sql_path(path):
    return str(Path(path).resolve()).replace("'", "''")


def schema_field(name, duck_type):
    """Map the existing canonical schema, not an inferred BigQuery schema."""
    if duck_type.endswith("[]"):
        field = schema_field(name, duck_type[:-2])
        field["mode"] = "REPEATED"
        return field
    if duck_type == "STRUCT(rule_id VARCHAR, severity VARCHAR, field VARCHAR, reason VARCHAR)":
        return {
            "name": name, "type": "RECORD", "mode": "NULLABLE",
            "fields": [schema_field(key, "VARCHAR") for key in ("rule_id", "severity", "field", "reason")],
        }
    types = {
        "VARCHAR": "STRING", "BIGINT": "INTEGER", "INTEGER": "INTEGER",
        "SMALLINT": "INTEGER", "BOOLEAN": "BOOLEAN", "DOUBLE": "FLOAT",
        "DATE": "DATE", "TIMESTAMP": "DATETIME", "DECIMAL(38,18)": "BIGNUMERIC",
    }
    if duck_type not in types:
        raise CloudPublishError(f"unsupported cloud type: {duck_type}")
    return {"name": name, "type": types[duck_type], "mode": "NULLABLE"}


def metric_expressions(table):
    """Portable SQL expressions; expectations are computed from local inputs."""
    result = {"row_count": "COUNT(*)"}
    if table == "fact_trip_trusted":
        result.update({
            "unique_rows": "COUNT(DISTINCT source_row_id)",
            "invalid_rows": "COUNT(CASE WHEN row_quarantined OR logical_trip_conflict OR NOT trusted_fact_eligible THEN 1 END)",
            "completed": "COUNT(CASE WHEN is_completed_trip THEN 1 END)",
            "missing_return": "COUNT(CASE WHEN NOT is_completed_trip THEN 1 END)",
            "valid_distance": "COUNT(CASE WHEN distance_metric_eligible THEN 1 END)",
            "station_matched": "COUNT(CASE WHEN station_history_match THEN 1 END)",
            "warning_rows": "COUNT(CASE WHEN dq_warning_count > 0 THEN 1 END)",
            "usage_sum": "SUM(usage_min)",
            "distance_sum": "SUM(CASE WHEN distance_metric_eligible THEN distance_m END)",
            "first_rent": "MIN(rent_at)", "last_rent": "MAX(rent_at)",
        })
    elif table == "mart_station_day":
        for key in ("rental_count", "return_count", "completed_trip_count", "unique_bike_count",
                    "valid_distance_trip_count", "missing_return_count", "station_history_match_count"):
            result[key] = f"SUM({key})"
        for key in ("avg_usage_min", "median_usage_min"):
            result[key] = f"SUM({key})"
    elif table == "mart_od_day":
        for key in ("trip_count", "unique_bike_count", "valid_distance_trip_count",
                    "avg_usage_min", "median_usage_min", "valid_distance_avg", "valid_distance_median"):
            result[key] = f"SUM({key})"
    elif table == "mart_source_month_quality":
        for key in ("source_row_count", "clean_row_count", "trusted_fact_count", "quarantine_count",
                    "logical_conflict_group_count", "exact_duplicate_group_count",
                    "incomplete_trip_count", "station_unmatched_count", "distance_zero_rate",
                    "extreme_distance_count", "invalid_birth_year_rate", "sex_missing_rate"):
            result[key] = f"SUM({key})"
    else:
        raise CloudPublishError("unsupported target table")
    return result


def metric_select(table):
    return ", ".join(f"{expression} AS {key}" for key, expression in metric_expressions(table).items())


def compare_metrics(expected, actual, schema):
    if set(expected) != set(actual):
        raise CloudPublishError("verification metric keys differ")
    float_columns = {field["name"] for field in schema if field["type"] == "FLOAT"}
    for key, value in expected.items():
        observed = scalar(actual[key])
        if value is None or observed is None:
            equal = value is None and observed is None
        elif key in float_columns:
            equal = abs(float(value) - float(observed)) <= 1e-9 * max(1, abs(float(value)))
        elif isinstance(value, (int, float)) or key == "distance_sum":
            equal = Decimal(str(value)) == Decimal(str(observed))
        else:
            equal = value == observed
        if not equal:
            raise CloudPublishError(f"verification mismatch {key}: expected={value!r}, actual={observed!r}")


def capture_cloud_plan(*, manifest, raw_root, station_root, clean_root, warehouse_root,
                       export_root, months, config, station_months=()):
    """Validate immutable current bundles and export; no cloud client is created."""
    config.validate()
    months = require_months(months)
    station_months = sorted(station_months)
    if len(set(station_months)) != len(station_months) or not set(station_months) <= set(months):
        raise CloudPublishError("station_months must be a distinct subset of selected months")
    roots = dict(manifest=manifest, raw_root=raw_root, station_root=station_root,
                 clean_root=clean_root, warehouse_root=warehouse_root)
    export_root = Path(export_root).resolve()
    export_root.mkdir(parents=True, exist_ok=True)
    sources = {record.source_version_id: record for record in load_manifest(manifest)}
    artifacts = []
    pointers = {}

    def pointer(path, key):
        path = Path(path)
        data = read_json(path)
        pointers[str(path.resolve())] = data
        return data[key]

    def add(table, month, version, bundle, metadata, lineage):
        if table == "clean":
            path = bundle / "clean.parquet"
            summary = {key: metadata[key] for key in
                       ("source_row_count", "clean_row_count", "trusted_fact_eligible_count",
                        "quarantined_row_count", "quarantine_issue_count")}
            schema = []
        else:
            extra = f", DATE '{month}-01' AS source_month_date" if TABLES[table][0] == "source_month_date" else ""
            order = "source_row_id" if table == "fact_trip_trusted" else (
                "service_date, station_no" if table == "mart_station_day" else
                "service_date, origin_station_no, destination_station_no" if table == "mart_od_day" else "source_month"
            )
            with duckdb.connect(str(bundle / "warehouse.duckdb"), read_only=True) as connection:
                select = f"SELECT *{extra} FROM {table}"
                schema = [schema_field(row[0], row[1]) for row in connection.execute(f"DESCRIBE ({select})").fetchall()]
                values = connection.execute(f"SELECT {metric_select(table)} FROM {table}").fetchone()
                summary = dict(zip(metric_expressions(table), map(scalar, values)))
                partition_field = TABLES[table][0]
                wrong = connection.execute(
                    f"SELECT COUNT(*) FROM ({select}) WHERE {partition_field} IS NULL "
                    f"OR strftime({partition_field}, '%Y-%m') <> ?", [month]
                ).fetchone()[0]
                if wrong:
                    raise CloudPublishError(f"{table} contains rows outside {month}")
                with tempfile.TemporaryDirectory(prefix="cloud-export-", dir=export_root) as temporary:
                    candidate = Path(temporary) / "data.parquet"
                    connection.execute(
                        f"COPY ({select} ORDER BY {order}) TO '{_sql_path(candidate)}' (FORMAT PARQUET, COMPRESSION ZSTD)"
                    )
                    sha = fingerprint(candidate)["sha256"]
                    path = export_root / f"{sha}.parquet"
                    if path.exists():
                        if fingerprint(path) != fingerprint(candidate):
                            raise CloudPublishError("existing export hash mismatch")
                    else:
                        candidate.replace(path)
        artifact = {
            "table": table, "month": month, "version": version,
            "path": str(path.resolve()), **fingerprint(path),
            "metadata": metadata, "lineage": lineage, "schema": schema,
            "expected": summary,
        }
        if table == "clean":
            prefix = "clean"
        elif table == "fact_trip_trusted":
            prefix = "trusted"
        else:
            prefix = f"marts/{table}"
        artifact["object"] = f"{config.object_prefix}/{prefix}/month={month}/{version}/{artifact['sha256']}/data.parquet"
        artifact["artifact_id"] = identity(artifact)
        artifacts.append(artifact)

    for month in months:
        clean_id = pointer(current_pointer_path(clean_root, month), "clean_version_id")
        clean_path = clean_version_path(clean_root, month, clean_id)
        clean = validate_clean_version(clean_path)
        source = sources[clean["source_version_id"]].to_json()
        if (clean["source_month"] != month or clean["clean_version_id"] != clean_id
                or clean["source_artifact_sha256"] != source["sha256"]
                or clean["source_artifact_size_bytes"] != source["size_bytes"]):
            raise CloudPublishError("Clean/manifest lineage mismatch")
        fact_id = pointer(fact_current_pointer_path(warehouse_root, month), "fact_version_id")
        fact_path = fact_version_path(warehouse_root, month, fact_id)
        fact = validate_fact_trip_version(fact_path, **roots)
        if fact["clean_version_id"] != clean_id or fact["source_version_id"] != source["source_version_id"]:
            raise CloudPublishError("current Clean/fact lineage mismatch")
        if clean["source_row_count"] != clean["clean_row_count"] or (
            clean["clean_row_count"] != fact["fact_row_count"] + clean["quarantined_row_count"]
        ):
            raise CloudPublishError("source = Trusted + quarantined row reconciliation failed")
        lineage = {"source": source, "clean_version_id": clean_id, "fact_version_id": fact_id}
        add("clean", month, clean_id, clean_path, clean, lineage)
        add("fact_trip_trusted", month, fact_id, fact_path, fact, lineage)
        for table, pointer_function, path_function, validator in (
            ("mart_od_day", od_day_mart_current_pointer_path, od_day_mart_version_path, validate_od_day_mart_version),
            ("mart_source_month_quality", source_month_quality_mart_current_pointer_path,
             source_month_quality_mart_version_path, validate_source_month_quality_mart_version),
        ):
            mart_id = pointer(pointer_function(warehouse_root, month), "mart_version_id")
            bundle = path_function(warehouse_root, month, mart_id)
            metadata = validator(bundle, **roots)
            if metadata["fact_version_id"] != fact_id or (
                table == "mart_source_month_quality" and metadata["clean_version_id"] != clean_id
            ):
                raise CloudPublishError("current mart/fact lineage mismatch")
            add(table, month, mart_id, bundle, metadata, lineage)

    for month in station_months:
        mart_id = pointer(station_day_mart_current_pointer_path(warehouse_root, month), "mart_version_id")
        bundle = station_day_mart_version_path(warehouse_root, month, mart_id)
        metadata = validate_station_day_mart_version(bundle, **roots)
        for dependency in metadata["fact_scan_lineage"]:
            current = pointer(fact_current_pointer_path(warehouse_root, dependency["source_month"]), "fact_version_id")
            if current != dependency["fact_version_id"]:
                raise CloudPublishError("station-day references a superseded fact; rebuild locally first")
        add("mart_station_day", month, mart_id, bundle, metadata, {
            "fact_lineage": metadata["fact_lineage"],
            "fact_scan_lineage": metadata["fact_scan_lineage"],
            "scope": "local historical dependencies; not limited to cloud sample",
        })
    for path, captured in pointers.items():
        if read_json(path) != captured:
            raise CloudPublishError("local current lineage changed during cloud capture")
    plan = {
        "schema_version": 1, "config": asdict(config), "months": months,
        "station_months": station_months, "artifacts": artifacts,
        "export_root": str(export_root),
    }
    plan["plan_id"] = identity(plan)
    validate_cloud_plan(plan)
    write_json(export_root / f"{plan['plan_id']}.json", plan)
    return plan


def validate_cloud_plan(plan):
    if not isinstance(plan, dict) or plan.get("schema_version") != 1:
        raise CloudPublishError("unsupported cloud plan")
    if plan.get("plan_id") != identity({key: value for key, value in plan.items() if key != "plan_id"}):
        raise CloudPublishError("cloud plan binding mismatch")
    config = CloudConfig(**plan["config"])
    config.validate()
    months = require_months(plan["months"])
    targets = set()
    for artifact in plan["artifacts"]:
        if artifact["month"] not in months or artifact["table"] not in {"clean", *TABLES}:
            raise CloudPublishError("artifact outside selected scope")
        target = artifact["table"], artifact["month"]
        if target in targets:
            raise CloudPublishError("duplicate target partition")
        targets.add(target)
        if artifact["artifact_id"] != identity({key: value for key, value in artifact.items() if key != "artifact_id"}):
            raise CloudPublishError("artifact binding mismatch")
        if not SHA.fullmatch(artifact["sha256"]) or fingerprint(artifact["path"]) != {
            key: artifact[key] for key in ("size_bytes", "sha256")
        }:
            raise CloudPublishError("captured artifact has changed")
    required = {(table, month) for month in months for table in
                ("clean", "fact_trip_trusted", "mart_od_day", "mart_source_month_quality")}
    required.update(("mart_station_day", month) for month in plan["station_months"])
    if targets != required:
        raise CloudPublishError("cloud plan target coverage differs")
    if sum(item["size_bytes"] for item in plan["artifacts"]) > config.maximum_upload_bytes:
        raise CloudPublishError("representative artifacts exceed upload envelope; reduce selected months")
    return config


def publish_cloud_plan(plan, *, storage=None, bigquery=None, previous=None):
    """Publish/recover independently per partition, under a single-runtime lock."""
    config = validate_cloud_plan(plan)  # Must precede even credential discovery.
    allowed = None
    if previous is not None:
        if previous.get("plan_id") != plan["plan_id"] or previous.get("binding") != identity(
            {key: value for key, value in previous.items() if key != "binding"}
        ):
            raise CloudPublishError("retry result is not bound to captured cloud plan")
        if (len(previous["targets"]) != len(plan["artifacts"])
                or {row["artifact_id"] for row in previous["targets"]} !=
                {artifact["artifact_id"] for artifact in plan["artifacts"]}):
            raise CloudPublishError("retry result target coverage differs")
        allowed = {row["artifact_id"] for row in previous["targets"] if row["status"] == "FAILED"}
    if storage is None or bigquery is None:
        try:
            from google.auth.exceptions import DefaultCredentialsError
            if storage is None:
                from .cloud_storage import GoogleCloudStorage
                storage = GoogleCloudStorage(config)
            if bigquery is None:
                from .bigquery_load import GoogleBigQuery
                bigquery = GoogleBigQuery(config)
        except ImportError as exc:
            raise CloudPublishError("install the optional .[cloud] dependencies before cloud publication") from exc
        except DefaultCredentialsError as exc:
            raise CloudPublishError("cloud ADC is not configured; follow docs/cloud-setup-ko.md") from exc
    root = Path(plan["export_root"])
    with _advisory_lock(root / "cloud-publish.lock", label="cloud",
                        error_message="another cloud publisher is active in this runtime"):
        storage.preflight()
        bigquery.preflight()
        rows = []
        for artifact in plan["artifacts"]:
            if allowed is not None and artifact["artifact_id"] not in allowed:
                rows.append(next(row for row in previous["targets"] if row["artifact_id"] == artifact["artifact_id"]))
                continue
            row = {key: artifact[key] for key in ("artifact_id", "table", "month")}
            try:
                obj = storage.ensure_file(artifact["object"], Path(artifact["path"]),
                                          {key: str(artifact[key]) for key in ("sha256", "size_bytes", "artifact_id")})
                row["object"] = obj
                if artifact["table"] == "clean":
                    row["status"] = "UPLOADED" if obj["created"] else "SKIP_UNCHANGED"
                else:
                    row.update(load_partition(config, artifact, obj, bigquery))
            except Exception as exc:
                row.update(status="FAILED", error_type=type(exc).__name__, error=str(exc))
            rows.append(row)
        # Small content-bound manifest is uploaded only once all target attempts
        # have been recorded. Its presence alone is NOT a global success pointer.
        manifest_path = root / f"{plan['plan_id']}.json"
        write_json(manifest_path, plan)
        storage.ensure_file(f"{config.object_prefix}/manifests/{plan['plan_id']}.json", manifest_path,
                            {key: str(value) for key, value in fingerprint(manifest_path).items()})
        result = {"plan_id": plan["plan_id"], "targets": rows,
                  "status": "FAILED" if any(row["status"] == "FAILED" for row in rows) else "SUCCEEDED"}
        result["binding"] = identity(result)
        return result


def load_partition(config, artifact, obj, backend):
    target = config.table(artifact["table"])
    latest = backend.latest(target, artifact["month"])
    same = latest is not None and latest["artifact_id"] == artifact["artifact_id"]
    if latest and latest["status"] == "PENDING" and not same:
        raise CloudPublishError("recover the pending captured artifact before publishing a different revision")
    if same and latest["status"] == "SUCCEEDED":
        verification = backend.verify(artifact)
        compare_metrics(artifact["expected"], verification["metrics"], artifact["schema"])
        return {"status": "SKIP_UNCHANGED", "job_id": latest["job_id"], "verification": verification}
    if same and latest["status"] == "PENDING":
        event = latest
    else:
        attempt = 1 if latest is None else latest["attempt"] + 1
        job_id = "bike_" + identity([target, artifact["month"], artifact["artifact_id"], attempt])
        event = {
            "target": target, "month": artifact["month"], "artifact_id": artifact["artifact_id"],
            "attempt": attempt, "job_id": job_id, "status": "PENDING",
            "payload": {"artifact": artifact, "object": obj},
        }
        backend.record(event)  # Intent is durable before submitting the load.
    try:
        stats = backend.load(artifact, obj["uri"], event["job_id"])
    except DefinitiveLoadError as exc:
        backend.record({**event, "status": "FAILED", "error": str(exc)})
        raise
    verification = backend.verify(artifact)
    compare_metrics(artifact["expected"], verification["metrics"], artifact["schema"])
    backend.record({**event, "status": "SUCCEEDED", "load": stats, "verification": verification})
    return {"status": "LOADED", "job_id": event["job_id"], "load": stats, "verification": verification}
