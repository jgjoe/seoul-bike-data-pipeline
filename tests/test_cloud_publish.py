from __future__ import annotations

import copy
import json
from pathlib import Path

import duckdb
import pytest

from seoul_bike_pipeline.airflow_control import (
    capture_cloud_payload, publish_cloud_payload, require_cloud_success_payload,
    resolve_backfill_request,
)
from seoul_bike_pipeline.backfill import run_backfill_range
from seoul_bike_pipeline.cli import main
from seoul_bike_pipeline.cloud_publish import (
    CloudConfig, CloudPublishError, DefinitiveLoadError, capture_cloud_plan,
    fingerprint, identity, metric_select, publish_cloud_plan, scalar, schema_field,
    validate_cloud_plan,
)
from tests.test_airflow_control import _params
from tests.test_backfill import (
    TRANSFORMATION_VERSION, _build_backfill_base, _month_row, _register_rental,
)
from tests.test_station_day_mart import rental_row


class FakeStorage:
    def __init__(self):
        self.objects = {}
        self.uploads = []

    def preflight(self):
        pass

    def ensure_file(self, name, path, metadata):
        data = Path(path).read_bytes()
        created = name not in self.objects
        if created:
            self.objects[name] = (data, metadata)
            self.uploads.append(name)
        elif self.objects[name] != (data, metadata):
            raise CloudPublishError("object mismatch")
        return {"uri": "gs://offline-bucket/" + name, "generation": "1", "created": created,
                "size_bytes": len(data), "sha256": metadata["sha256"]}


class FakeBigQuery:
    """Server state independent of publication expectations, backed by Parquet."""

    def __init__(self):
        self.events = []
        self.jobs = {}
        self.partitions = {}
        self.submitted = []
        self.fail = set()
        self.timeout_after_commit = set()
        self.crash_before_success_history = False
        self.corrupt_metrics = False

    def preflight(self):
        pass

    def latest(self, target, month):
        rows = [event for event in self.events if event["target"] == target and event["month"] == month]
        return copy.deepcopy(rows[-1]) if rows else None

    def record(self, event):
        if event["status"] == "SUCCEEDED" and self.crash_before_success_history:
            self.crash_before_success_history = False
            raise TimeoutError("injected lost history response")
        self.events.append(copy.deepcopy(event))

    def load(self, artifact, uri, job_id):
        target = artifact["table"], artifact["month"]
        if job_id not in self.jobs:
            self.submitted.append((target, job_id))
            if target in self.fail:
                self.jobs[job_id] = {"error": "injected invalid load schema"}
            else:
                self.jobs[job_id] = {"artifact": copy.deepcopy(artifact), "uri": uri}
                self.partitions[target] = copy.deepcopy(artifact)
                if target in self.timeout_after_commit:
                    self.timeout_after_commit.remove(target)
                    raise TimeoutError("injected lost load response")
        if "error" in self.jobs[job_id]:
            raise DefinitiveLoadError(self.jobs[job_id]["error"])
        assert self.jobs[job_id]["artifact"] == artifact
        assert self.jobs[job_id]["uri"] == uri
        return {"output_rows": self.verify(artifact)["metrics"]["row_count"]}

    def verify(self, artifact):
        current = self.partitions[artifact["table"], artifact["month"]]
        with duckdb.connect() as connection:
            connection.read_parquet(current["path"], hive_partitioning=False).create_view("input")
            cursor = connection.execute(f"SELECT {metric_select(artifact['table'])} FROM input")
            metrics = dict(zip([column[0] for column in cursor.description], map(scalar, cursor.fetchone())))
        if self.corrupt_metrics:
            metrics["row_count"] += 1
        return {"metrics": metrics, "bytes_processed": current["size_bytes"], "bytes_billed": 0}


@pytest.fixture(scope="module")
def cloud_base(tmp_path_factory):
    root = tmp_path_factory.mktemp("cloud-base")
    _build_backfill_base(root)
    # One warning row, an incomplete trip, and a logical-key conflict group.
    row = _month_row("2020-02")
    warning = rental_row(bike_id="WARN", rent_at="2020-02-03 09:00:00",
                         rent_station_no="00001", return_at="", return_station_no="",
                         usage_min="10", distance_m="0")
    conflict = list(row)
    conflict[10] = "200"
    _register_rental(root, source_month="2020-02", rows=[row, tuple(conflict), warning], revision="dq")
    _, result = run_backfill_range(
        **roots(root), start_month="2020-02", end_month="2020-02",
        transformation_version=TRANSFORMATION_VERSION, configuration={},
    )
    assert result.status == "SUCCEEDED"
    return root


def roots(root):
    return dict(manifest=root / "manifest.json", raw_root=root / "raw",
                station_root=root / "station", clean_root=root / "clean",
                warehouse_root=root / "warehouse")


def capture(root, export, **overrides):
    args = dict(**roots(root), export_root=export, months=["2020-01", "2020-02"],
                station_months=["2020-02"],
                config=CloudConfig("seoul-bike-dw", "offline-bucket"))
    args.update(overrides)
    return capture_cloud_plan(**args)


def test_capture_schema_reconciliation_and_local_immutability(cloud_base, tmp_path):
    files = [path for path in cloud_base.rglob("*") if path.is_file()]
    before = {str(path): fingerprint(path) for path in files}
    plan = capture(cloud_base, tmp_path)
    assert len(plan["artifacts"]) == 9
    assert before == {str(path): fingerprint(path) for path in files}
    clean = next(a for a in plan["artifacts"] if a["table"] == "clean" and a["month"] == "2020-02")
    fact = next(a for a in plan["artifacts"] if a["table"] == "fact_trip_trusted" and a["month"] == "2020-02")
    assert clean["expected"]["clean_row_count"] == 3
    assert clean["expected"]["quarantined_row_count"] == 2
    assert fact["expected"]["row_count"] == 1
    assert fact["expected"]["invalid_rows"] == 0
    assert fact["expected"]["warning_rows"] == 1
    assert fact["expected"]["missing_return"] == 1
    fields = {field["name"]: field for field in fact["schema"]}
    assert fields["rent_at"]["type"] == "DATETIME"
    assert fields["distance_m"]["type"] == "BIGNUMERIC"
    assert fields["dq_issues"]["mode"] == "REPEATED"
    assert fields["source_month_date"]["type"] == "DATE"
    assert fact["lineage"]["source"]["sha256"] == clean["metadata"]["source_artifact_sha256"]
    assert capture(cloud_base, tmp_path)["plan_id"] == plan["plan_id"]


def test_first_publish_and_identical_rerun_do_not_submit_or_upload_again(cloud_base, tmp_path):
    plan = capture(cloud_base, tmp_path)
    storage, bq = FakeStorage(), FakeBigQuery()
    first = publish_cloud_plan(plan, storage=storage, bigquery=bq)
    assert first["status"] == "SUCCEEDED"
    assert len(bq.submitted) == 7
    uploads, loads = len(storage.uploads), len(bq.submitted)
    again = publish_cloud_plan(plan, storage=storage, bigquery=bq)
    assert again["status"] == "SUCCEEDED"
    assert {row["status"] for row in again["targets"]} == {"SKIP_UNCHANGED"}
    assert len(storage.uploads) == uploads
    assert len(bq.submitted) == loads
    assert all("verification" in row for row in again["targets"] if row["table"] != "clean")


def test_verification_artifacts_and_history_target_do_not_collide_with_production(cloud_base, tmp_path):
    plan = capture(cloud_base, tmp_path,
                   config=CloudConfig("seoul-bike-dw", "offline-bucket", verification=True))
    storage, bq = FakeStorage(), FakeBigQuery()
    assert publish_cloud_plan(plan, storage=storage, bigquery=bq)["status"] == "SUCCEEDED"
    assert all(name.startswith("v1/verification/") for name in storage.objects)
    assert all(event["target"].startswith("seoul-bike-dw.bike_cloud.verify_") for event in bq.events)


def test_changed_month_replaces_only_its_fact_and_affected_marts(cloud_base, tmp_path):
    import shutil
    root = tmp_path / "local"
    shutil.copytree(cloud_base, root)
    export = tmp_path / "exports"
    old = capture(root, export)
    storage, bq = FakeStorage(), FakeBigQuery()
    assert publish_cloud_plan(old, storage=storage, bigquery=bq)["status"] == "SUCCEEDED"
    old_jobs = len(bq.submitted)
    _register_rental(root, source_month="2020-02", rows=[_month_row("2020-02")], revision="changed")
    _, local = run_backfill_range(
        **roots(root), start_month="2020-02", end_month="2020-02",
        transformation_version=TRANSFORMATION_VERSION, configuration={},
    )
    assert local.status == "SUCCEEDED"
    changed = capture(root, export)
    result = publish_cloud_plan(changed, storage=storage, bigquery=bq)
    assert result["status"] == "SUCCEEDED"
    assert {target[1] for target, _ in bq.submitted[old_jobs:]} == {"2020-02"}
    assert len(bq.submitted[old_jobs:]) == 4
    assert all(row["status"] == "SKIP_UNCHANGED" for row in result["targets"] if row["month"] == "2020-01")


@pytest.mark.parametrize("failure", ["load", "timeout", "history"])
def test_failure_recovery_preserves_partitions_and_uses_correct_job_identity(cloud_base, tmp_path, failure):
    plan = capture(cloud_base, tmp_path)
    storage, bq = FakeStorage(), FakeBigQuery()
    target = "fact_trip_trusted", "2020-01"
    if failure == "load":
        bq.fail.add(target)
    elif failure == "timeout":
        bq.timeout_after_commit.add(target)
    else:
        bq.crash_before_success_history = True
    first = publish_cloud_plan(plan, storage=storage, bigquery=bq)
    assert first["status"] == "FAILED"
    assert sum(row["status"] == "FAILED" for row in first["targets"]) == 1
    submitted = list(bq.submitted)
    bq.fail.clear()
    recovered = publish_cloud_plan(plan, storage=storage, bigquery=bq, previous=first)
    assert recovered["status"] == "SUCCEEDED"
    assert len(bq.submitted) == len(submitted) + (1 if failure == "load" else 0)
    if failure == "load":
        assert bq.submitted[-1][0] == target
        assert bq.submitted[-1][1] != next(job for key, job in submitted if key == target)


def test_failed_revision_keeps_previous_partition_and_recovers(cloud_base, tmp_path):
    plan = capture(cloud_base, tmp_path)
    storage, bq = FakeStorage(), FakeBigQuery()
    assert publish_cloud_plan(plan, storage=storage, bigquery=bq)["status"] == "SUCCEEDED"
    artifact = next(item for item in plan["artifacts"] if item["table"] == "fact_trip_trusted")
    key = artifact["table"], artifact["month"]
    original = bq.partitions[key]
    revised = copy.deepcopy(plan)
    changed = next(item for item in revised["artifacts"] if item["artifact_id"] == artifact["artifact_id"])
    changed["metadata"]["cloud_test_revision"] = "synthetic identity only"
    changed["version"] += "_synthetic"
    changed["object"] = changed["object"].replace(artifact["version"], changed["version"])
    changed["artifact_id"] = identity({k: v for k, v in changed.items() if k != "artifact_id"})
    revised["plan_id"] = identity({k: v for k, v in revised.items() if k != "plan_id"})
    bq.fail.add(key)
    result = publish_cloud_plan(revised, storage=storage, bigquery=bq)
    assert result["status"] == "FAILED"
    assert bq.partitions[key] == original
    bq.fail.clear()
    assert publish_cloud_plan(revised, storage=storage, bigquery=bq, previous=result)["status"] == "SUCCEEDED"


def test_invalid_scope_or_bound_artifact_fails_before_client_creation(cloud_base, tmp_path, monkeypatch):
    plan = capture(cloud_base, tmp_path)
    for months in ([], ["2020-01"] * 2, ["2020-01", "2020-02", "2020-03", "2020-04"]):
        with pytest.raises(CloudPublishError):
            capture(cloud_base, tmp_path, months=months)
    broken = copy.deepcopy(plan)
    broken["config"]["location"] = "asia-northeast3"
    with pytest.raises(CloudPublishError, match="binding"):
        publish_cloud_plan(broken)
    changed = next(a for a in plan["artifacts"] if a["table"] == "fact_trip_trusted")
    Path(changed["path"]).write_bytes(b"corrupted")
    with pytest.raises(CloudPublishError, match="artifact has changed"):
        publish_cloud_plan(plan)
    with pytest.raises(CloudPublishError, match="unsupported"):
        schema_field("value", "BLOB")


def test_upload_envelope_and_verification_mismatch(cloud_base, tmp_path):
    with pytest.raises(CloudPublishError, match="envelope"):
        capture(cloud_base, tmp_path, config=CloudConfig("seoul-bike-dw", "offline-bucket", maximum_upload_bytes=1))
    plan = capture(cloud_base, tmp_path)
    bq = FakeBigQuery()
    bq.corrupt_metrics = True
    result = publish_cloud_plan(plan, storage=FakeStorage(), bigquery=bq)
    assert result["status"] == "FAILED"
    assert all(event["status"] != "SUCCEEDED" for event in bq.events)
    assert all("verification mismatch" in row["error"] for row in result["targets"] if row["status"] == "FAILED")


def test_missing_adc_is_actionable_without_network_access(cloud_base, tmp_path, monkeypatch):
    google_auth = pytest.importorskip("google.auth")
    from google.auth.exceptions import DefaultCredentialsError

    def absent(*args, **kwargs):
        raise DefaultCredentialsError("injected missing ADC")

    monkeypatch.setattr(google_auth, "default", absent)
    plan = capture(cloud_base, tmp_path)
    with pytest.raises(CloudPublishError, match="ADC is not configured"):
        publish_cloud_plan(plan, bigquery=FakeBigQuery())


def test_airflow_disabled_is_credentials_free_and_enabled_uses_bound_reference(cloud_base, tmp_path):
    assert capture_cloud_payload(None, {"enabled": False}, None) == {"enabled": False}
    assert publish_cloud_payload({"enabled": False}) == {"enabled": False}
    assert require_cloud_success_payload({"enabled": False}) == {"enabled": False}
    request = resolve_backfill_request(_params(cloud_base, start_month="2020-01", end_month="2020-02"),
                                       scheduled_month=None)
    settings = {"enabled": True, "months": ["2020-01", "2020-02"], "station_months": ["2020-02"],
                "export_root": str(tmp_path), "config": {"project": "seoul-bike-dw", "bucket": "offline-bucket"}}
    summary = {"status": "SUCCEEDED", "start_month": "2020-01", "end_month": "2020-02"}
    payload = capture_cloud_payload(request, settings, summary)
    assert "artifacts" not in payload
    storage, bq = FakeStorage(), FakeBigQuery()
    result = publish_cloud_payload(payload, storage=storage, bigquery=bq)
    assert require_cloud_success_payload(result)["status"] == "SUCCEEDED"
    tampered = {**payload, "plan_id": "0" * 64}
    with pytest.raises(CloudPublishError, match="binding"):
        publish_cloud_payload(tampered)
    with pytest.raises(CloudPublishError, match="matching successful"):
        capture_cloud_payload(request, settings, {"status": "FAILED"})


def test_cli_plan_is_offline_and_publish_returns_failure_code(cloud_base, tmp_path, monkeypatch, capsys):
    import seoul_bike_pipeline.cloud_publish as module
    args = ["cloud", "plan", "--bucket", "offline-bucket", "--months", "2020-01", "2020-02",
            "--export-root", str(tmp_path)]
    for key, value in roots(cloud_base).items():
        args.extend(["--" + key.replace("_", "-"), str(value)])
    assert main(args) == 0
    line = capsys.readouterr().out.strip()
    plan_path = Path(line.removeprefix("CLOUD_PLAN "))
    plan = json.loads(plan_path.read_text())
    validate_cloud_plan(plan)
    bq = FakeBigQuery()
    bq.fail.add(("fact_trip_trusted", "2020-01"))
    original = module.publish_cloud_plan
    storage = FakeStorage()
    monkeypatch.setattr(module, "publish_cloud_plan", lambda plan, previous=None: original(
        plan, previous=previous, storage=storage, bigquery=bq))
    result_path = tmp_path / "result.json"
    assert main(["cloud", "publish", "--plan", str(plan_path), "--result", str(result_path)]) == 1
    assert json.loads(result_path.read_text())["status"] == "FAILED"
