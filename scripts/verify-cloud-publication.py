"""Approved live acceptance harness; never run in CI or without the setup gate.

Run from the repository checkout with its [test,cloud] environment. Production uses only
2020-08/09; synthetic revisions use verify_* tables and v1/verification objects.
Generated plans/results stay under the Linux replay root, never in Git.
"""

from argparse import ArgumentParser
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from google.cloud import bigquery

from seoul_bike_pipeline.bigquery_load import GoogleBigQuery
from seoul_bike_pipeline.cloud_publish import (
    CloudConfig, capture_cloud_plan, compare_metrics, publish_cloud_plan, read_json, write_json,
)
from seoul_bike_pipeline.cloud_storage import GoogleCloudStorage


class AuditedClient:
    """Keep submitted job evidence; inject only an isolated synthetic load."""

    def __init__(self, config):
        self.client = bigquery.Client(project=config.project, location=config.location)
        self.queries = []
        self.loads = []
        self.fail_next_february = False

    def __getattr__(self, name):
        return getattr(self.client, name)

    def query(self, *args, **kwargs):
        job = self.client.query(*args, **kwargs)
        if not kwargs["job_config"].dry_run:
            self.queries.append(job)
        return job

    def load_table_from_uri(self, uri, destination, **kwargs):
        if self.fail_next_february and destination.endswith("verify_fact_trip_trusted$202002"):
            self.fail_next_february = False
            configuration = kwargs["job_config"]
            configuration.schema = [
                *configuration.schema,
                bigquery.SchemaField("injected_required_column", "INTEGER", mode="REQUIRED"),
            ]
        job = self.client.load_table_from_uri(uri, destination, **kwargs)
        self.loads.append(job)
        return job

    def usage(self):
        return {
            "queries": [
                {"job_id": job.job_id, "processed": job.total_bytes_processed or 0,
                 "billed": job.total_bytes_billed or 0, "cache_hit": job.cache_hit}
                for job in self.queries
            ],
            "loads": [
                {"job_id": job.job_id, "destination": str(job.destination),
                 "input_bytes": job.input_file_bytes, "output_rows": job.output_rows,
                 "error_result": job.error_result}
                for job in self.loads
            ],
        }


class LostHistoryResponse(GoogleBigQuery):
    """Interrupt after real load/verification, before its success history write."""

    lose_next_success = False

    def record(self, event):
        if (self.lose_next_success and event["status"] == "SUCCEEDED"
                and event["target"].endswith("verify_fact_trip_trusted")
                and event["month"] == "2020-02"):
            self.lose_next_success = False
            raise TimeoutError("injected process interruption before success history")
        return super().record(event)


def main():
    parser = ArgumentParser(description=__doc__)
    parser.add_argument("phase", choices=("production", "synthetic"))
    parser.add_argument("--root", type=Path, default=Path.home() / "seoul-bike-production-replay")
    parser.add_argument("--retry-result", type=Path,
                        help="retry a failed production result with its original captured manifest")
    args = parser.parse_args()
    root = args.root.resolve()
    verification = args.phase == "synthetic"
    config = CloudConfig("seoul-bike-dw", "seoul-bike-dw-representative",
                         verification=verification)
    output = root / "run" / ("cloud-verification" if verification else "cloud")
    output.mkdir(parents=True, exist_ok=True)
    audited = AuditedClient(config)
    backend = LostHistoryResponse(config, client=audited)
    storage = GoogleCloudStorage(config)
    evidence = {"phase": args.phase, "events": []}

    def checkpoint():
        evidence["usage"] = audited.usage()
        write_json(output / "acceptance-evidence.json", evidence)

    def publish(label, plan, previous=None, expected="SUCCEEDED"):
        result = publish_cloud_plan(plan, storage=storage, bigquery=backend, previous=previous)
        write_json(output / f"{label}-result.json", result)
        evidence["events"].append({
            "label": label, "plan_id": plan["plan_id"], "status": result["status"],
            "targets": result["targets"],
        })
        checkpoint()
        print(label, result["status"],
              [(r["table"], r["month"], r["status"]) for r in result["targets"]], flush=True)
        assert result["status"] == expected, result
        return result

    def capture(local, months, station_months=()):
        return capture_cloud_plan(
            **local, export_root=output, months=months,
            station_months=station_months, config=config,
        )

    try:
        if not verification:
            local = dict(manifest=root / "source/manifest.json", raw_root=root / "source/raw",
                         station_root=root / "source/station", clean_root=root / "run/clean",
                         warehouse_root=root / "run/warehouse")
            previous = read_json(args.retry_result) if args.retry_result else None
            plan = (read_json(output / f"{previous['plan_id']}.json") if previous else
                    capture(local, ["2020-08", "2020-09"]))
            clean = next(a for a in plan["artifacts"]
                         if a["table"] == "clean" and a["month"] == "2020-09")
            fact = next(a for a in plan["artifacts"]
                        if a["table"] == "fact_trip_trusted" and a["month"] == "2020-09")
            assert clean["expected"]["source_row_count"] == 2_811_974
            assert clean["expected"]["quarantined_row_count"] == 6
            assert fact["expected"]["row_count"] == 2_811_968
            assert fact["expected"]["invalid_rows"] == 0
            od = next(a for a in plan["artifacts"]
                      if a["table"] == "mart_od_day" and a["month"] == "2020-09")
            assert od["expected"]["row_count"] == 1_536_122
            assert od["expected"]["trip_count"] == 2_796_989
            assert od["expected"]["valid_distance_trip_count"] == 322_289
            evidence["local"] = [
                {k: a[k] for k in ("table", "month", "version", "sha256", "size_bytes", "expected", "lineage")}
                for a in plan["artifacts"]
            ]
            publish("production-recovered" if previous else "production-initial", plan, previous=previous)
            count = len(audited.loads)
            repeated = publish("production-identical", plan)
            assert all(r["status"] == "SKIP_UNCHANGED" for r in repeated["targets"])
            assert len(audited.loads) == count
            assert all(not r["object"]["created"] for r in repeated["targets"])
        else:
            import tempfile
            from seoul_bike_pipeline.backfill import run_backfill_range
            from tests.test_backfill import (
                TRANSFORMATION_VERSION, _build_backfill_base, _month_row, _register_rental,
            )
            from tests.test_cloud_publish import roots
            from tests.test_station_day_mart import rental_row

            fixture = Path(tempfile.mkdtemp(prefix="synthetic-local-", dir=root))
            _build_backfill_base(fixture)
            original = _month_row("2020-02")
            conflict = list(original)
            conflict[10] = "200"
            warning = rental_row(bike_id="WARN", rent_at="2020-02-03 09:00:00",
                                 rent_station_no="00001", return_at="", return_station_no="",
                                 usage_min="10", distance_m="0")
            local = roots(fixture)

            def revision(rows, name):
                _register_rental(fixture, source_month="2020-02", rows=rows, revision=name)
                _, result = run_backfill_range(
                    **local, start_month="2020-02", end_month="2020-02",
                    transformation_version=TRANSFORMATION_VERSION, configuration={},
                )
                assert result.status == "SUCCEEDED"
                return capture(local, ["2020-01", "2020-02"], ["2020-02"])

            old = revision([original, tuple(conflict), warning], "initial-dq")
            old_fact = next(a for a in old["artifacts"]
                            if a["table"] == "fact_trip_trusted" and a["month"] == "2020-02")
            assert old_fact["expected"]["row_count"] == 1
            assert old_fact["expected"]["invalid_rows"] == 0
            assert old_fact["expected"]["warning_rows"] == 1
            initial = publish("synthetic-initial", old)
            repeated = publish("synthetic-identical", old)
            assert all(r["status"] == "SKIP_UNCHANGED" for r in repeated["targets"])
            revised = revision([original], "changed-february")
            before = len(audited.loads)
            audited.fail_next_february = True
            failed = publish("synthetic-failed-revision", revised, expected="FAILED")
            failures = [r for r in failed["targets"] if r["status"] == "FAILED"]
            assert [(r["table"], r["month"]) for r in failures] == [("fact_trip_trusted", "2020-02")]
            compare_metrics(old_fact["expected"], backend.verify(old_fact)["metrics"], old_fact["schema"])
            pending = backend.latest(config.table("fact_trip_trusted"), "2020-02")
            assert pending["status"] == "FAILED"
            evidence["failed_job"] = pending["job_id"]
            assert len(audited.loads) - before == 4
            assert all(str(job.destination).endswith("$202002") for job in audited.loads[before:])
            for row in failed["targets"]:
                if row["month"] == "2020-01":
                    assert row["status"] == "SKIP_UNCHANGED"
                    original_row = next(r for r in initial["targets"] if r["artifact_id"] == row["artifact_id"])
                    assert row.get("job_id") == original_row.get("job_id")
            before = len(audited.loads)
            publish("synthetic-recovered", revised, previous=failed)
            assert len(audited.loads) - before == 1
            assert str(audited.loads[-1].destination).endswith("verify_fact_trip_trusted$202002")
            lost_plan = revision([original, warning], "history-interruption")
            backend.lose_next_success = True
            interrupted = publish("synthetic-history-interruption", lost_plan, expected="FAILED")
            assert sum(r["status"] == "FAILED" for r in interrupted["targets"]) == 1
            pending = backend.latest(config.table("fact_trip_trusted"), "2020-02")
            assert pending["status"] == "PENDING"
            evidence["recovered_existing_job"] = pending["job_id"]
            before = len(audited.loads)
            publish("synthetic-history-recovered", lost_plan, previous=interrupted)
            assert len(audited.loads) == before

        evidence["storage"] = {
            "gcs_live_bytes": storage.live_bytes,
            "tables": [
                {"table": str(item.reference), "rows": table.num_rows, "logical_bytes": table.num_bytes}
                for item in audited.list_tables(backend.dataset)
                for table in [audited.get_table(item.reference)]
            ],
        }
        evidence["status"] = "SUCCEEDED"
    except Exception as exc:
        evidence.update(status="FAILED", error_type=type(exc).__name__, error=str(exc))
        raise
    finally:
        checkpoint()
        print("EVIDENCE", output / "acceptance-evidence.json", flush=True)


if __name__ == "__main__":
    main()
