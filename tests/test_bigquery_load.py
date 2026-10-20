from datetime import datetime
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

pytest.importorskip("google.cloud.bigquery")

from google.api_core.exceptions import NotFound
from google.cloud import bigquery

from seoul_bike_pipeline.bigquery_load import GoogleBigQuery
from seoul_bike_pipeline.cloud_publish import (
    CloudConfig, CloudPublishError, DefinitiveLoadError, compare_metrics, schema_field,
)


def artifact():
    return {
        "table": "fact_trip_trusted", "month": "2020-09", "artifact_id": "a" * 64,
        "schema": [schema_field("rent_at", "TIMESTAMP"), schema_field("distance_m", "DECIMAL(38,18)"),
                   schema_field("source_month_date", "DATE")],
    }


def backend():
    client = MagicMock()
    client.create_table.side_effect = lambda table, **kwargs: table
    return GoogleBigQuery(CloudConfig("seoul-bike-dw", "offline-bucket"), client=client), client


def job_for(item):
    return SimpleNamespace(
        destination=bigquery.TableReference.from_string("seoul-bike-dw.bike_cloud.fact_trip_trusted$202009"),
        source_uris=["gs://offline-bucket/data.parquet"],
        labels={"artifact": item["artifact_id"][:63]}, error_result=None,
        output_rows=3, input_file_bytes=100, result=lambda: None,
        _properties={"statistics": {"load": {"badRecords": "0"}}},
    )


def test_month_partition_overwrite_explicit_schema_and_recovery():
    adapter, client = backend()
    item = artifact()
    job = job_for(item)
    client.get_job.side_effect = NotFound("no job")
    client.load_table_from_uri.return_value = job
    result = adapter.load(item, job.source_uris[0], "stable_job")
    assert result["output_rows"] == 3
    args, kwargs = client.load_table_from_uri.call_args
    assert args[1] == "seoul-bike-dw.bike_cloud.fact_trip_trusted$202009"
    configuration = kwargs["job_config"]
    assert configuration.write_disposition == "WRITE_TRUNCATE"
    assert configuration.source_format == "PARQUET"
    assert configuration.max_bad_records == 0
    assert configuration.parquet_options.enable_list_inference
    assert not configuration.decimal_target_types
    assert configuration.schema[0].field_type == "DATETIME"
    assert configuration.schema[1].field_type == "BIGNUMERIC"
    assert result["bad_records"] == 0
    table = client.create_table.call_args.args[0]
    assert table.time_partitioning.type_ == "MONTH"
    assert table.time_partitioning.field == "source_month_date"
    assert table.require_partition_filter
    assert table.clustering_fields == ["rent_station_no", "return_station_no"]
    client.get_job.side_effect = None
    client.get_job.return_value = job
    adapter.load(item, job.source_uris[0], "stable_job")
    assert client.load_table_from_uri.call_count == 1


def test_load_error_classifies_failed_vs_ambiguous_and_rejects_foreign_job():
    adapter, client = backend()
    item = artifact()
    job = job_for(item)

    def lost_response():
        raise TimeoutError("lost response")

    job.result = lost_response
    client.get_job.return_value = job
    with pytest.raises(TimeoutError):
        adapter.load(item, job.source_uris[0], "stable")
    job.error_result = {"reason": "invalid", "message": "bad schema"}
    with pytest.raises(DefinitiveLoadError, match="bad schema"):
        adapter.load(item, job.source_uris[0], "stable")
    job.source_uris = ["gs://foreign/object"]
    with pytest.raises(CloudPublishError, match="not bound"):
        adapter.load(item, "gs://offline-bucket/data.parquet", "stable")


def test_dry_run_query_cap_and_exact_metrics():
    adapter, client = backend()
    client.query.side_effect = [
        SimpleNamespace(total_bytes_processed=10),
        SimpleNamespace(result=lambda: [{"answer": 1}], job_id="query",
                        total_bytes_processed=10, total_bytes_billed=100, cache_hit=False),
    ]
    rows, stats = adapter._query("SELECT 1")
    assert rows == [{"answer": 1}]
    assert stats["bytes_billed"] == 100
    assert client.query.call_args_list[0].kwargs["job_config"].dry_run
    assert client.query.call_args_list[1].kwargs["job_config"].maximum_bytes_billed == 1024**3
    client.query.side_effect = [SimpleNamespace(total_bytes_processed=1024**3 + 1)]
    with pytest.raises(CloudPublishError, match="maximum_bytes"):
        adapter._query("SELECT 1")
    compare_metrics({"distance_sum": "0.100000000000000001", "first_rent": "2020-09-01T09:00:00"},
                    {"distance_sum": "0.100000000000000001", "first_rent": datetime(2020, 9, 1, 9)}, [])
    with pytest.raises(CloudPublishError, match="distance_sum"):
        compare_metrics({"distance_sum": "0.100000000000000001"}, {"distance_sum": "0.1"}, [])


def test_history_intent_is_sql_not_streaming_and_repeated_record_is_reused():
    adapter, client = backend()
    event = {"target": "seoul-bike-dw.bike_cloud.fact_trip_trusted", "month": "2020-09",
             "attempt": 1, "status": "PENDING", "artifact_id": "a" * 64, "job_id": "load"}
    client.get_job.side_effect = NotFound("missing")
    queries = []
    adapter._query = lambda sql, params=(), **kwargs: queries.append((sql, params, kwargs))
    adapter.record(event)
    assert queries[0][0].startswith("INSERT INTO")
    assert "bike_history_" in queries[0][2]["job_id"]
    client.get_job.side_effect = None
    client.get_job.return_value = SimpleNamespace(result=lambda: None)
    adapter.record(event)
    assert len(queries) == 1
    client.insert_rows_json.assert_not_called()


def test_preflight_rejects_historical_partition_expiration():
    adapter, client = backend()
    client.get_dataset.return_value = SimpleNamespace(location="US-CENTRAL1",
                                                       default_partition_expiration_ms=86400000)
    with pytest.raises(CloudPublishError, match="expiration"):
        adapter.preflight()


def test_verification_namespace_isolates_table_partition_history_and_query():
    client = MagicMock()
    client.create_table.side_effect = lambda table, **kwargs: table
    adapter = GoogleBigQuery(
        CloudConfig("seoul-bike-dw", "offline-bucket", verification=True), client=client
    )
    item = artifact()
    job = job_for(item)
    job.destination = bigquery.TableReference.from_string(
        "seoul-bike-dw.bike_cloud.verify_fact_trip_trusted$202009"
    )
    client.get_job.side_effect = NotFound("missing")
    client.load_table_from_uri.return_value = job
    adapter.load(item, job.source_uris[0], "isolated_job")
    assert adapter.history == "seoul-bike-dw.bike_cloud.verify_cloud_load_history"
    assert client.load_table_from_uri.call_args.args[1] == str(job.destination)
    queries = []
    adapter._query = lambda sql, params=(), **kwargs: (queries.append(sql) or [{}], {})
    adapter.verify(item)
    assert "`seoul-bike-dw.bike_cloud.verify_fact_trip_trusted`" in queries[0]


def test_existing_partition_api_mirror_is_equivalent_not_a_contract_conflict():
    adapter, client = backend()

    def round_trip(table, **kwargs):
        result = bigquery.Table.from_api_repr(table.to_api_repr())
        # BigQuery echoes this deprecated mirror inside timePartitioning.
        result._properties["timePartitioning"]["requirePartitionFilter"] = True
        return result

    client.create_table.side_effect = round_trip
    adapter._ensure_table(artifact())


def test_unclustered_quality_table_omits_empty_clustering_configuration():
    adapter, client = backend()
    item = {**artifact(), "table": "mart_source_month_quality"}
    table = adapter._ensure_table(item)
    assert "clustering" not in table.to_api_repr()
