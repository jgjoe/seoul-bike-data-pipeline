"""BigQuery batch adapter: monthly atomic loads and append-only SQL history."""

from datetime import datetime, timezone
import json

from .cloud_publish import (
    CloudPublishError, DefinitiveLoadError, TABLES, canonical, identity, metric_select, scalar,
)


class GoogleBigQuery:
    def __init__(self, config, *, client=None):
        from google.cloud import bigquery
        self.api = bigquery
        self.config = config
        self.client = client if client is not None else bigquery.Client(
            project=config.project, location=config.location
        )
        self.dataset = f"{config.project}.{config.dataset}"
        self.history = config.table("cloud_load_history")

    def preflight(self):
        dataset = self.client.get_dataset(self.dataset)
        if dataset.location.lower() != self.config.location:
            raise CloudPublishError("BigQuery dataset must be in us-central1")
        if dataset.default_partition_expiration_ms is not None:
            raise CloudPublishError("historical sample requires no default partition expiration")
        table = self.api.Table(self.history, schema=[
            self.api.SchemaField("event_at", "TIMESTAMP"),
            self.api.SchemaField("target", "STRING"),
            self.api.SchemaField("partition_month", "DATE"),
            self.api.SchemaField("attempt", "INTEGER"),
            self.api.SchemaField("status", "STRING"),
            self.api.SchemaField("event_json", "STRING"),
        ])
        table.time_partitioning = self.api.TimePartitioning(field="event_at", type_="DAY")
        table.clustering_fields = ["target", "partition_month"]
        self.client.create_table(table, exists_ok=True)

    def _query(self, sql, parameters=(), *, job_id=None):
        configuration = self.api.QueryJobConfig(
            query_parameters=list(parameters), use_legacy_sql=False,
            maximum_bytes_billed=self.config.maximum_bytes_billed,
        )
        # Dry-run every query, including history DML. Enforce an explicit cap.
        dry = self.api.QueryJobConfig(
            query_parameters=list(parameters), use_legacy_sql=False,
            dry_run=True, use_query_cache=False,
            maximum_bytes_billed=self.config.maximum_bytes_billed,
        )
        estimate = self.client.query(sql, job_config=dry, location=self.config.location)
        if (estimate.total_bytes_processed or 0) > self.config.maximum_bytes_billed:
            raise CloudPublishError("query exceeds maximum_bytes_billed")
        job = self.client.query(sql, job_config=configuration, job_id=job_id,
                                location=self.config.location)
        rows = list(job.result())
        return rows, {
            "job_id": job.job_id, "bytes_processed": job.total_bytes_processed or 0,
            "bytes_billed": job.total_bytes_billed or 0, "cache_hit": job.cache_hit,
        }

    def latest(self, target, month):
        rows, _ = self._query(
            f"SELECT event_json FROM `{self.history}` "
            "WHERE target = @target AND partition_month = @month "
            "ORDER BY attempt DESC, CASE status WHEN 'PENDING' THEN 0 ELSE 1 END DESC LIMIT 1",
            [self.api.ScalarQueryParameter("target", "STRING", target),
             self.api.ScalarQueryParameter("month", "DATE", month + "-01")],
        )
        return json.loads(rows[0]["event_json"]) if rows else None

    def record(self, event):
        # Deterministic DML job ID avoids duplicate history after response loss.
        job_id = "bike_history_" + identity([event["job_id"], event["status"]])
        from google.api_core.exceptions import NotFound
        try:
            existing = self.client.get_job(job_id, location=self.config.location)
        except NotFound:
            existing = None
        if existing is not None:
            existing.result()
            return
        self._query(
            f"INSERT INTO `{self.history}` "
            "(event_at, target, partition_month, attempt, status, event_json) "
            "VALUES (@now, @target, @month, @attempt, @status, @payload)",
            [
                self.api.ScalarQueryParameter("now", "TIMESTAMP", datetime.now(timezone.utc)),
                self.api.ScalarQueryParameter("target", "STRING", event["target"]),
                self.api.ScalarQueryParameter("month", "DATE", event["month"] + "-01"),
                self.api.ScalarQueryParameter("attempt", "INTEGER", event["attempt"]),
                self.api.ScalarQueryParameter("status", "STRING", event["status"]),
                self.api.ScalarQueryParameter("payload", "STRING", canonical(event)),
            ], job_id=job_id,
        )

    def _ensure_table(self, artifact):
        schema = [self.api.SchemaField.from_api_repr(field) for field in artifact["schema"]]
        table = self.api.Table(self.config.table(artifact["table"]), schema=schema)
        field, clustering = TABLES[artifact["table"]]
        table.time_partitioning = self.api.TimePartitioning(field=field, type_="MONTH")
        table.require_partition_filter = True
        if clustering:
            table.clustering_fields = list(clustering)
        actual = self.client.create_table(table, exists_ok=True)
        if (actual.schema != table.schema or actual.time_partitioning is None
                or actual.time_partitioning.type_ != table.time_partitioning.type_
                or actual.time_partitioning.field != table.time_partitioning.field
                or list(actual.clustering_fields or []) != list(clustering)
                or not actual.require_partition_filter):
            raise CloudPublishError("existing table schema/partition/clustering contract differs")
        if actual.time_partitioning.expiration_ms is not None:
            raise CloudPublishError("historical partitions must not expire by partition age")
        return table

    def load(self, artifact, uri, job_id):
        from google.api_core.exceptions import NotFound
        table = self._ensure_table(artifact)
        destination = f"{self.config.table(artifact['table'])}${artifact['month'].replace('-', '')}"
        try:
            job = self.client.get_job(job_id, location=self.config.location)
        except NotFound:
            parquet = self.api.ParquetOptions()
            parquet.enable_list_inference = True
            configuration = self.api.LoadJobConfig(
                schema=table.schema, source_format=self.api.SourceFormat.PARQUET,
                write_disposition=self.api.WriteDisposition.WRITE_TRUNCATE,
                create_disposition=self.api.CreateDisposition.CREATE_NEVER,
                max_bad_records=0,
                parquet_options=parquet,
                ignore_unknown_values=False,
                labels={"component": "bike-cloud", "artifact": artifact["artifact_id"][:63]},
            )
            job = self.client.load_table_from_uri(
                uri, destination, job_id=job_id, job_config=configuration,
                location=self.config.location,
            )
        if (str(job.destination) != destination or list(job.source_uris or []) != [uri]
                or (job.labels or {}).get("artifact") != artifact["artifact_id"][:63]):
            raise CloudPublishError("existing job ID is not bound to the captured load")
        try:
            job.result()
        except Exception as exc:
            # Network errors remain PENDING; only server-confirmed job failures
            # authorize a different job ID on recovery.
            refreshed = self.client.get_job(job_id, location=self.config.location)
            if refreshed.error_result:
                raise DefinitiveLoadError(str(refreshed.error_result)) from exc
            raise
        if job.error_result:
            raise DefinitiveLoadError(str(job.error_result))
        return {"job_id": job_id, "output_rows": job.output_rows,
                "input_bytes": job.input_file_bytes,
                "bad_records": int(job._properties["statistics"]["load"].get("badRecords", 0))}

    def verify(self, artifact):
        table = self.config.table(artifact["table"])
        field = TABLES[artifact["table"]][0]
        rows, stats = self._query(
            f"SELECT {metric_select(artifact['table'])} FROM `{table}` "
            f"WHERE {field} >= @month AND {field} < DATE_ADD(@month, INTERVAL 1 MONTH)",
            [self.api.ScalarQueryParameter("month", "DATE", artifact["month"] + "-01")],
        )
        metrics = {key: scalar(value) for key, value in dict(rows[0]).items()}
        metadata = self.client.get_table(table)
        return {"metrics": metrics, **stats, "table_rows": metadata.num_rows,
                "table_logical_bytes": metadata.num_bytes}
