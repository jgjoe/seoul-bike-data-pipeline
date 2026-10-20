# Addendum 003 — Representative GCS / BigQuery publication

Approved extension; `docs/prd-v1.0-final.md` remains frozen. Local Python/DuckDB
transforms, immutable Raw/manifest, Clean/DQ/Quarantine/Trusted contracts and
existing month/revision/retry behavior remain canonical.

## Scope

Publish an explicit allowlist of 1–3 source months, provisionally 2020-08 and
2020-09. Never implicitly expand to 78 months. Clean Parquet is uploaded as-is;
Trusted and existing marts are exported read-only from their immutable DuckDB
bundles. OD and quality accompany each selected month. Station-day is separately
selected from those months (initially 2020-09 only), not rebuilt for this feature.

Private GCS Standard bucket and BigQuery dataset are colocated in `us-central1`.
US Free Tier eligibility is preferred over Korea latency/residency; this does
not meet a hypothetical Korean data-residency requirement.

## Publication contract

- Explicit schema: Seoul-local naive timestamps are DATETIME; DECIMAL(38,18)
  is BIGNUMERIC; DQ issue arrays remain repeated records. No lossy NUMERIC or
  accidental timezone conversion.
- Fact/quality add a first-of-month DATE partition column. Marts use service_date.
  All data tables are monthly partitioned and require partition filters.
  Fact clusters by rental/return station; OD by origin/destination; station-day
  by station. No short historical-partition expiration.
- Exact GCS URIs, content-addressed names, create-only generation preconditions.
  Hash, size, immutable metadata, manifest source identity, versions and object
  generation are bound to each load attempt. Raw and quarantine issue files
  are not uploaded. Quarantined rows remain in Clean, never Trusted.
- GCS load jobs replace one `table$YYYYMM` partition with WRITE_TRUNCATE.
  MERGE is inappropriate for a non-PK logical key and complete revised-month
  snapshots. Streaming is not used, including for load history.
- SQL history records PENDING intent before load submission. Known jobs are
  recovered by stable job ID after ambiguous errors. Only confirmed failed
  jobs authorize a new attempt ID. Successful identical input skips loading
  but re-verifies cloud metrics.
- A partition load is atomic; the whole fact/mart publication is NOT a
  transaction. Failure is visible and successful partitions remain available.
  A GCS manifest is lineage, not an all-tables-success pointer.
- A local single-runtime lock and Airflow max_active_runs=1 serialize this
  representative path. Distributed concurrent writers are not supported.
  Use one export root for a target bucket/dataset; recover a pending plan before
  switching revisions. No leases or multi-user production guarantees claimed.
- Station-day retains all historical local fact dependencies, including months
  outside the cloud sample. Its return totals must not be recomputed from the
  incomplete uploaded fact subset.

## Acceptance and boundary

Offline fake-client tests cover DQ exclusion/reconciliation, exact schemas,
identical reruns, changed-month-only replacement, failed load, lost load response,
lost success-history response, bad lineage/artifacts, cost envelopes and
Airflow's disabled default. Real cloud acceptance is a separate gated activity.

Live evidence must include source/Clean = Trusted + quarantined row counts,
existing mart grain/aggregates, exact integer/decimal checks and 1e-9 relative
floating tolerance, job IDs and changed-only logs, failure/recovery, bytes
processed/billed, GCS live/soft-deleted storage, BigQuery logical/physical/time-
travel storage, and Billing Reports cost/credits/net charge after reporting delay.
No live success, free usage or zero cost is inferred from offline tests.

Trial remains non-upgraded; budget alerts are not a cap. Setup, cost controls,
explicit destructive-cleanup approval and study topics are in
`docs/cloud-setup-ko.md` and the cloud runbook section.
