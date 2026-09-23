# Seoul Bike Data Pipeline

서울 따릉이 대여이력의 **source revision, schema drift, data quality, provenance, backfill, failure recovery**를 다루는 로컬 batch data reliability / DW 프로젝트다.

**Stack / responsibility:** Python · DuckDB · Parquet · plain SQL · Apache Airflow — Python은 source/control/DQ, DuckDB는 analytical execution, Parquet은 immutable Clean storage, SQL은 warehouse/mart transformation, Airflow는 orchestration만 담당한다.

구현 기준선은 [PRD v1.0 FINAL](docs/prd-v1.0-final.md)이며, 실제 production-scale 완료 증거는 [Production Validation](docs/production-validation.md)에 기록한다.

## What this project proves

- 서울시 공식 대여이력 **2020-01~2026-06, 78개월**을 immutable source identity와 함께 처리한다.
- source가 수정되면 전체 재처리 대신 **영향받은 month만 탐지·재빌드**한다.
- schema drift와 row-level/group-level DQ를 구분하고, 신뢰할 수 없는 row는 provenance를 유지한 채 quarantine한다.
- Clean/fact/mart는 검증된 immutable version을 먼저 만든 뒤 **atomic current pointer**를 이동해 partial publish를 막는다.
- CLI, tests, Airflow가 동일한 business layer를 호출해 backfill/retry semantics가 경로마다 달라지지 않게 한다.
- 공식 station source가 실제 변경일을 제공하지 않는 한, snapshot 관측 시점 이상의 정확도를 주장하지 않는다.

## Architecture

```text
Official Seoul files
  -> immutable Raw + source manifest/hash
  -> Python source contract + DQ
  -> DuckDB canonical processing
  -> partitioned immutable Clean Parquet + quarantine provenance
  -> DuckDB fact/dimension warehouse
  -> plain-SQL station-day / OD / source-quality marts

Apache Airflow
  -> schedule / range backfill / failed-month retry / run visibility
  -> calls the same Python business layer used by CLI and tests
```

The design keeps transformation truth outside the DAG. Airflow coordinates work; it does not reimplement business SQL or DQ logic.

## Verified v1 evidence

| Evidence | Measured result |
|---|---:|
| Clean production coverage | **78 / 78 months** (`2020-01..2026-06`) |
| Full-period source / Clean rows | **241,350,472 / 241,350,472** |
| Trusted / quarantined rows | **241,320,806 / 29,666** |
| Station history | **12 snapshots · 31,986 observations · 14,992 intervals · 3,047 stations** |
| Representative real warehouse month | `2020-09`: fact **2,811,968** · station-day **60,979** · OD **1,536,122** |
| Cross-model reconciliation | fact↔station-day, station-day↔OD, quality↔fact **exact match** |
| Production-sensitive WSL regression | **139 passed, 1 skipped** |
| Windows lightweight state/control regression | **3 passed** + `compileall` PASS |

Downstream production acceptance is intentionally bounded by the frozen v1 contract: fact, source-quality, and OD have real production evidence across 9 months; station-day has one representative production month. This is evidence coverage, not a claim that every downstream model was materialized for all 78 months. See [Production Validation](docs/production-validation.md) for exact scope and measurements.

## Key engineering decisions

### Why month is the rebuild unit

Seoul publishes and revises rental history at month/year source boundaries. The pipeline records immutable source versions and compares month-level member content, so an upstream revision can invalidate only the affected months.

### Why `(bike_id, rent_at)` is not treated as a source PK

It is a logical grouping key, not a guaranteed stable identifier. Exact duplicates and conflicting groups are detected explicitly; conflicting groups are preserved for provenance and excluded from trusted facts instead of selecting an arbitrary winner.

### Why observed station history is conservative

The official station files are snapshots, not exact change-event logs. `dim_station_history` therefore models observed applicability intervals and never backfills future observations into earlier trips.

### Why DuckDB + Parquet + plain SQL

The target is a local analytical batch workload. DuckDB provides bounded single-node analytical execution, Parquet gives durable columnar Clean storage, and plain SQL keeps fact/dimension/mart logic inspectable without adding a framework that the v1 requirements do not need.

### Why Airflow

The business layer already supports deterministic month/range execution and retry. Airflow adds scheduling, dependency control, failed-month retry, and run visibility while preserving one implementation path.

### How failed publishes stay safe

Each durable output is built and validated as an immutable version. The mutable `current` pointer moves only after validation succeeds, so a failed rebuild does not replace the last valid published result.

## Data and repository boundary

Large official source files and generated Parquet/DuckDB outputs are not committed. The repository contains:

- pipeline, SQL, Airflow DAG/plugin, and tests
- synthetic regression fixtures
- frozen PRD/addenda and production measurements
- scripts that prepare the official files locally and verify pinned byte size / SHA-256 before use

Official source bytes, generated warehouse state, Airflow metadata/XCom/logs, credentials, and machine-specific runtime paths remain local.

## Reproduce locally

Reference runtime is **Windows host + WSL2/Linux**, with Python >= 3.12. Airflow itself runs in Linux/WSL2.

For a quick source/test checkout:

```bash
python3 -m venv .venv
.venv/bin/python -m pip install -e '.[test]'
.venv/bin/python -m pytest -q
```

For the full production replay, source registration, DQ inspection, warehouse/mart queries, retry/idempotency checks, and Airflow commands, follow:

- [Implementation & Reproduction Runbook](docs/implementation-and-runbook.md)
- [Production Validation](docs/production-validation.md)

The runbook keeps the exhaustive operational commands and contracts out of this landing page while preserving the reproducible path.

## Documentation

- [PRD v1.0 FINAL](docs/prd-v1.0-final.md) — frozen requirements and Definition of Done
- [Addendum 001](docs/prd-v1.0-addendum-001-station-history-match-nullability.md) — station-history match nullability
- [Addendum 002](docs/prd-v1.0-addendum-002-station-observation-precision-and-applicability.md) — station observation precision/applicability
- [Production Validation](docs/production-validation.md) — production-scale evidence, reconciliation, recovery, and acceptance scope
- [Implementation & Reproduction Runbook](docs/implementation-and-runbook.md) — CLI contracts, production replay, Airflow setup, manifest and publication details

## Scope

v1 is a batch reliability / DW project. Streaming, real-time bike availability, ML forecasting, recommendation, REST serving, Kubernetes, Spark, dbt, Iceberg, and Delta Lake are outside the frozen v1 requirements.
