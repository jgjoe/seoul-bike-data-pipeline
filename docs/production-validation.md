# Production Validation — 2026-09-23

이 문서는 PRD v1.0 §35의 production-scale 완료 증거를 기록한다. Generated Raw/Clean/DuckDB/Parquet 자체는 Git에 커밋하지 않고, source identity와 실행 방법, 검증 결과만 보존한다.

## Scope and source evidence

- Rental production scope: `2020-01` through `2026-06` inclusive, 78 logical months.
- Official rental source: Seoul Open Data `OA-15182`, 공공누리 1유형(출처표시, 상업적 이용 및 변경 가능). The official catalog was rechecked on 2026-09-22; it reported data updated on 2026-07-15 and exposed the target through `2606`.
- Observed rental artifacts: yearly ZIP `2020`~`2025` 6개 + standalone monthly CSV `2026-01`~`2026-06` 6개.
- Official station source: Seoul Open Data `OA-13252`, 공공누리 제1유형. The official catalog was rechecked on 2026-09-22; it reported data updated on 2026-07-15 and the same 12 observed snapshots used here.
- Observed station snapshots: `2021-01-31`, then half-year snapshots through `2026-06`, total 12 snapshots.
- `scripts/prepare-production-sources-wsl.sh` records the exact observed byte size and SHA-256 for all 24 official files and fails closed when the official bytes drift.
- Rental and station records share one manifest/raw namespace for final fact/history provenance validation. Large source bytes remain local and ignored by Git.

The station archive begins at 2021-01-31. Therefore low/zero station-history coverage for 2020 trips is an expected consequence of the observed-history contract, not a pipeline failure; the pipeline does not backfill future station observations into earlier trips.

## Reference runtime

- Host workflow: Windows + WSL2 Linux, as required by PRD §25.
- WSL distribution: Ubuntu 24.04.4 LTS.
- Kernel: `Linux 5.15.167.4-microsoft-standard-WSL2`.
- Python: `3.12.3`.
- DuckDB: `1.5.5`.
- WSL logical CPUs visible for the final completion run: 4 (`.wslconfig` cap).
- WSL memory available to the VM for the final completion run: approximately 9.7 GiB (`memory=10GB`) plus 3 GiB swap.
- Artifact transformation label recorded in the final production artifacts: `git-b9de90a`. Later commits through `89c23c3` only reduce planning/validation resource use (streaming quality/station-day/OD validation and bounded transient fact/enrichment DuckDB execution); the materialization SQL and business-output rules are unchanged. The measured run therefore keeps `git-b9de90a` as the artifact lineage label while using the latest memory-safe runtime bytes.

Production Clean builds use a bounded DuckDB build configuration (`5GB`, 2 threads, insertion-order preservation disabled). A real `2020-01` smoke completed in 4m02s at about 2.9 GB peak RSS. The largest checked month, `2026-06`, completed in 19m19s at 5,419,040 KB peak RSS with no swap and produced 4,182,797 Clean rows. Two concurrent heavy Clean publishers were observed to starve the Windows host, so the final 14 missing months were completed with one publisher under the 10GB/4-CPU WSL cap. Concurrency is not a v1 acceptance criterion.

## Reproduction path

From a fresh WSL2/Linux checkout of the completed project, follow [Implementation & Reproduction Runbook](implementation-and-runbook.md) under **Production replay (WSL2/Linux)** through source preparation and execution, using `VERSION=git-b9de90a` and the same `REPLAY_ROOT`, `SOURCE_ROOT`, and `RUN_ROOT` layout shown there. The release checkout intentionally retains the post-`b9de90a` memory-safe validation/runtime fixes while the explicit `VERSION` keeps the measured artifact lineage label stable. A sanitized public release snapshot need not preserve the internal development commit graph; it contains the final memory-safe code and evidence while this explicit label preserves the measured production lineage.

```bash
VERSION=git-b9de90a
REPLAY_ROOT="$HOME/seoul-bike-production-replay"
SOURCE_ROOT="$REPLAY_ROOT/source"
RUN_ROOT="$REPLAY_ROOT/run"
```

The canonical business-layer command is:

```bash
.venv/bin/python -m seoul_bike_pipeline pipeline backfill-range \
  --manifest "$SOURCE_ROOT/manifest.json" \
  --raw-root "$SOURCE_ROOT/raw" \
  --station-root "$SOURCE_ROOT/station" \
  --clean-root "$RUN_ROOT/clean" \
  --warehouse-root "$RUN_ROOT/warehouse" \
  --start-month 2020-01 \
  --end-month 2026-06 \
  --transformation-version "$VERSION" \
  --configuration-json '{}'
```

For the measured run, existing durable outputs may be prebuilt with the same publisher/business-layer functions exposed by the CLI rather than rerunning already-current partitions solely for evidence collection. Dataset completion requires all 78 target rental months to reach validated Clean current state. Warehouse/mart acceptance requires real production materialization and grain/reconciliation evidence for each required model, but does **not** add a stronger rule that every downstream mart must also have 78 current partitions. Idempotency/backfill acceptance is exercised on a representative production rerun plus deterministic regression tests rather than rerunning the full period twice.

## Measured full-period result

- **Clean coverage:** all 78 target months `2020-01..2026-06` have validated current pointers. The final missing sequence ended with `DONE 2026-09-23T09:57:17+09:00`.
- **Full-period row accounting:** source rows = Clean rows = **241,350,472**; trusted eligible rows = **241,320,806**; quarantined rows = **29,666**; therefore trusted + quarantined = Clean exactly. The 78 metadata bundles record 42,508 quarantine issues and 13,034 logical-conflict groups covering 28,144 rows; 76/78 months contain at least one quarantined row.
- **Dimensions:** `dim_station_history` = **14,992 intervals / 3,047 stations / 2,789 open intervals** from 31,986 observed station rows across 12 snapshots. `dim_date` = **2,373** dates covering 2020-01-01..2026-06-30.
- **Representative real warehouse month (`2020-09`):** `fact_trip_trusted` = **2,811,968** rows. `mart_source_month_quality` reconciles source/Clean **2,811,974 = 2,811,968 trusted + 6 quarantined**. `mart_station_day` = **60,979** grain rows with rental total **2,811,968**, completed total **2,796,989**, return total **2,797,391**, missing-return total **14,979**, and valid-distance total **322,289**. `mart_od_day` = **1,536,122** grain rows with trip total **2,796,989** and valid-distance total **322,289**. Thus fact↔station-day rental, station-day↔OD completed-trip, quality trusted↔fact, and station-day↔OD valid-distance reconciliation all match exactly.
- The representative month predates the first station snapshot (2021-01-31), so its fact metadata reports 0 matched / 2,811,968 unmatched rows by design; future station observations are not backfilled into 2020. Positive/interval station-history semantics are covered by the production station-history build plus deterministic enrichment/fact regressions rather than by materializing an additional multi-million-row post-snapshot month solely for acceptance.
- `mart_station_day 2020-09` published current successfully with exit 0; measured wall time was **1:06:30**, peak RSS **3,326,644 KB**, swap **0**. Because the validator performs independent grain/aggregate recomputation before current publication, this single real materialization is the minimum sufficient station-day production evidence under D-007.
- **Same-input retry:** `dim_date` was rerun on the final production warehouse with the same transformation/configuration identity and returned the identical `dv1_985770...` as `REUSED_CURRENT`, 2,373 rows, exit 0, wall time **3.55s**, peak RSS **80,092 KB**, swap **0**. Month/range same-input skip/reuse behavior remains covered by the backfill regression suite rather than replaying 78 months.
- **Observed recovery:** bounded reruns successfully recovered Clean `2020-10` and fact `2020-06` after earlier DuckDB SIGSEGV/general-protection failures; quality and OD recovery logs for `2020-06` are also retained. No OOM claim is made because kernel evidence did not show an OOM event.
- **Downstream materialization at acceptance:** fact = 9 months, source-month quality = 9 months, OD = 9 months, station-day = 1 month. This is evidence coverage, not a stronger all-78 downstream requirement.
- **Final current-HEAD regression:** WSL targeted production-sensitive suite (Clean publish, fact, three marts, backfill, Airflow control/DAG) = **139 passed, 1 skipped**. Windows lightweight state/control regression = **3 passed** and `compileall` passed.

## PRD §35 completion evidence

- **Dataset — PASS.** Production scope is `2020-01..2026-06` (78 rental months): 6 yearly ZIPs + 6 monthly CSVs, plus 12 observed station snapshots. All 78 rental months are processed to validated Clean current state. The preparation script pins byte size + SHA-256 for all 24 official files and registers immutable source identity. Official 2015 compatibility evidence is Clean `41,005` / trusted `40,908` / quarantined `97`.
- **Pipeline — PASS.** Regression tests cover NEW/UNCHANGED/REVISED sources, ZIP member-level changed-month rebuilds, arbitrary range backfill, partial-failure recovery, failed-month-only retry, and same-input idempotency. Production same-input reuse is additionally demonstrated by the final warehouse retry above; no full-period second replay is required.
- **Data Contract / Quality — PASS.** Known v16/v17 schemas, unknown-schema hard fail, source↔Clean and trusted↔quarantine reconciliation, severity rules, and quarantine provenance are validated. Regressions cover logical conflicts, exact duplicates, noncanonical/missing sex, impossible birth year, missing return, zero/extreme distance, station unmatched, and schema drift. Full-period Clean metadata supplies the dataset-wide row-accounting evidence; `mart_source_month_quality` is additionally materialized and reconciled on real production data as the warehouse-facing quality model.
- **Warehouse — PASS.** Official station data produced `dim_station_history` = 14,992 intervals / 3,047 stations / 2,789 open intervals from 31,986 observations; `dim_date` = 2,373 dates covering 2020-01-01 through 2026-06-30. Real production `fact_trip_trusted`, `mart_station_day`, `mart_od_day`, and `mart_source_month_quality` materializations pass their model-specific validators, and the representative cross-model reconciliations above close the grain/reconciliation requirement.
- **Reliability — PASS.** Deterministic fault injection covers transform failure, pre-publish failure, mart failure, historical source revision, partial backfill failure, retry after failure, and preservation of previously published current data. Real Clean/fact failure→recovery evidence is retained in production logs.
- **Orchestration — PASS.** Airflow 3.3.2 acceptance uses the same business layer as CLI/tests. A clean metadata DB registers exactly one intended DAG with zero import errors; integration tests cover scheduled month, manual range, post-horizon historical manual execution with real task instances, failed-month-only retry, and unrecovered failure surfacing as a failed DagRun.
- **Environment — PASS.** Reference execution is Windows host + WSL2 Ubuntu 24.04.4; Airflow remains Linux/WSL-only. Business logic contains no hard-coded Windows absolute path. Production-scale DuckDB execution is bounded, and the final completion run used the 10GB/4-CPU WSL cap with one heavy publisher at a time.
- **Reproducibility — PASS.** README documents dependency setup → exact source preparation/registration → station history → range execution → DQ/mart inspection → retry. `scripts/prepare-production-sources-wsl.sh` performs the source registration itself, source bytes are hash-pinned, and generated data remains outside Git.
- **Portfolio Explainability — PASS (artifact coverage).** README and this evidence cover every §35 explanation topic: rental-month partitioning; logical key vs PK; revision detection and changed-month invalidation; Raw/Clean/Quarantine/Trusted responsibilities; observed station-history limits; hard-fail/quarantine/warning; DuckDB vs Spark; Airflow as control plane; why dbt is not needed for v1; and immutable publish + atomic current pointers for partial-failure safety. A live interview explanation is a human rehearsal activity and is not falsely claimed as machine-verifiable evidence.

## Reliability evidence retained from acceptance tests

The full production run complements rather than replaces deterministic fault-injection tests. The test suite already covers transform failure, pre-publish failure, mart failure, historical source revision, partial range failure, failed-month-only retry, stale lineage rejection, and same-input idempotency. Airflow acceptance additionally proves scheduled/manual range execution, post-horizon historical manual runs, and unrecovered business failure surfaced as a failed DagRun.

## Portfolio explanation checkpoints

The project should be explained as a data reliability problem rather than a tool demo:

- **Why rental-month partitioning?** Seoul publishes and revises rental history at month/year source boundaries, and month is the smallest useful rebuild unit for incremental/backfill work.
- **Why immutable Raw/Clean versions?** Source revision and rerun behavior are provable only if the exact bytes and derived lineage are preserved instead of overwritten.
- **Why quarantine instead of choosing a duplicate/conflict winner?** `(bike_id, rent_at)` is a logical grouping key, not a guaranteed source PK; conflicting groups are preserved for provenance and excluded from trusted facts.
- **Why observed station history instead of pretending SCD change dates?** Official snapshots reveal observed states, not exact real-world change timestamps. Applicability therefore follows the documented snapshot precision contract.
- **Why DuckDB + Parquet + plain SQL?** The workload is local analytical batch processing where columnar files, embedded SQL execution, deterministic lineage, and simple reproducibility are stronger evidence than distributed infrastructure that the data volume does not require.
- **Why Airflow?** Business logic already runs from Python/CLI/tests; Airflow adds dependencies, scheduling, retry/backfill control, and run visibility without duplicating transformation logic in DAGs.
- **How does failure recovery work?** Immutable published versions stay intact, current pointers move only after validation, successful months remain published, and retries select failed months from the captured original plan rather than silently replanning history.
