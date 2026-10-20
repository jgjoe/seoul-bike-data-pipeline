# Production Validation — 2026-09-23

## Representative cloud extension — live core acceptance passed; Billing pending

### 2026-10-20 UTC: final representative cloud result

Only **2020-08 and 2020-09** production Clean/Trusted/OD/quality were rebuilt and
published to private GCS and `seoul-bike-dw.bike_cloud` in `us-central1`.
No other production month, Raw artifact, or station-day mart was uploaded.
The station-day dependency boundary is explained below; do not describe the
bounded replay as reproducing its historical full-input totals.

| Source month | Source = Clean | Trusted fact | Quarantined rows | OD rows | Completed / OD trips | Valid-distance trips |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| 2020-08 | 1,733,967 | 1,733,967 | 0 | 1,056,635 | 1,722,198 | 191,640 |
| 2020-09 | 2,811,974 | 2,811,968 | 6 | 1,536,122 | 2,796,989 | 322,289 |

All local/cloud row counts and integer/decimal aggregates matched. In both
cloud fact partitions, source-row IDs are unique and
`row_quarantined OR logical_trip_conflict OR NOT trusted_fact_eligible` counts
**0**. Warning rows remain in Trusted: 1,733,967 / 2,811,968 respectively,
including the expected pre-2021 station-history-unmatched warning.
Missing-return counts are **11,769 / 14,979**. Usage sums are
**49,478,794 / 97,203,903**; eligible-distance sums are exactly
**1,088,126,962.09 / 2,201,878,417.69**. OD/quality floating aggregates matched
within the documented 1e-9 relative tolerance. Six quarantined September
**rows**, not its 14 issue records, are excluded.

Successful production load job IDs:

| Target / partition | Job ID |
| --- | --- |
| fact_trip_trusted / 202008 | `bike_7b3e9f2b43aa05d9759195b1ad1e51e5ead023100f0b20618d04cdc069026444` |
| mart_od_day / 202008 | `bike_1f6faf043780ff61675153690f16b9ea5bd7a74df84c950870dee50967841cf3` |
| mart_source_month_quality / 202008 | `bike_5a1ba73543b0a1ecb231172b3674559be7bc1e3676b90cea4c3a70d5a9bf6aa9` |
| fact_trip_trusted / 202009 | `bike_bdab58b60ec4a45ff8f3c0a97e3334bab9983ff4e16de06437242e70c81a92ce` |
| mart_od_day / 202009 | `bike_bb7256dc7db54aa59f6113ca160886680b03e28021ea41021fbd61b55fa08f22` |
| mart_source_month_quality / 202009 | `bike_db50773f665edf5004f7787703149494d9f02caa877a9bdaf77465c8457b01bd` |

September verification query IDs include
`7d159889-6589-4b1e-ad85-1bb5dc3e8bd1` (fact),
`fb12ba2b-faf2-49b0-bd84-9c2d46c030e8` (OD), and
`d82d8238-84c6-4cf8-a609-00e55ac4c958` (quality).
The publication manifest, content-addressed GCS URI/generation and
`cloud_load_history.event_json` bind each file SHA-256 to the source revision
and pinned 2020 ZIP hash already recorded below.

**Identical production rerun:** all eight artifacts returned `SKIP_UNCHANGED`;
no new object uploads or load jobs occurred, and the same six successful job
IDs were retained while metrics were rechecked.

### Live isolated revision, failure and recovery checks

The synthetic acceptance phase finished **SUCCEEDED** in `verify_*` tables and
`v1/verification/` objects. These are deliberately tiny fixtures, not claims of
official source revision or additional production-month coverage.

- Initial fixture: three February Clean rows = one warning/incomplete Trusted
  row + two quarantined conflicting rows; BigQuery contains only the eligible
  row. Its identical rerun skipped all nine artifacts.
- Changed February source SHA-256:
  `52322e92a0545017a18b184ae62e1461b3fcab56d479585610ddc046f65c65b7`
  → `cd74e262c148b964a2835a179754cdc39d7780bab6a1cd5ef2bf33873378171a`.
  Four February partition loads were attempted; the three January loads were
  skipped with their original successful IDs. Clean/version/file hashes and
  source revisions are retained in the plans and SQL history.
- A deliberately incompatible REQUIRED field caused real failed load
  `bike_bddd37800281964fe65f2e74d7d03db3800161a229e171209c669af39d1916ee`.
  The previous February fact metrics were unchanged. Only that failed target
  was loaded on recovery:
  `bike_3ac2267db36bfc47d59a1bbd9876091d9f10cbc1100f93948b0399f56d6c8439`.
  Other successful partitions were not reloaded.
- An interruption after a successful fact load/verification but before success
  history left its intent PENDING. Recovery fetched and reused
  `bike_2df672b80960e5ab19ba9e4613575f4939b9745c38124b7b1c4519009d7dedc0`;
  **zero new load jobs** were submitted on that recovery.
- A partition replacement is atomic; the whole multi-table publication is not.
  Failed intermediate phases stayed visible instead of being reported as
  all-tables success.

### Live API findings, retained failures and final tests

Real requests exposed SDK/service details not exercised by the original fakes:

1. BigQuery rejects `decimalTargetTypes` together with explicit schema. Keep
   the explicit BIGNUMERIC/DATETIME schema; remove the redundant option.
2. An unclustered table must omit the clustering configuration, not send
   an empty fields array.
3. The API mirrors `requirePartitionFilter` inside `timePartitioning`.
   Compare the required semantic type/field/expiration/filter, not raw object
   equality that incorrectly treats this equivalent representation as drift.
4. Parquet LIST inference must be enabled for the canonical repeated DQ records.
5. `LoadJob.bad_records` is not an SDK property; read documented REST
   `statistics.load.badRecords` (omitted means zero).

Four server-confirmed production load failures were retained, not hidden.
Four marts had already loaded successfully when the old statistics accessor
failed; retry recovered their existing jobs instead of loading them again.
The final production retry submitted only the two still-failed fact loads.
Failure evidence is retained at:

- `$HOME/seoul-bike-production-replay/run/cloud/acceptance-initial-failed-evidence.json`
- `$HOME/seoul-bike-production-replay/run/cloud/acceptance-schema-retry-failed-evidence.json`

Successful phase results, every submitted query/load ID, captured hashes and
before/after metrics are retained at:

- `$HOME/seoul-bike-production-replay/run/cloud/acceptance-evidence.json`
- `$HOME/seoul-bike-production-replay/run/cloud-verification/acceptance-evidence.json`

Before live publication the full regression passed **405 tests / 2 skipped**.
After the API fixes, affected adapter tests passed and the final complete WSL
suite finished **407 passed, 2 skipped**, exit 0, **908.11 seconds**.
The skips are the opt-in Airflow integrations, not new live-cloud DAG evidence.
An SDK FutureWarning about explicitly setting `job_retry=None` for fixed query
IDs remains non-fatal under the installed SDK; it did not prevent the tested
deterministic history/job recovery. No unrelated dependency upgrade was made.

### Measured usage and remaining billing evidence

Across production attempts plus synthetic acceptance, deduplicated by job ID:

- **150 query jobs**; **769,031,948 processed bytes**;
  **1,325,400,064 billed query bytes**.
- **26 load jobs**: 21 succeeded, four real production compatibility failures
  and one intentionally failed synthetic load; reported load input totals
  **653,065,906 bytes**.
- GCS live storage: **1,290,429,462 bytes**, including verification artifacts.
  Soft-delete retention was zero; no cloud resources/objects were deleted.
- BigQuery logical storage: **4,158,475,360 bytes** across production and
  verification tables/history. Production fact alone:
  **4,545,935 rows / 3,998,042,099 logical bytes**; OD:
  **2,592,757 rows / 160,159,616 logical bytes**.
- These observed volumes are below the stated US Storage / BigQuery free
  allowances for this exercise. Account-wide other usage is not inferred.
  **Actual gross cost, credits used and net billed currency remain unmeasured**:
  billed query bytes are not a currency invoice or proof of ₩0 cost.
- After 24–48 hours or longer reporting delay, the user must filter Billing
  Reports to **seoul-bike-dw**, **2026-10-19/20 UTC**, and BigQuery/Cloud Storage;
  record service cost, promotional/trial credits and net cost separately.
  Confirm remaining trial credit, budget credit treatment and the unconfirmed
  10 GiB/day quota. Do not upgrade the trial account.
- Project-regional physical/time-travel storage evidence remains for the user's
  administrator console; runtime IAM was not widened just to query it.
  Generated data and cloud resources are retained. Cleanup, push, PR and merge
  remain separately gated.

### 2026-10-19 UTC: station request cause resolved without changing pins

The 183-byte station payload was the site's HTML invalid-access alert, not a
source revision. `download_checked` incorrectly hard-coded `infSeq=1` for both
datasets. OA-15182 rental requires **1**; OA-13252 station requires **2**.
The script now passes that parameter per dataset, with a regression that
observes the actual curl arguments. All expected sizes and hashes are unchanged.

A direct OA-13252 `seq=11, infSeq=2` request **without Referer** returned
**206,960 bytes** in **0.738 seconds**, SHA-256
`4adc277768ba0c4ddbf6eaa80784a48e3ed43a6e2d199b1e27e22416043314b5`.
Referer is therefore unnecessary for this pinned station artifact and was not
added to either dataset's requests. This successful request-parameter correction
supersedes the prior station-source blocker; no revision or alternate snapshot
was substituted.

The corrected source preparation completed successfully in **56.449 seconds**:
rental **1,022,004,940 bytes / 30.653338 seconds** (KNOWN_UNCHANGED), station
**206,960 bytes / 0.496695 seconds** (full pinned hash matched). Its source ID is
`sv1_120581c0bbe9d5bf7c77a5a4cb44444fc7f778a03c4a606fb29b8a02f3ed3824`.
Station publication contains **2,154 input rows / 2,153 observations**.
The earlier rejected payload
`$HOME/seoul-bike-production-replay/source/downloads/station/2021-01-31.csv.part.Ar8But`
was removed after verifying its exact path and 183-byte
size; no cloud resource was deleted.

The bounded replay intentionally does **not** rebuild station-day. Inspection
of `publish_current_station_day_mart` shows that it requires fact current
pointers for **every month 2020-01 through the service month**, including
2020-01..07 for a September mart. Building those seven other production months
would violate the approved two-month limit. Preserve that local completeness
contract instead of weakening it: only the selected months' Clean, fact, OD and
source-quality outputs are regenerated/published; historical station-day
counts below remain previous evidence, not a fresh two-month result.

The live acceptance harness `scripts/verify-cloud-publication.py` uses the same
publisher/adapters as CLI/Airflow. Its `production` phase targets 2020-08/09;
`synthetic` exercises revisions/failures in **verify_* tables**, separate history
and **v1/verification/** objects in the existing scoped dataset/bucket. No new
dataset or broader IAM is required. Synthetic data is never a replacement for
the production count checks.

### 2026-10-20 UTC: bounded replay exposed and fixed shared DuckDB spill files

August Clean/fact/quality/OD published successfully. September Clean matched
the reference **2,811,974 source/Clean = 2,811,968 eligible + 6 quarantined rows**
(14 quarantine issues). September fact initially failed in enrichment:
`Could not read enough bytes from file ".tmp/duckdb_temp_storage_DEFAULT-0.tmp"`.
Moving the working directory from the Windows mount to Linux did **not** fix it;
the next failure named `.tmp/duckdb_temp_storage_S32K-0.tmp`. Neither error is
reported as OOM or silently treated as success.

The underlying assumption was tested separately with DuckDB **1.5.5**: two
independent in-memory connections, each limited to 16 MB, materialized 30,000
1,024-character rows into the **same** spill directory. Closing the second
connection made the first connection's scan fail with the same short-read IO
error. This reproduces shared spill-file corruption independently of the
production transformation and filesystem choice.

`evaluate_trip_station_enrichment` now allocates a private temporary spill
directory for its connection and cleans it after closing. Nested input
validators can no longer overwrite/remove that connection's spill files.
Memory limit (3 GB), thread count (2), SQL, schemas and business semantics are
unchanged. A behavior regression forces a spilling enrichment connection and
a second spilling validator, then verifies the first still reads exactly
`30,000 × 32 × 32` characters and its private directory is cleaned. The focused
regression passed; a full regression and only the failed downstream September
steps were restarted after this cause fix, without rebuilding either Clean.

### 2026-10-19 UTC: approved two-month regeneration stopped at size guard

The replay scope was limited to `2020-08` and `2020-09`, plus required station
history, under `$HOME/seoul-bike-production-replay`, using
`VERSION=git-b9de90a`. The source preparation script now accepts
`--months 2020-08 2020-09`; `--list-only` verifies the selected pinned catalog
without downloads or generated directories. The original unfiltered mode is
preserved. **9 offline selector/size/SHA guard tests passed**; the previously
completed **392 passed, 2 skipped** core regression remains valid because no
Python pipeline/cloud implementation changed.

Only the pinned 2020 annual rental ZIP is needed: its annual packaging requires
downloading the ZIP, but not building its other ten months. The first station
snapshot anchors the pre-observation/unmatched 2020 contract; later snapshots
are not downloaded. The selected transfer results were:

| Source | Pinned bytes | Received bytes | Transfer time | Result |
| --- | ---: | ---: | ---: | --- |
| OA-15182 seq=91, 2020 ZIP | 1,022,004,940 | 1,022,004,940 | 20.752671 s | Size and SHA-256 matched |
| OA-13252 seq=11, 2021-01-31 station CSV | 206,960 | 183 | 0.420400 s | **SIZE_MISMATCH; stopped** |

- Verified rental SHA-256:
  `a8f3a38f2e73661c2bc0d2d01becd08712cc5226c433da9d5ff8348f9e1b3a91`.
- Registered rental source:
  `sv1_8c8f129f2542664b55f42546a6153a2b3149806b1a8851b3fc75ead664fa7e9e`;
  its 12 ZIP members were inventoried, not materialized into 12 Clean months.
- Exact stop:
  `SIZE_MISMATCH infId=OA-13252 seq=11 expected=206960 actual=183`.
  Station SHA-256 was not evaluated after size failed. Expected station SHA-256
  remains `4adc277768ba0c4ddbf6eaa80784a48e3ed43a6e2d199b1e27e22416043314b5`.
  A 183-byte response alone does not establish that the official dataset was
  revised; no replacement or pin update is justified by this result.
- Total received payload: **1,022,005,123 bytes**; transfer time sum
  **21.173071 seconds**. Source preparation including validation, registration
  and ZIP inventory took **52.389 seconds**, exit **3**.
- The verified rental Raw artifact and inventory remain recoverable under
  `$HOME/seoul-bike-production-replay/source/raw`; the rejected station
  payload remains in a unique `2021-01-31.csv.part.*` file under
  `$HOME/seoul-bike-production-replay/source/downloads/station`.
- The existing Windows checkout had CRLF shell scripts (`set: pipefail` failed
  before script logic). Shell files now declare LF in `.gitattributes`; this
  execution used an LF-normalized generated copy under the Linux replay root,
  with the repository checkout's Python explicitly set as `PYTHON_BIN`.

**No DuckDB publisher or new cloud call ran after this source attempt.**
Local representative counts and all remaining live acceptance checks are still
unverified. No later station snapshot, changed pin, synthetic substitute, other
rental year, or full-period replay was used. To resume, the exact pinned station
artifact must be made available, or the maintainer must review a separate
source-revision change. Do not rerun past this mismatch automatically.

### 2026-10-19 UTC: regression and authenticated preflight

- Before the first cloud call, the complete WSL regression suite finished:
  **392 passed, 2 skipped**, exit 0, **921.85 seconds**. Command:
  `.venv-airflow/bin/python -m pytest -q`, in the `feat/cloud-bigquery` checkout.
  The earlier 15-minute timeout is superseded by this completed run; skipped
  opt-in Airflow integration tests are not claimed as newly executed.
- SDK clients explicitly used `project="seoul-bike-dw"` and
  `location="us-central1"` where applicable. WSL ADC impersonation was used;
  no `bq` CLI, credential/key export, or `set-quota-project` was used.
- GCS publisher preflight passed for
  `gs://seoul-bike-dw-representative`: Standard storage, `US-CENTRAL1`,
  uniform bucket-level access and enforced public-access prevention.
  Observed **0 live objects / 0 bytes**; soft-delete retention was **0 seconds**.
- BigQuery publisher preflight passed and created the empty history table
  `seoul-bike-dw.bike_cloud.cloud_load_history`, with its configured partitioning
  and clustering. Dataset-scoped table creation therefore worked; no IAM roles
  were widened. This is not proof of partition loading or history DML.
- Bounded `SELECT 1 AS ok` succeeded using the Python BigQuery adapter:
  job **`957a602a-8ca5-41d4-ab67-8cbfde17b36f`**, **0 processed bytes /
  0 billed bytes**, cache hit. The history table reported **0 rows /
  0 logical bytes**. No production data tables or load jobs were created.
- The first telemetry probe stopped on an SDK attribute error
  (`SoftDeletePolicy.duration_seconds` does not exist), after GCS preflight and
  before BigQuery calls. Reading the API's `retentionDurationSeconds` field
  resolved the probe; this was not a publisher or authentication failure.

### Earlier missing-runtime blocker and unexecuted acceptance checks

Before the bounded regeneration attempt above, the documented runtime
`$HOME/seoul-bike-production-replay/source/manifest.json` is absent in this
WSL environment (`FileNotFoundError`). The WSL home listing contains no
`seoul-bike-production-replay` directory. A generated-manifest lookup under
the Windows project parent directory found no production source manifest;
the canonical checkout also has no matching generated manifest/current
`2020-09` pointer. The historical local results below are recorded evidence,
not accessible input bundles in this environment.

Consequently **no representative production month was uploaded**. Local/cloud
reconciliation, same-input skip, changed-month-only reload, real failed-load
recovery and production storage/scan measurements remain **NOT RUN**. Offline
fake tests and a successful `SELECT 1` do not close those acceptance items.
No synthetic rows were substituted for official production data. No additional
source download, full-period replay, or resource deletion was performed.

To resume, provide the actual `SOURCE_ROOT` and `RUN_ROOT`, or restore the
manifest/immutable Raw/station and selected Clean/Trusted/mart bundles to the
documented layout. If those artifacts are unavailable, a bounded production
replay needs a revised scope/time estimate reviewed by the maintainer. Prefer one
already-materialized month over rebuilding the 78-month pipeline.

### Earlier preflight cost snapshot (before data publication)

- Observed data-query usage above is **0 bytes**, GCS live storage **0 bytes**,
  and BigQuery history logical storage **0 bytes**. API/metadata operations
  still occurred; these observations are **not a statement of ₩0 billed cost**.
  Physical/time-travel storage and settled billing were not measured.
- The user's **₩1,000 / 50·90·100% budget alert** and active trial credit are
  setup facts. Application of the **10 GiB/day query quota remains unconfirmed**;
  the runtime's dry runs and per-query `maximum_bytes_billed` are not a global
  daily quota or spending cap.
- After at least 24–48 hours, the user should filter Billing Reports to
  **`seoul-bike-dw`** and this execution period, inspect BigQuery and Cloud
  Storage separately, and record **gross service cost, trial/promotional credits,
  and net billed cost separately**. Check delayed usage, the budget's credit
  treatment, and remaining trial credit; do not manually upgrade the account.
- No push, PR, merge, or resource deletion was performed. The empty history
  table is retained; cleanup requires separate approval with exact targets.

Existing local production evidence below remains the baseline, not cloud
production evidence.

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
