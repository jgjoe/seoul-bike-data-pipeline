# Seoul Bike Data Pipeline

서울 따릉이 대여이력의 source revision, schema drift, data quality, backfill, provenance를 다루는 로컬 batch data reliability / DW pipeline이다. 구현 기준선은 [PRD v1.0 FINAL](docs/prd-v1.0-final.md)이다.

현재 구현 범위는 **source registration + immutable Raw artifact preservation + ZIP member inventory + month impact planning + streaming CSV ingestion + typed source projection + canonical normalization / row-level DQ + DuckDB-backed month-level group DQ + immutable Clean Parquet publish + station snapshot ingestion/consolidation/publish + `dim_station_history` warehouse publish + trusted Clean trip station-history enrichment + immutable/versioned `fact_trip_trusted` month publish + deterministic `dim_date` warehouse publish + versioned `mart_station_day` / `mart_od_day` / `mart_source_month_quality` publish + orchestration-independent month/range backfill workflow + Apache Airflow control plane**이다. Rental Clean과 station snapshot은 immutable source-facing version을 만들고, station history/trusted fact/date dimension/marts는 plain SQL + persistent DuckDB warehouse bundle로 materialize한다. 모든 durable publish는 hard validation을 통과한 version만 atomic current pointer로 노출한다. Airflow DAG는 transformation/DQ를 복제하지 않고 동일 backfill business layer를 호출해 scheduled month, manual range, failed-month retry와 run visibility를 제공한다 (PRD §6~§25, §26~§29 + Addendum 001/002).

- Runtime은 Python >= 3.12이고 DuckDB Python package를 analytical execution dependency로 사용한다. test dependency는 pytest다.
- 기준 실행 환경은 WSL2 Linux다 (PRD §25). manifest에는 local path를 기록하지 않는다.

## Architecture

```text
Official Seoul files
  -> immutable Raw + source manifest/hash
  -> Python contract/DQ edge
  -> DuckDB canonical processing
  -> partitioned immutable Clean Parquet + quarantine provenance
  -> DuckDB fact/dim warehouse
  -> plain-SQL station-day / OD / source-quality marts
```

Apache Airflow는 이 business layer를 다시 구현하지 않고 scheduled month, manual range backfill, failed-month retry를 제어하는 control plane으로만 사용한다. Durable output은 검증된 immutable version을 만든 뒤 atomic current pointer를 이동하므로 실패한 실행이 기존 publish를 덮어쓰지 않는다.

실제 `2020-01~2026-06` production-scale 재현 결과와 source identity, DQ/warehouse/mart coverage, 실패→복구 증거는 [`docs/production-validation.md`](docs/production-validation.md)에 기록한다.

## Verified v1 evidence

- Clean production coverage: **78 / 78 months** (`2020-01..2026-06`).
- Full-period reconciliation: **241,350,472 source/Clean rows = 241,320,806 trusted + 29,666 quarantined**.
- Observed station history: **12 snapshots / 31,986 observations / 14,992 intervals / 3,047 stations**.
- Representative real warehouse month `2020-09`: fact **2,811,968**, station-day **60,979**, OD **1,536,122**, with cross-model reconciliation matching exactly.
- Final release-candidate baseline before public packaging: WSL production-sensitive regression **139 passed, 1 skipped**; Windows lightweight state/control regression **3 passed** plus `compileall` PASS.

The downstream production acceptance evidence is intentionally bounded under PRD §35 / D-007: fact, source-quality, and OD have real production evidence across 9 months, while station-day has one representative production month. This is not a claim that every downstream model was materialized for all 78 months.

## Public repository boundary

The public repository is intended to contain the pipeline code, SQL, Airflow DAG/plugin, synthetic regression fixtures, the frozen PRD/addenda, and measured production evidence. It intentionally excludes official source bytes, generated Parquet/DuckDB outputs, Airflow metadata/XCom/logs, passwords or credentials, local environment files, machine-specific runtime paths, and internal project-management state. Official source files are downloaded locally from the documented Seoul Open Data sources and validated against the pinned size/SHA-256 contract before use.

## 사용법

```bash
python3 -m pytest -q

python3 -m seoul_bike_pipeline source register path/to/download.csv \
  --manifest data/source-manifest.json \
  --raw-root data/raw \
  --dataset-id rental_history \
  --logical-period 2020-01 \
  --published-filename "서울특별시 공공자전거 대여이력 정보_202001.csv" \
  --source-url "<official locator>" \
  --license "<이용허락>" \
  --source-modified-at 2021-03-05

python3 -m seoul_bike_pipeline source inventory-zip \
  --manifest data/source-manifest.json \
  --raw-root data/raw \
  --source-version-id sv1_<parent-id>

python3 -m seoul_bike_pipeline source plan-zip-months \
  --manifest data/source-manifest.json \
  --raw-root data/raw \
  --current-source-version-id sv1_<current-parent-id> \
  --previous-source-version-id sv1_<previous-parent-id>

python3 -m seoul_bike_pipeline source inspect-csv \
  --manifest data/source-manifest.json \
  --raw-root data/raw \
  --source-version-id sv1_<source-id> \
  --member-name "<exact ZIP member name>"

python3 -m seoul_bike_pipeline source project-csv \
  --manifest data/source-manifest.json \
  --raw-root data/raw \
  --source-version-id sv1_<source-id> \
  --member-name "<exact ZIP member name>"

python3 -m seoul_bike_pipeline source canonicalize-csv \
  --manifest data/source-manifest.json \
  --raw-root data/raw \
  --source-version-id sv1_<source-id> \
  --member-name "<exact ZIP member name>"

python3 -m seoul_bike_pipeline source classify-month \
  --manifest data/source-manifest.json \
  --raw-root data/raw \
  --source-version-id sv1_<current-source-id> \
  --source-month 2026-01

python3 -m seoul_bike_pipeline source publish-clean-month \
  --manifest data/source-manifest.json \
  --raw-root data/raw \
  --clean-root data/clean \
  --source-version-id sv1_<current-source-id> \
  --source-month 2026-01 \
  --transformation-version <explicit-code-version> \
  --configuration-json '{}'

python3 -m seoul_bike_pipeline source publish-station-snapshot \
  --manifest data/source-manifest.json \
  --raw-root data/raw \
  --station-root data/station \
  --source-version-id sv1_<station-source-id> \
  --transformation-version <explicit-code-version> \
  --configuration-json '{}'

python3 -m seoul_bike_pipeline warehouse publish-station-history \
  --manifest data/source-manifest.json \
  --raw-root data/raw \
  --station-root data/station \
  --warehouse-root data/warehouse \
  --transformation-version <explicit-code-version> \
  --configuration-json '{}'

python3 -m seoul_bike_pipeline warehouse evaluate-station-enrichment \
  --manifest data/source-manifest.json \
  --raw-root data/raw \
  --station-root data/station \
  --clean-root data/clean \
  --warehouse-root data/warehouse \
  --source-month 2026-01

python3 -m seoul_bike_pipeline warehouse publish-fact-trip-month \
  --manifest data/source-manifest.json \
  --raw-root data/raw \
  --station-root data/station \
  --clean-root data/clean \
  --warehouse-root data/warehouse \
  --source-month 2026-01 \
  --transformation-version <explicit-code-version> \
  --configuration-json '{}'

python3 -m seoul_bike_pipeline warehouse publish-date-dimension \
  --warehouse-root data/warehouse \
  --transformation-version <explicit-code-version> \
  --configuration-json '{}'

python3 -m seoul_bike_pipeline warehouse publish-station-day-mart \
  --manifest data/source-manifest.json \
  --raw-root data/raw \
  --station-root data/station \
  --clean-root data/clean \
  --warehouse-root data/warehouse \
  --service-month 2026-01 \
  --transformation-version <explicit-code-version> \
  --configuration-json '{}'

python3 -m seoul_bike_pipeline warehouse plan-station-day-revision \
  --manifest data/source-manifest.json \
  --raw-root data/raw \
  --station-root data/station \
  --clean-root data/clean \
  --warehouse-root data/warehouse \
  --source-month 2020-01 \
  --old-fact-version-id fv1_<old> \
  --new-fact-version-id fv1_<new>

python3 -m seoul_bike_pipeline warehouse publish-od-day-mart \
  --manifest data/source-manifest.json \
  --raw-root data/raw \
  --station-root data/station \
  --clean-root data/clean \
  --warehouse-root data/warehouse \
  --service-month 2026-01 \
  --transformation-version <explicit-code-version> \
  --configuration-json '{}'

python3 -m seoul_bike_pipeline warehouse plan-od-day-revision \
  --manifest data/source-manifest.json \
  --raw-root data/raw \
  --station-root data/station \
  --clean-root data/clean \
  --warehouse-root data/warehouse \
  --source-month 2020-01 \
  --old-fact-version-id fv1_<old> \
  --new-fact-version-id fv1_<new>

python3 -m seoul_bike_pipeline warehouse publish-source-month-quality \
  --manifest data/source-manifest.json \
  --raw-root data/raw \
  --station-root data/station \
  --clean-root data/clean \
  --warehouse-root data/warehouse \
  --source-month 2026-01 \
  --transformation-version <explicit-code-version> \
  --configuration-json '{}'

python3 -m seoul_bike_pipeline warehouse plan-source-month-quality-revision \
  --manifest data/source-manifest.json \
  --raw-root data/raw \
  --source-month 2020-01 \
  --old-source-version-id sv1_<old> \
  --new-source-version-id sv1_<new>

python3 -m seoul_bike_pipeline pipeline backfill-range \
  --manifest data/source-manifest.json \
  --raw-root data/raw \
  --station-root data/station \
  --clean-root data/clean \
  --warehouse-root data/warehouse \
  --start-month 2020-01 \
  --end-month 2026-06 \
  --transformation-version <explicit-code-version> \
  --configuration-json '{}'

# WSL2/Linux only: reproducible Airflow 3.3.2 environment
bash scripts/install-airflow-wsl.sh .venv-airflow

export AIRFLOW_HOME=/tmp/seoul-bike-airflow
export AIRFLOW__CORE__DAGS_FOLDER="$PWD/dags"
export AIRFLOW__CORE__PLUGINS_FOLDER="$PWD/plugins"
export AIRFLOW__CORE__LOAD_EXAMPLES=False
export SEOUL_BIKE_TRANSFORMATION_VERSION=<explicit-code-version>

# These exports are shell-local. Repeat this same block in every WSL2 terminal
# used for Airflow (setup, standalone runtime, and manual trigger commands).

.venv-airflow/bin/airflow db migrate
.venv-airflow/bin/airflow dags reserialize
.venv-airflow/bin/airflow dags list
.venv-airflow/bin/airflow dags list-import-errors
.venv-airflow/bin/airflow dags unpause seoul_bike_monthly_backfill

# In a dedicated WSL2 terminal, repeat the Airflow exports above, then keep
# this running before using `dags trigger`.
# Airflow 3 standalone starts the local API server, scheduler, DAG processor,
# triggerer, and executor-facing services needed to execute queued DagRuns.
.venv-airflow/bin/airflow standalone
```

`src/` layout이므로 설치하지 않고 실행할 때는 `PYTHONPATH=src`를 앞에 붙인다 (`pip install -e .` 후에는 필요 없다).

### Production replay (WSL2/Linux)

실제 `2020-01~2026-06` rental + observed station history를 처음부터 재현할 때 generated data는 Git에 넣지 않는다. WSL2에서는 대용량 DuckDB/Parquet I/O를 `/mnt/c`보다 Linux filesystem에 두는 것을 권장하므로 아래 예시는 `$HOME` 아래를 사용한다. Repo-local `data/production-*` 경로도 `.gitignore`에 포함되어 있다. `prepare-production-sources-wsl.sh`는 2026-09-21/22에 확인한 서울 열린데이터 파일의 byte size와 SHA-256을 함께 고정하므로, 공식 파일이 교체되면 조용히 다른 입력을 쓰지 않고 중단한다.

최종 workstation validation에서는 WSL2를 `memory=10GB`, `processors=4`로 제한하고 heavy publisher를 한 번에 하나만 실행했다. 이 값 자체가 business contract는 아니지만, 여러 production-scale DuckDB publisher를 동시에 실행해 Windows host를 굶기지 않는 것이 권장 운영 방식이다. 실제 측정 결과와 완료 증거는 `docs/production-validation.md`에 기록한다.

```bash
# Fresh Ubuntu/WSL2 image only; skip when `python3 -m venv` already works.
sudo apt-get update
sudo apt-get install -y python3.12-venv

python3 -m venv .venv
.venv/bin/python -m pip install -e '.[test]'

VERSION=git-b9de90a  # measured business transformation identity
REPLAY_ROOT="$HOME/seoul-bike-production-replay"
SOURCE_ROOT="$REPLAY_ROOT/source"
RUN_ROOT="$REPLAY_ROOT/run"

bash scripts/prepare-production-sources-wsl.sh "$SOURCE_ROOT" "$VERSION"

.venv/bin/python -m seoul_bike_pipeline warehouse publish-station-history \
  --manifest "$SOURCE_ROOT/manifest.json" \
  --raw-root "$SOURCE_ROOT/raw" \
  --station-root "$SOURCE_ROOT/station" \
  --warehouse-root "$RUN_ROOT/warehouse" \
  --transformation-version "$VERSION" \
  --configuration-json '{}'

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

# Inspect one month's DQ result and one materialized mart using the durable
# current pointer rather than assuming a generated version directory name.
.venv/bin/python - "$RUN_ROOT/warehouse" 2020-01 <<'PY'
import json
import sys
from pathlib import Path

import duckdb

root = Path(sys.argv[1])
month = sys.argv[2]

quality_pointer = json.loads(
    (root / "mart_source_month_quality" / "current" / f"source_month={month}.json").read_text()
)
quality_db = (
    root
    / "mart_source_month_quality"
    / "versions"
    / f"source_month={month}"
    / f"mart_version_id={quality_pointer['mart_version_id']}"
    / "warehouse.duckdb"
)
with duckdb.connect(str(quality_db), read_only=True) as con:
    print(con.execute("SELECT * FROM mart_source_month_quality").fetchone())

od_pointer = json.loads(
    (root / "mart_od_day" / "current" / f"service_month={month}.json").read_text()
)
od_db = (
    root
    / "mart_od_day"
    / "versions"
    / f"service_month={month}"
    / f"mart_version_id={od_pointer['mart_version_id']}"
    / "warehouse.duckdb"
)
with duckdb.connect(str(od_db), read_only=True) as con:
    print(con.execute(
        "SELECT service_date, origin_station_no, destination_station_no, trip_count "
        "FROM mart_od_day ORDER BY trip_count DESC LIMIT 5"
    ).fetchall())
PY

# Explicit retry/idempotency check: run a completed month again through the
# same range business layer. It must converge without duplicating or replacing
# unchanged published data.
.venv/bin/python -m seoul_bike_pipeline pipeline backfill-range \
  --manifest "$SOURCE_ROOT/manifest.json" \
  --raw-root "$SOURCE_ROOT/raw" \
  --station-root "$SOURCE_ROOT/station" \
  --clean-root "$RUN_ROOT/clean" \
  --warehouse-root "$RUN_ROOT/warehouse" \
  --start-month 2020-01 \
  --end-month 2020-01 \
  --transformation-version "$VERSION" \
  --configuration-json '{}'
```

`SOURCE_ROOT/manifest.json`에는 rental과 station source version을 함께 보존한다. Fact/history validator가 두 lineage를 같은 immutable Raw namespace에서 다시 검증하므로 둘을 별도 manifest로 분리하면 안 된다. 실제 production-scale acceptance 결과와 측정치는 [`docs/production-validation.md`](docs/production-validation.md)에 기록한다.

Raw artifact는 `<raw-root>/artifacts/<source_version_id>/artifact`에 공식 filename과 분리된 deterministic path로 저장된다. 새 version은 temp copy의 size/SHA-256 검증이 끝난 뒤에만 final path에 노출되며, 기존 Raw object가 있으면 overwrite하지 않고 checksum을 다시 검증한다. manifest에 알려진 version의 Raw object가 누락되거나 손상됐으면 자동 복구하지 않고 hard fail한다. local Raw path는 manifest에 기록하지 않는다.

`<raw-root>/artifacts/`와 `<raw-root>/member-inventories/` namespace는 generated Raw state 전용이므로 source manifest 경로로 사용할 수 없다.

ZIP inventory는 `<raw-root>/member-inventories/<parent_source_version_id>/inventory.json`에 저장한다. 각 regular file member는 `member_name`, uncompressed `member_size`, `member_sha256`을 가지며 parent source version과 결합해 PRD §7 member identity를 구성한다. Archive provenance를 먼저 완전하게 보존하기 위해 CSV가 아닌 regular file도 inventory에 포함하고 directory entry만 제외한다. 어떤 member가 monthly CSV processing unit인지 판별하는 filename/month mapping은 다음 단계의 책임이다. duplicate member name은 이후 `raw_row_id` provenance가 모호해지므로 hard fail한다. 기존 inventory가 동일하면 다시 쓰지 않고 검증만 하며, 다르면 자동 overwrite하지 않는다.

Month planning은 CSV member 이름의 실제 source drift를 보수적으로 해석한다. `YYYYMM`, `YYYY.MM`, `YYMM`, `YY.MM`, compact part suffix(예: `22081`)와 same-year range(예: `2020.07~08`)를 지원한다. Non-CSV regular file은 provenance에 남되 month processing unit에서는 제외하고, 인식할 수 없는 CSV filename은 silent skip하지 않고 hard fail한다. Range member는 해당되는 모든 logical month에 영향을 주며 실제 row split은 이후 CSV parsing 단계가 담당한다.

Parent ZIP revision 비교는 caller가 current/previous source version을 명시적으로 지정한다. 같은 logical month의 member content `(size, sha256)` multiset이 같으면 parent ZIP hash나 member filename이 달라져도 `UNCHANGED`로 보고 downstream rebuild 대상에서 제외한다. 새 month는 `NEW`, content가 달라진 month는 `REVISED`, 이전 parent에만 있던 month는 `REMOVED`로 명시해 source disappearance를 숨기지 않는다.

CSV ingestion edge는 `UTF-8 BOM/UTF-8`을 먼저 시도하고 실제 서울 source에서 확인한 `CP949`를 fallback으로 사용한다. 공식 source에서 확인한 두 header family만 허용한다: `rental_v16`은 16열이고 `rental_v17`은 동일 16열 뒤에 `자전거구분`이 추가된 17열이다. Header가 정확히 일치하지 않거나 row column count가 어긋나거나 structural CSV parse가 실패하거나 data row가 0건이면 hard fail한다. 이 단계에서는 source field text를 normalize하지 않는다.

Physical source row number는 header를 제외한 CSV data record 기준 1-based다. `raw_row_id`는 `(source_version_id, member_name, source_row_number)`의 canonical JSON SHA-256에 `rr1_` prefix를 붙여 생성하고, 별도로 decoded source field sequence의 canonical JSON SHA-256을 `row_content_sha256`으로 계산한다. Standalone CSV의 `member_name`은 immutable manifest의 `published_filename`, ZIP source는 exact archive member name이다. ZIP member는 persisted member inventory의 size/SHA-256과 다시 대조한 뒤 parsing한다.

Typed source projection은 구조적으로 정상인 row를 버리지 않기 위해 DQ 판정과 분리한다. Timestamp는 `YYYY-MM-DD HH:MM:SS`, dock/usage/birth는 integer, distance는 finite decimal로 변환한다. Empty/whitespace와 실제 legacy source에서 확인한 `\N`은 known null marker로 `None` 처리한다. 그 외 non-null scalar가 타입으로 변환되지 않으면 row를 중단하지 않고 field-level conversion issue와 `None`을 남긴다. Source string은 non-null이면 trim/대소문자 normalization 없이 그대로 유지한다. 따라서 leading-zero station number, raw sex, user type, bike type은 아직 source-facing representation이다.

Standalone monthly CSV의 expected source month는 manifest `logical_period=YYYY-MM`, ZIP member는 filename month mapping에서 가져온다. `rent_at`이 parse되면 그 rental month를 expected set과 비교해 `MATCH` / `MISMATCH`를 기록하고, missing/invalid `rent_at`은 `UNAVAILABLE`로 둔다. Return timestamp가 다음 달로 넘어가는 것은 정상이며 source-month validation 기준에 사용하지 않는다.

`source canonicalize-csv`는 row output을 노출하기 전에 source unit을 한 번 preflight한다. Source/rent-month mismatch가 하나라도 있으면 hard fail하고, 여러 rental month를 포함하는 member에서 `rent_at`이 unavailable해 row의 month를 결정할 수 없어도 hard fail한다. Single-month source에서 missing/invalid `rent_at`은 source metadata month에 귀속한 뒤 row-level quarantine한다. 이 두-pass contract는 hard gate가 뒤늦게 발견되어 부분 canonical output이 노출되는 것을 방지한다.

Row-level canonicalization은 source-facing provenance를 유지하면서 `bike_id`의 surrounding whitespace만 제거하고, station number는 ASCII decimal digits만 허용한 뒤 leading zero를 제거한다. `ST-*` internal station ID는 station-number fallback으로 사용하지 않는다. `sex_code`는 `M | F | null`이며 whitespace/case만 보정 가능한 값은 canonicalize하면서 warning을 남긴다. DQ issue는 `rule_id`, `severity`, `field`, `reason`을 가지며 `dq_warning_count`는 distinct WARNING rule 수다. Negative usage/distance와 invalid/missing rental identity는 quarantine이고, missing/invalid return 정보, missing/noncanonical sex, missing/impossible birth year, zero/extreme distance, `usage_min > 1440` 등은 warning이다. `distance_metric_eligible`은 `0 < distance_m <= 100000`일 때만 true다. 이 단계는 `row_quarantined`까지만 계산하며 group-level DQ와 최종 `trusted_fact_eligible`은 후속 단계 책임이다.

`source classify-month`는 하나의 explicit current `source_version_id`와 rental `source_month`를 입력으로 받아 그 month에 기여하는 모든 member row를 scratch DuckDB에 staging한다. `logical_trip_key`는 canonical `(bike_id, rent_at)`의 canonical JSON SHA-256(`ltk1_...`)이며 key가 없는 row는 group analysis에서 제외된다. 동일 key가 반복될 때 source `row_content_sha256`가 모두 같으면 exact duplicate group, 하나라도 다르면 logical conflict group이며 어느 경우에도 winner를 선택하지 않고 group 전체를 quarantine한다. Exact duplicate는 `logical_trip_conflict=false`를 유지한다. 최종 `trusted_fact_eligible`은 QUARANTINE issue가 하나도 없는 row만 true이고 WARNING-only row는 trusted 후보로 남는다. 외부 row callback은 month staging, group classification, `clean = trusted + quarantined` reconciliation이 끝난 뒤에만 실행되므로 group hard failure가 partial downstream row output을 만들지 않는다. Scratch DuckDB는 durable Clean storage가 아니며 Clean Parquet publish는 후속 단계 책임이다.

`source publish-clean-month`는 D-004에 따라 explicit `source_month + source_version_id + transformation_version + configuration_fingerprint`에서 `cv1_...` Clean version identity를 계산한다. Build는 `clean/staging/`에서 수행하고 `clean.parquet`, quarantined row의 issue-grain `quarantine.parquet`, deterministic `metadata.json`을 만든다. 두 Parquet의 fixed schema, source/month identity, row accounting, trusted/quarantine reconciliation, `source_row_id` uniqueness, nested DQ issue ↔ quarantine exact explosion, file SHA-256, metadata counts를 모두 재조회 검증한 뒤에만 `clean/versions/source_month=.../clean_version_id=.../`로 immutable promotion한다. 마지막 mutable state는 `clean/current/source_month=YYYY-MM.json` 하나이며 month별 OS advisory lock 아래 pointer를 재확인한 후 temp file + `os.replace`로 atomic 전환한다. Lock 파일은 persistent coordination file이지만 ownership은 file descriptor에 묶여 process crash 시 자동 해제되므로 stale file 자체가 retry를 막지 않는다. 같은 input retry는 같은 version/hash를 요구하고, 기존 immutable version이 다르면 overwrite/repair하지 않고 hard fail한다. 과거 source revision rebuild는 version을 남길 수 있지만 더 최신 current를 rollback하지 않는다. Addendum 001에 따라 Slice 9의 `station_history_match`는 nullable boolean이고 enrichment 전에는 항상 `null`이다.

`source publish-station-snapshot`은 D-005/Addendum 002에 따라 공식 station CSV/XLSX의 5-row header contract를 검증하고 source가 제공한 observation precision(`DAY | MONTH`)을 보존한다. Station number는 decimal canonical identity로 정규화하고, same-snapshot 동일 station의 null/non-null identity 보완과 LCD/QR complementary merge를 수행한다. 서로 다른 non-null identity/capacity 값이 충돌하면 group 전체를 quarantine하고 winner를 선택하지 않는다. Exact duplicate source row는 observation 하나로 collapse하면서 WARNING과 duplicate count를 남긴다. `설치시기`는 provenance로 보존하지만 state equality에는 사용하지 않는다. XLSX numeric serialization이 `37.550628660000001`처럼 불필요한 binary-float tail을 만들 수 있어 위도/경도는 canonical observation에서 소수 8자리 Decimal로 정규화한다. Durable output은 snapshot version별 `observations.parquet`, issue-grain `quarantine.parquet`, `metadata.json`이며 contributor source-row partition, nested DQ semantics, conflict/duplicate counts, source/version identity, file hash를 immutable Raw 재해석 결과와 read-back 검증한 뒤에만 period별 current pointer를 OS advisory lock 아래 atomic publish한다. 이 단계는 cross-snapshot interval을 아직 만들지 않는다.

`warehouse publish-station-history`는 모든 station period의 **current** immutable snapshot을 먼저 검증·캡처하고, `src/seoul_bike_pipeline/sql/dim_station_history.sql`의 plain SQL을 DuckDB에서 실행해 `dim_station_history`를 만든다. `DAY` observation은 해당 날짜, `MONTH` observation은 다음 달 1일을 `applicable_from`으로 사용하며 이 값은 실제 변경일이 아니라 conservative join boundary다. Station이 다음 snapshot에서 사라지거나 conflict quarantine되어 canonical observation이 없으면 이전 interval은 그 snapshot boundary에서 끝나고 carry-forward하지 않는다. Station이 연속 snapshot에 존재하면서 D-005 state attribute가 모두 같을 때만 하나의 interval로 압축하고, state change 또는 observation gap은 새 interval을 시작한다. `applicable_until`은 exclusive이고 마지막 snapshot까지 관찰된 state만 `null` open interval을 가진다.

Warehouse publication은 `warehouse/versions/history_version_id=wh1_.../warehouse.duckdb`와 deterministic `metadata.json`을 immutable하게 보존하고 `warehouse/current.json`만 atomic replace한다. Version identity에는 ordered station snapshot lineage, transformation/config identity, SQL SHA-256이 포함된다. Validator는 DuckDB schema/count/interval overlap뿐 아니라 metadata가 가리키는 각 immutable station version을 Raw까지 재검증하고, Python으로 interval을 독립 재계산해 SQL 결과와 exact 비교한다. Build 중 station current pointer가 바뀌거나 pre-current bundle이 훼손되면 기존 warehouse current는 유지한다.

SQL SHA-256은 UTF-8 text의 line ending을 LF로 canonicalize한 뒤 계산하므로 Windows CRLF checkout과 WSL2 LF checkout이 같은 transformation identity를 만든다. PRD §25의 v1 mutation/runtime contract는 **WSL2 Linux 단일 runtime**이다. Windows-native pytest/CLI compatibility는 유지하지만, 같은 NTFS worktree에 대해 Windows native process와 WSL process가 동시에 publish하는 cross-OS writer concurrency는 v1 보장 범위가 아니다. Current-set/history advisory lock의 correctness guarantee는 같은 supported runtime/OS lock namespace 안의 concurrent writers에 적용한다.

`warehouse evaluate-station-enrichment`는 current Clean month와 current `dim_station_history`의 immutable version을 먼저 hard-validate한 뒤 `rent_station_no` + `DATE(rent_at)` 기준 half-open temporal join을 수행한다. Canonical contract의 singular `station_history_match`는 v1에서 **필수 rental/origin station**에 대해 평가한다. Return station 정보는 source-facing canonical value로 그대로 유지하며 return-side temporal enrichment는 이 slice의 범위가 아니다. Applicable interval이 있으면 `true`, 첫 observation 이전·observation gap·never-seen station이면 `false`이고 `station_history_unmatched` WARNING을 추가한다. Future interval은 match에 사용하지 않으며 SQL 결과에 future applicability가 나타나면 hard fail한다. 기존 WARNING과 새 station WARNING을 합쳐 distinct WARNING rule 기준 `dq_warning_count`를 다시 계산하지만, unmatched WARNING은 `trusted_fact_eligible`을 false로 바꾸지 않는다. Quarantined Clean row는 trusted enrichment 후보에서 제외된다.

이 enrichment 단계는 **평가/재사용 가능한 business layer**이고 durable Clean bytes를 수정하지 않는다. CLI는 summary만 출력하고 tests/future `fact_trip_trusted` publisher는 같은 function을 immutable version path에 대해 호출할 수 있다. Current-pointer wrapper는 평가 전후 Clean/history pointer가 동일한지 다시 확인해 중간 lineage 변경을 silent하게 받아들이지 않는다.

`warehouse publish-fact-trip-month`는 current Clean month와 current station-history version을 캡처해 Slice 12 enrichment business layer로 trusted row만 평가한 뒤, `src/seoul_bike_pipeline/sql/fact_trip_trusted.sql`을 실행해 persistent DuckDB의 `fact_trip_trusted` BASE TABLE로 materialize한다. Durable layout은 `warehouse/fact_trip_trusted/versions/source_month=YYYY-MM/fact_version_id=fv1_.../{warehouse.duckdb,metadata.json}`이고 mutable state는 `warehouse/fact_trip_trusted/current/source_month=YYYY-MM.json`뿐이다. `fv1_` identity는 source month, exact Clean/history version, explicit transformation/configuration identity, LF-canonicalized enrichment SQL SHA-256과 fact SQL SHA-256을 모두 포함한다.

Fact validator는 DB fingerprint뿐 아니라 fixed schema, BASE TABLE, unique/exact trusted Clean `source_row_id` set, unchanged canonical projection, no QUARANTINE issue, distinct WARNING count, exact station-unmatched WARNING, independently recomputed half-open station applicability와 row count reconciliation을 다시 확인한다. Final current publish 직전에는 Clean month lock + warehouse history-current lock + fact month lock을 같은 supported runtime에서 잡고 세 pointer/immutable bundle을 다시 검증한다. Same-input/current winner는 검증 후 `REUSED_CURRENT`로 수렴하고, different concurrent current change, pre-current failure, input/fact tamper는 기존 fact current를 유지한다. 이 locking guarantee도 PRD §25의 WSL2 Linux single-runtime mutation boundary를 따른다.

`warehouse publish-date-dimension`은 frozen production scope에 맞춰 **2020-01-01~2026-06-30의 2,373 calendar dates만** `src/seoul_bike_pipeline/sql/dim_date.sql`로 생성한다. 2015 compatibility data는 production mart 대상이 아니므로 date dimension에 확장하지 않는다. `day_of_week`는 ISO 8601 `1=Monday ... 7=Sunday`, `weekend_flag`는 6/7(Saturday/Sunday)만 true다. Holiday 같은 외부 calendar 속성은 PRD 최소 요구가 아니므로 v1에 추가하지 않는다.

Date dimension durable layout은 `warehouse/dim_date/versions/date_version_id=dv1_.../{warehouse.duckdb,metadata.json}` + singleton `warehouse/dim_date/current.json`이다. `dv1_`은 fixed coverage/weekday convention, explicit transformation/configuration identity, LF-canonicalized exact SQL SHA-256을 묶는다. Validator는 BASE TABLE/fixed schema, 정확히 2,373개의 unique DATE grain, start/end boundary, leap day를 포함한 모든 calendar attribute의 독립 재계산, WAL 부재, DB fingerprint, metadata/path binding을 확인한다. Singleton advisory lock 아래 captured current를 다시 비교하므로 different concurrent publisher가 먼저 이긴 경우 rollback하지 않고 hard fail하며, same-ID winner는 검증 후 `REUSED_CURRENT`로 수렴한다.

`warehouse publish-station-day-mart`는 `service_month` 하나를 durable rebuild unit으로 사용하고 논리 grain을 PRD 그대로 `service_date × station_no`로 유지한다. `rental_count`, `completed_trip_count`, `unique_bike_count`, `avg_usage_min`, `median_usage_min`, `valid_distance_trip_count`, `missing_return_count`, `station_history_match_count`는 모두 **rental/origin cohort**(`DATE(rent_at) + rent_station_no`) 기준이다. Usage 평균/중앙값은 그 rental cohort의 non-null `usage_min`만 사용하고, distance count는 `distance_metric_eligible=true`만 센다. `missing_return_count`는 trusted fact에서 `is_completed_trip=false`인 rental event 수이며, fact contract상 return-before-rent 같은 quarantine row는 이미 제외되어 있다.

`return_count`만 실제 return event side를 사용해 `DATE(return_at) + return_station_no`에 귀속한다. 단, 여기서 “return”은 **production-scope rental facts(2020-01 이후 대여) 중 `is_completed_trip=true`인 event**를 뜻한다. 따라서 2020-01 이전 대여가 2020-01에 반납된 실세계 event는 v1 production mart에 포함하지 않으며, 2026-06 이후 날짜도 production `dim_date` 범위 밖이라 station-day partition을 만들지 않는다. Rental month와 return month가 달라도 return은 실제 return service month에 합쳐지므로, 예를 들어 1월 대여의 2월 반납과 2월 대여가 같은 2월 station-day row에서 reconcile된다.

Station-day durable layout은 `warehouse/mart_station_day/versions/service_month=YYYY-MM/mart_version_id=msd1_.../{warehouse.duckdb,metadata.json}`과 month별 current pointer다. Target month M의 return completeness를 보장하려고 current `fact_trip_trusted`를 **2020-01부터 M까지 모두** 캡처·검증한다. Metadata의 `fact_scan_lineage`는 이 completeness scan의 immutable fact vector를 보존하고, deterministic `msd1_` identity의 `fact_lineage`는 **M의 rental fact + 실제로 M에 completed return을 기여한 이전 fact**만 포함한다. 따라서 과거 fact가 수정됐지만 M의 실제 결과가 바뀌지 않은 경우 current full-scan으로 exact 재검증한 뒤 기존 `msd1_`을 `REUSED_CURRENT`로 유지할 수 있다.

Historical fact revision 영향 범위는 새 version만 보지 않는다. `warehouse plan-station-day-revision`은 explicit old/new immutable fact version을 둘 다 검증한 뒤 `source_month ∪ old completed-return service months ∪ new completed-return service months`를 production scope로 clip해 반환한다. 예를 들어 1월 fact의 return이 2월에서 3월로 이동하면 1월 rental cohort뿐 아니라 **기존 2월 return 제거와 새 3월 return 추가**가 모두 필요하므로 1·2·3월을 rebuild 대상으로 잡는다. 반대로 영향 없는 4월 mart는 material lineage와 exact output이 같으면 pointer를 유지한다. 6월 return이 7월로 이동해도 7월은 v1 production scope 밖이므로 6월 rental month만 영향 범위에 남는다.

Final publish는 current `dim_date`, 모든 fact month 2020-01..M, target mart month를 same-runtime advisory lock으로 묶고 pointer를 다시 확인한다. Validator는 source mart SQL을 신뢰하지 않고 immutable fact scan에서 station-day rows와 **material contributor lineage 자체**를 독립 재계산해 exact grain/count/metric/lineage를 대조하며, DB fingerprint/WAL/path binding도 hard gate로 확인한다.

`warehouse publish-od-day-mart`는 `service_month`의 current trusted fact에서 **completed trip만** 사용해 `DATE(rent_at) × rent_station_no × return_station_no` grain을 materialize한다. 이 grain이 PRD의 `service_date × origin_station_no × destination_station_no`다. `trip_count`와 `unique_bike_count`는 completed OD cohort 전체를 사용하고, usage 평균/중앙값은 non-null `usage_min`만 사용한다. Distance count/평균/중앙값은 `distance_metric_eligible=true`인 row만 사용하므로 zero/extreme/parse-failed distance는 OD trip 자체를 제거하지 않고 distance metric에서만 제외된다.

OD-day는 return service date를 별도 attribution하지 않으므로 target month M의 output은 **current `fact_trip_trusted` M + current `dim_date`**만 material lineage로 가진다. Durable layout은 `warehouse/mart_od_day/versions/service_month=YYYY-MM/mart_version_id=mod1_.../{warehouse.duckdb,metadata.json}`과 month별 current pointer다. `warehouse plan-od-day-revision`은 old/new immutable fact를 모두 검증한 뒤 revised rental source month 하나만 affected month로 반환한다. Final publish는 date current + target fact current + target OD mart lock 아래 pointer와 immutable metadata/fingerprint를 다시 검증하며, same-input retry는 `REUSED_CURRENT`, different concurrent winner는 rollback refusal, transform/pre-current/input tamper failure는 이전 current 보존으로 닫는다. Validator는 immutable fact에서 OD grain과 모든 metric을 Python으로 독립 재계산해 exact 비교한다.

`warehouse publish-source-month-quality`는 PRD §18.3의 `source_month × source_version` grain을 current Clean month와 그 Clean을 정확히 lineage로 가진 current trusted fact에서 materialize한다. Current Clean/fact의 `source_version_id`와 `clean_version_id`가 다르면 pipeline 중간 상태이므로 publish하지 않는다. Source record는 manifest에서 다시 찾고 immutable Raw artifact의 size/SHA-256까지 재검증한다.

D-006에 따라 `source_row_count = clean_row_count`, `trusted_fact_count = fact row count`, `quarantine_count`는 issue 수가 아니라 quarantined source-row 수다. `incomplete_trip_count`는 Clean 전체의 `is_completed_trip=false`, `station_unmatched_count`는 trusted fact의 `station_history_match=false`다. `distance_zero_rate`, `invalid_birth_year_rate`, `sex_missing_rate` denominator는 모두 `clean_row_count`이며 numerator는 각각 `distance_m_zero`, `birth_year_invalid | birth_year_impossible`(missing 제외), `sex_missing` rule이 있는 source row다. `extreme_distance_count`는 `distance_m_extreme_positive` source-row 수이고, D-003의 `exact_duplicate_group_count`도 additive metric으로 보존한다. 한 month의 schema가 하나면 그 schema name을, 여러 known schema면 정렬된 `mixed[...]` 문자열을 저장하며 `pipeline_version`은 volatile run ID가 아니라 explicit quality-mart transformation version이다.

Quality mart durable layout은 `warehouse/mart_source_month_quality/versions/source_month=YYYY-MM/mart_version_id=msq1_.../{warehouse.duckdb,metadata.json}` + month별 current pointer다. `msq1_` identity는 exact source/Clean/fact lineage, transformation/configuration identity, LF-canonical SQL SHA를 포함한다. Final publish는 Clean month → fact month → quality month lock 순서로 current pointer와 immutable input/Raw checksum을 재검증한다. Validator는 source SQL 결과를 신뢰하지 않고 immutable Clean/fact를 읽어 모든 count/rate/group metric과 lineage를 Python으로 독립 재계산하며 DB fingerprint/WAL/path binding도 확인한다. Historical source revision 영향 범위는 revised rental source month 하나다.

`pipeline backfill-range`는 PRD §11~§13/§22의 range workflow를 DAG와 분리된 Python business layer로 실행한다. Planner는 요청 범위를 production scope `2020-01..2026-06`으로 제한하고, 각 month의 target source와 실행 전 current Clean/fact lineage를 캡처한다. Standalone monthly source는 append-only manifest의 최신 version을 사용하고, yearly ZIP revision은 parent ZIP hash가 달라도 해당 month member의 content multiset이 같으면 이전 parent lineage를 유지해 `UNCHANGED`로 skip한다. 실제 changed/new member만 latest parent를 target으로 사용하며 removed/missing month는 기존 published state를 조용히 삭제하지 않고 failure로 보고한다.

Range 실행은 하나의 global transaction이 아니다. 각 Clean/fact/mart publisher의 기존 atomic current contract를 그대로 사용해 성공 month는 보존하고 실패 month만 `FAILED`로 남기며 run status는 `SUCCEEDED | PARTIAL_FAILURE | FAILED`다. Clean→fact→quality/OD를 source-month phase에서 먼저 처리한 뒤, 모든 current fact가 준비된 상태에서 station-day를 두 번째 phase로 publish한다. Historical fact revision은 실행 전에 캡처한 old fact와 새 fact를 함께 비교해 old/new completed-return service month를 모두 rebuild 대상으로 계산하므로 return month가 이동한 경우 기존 return 제거도 놓치지 않는다. 실패 retry는 새 plan을 추측해 만들지 않고 최초 `BackfillPlan`의 failed-month entry를 재사용해야 한다; 향후 Airflow control plane은 이 captured plan/run state의 보존과 task retry visibility를 담당한다.

Production rental Raw `2020-01..2026-06` 전체 artifact는 repository에 커밋하지 않는다. Slice 18의 workflow contract는 78-month range boundary/planning과 synthetic multi-month end-to-end fault injection으로 검증했고, 이후 공식 source artifact를 local Raw/manifest에 준비해 실제 78개월 Clean production acceptance까지 완료했다. 측정 결과와 downstream evidence 범위는 `docs/production-validation.md`가 정본이다.

Airflow reference runtime은 PRD §24~§25대로 WSL2/Linux이며 `scripts/install-airflow-wsl.sh`가 Python 3.12 전용 `.venv-airflow`에 `apache-airflow==3.3.2`를 공식 release constraints로 설치한다. Base project venv와 분리하는 이유는 Airflow가 library가 아니라 dependency set 자체를 관리하는 application이기 때문이다. DAG authoring은 Airflow 3 stable `airflow.sdk` interface만 사용한다.

`dags/seoul_bike_monthly_backfill.py` 하나가 scheduled single-month와 manual range를 모두 담당한다. Schedule은 Airflow 3의 bare-cron default에 의존하지 않고 registered `BoundedCronDataIntervalTimetable("0 0 1 * *")`을 Seoul timezone으로 사용한다. 따라서 source month M의 data interval `[M-01 00:00, next-month-01 00:00)`이 끝난 뒤 run이 생성되고 `data_interval_start`의 M을 처리한다. Timetable 자체가 latest interval start `2026-06-01`을 제한해 production horizon `2020-01..2026-06` 이후 자동 run을 만들지 않는다. 이 horizon을 DAG/task `end_date`로 표현하지 않기 때문에 그 이후 시점에 수동으로 historical range를 trigger해도 task가 정상 생성된다. Custom timetable은 `plugins/seoul_bike_timetable_plugin.py`로 Airflow serialization에 등록한다. Manual run은 `start_month` + `end_month` Param을 함께 넘겨 arbitrary range를 지정한다. `SEOUL_BIKE_TRANSFORMATION_VERSION` 또는 run Param으로 explicit transformation version을 반드시 제공해야 하며, Windows absolute path는 DAG/business code에 hard-code하지 않는다.

Task graph는 `resolve_request → capture_plan → execute_initial → retry_failed_months → require_success`다. `capture_plan`은 exact source/Clean/fact baseline을 versioned JSON-compatible payload로 XCom에 보존한다. XCom에는 dataframe/raw data가 아니라 작은 control-plane state만 저장한다. Downstream task는 request/plan content fingerprint와 result의 month/source/Clean/fact/mart identifiers를 immutable durable metadata에 다시 bind해 다른 request의 state 혼입이나 lineage 불일치를 성공으로 받아들이지 않는다. Initial attempt가 `PARTIAL_FAILURE | FAILED`이면 `retry_failed_months`가 새 plan을 만들지 않고 original captured plan + previous result에서 **failed source month만** 추려 business layer를 재호출한다. Task process 자체의 transient exception은 Airflow task retry가 같은 XCom input으로 재실행한다. Retry 후에도 business failure가 남으면 `require_success`가 failed month를 포함한 exception을 발생시켜 DagRun을 `failed`로 노출한다.

Manual range trigger 예시는 다음과 같다.

```bash
# In this trigger terminal, repeat the Airflow exports from the setup block.
.venv-airflow/bin/airflow dags trigger seoul_bike_monthly_backfill \
  -c '{
    "start_month": "2020-01",
    "end_month": "2020-06",
    "transformation_version": "git-<commit>",
    "configuration": {}
  }'
```

Local Airflow acceptance는 clean WSL2 metadata DB에서 `airflow dags reserialize` 후 DAG 1개 등록 / import error 0을 확인하고, `dag.test()`로 same-input range 성공과 unrecovered missing-month DagRun 실패를 모두 검증한다. Runtime metadata/XCom/log는 Airflow control-plane state이며 repository의 deterministic source/Clean/warehouse identity에는 포함하지 않는다.

`source register` 출력은 `<STATUS> <source_version_id>` 한 줄이고, STATUS는 다음 중 하나다.

- `REGISTERED_NEW`: 이 `(dataset_id, logical_period)`의 첫 source version.
- `KNOWN_UNCHANGED`: 같은 source_version_id가 이미 등록됨. 기존 record를 그대로 반환하고 manifest를 다시 쓰지 않는다.
- `REGISTERED_REVISION`: 같은 `(dataset_id, logical_period)`의 새 source version(bytes 또는 published filename이 바뀜). 새 version을 append하고 이전 version은 보존한다.

`source inventory-zip`은 `INVENTORIED_NEW <source_version_id> members=<N>` 또는 `INVENTORY_UNCHANGED <source_version_id> members=<N>`을 출력한다.

## Manifest

manifest는 JSON object 하나이며 `schema_version`(정수 `1`)과 `records` array로만 구성된다.

```json
{
  "schema_version": 1,
  "records": [
    {
      "dataset_id": "rental_history",
      "published_filename": "서울특별시 공공자전거 대여이력 정보_202001.csv",
      "logical_period": "2020-01",
      "source_modified_at": "2021-03-05T00:00:00Z",
      "observed_at": "2026-09-19T09:00:00Z",
      "size_bytes": 687,
      "sha256": "<artifact SHA-256>",
      "source_url": "<official locator>",
      "license": "<이용허락>",
      "source_version_id": "sv1_<identity hash>"
    }
  ]
}
```

record 필드는 PRD §6의 필드로 고정되어 있고 `observed_at`(최초 등록 시각, UTC)과 `source_version_id`를 제외하면 모두 등록 시 입력한 metadata다. `source_version_id`는 `dataset_id`, `logical_period`, `published_filename`, `sha256`의 canonical JSON에 대한 SHA-256이라서, artifact를 어디에서 받았는지와 언제 관측했는지는 identity에 영향을 주지 않는다. record는 수정하거나 삭제하지 않는다.

manifest 파일이 없으면 빈 manifest로 보고 새로 쓰며, 있으면 malformed JSON, 정수 `1`이 아닌 `schema_version`, 필드가 맞지 않는 record는 복구하지 않고 hard fail한다. 갱신은 같은 directory의 temp file에 쓰고 `fsync` 후 `os.replace`로 교체한다.

원천 데이터는 이 저장소에 커밋하지 않는다 (PRD §32). `tests/fixtures/source/`의 synthetic fixture만 예외다.
