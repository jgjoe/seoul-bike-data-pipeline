# PRD v1.0 FINAL

## 서울 따릉이 대여이력 Batch Data Reliability / DW Pipeline

**상태:** FINAL / Frozen
**기준일:** 2026-09-19
**프로젝트 주제:** 확정
**구현 상태:** 미착수
**Repository:** 미생성

이 문서는 repository bootstrap 및 구현의 기준선이다.

새로운 요구사항이나 데이터 실사 결과가 나오기 전까지 구현은 이 PRD의 데이터 계약, 품질 기준, 실패·복구 계약을 만족하는 방향으로 진행한다.

---

# 1. Executive Summary

서울특별시 공공자전거 따릉이 대여이력은 장기간 대규모 batch 분석에 적합하지만, 원천 파일을 그대로 누적하는 것만으로는 신뢰 가능한 분석 데이터셋을 만들 수 없다.

Dataset due diligence에서 다음을 직접 확인했다.

* 2020년 한 해 약 2,370만 행, 비압축 약 4.4GB.
* 과거 연도 source가 수년 뒤 다시 수정될 수 있음.
* yearly ZIP과 monthly CSV가 혼재하는 packaging 변화.
* 2015·2020의 17-column schema와 2026-01의 16-column schema 차이.
* stable trip ID 부재.
* `(자전거번호, 대여일시)` logical key의 실제 충돌.
* 동일 logical trip 후보에 서로 다른 이용거리 또는 반납시각 존재.
* 현재 station snapshot만으로 과거 fact를 완전히 설명할 수 없음.
* 결측, categorical inconsistency, 비현실적 거리·생년 등 실제 Data Quality 문제 존재.

본 프로젝트는 이 문제를 대상으로 다음을 구축한다.

**immutable source provenance → monthly incremental/backfill → schema normalization → DQ/quarantine → observed station history → trusted fact → SQL marts → failure/recovery**

핵심 질문은 다음과 같다.

> 이미 처리한 과거 데이터가 수정되었을 때 이를 어떻게 탐지하고, 어떤 partition만 다시 계산하며, 불완전하거나 모순된 데이터가 trusted analytics에 조용히 섞이지 않도록 어떻게 통제할 것인가?

---

# 2. Problem Definition

따릉이 원천은 개별 rental event 분석에는 유용하지만 장기 DW source로 사용할 때 다음 문제가 존재한다.

| 문제                | 확인된 특성                                        |
| ----------------- | --------------------------------------------- |
| Source revision   | 과거 연도 파일도 이후 다시 수정됨                           |
| Schema drift      | 17 column → 16 column                         |
| Packaging drift   | yearly ZIP / monthly CSV                      |
| Stable PK 부재      | source trip ID 없음                             |
| Logical conflict  | 동일 bike + rent timestamp에 서로 다른 row 존재        |
| Reference history | station source가 event history가 아니라 snapshot   |
| Quality drift     | 결측률·categorical representation·측정 특성이 시점별로 다름 |
| Incomplete event  | return 정보 누락 존재                               |
| Extreme values    | 비현실적인 distance/birth year 존재                  |

따라서 단순 CSV → Parquet 변환은 프로젝트의 완료 조건이 아니다.

---

# 3. Goals

## 3.1 Data Engineering Goals

G1. 2020-01~2026-06 rental source를 month 단위 batch pipeline으로 처리한다.

G2. 모든 처리 결과를 정확한 source version까지 역추적할 수 있게 한다.

G3. 새로운 month와 historical source revision을 구분해 처리한다.

G4. 과거 month가 수정되면 영향받은 partition만 재처리한다.

G5. 알려진 source schema들을 하나의 canonical schema로 정규화한다.

G6. Data Quality 문제를 hard fail / quarantine / warning으로 구분한다.

G7. station snapshot으로부터 과장 없는 observed-history dimension을 구성한다.

G8. trusted fact와 SQL mart를 reproducible하게 생성한다.

G9. retry/backfill/reprocessing 과정에서 duplicate 또는 partial publish를 만들지 않는다.

G10. 개인 로컬 개발환경에서 전체 workflow를 재현할 수 있어야 한다.

## 3.2 Portfolio Goals

본 프로젝트는 기존 포트폴리오와 별개로 다음을 증명한다.

* 수천만 행 batch 처리
* file/partition incremental processing
* historical backfill
* source revision detection
* data contract/schema drift 대응
* fact/dimension/mart modeling
* SQL transformation
* DQ severity 및 quarantine
* idempotency
* atomic publish
* failure recovery
* provenance/lineage
* orchestration

---

# 4. Scope

## 4.1 Core Production Scope

**2020-01 ~ 2026-06**

대여일시가 이 기간에 포함되는 rental month를 production data scope로 한다.

## 4.2 Compatibility Scope

**2015 데이터는 production mart 대상이 아니다.**

2015 source는 다음 용도로만 사용한다.

* legacy source compatibility
* schema regression
* parser regression
* old categorical/null representation regression

## 4.3 Scope Rationale

2020 한 해만으로도 수천만 행 규모가 확인됐으며 2020~2026 범위에서 다음을 모두 검증할 수 있다.

* 대용량 batch
* historical source revision
* 17→16 column schema drift
* station snapshot history
* 장기간 DQ drift
* categorical 변화
* backfill/reprocessing

2015~2019 전체 mart 확장은 v1 목적 대비 추가 증거가 제한적이므로 포함하지 않는다.

---

# 5. Input Data

## 5.1 Rental History

서울특별시 공공자전거 따릉이 대여이력 공식 파일을 canonical ingestion source로 한다.

확인된 schema family:

### `rental_v17`

2015 및 2020 파일에서 확인한 17-column schema.

### `rental_v16`

2026-01 파일에서 확인한 16-column schema.

`rental_v16`에는 `자전거구분` column이 없다.

알려진 두 schema는 정상 처리한다.

알 수 없는 새로운 header/schema는 자동 추론해 publish하지 않고 hard fail한다.

## 5.2 Station Reference

서울특별시 공공자전거 대여소 정보 snapshot.

2021 이후 이용 가능한 snapshot을 station observation source로 사용한다.

Station source는 특정 날짜의 상태 snapshot이지 실제 변경 이벤트 로그가 아니다.

## 5.3 Open API

Open API는 v1 production ingestion source가 아니다.

파일 source 검증 또는 diagnostic에 사용할 수 있지만 file pipeline과 동일 source contract로 간주하지 않는다.

---

# 6. Source Provenance Contract

Raw source artifact는 immutable version으로 등록한다.

동일 filename이 다시 다운로드되더라도 hash가 달라지면 새로운 source version이다.

최소 metadata:

| Field                | Meaning                    |
| -------------------- | -------------------------- |
| `dataset_id`         | source dataset             |
| `published_filename` | 공식 filename                |
| `logical_period`     | year/month                 |
| `source_modified_at` | 공식 source 수정일              |
| `observed_at`        | 수집 시점                      |
| `size_bytes`         | source artifact bytes      |
| `sha256`             | artifact SHA-256           |
| `source_url`         | official locator           |
| `license`            | 이용허락                       |
| `source_version_id`  | immutable version identity |

이전 source version은 삭제하거나 덮어쓰지 않는다.

---

# 7. ZIP Member Identity

Year ZIP 내부 monthly CSV는 별도의 processing unit이다.

Member identity는 최소 다음으로 결정한다.

```text
parent_source_version_id
+ member_name
+ member_size
+ member_sha256
```

연도 ZIP hash가 변경됐더라도 특정 monthly member hash가 이전과 동일하면 해당 month는 downstream reprocessing 대상이 아니다.

---

# 8. Raw Row Identity

Source에 stable trip primary key가 없으므로 physical source row identity와 logical trip identity를 분리한다.

## Physical Identity

```text
raw_row_id =
hash(source_version_id, member_name, source_row_number)
```

추가로 source row content hash를 보존한다.

`raw_row_id`는 source version 내 물리적 행 identity이며 business trip ID가 아니다.

---

# 9. Logical Trip Conflict Contract

Conflict detection key:

```text
logical_trip_key = (bike_id, rent_at)
```

이 key는 **primary key가 아니다.**

실제 due diligence에서 동일 key에 서로 다른 distance 또는 return timestamp를 가진 row가 존재했다.

따라서 logical conflict 발생 시:

1. 모든 source row를 Raw에 유지한다.
2. 모든 정상 파싱 row를 Clean canonical dataset에 유지한다.
3. 해당 group 전체에 `logical_trip_conflict = true`를 부여한다.
4. 어느 row도 임의로 canonical winner로 선택하지 않는다.
5. group 전체를 trusted fact eligibility에서 제외한다.
6. quarantine side output에서 원인과 provenance를 조회할 수 있게 한다.

Source 근거 없이 `마지막 row`, `distance가 0이 아닌 row` 등을 선택하지 않는다.

---

# 10. Partition Contract

Production processing unit은 **rental month**다.

Canonical physical partition:

```text
rent_year=YYYY/
rent_month=MM/
```

근거:

* 실사한 monthly source에서 file month와 rent month가 일치.
* return timestamp는 다음 month로 넘어갈 수 있음.
* yearly ZIP 내부 member도 month 단위.
* incremental/backfill/revision의 최소 실용 단위와 일치.

`return_month`는 physical partition 기준으로 사용하지 않는다.

---

# 11. Incremental Processing Contract

본 프로젝트에서 incremental processing은 row CDC가 아니다.

Source에 stable update timestamp, sequence 또는 transaction ID가 없으므로:

**file/version-based incremental processing**

을 사용한다.

Pipeline은 각 실행에서 source manifest를 기준으로 다음을 구분한다.

* new month
* known unchanged version
* historical revised version

Unchanged source partition은 다시 처리하지 않는다.

---

# 12. Historical Revision Contract

동일 logical month의 새로운 source version이 발견되면 기존 version을 덮어쓰지 않는다.

개념적으로:

```text
2020-05
├── source_v1
└── source_v2  ← newer observed version
```

새 version의 downstream 결과가 validation과 DQ hard gate를 모두 통과하기 전까지 기존 current partition은 유지한다.

변경 detection 순서:

```text
official source metadata
→ filename / size / modified date
→ artifact SHA-256
→ ZIP member SHA-256
→ impacted month
```

변경된 month만 downstream invalidation/rebuild 대상이다.

---

# 13. Backfill / Reprocessing Contract

Pipeline business logic은 month 또는 month range를 입력받아 실행 가능해야 한다.

예:

```text
month = 2020-05
range = 2020-05 .. 2020-07
```

Airflow가 없어도 동일 business logic을 CLI/test에서 호출할 수 있어야 한다.

## 13.1 Idempotency

동일:

```text
source version
+ transformation code version
+ configuration
```

으로 재실행하면 business output이 동일해야 한다.

다음은 deterministic해야 한다.

* clean canonical row count
* trusted eligibility count
* quarantine count
* DQ metric
* warehouse reconciliation
* mart content/fingerprint

Run ID나 실행 timestamp는 달라질 수 있다.

## 13.2 Atomic Publish

Partition은 직접 current output으로 작성하지 않는다.

```text
build staging
→ validate
→ run hard DQ gates
→ publish current
```

실패하면 이전 current partition을 유지한다.

## 13.3 Multi-month Backfill

여러 month 중 일부가 실패하면 성공 month까지 rollback하지 않는다.

실패 run은 `PARTIAL_FAILURE`가 될 수 있으며 실패 month만 다시 실행 가능해야 한다.

---

# 14. Station History Model

Station source는 snapshot이므로 actual change date를 제공한다고 간주하지 않는다.

## 14.1 Station Observation

Canonical station source grain:

```text
(snapshot_date, source_row)
```

각 snapshot의 원래 관찰값을 보존한다.

## 14.2 Same-Snapshot Consolidation

동일 snapshot에서 동일 `station_no`가 여러 row에 존재할 수 있다.

Identity attribute가 일치하고 LCD/QR capacity처럼 상호 보완적인 속성만 다른 경우 하나의 station observation으로 consolidate할 수 있다.

Name/address/coordinate 등 identity attribute가 충돌하면 자동 winner를 선택하지 않고 DQ issue로 처리한다.

## 14.3 Observed History Dimension

DW dimension:

`dim_station_history`

Grain:

```text
station_no × observed state interval
```

최소 attribute:

* station_no
* observed_from
* observed_until
* station_name
* district
* address
* latitude
* longitude
* lcd_capacity
* qr_capacity
* operation_mode

`observed_from`은 실제 변경일이 아니다.

**그 상태가 source snapshot에서 처음 관찰된 날짜**를 의미한다.

## 14.4 Fact Enrichment

Trip 발생일보다 같거나 이전인 snapshot 중 가장 최근 applicable station observation을 사용한다.

Applicable prior observation이 없으면 future snapshot을 이용해 과거 상태를 추정하지 않는다.

Fact에는 source station number/name을 그대로 유지하고:

```text
station_history_match = false
```

warning을 남긴다.

따라서 첫 station snapshot보다 이른 2020 trip의 낮은 enrichment coverage는 expected condition이며 pipeline failure가 아니다.

---

# 15. Data Layer Responsibilities

## 15.1 Raw

Raw의 책임:

* original source artifact 보존
* source manifest
* source version
* artifact/member checksum
* source metadata
* license/provenance

Raw 값은 수정하지 않는다.

## 15.2 Clean Canonical

Clean은 **정상적으로 파싱할 수 있는 모든 source row**를 canonical schema로 보존한다.

책임:

* encoding interpretation
* known schema version mapping
* canonical column name/type
* null normalization
* categorical normalization
* station number normalization
* provenance 부착
* logical conflict detection
* DQ status
* trusted eligibility 결정

Quarantined business row도 Clean에서 사라지지 않는다.

## 15.3 Quarantine

Quarantine은 독립 truth layer가 아니라 **Clean canonical의 filtered side output**이다.

최소:

* source_row_id
* source_version_id
* source_month
* DQ rule ID
* severity
* reason
* logical conflict group

를 제공한다.

## 15.4 Trusted Warehouse / Mart

Warehouse는 trusted analytics용 output이다.

Quarantine 상태의 row는 trusted fact에 포함하지 않는다.

Warning-only row는 rental event 자체가 유효하다면 trusted fact에 포함될 수 있으며 해당 metric eligibility를 별도 column으로 표현한다.

---

# 16. Canonical Trip Contract

최소 canonical fields:

| Field                        | Type                 |
| ---------------------------- | -------------------- |
| `source_row_id`              | string               |
| `source_version_id`          | string               |
| `source_month`               | month                |
| `schema_version`             | string               |
| `bike_id`                    | string               |
| `rent_at`                    | timestamp            |
| `rent_station_no`            | string               |
| `rent_station_name`          | string               |
| `rent_dock_no`               | nullable integer     |
| `return_at`                  | nullable timestamp   |
| `return_station_no`          | nullable string      |
| `return_station_name`        | nullable string      |
| `return_dock_no`             | nullable integer     |
| `usage_min`                  | nullable integer     |
| `distance_m`                 | nullable numeric     |
| `birth_year`                 | nullable smallint    |
| `sex_raw`                    | nullable string      |
| `sex_code`                   | nullable categorical |
| `user_type`                  | nullable string      |
| `rent_station_internal_id`   | nullable string      |
| `return_station_internal_id` | nullable string      |
| `bike_type`                  | nullable string      |
| `logical_trip_key`           | string               |
| `logical_trip_conflict`      | boolean              |
| `is_completed_trip`          | boolean              |
| `distance_metric_eligible`   | boolean              |
| `station_history_match`      | boolean              |
| `dq_warning_count`           | integer              |
| `trusted_fact_eligible`      | boolean              |

Leading-zero station number의 raw representation은 provenance에 보존하되 canonical station key는 normalization한다.

---

# 17. Warehouse Model

## 17.1 `fact_trip_trusted`

Grain:

**one trusted rental event**

`trusted_fact_eligible = true`인 Clean row만 materialize한다.

Return 정보가 없어도 rental event 자체가 유효할 수 있으므로 incomplete trip 역시 fact에 존재할 수 있다.

```text
is_completed_trip = false
```

로 표현한다.

## 17.2 `dim_date`

Grain:

**calendar date**

최소:

* date
* year
* month
* day
* day_of_week
* weekend_flag

## 17.3 `dim_station_history`

Grain:

**station_no × observed state interval**

실제 station 변경일이 아닌 snapshot-observed history다.

---

# 18. Mart Requirements

## 18.1 `mart_station_day`

Grain:

```text
service_date × station_no
```

최소 metrics:

* rental_count
* return_count
* completed_trip_count
* unique_bike_count
* avg_usage_min
* median_usage_min
* valid_distance_trip_count
* missing_return_count
* station_history_match_count

## 18.2 `mart_od_day`

Grain:

```text
service_date
× origin_station_no
× destination_station_no
```

Completed OD-eligible trip만 포함한다.

최소 metrics:

* trip_count
* unique_bike_count
* avg_usage_min
* median_usage_min
* valid_distance_trip_count
* valid_distance_avg
* valid_distance_median

## 18.3 `mart_source_month_quality`

Grain:

```text
source_month × source_version
```

최소 metrics:

* source_row_count
* clean_row_count
* trusted_fact_count
* quarantine_count
* logical_conflict_group_count
* incomplete_trip_count
* station_unmatched_count
* distance_zero_rate
* extreme_distance_count
* invalid_birth_year_rate
* sex_missing_rate
* schema_version
* artifact hash
* pipeline run/version

---

# 19. Data Quality Severity

## HARD FAIL

Partition 전체를 안전하게 해석하거나 publish할 수 없는 문제.

최소 hard fail:

* source artifact read 실패
* corrupted ZIP/member
* source checksum inconsistency
* unknown/unsupported schema
* required column 자체가 없음
* row boundary를 신뢰할 수 없는 structural parse failure
* source data row 0건
* row accounting 불일치
* trusted warehouse grain uniqueness violation
* publish validation 미완료

Hard fail partition은 current output을 변경하지 않는다.

## QUARANTINE

Source row는 해석할 수 있지만 trusted rental fact로 사용할 수 없는 문제.

최소:

* invalid/missing bike_id
* invalid/missing rent_at
* invalid/missing rental station identity
* `return_at < rent_at`
* negative `usage_min`
* negative `distance_m`
* conflicting logical_trip group
* exact duplicate group

Row/group는 Raw와 Clean에 보존한다.

## WARNING

Rental event 자체는 trusted fact에서 사용할 수 있지만 특정 attribute 또는 metric의 신뢰성이 낮은 경우.

최소:

* missing return information
* station history unmatched
* missing sex
* lower-case/noncanonical sex representation
* missing birth year
* impossible birth year
* zero distance
* extreme positive distance
* `usage_min > 1440`
* station snapshot metadata inconsistency

---

# 20. Distance Quality Contract

Positive distance magnitude만으로 rental event를 quarantine하지 않는다.

v1 rule:

```text
distance < 0       → QUARANTINE
distance = 0       → WARNING, distance metric ineligible
distance > 100 km  → WARNING, extreme-distance flag
```

100km는 **trip validity 경계가 아니라 v1 monitoring threshold**다.

Extreme positive distance row는 rental count에서 제거하지 않는다.

Distance 기반 aggregate를 계산할 때 `distance_metric_eligible`을 사용한다.

---

# 21. Row Accounting and Reconciliation

성공적으로 publish 가능한 month에서는 structural parsing loss가 없어야 한다.

따라서:

```text
source data row count
= clean canonical row count
```

그리고 Clean 내부 classification은:

```text
clean canonical rows
= trusted-fact-eligible rows
+ quarantined rows
```

Warning-only row는 trusted-fact-eligible 집합에 포함될 수 있다.

Warehouse reconciliation:

```text
fact_trip_trusted row count
= trusted-fact-eligible clean row count
```

Station mart:

```text
SUM(mart_station_day.rental_count)
= fact_trip_trusted row count
```

OD mart:

```text
SUM(mart_od_day.trip_count)
= trusted completed OD-eligible trip count
```

Silent row loss는 허용하지 않는다.

---

# 22. Failure / Recovery Contract

## FR-01 Retry Idempotency

동일 source version의 재시도는 duplicate business output을 만들지 않는다.

## FR-02 Clean Build Failure

Clean staging 중 실패하면 이전 published Clean partition을 유지한다.

## FR-03 Publish Failure

Staging 완료 후 publish 전 실패해도 incomplete version을 current로 노출하지 않는다.

## FR-04 Mart Failure

Clean publish 후 mart generation 실패 시:

* Clean current는 유지
* 이전 Mart current는 유지
* Mart만 재실행 가능

해야 한다.

## FR-05 Historical Revision

이미 처리한 month에 새로운 source version이 들어오면:

* new source version 등록
* old source version 보존
* affected month만 rebuild
* validation 후 current 전환

해야 한다.

## FR-06 Partial Backfill Failure

여러 month 중 일부가 실패하면:

* 성공 month는 publish 가능
* 실패 month는 기존 state 유지
* run은 partial failure 기록
* 실패 month만 retry 가능

해야 한다.

## FR-07 Hard DQ Failure

Hard fail partition은 downstream trusted warehouse/mart를 publish하지 않는다.

## FR-08 Unaffected Partition Stability

특정 month revision/backfill이 unrelated month의 deterministic data output을 변경해서는 안 된다.

---

# 23. Failure Injection Acceptance Tests

최종 구현에서는 실제 실패를 주입해 다음을 검증한다.

| Scenario                          | Expected result            |
| --------------------------------- | -------------------------- |
| Clean transform 중 실패              | 기존 current 유지              |
| Publish 직전 실패                     | staging만 존재, current 불변    |
| Mart build 실패                     | Clean 유지, previous mart 유지 |
| Source checksum/version 변경        | revision 감지                |
| Historical month replacement      | 해당 month만 rebuild          |
| Multi-month backfill 중 1 month 실패 | 다른 성공 month 보존             |
| 동일 run retry                      | duplicate 없음               |

테스트 성공 조건은 단순 exception 발생이 아니라 **실패 후 데이터 상태가 계약과 일치하는 것**이다.

---

# 24. Airflow Orchestration Contract

Apache Airflow를 orchestration layer로 사용한다.

Airflow의 책임:

* task dependency
* scheduling
* retry
* month/range backfill orchestration
* run status
* failure visibility

Airflow DAG에는 business transformation SQL이나 핵심 DQ logic을 직접 구현하지 않는다.

DAG task는 독립 Python/CLI business entry point를 호출한다.

따라서 동일 processing logic은:

```text
CLI
test
Airflow task
```

에서 공통 사용된다.

Airflow 공식 best practice 역시 task를 retry 가능한 idempotent 작업으로 설계하고 불완전한 결과가 노출되지 않도록 하는 접근을 권장한다. ([Apache Airflow][1])

---

# 25. Windows / Linux Execution Contract

개발 host는 Windows일 수 있지만 **Airflow 자체를 Windows native runtime으로 실행하지 않는다.**

Airflow 공식 문서는 Windows에서는 WSL2 또는 Linux container를 사용하도록 안내하며, 지원되는 production 환경은 Linux 계열이다. ([Apache Airflow][2])

v1 reference local contract:

```text
Windows host
└── WSL2 Linux runtime
    ├── Python
    ├── DuckDB
    ├── SQL transformations
    └── Apache Airflow
```

따라서:

* business logic은 Linux path를 전제로 작성한다.
* Windows absolute path를 application contract에 포함하지 않는다.
* 원천 데이터 location은 configuration으로 주입한다.
* CLI/test도 WSL2 Linux runtime에서 재현 가능해야 한다.

Linux container는 허용 가능한 대체 실행 방식이지만 v1의 필수 dependency는 아니다.

---

# 26. Technology Responsibility Boundary

| Technology              | Responsibility                                                                                                          | Not responsible for                    |
| ----------------------- | ----------------------------------------------------------------------------------------------------------------------- | -------------------------------------- |
| **Python**              | source discovery/register, manifest, checksum, ZIP handling, schema dispatch, control logic, DQ orchestration glue, CLI | mart business SQL                      |
| **DuckDB**              | local analytical execution engine, SQL execution, canonical transform support, warehouse persistence/query              | workflow scheduling                    |
| **Partitioned Parquet** | Clean canonical durable columnar data, month partition storage                                                          | orchestration/metadata control         |
| **Plain SQL**           | fact/dimension/mart transformation, reconciliation query                                                                | source download/control flow           |
| **Airflow**             | dependency, schedule, retry, backfill orchestration, run visibility                                                     | business transformation implementation |

DuckDB는 disk spill을 통한 larger-than-memory execution을 지원하므로 현재 확인된 규모에서 single-node 기준선으로 적절하다. ([DuckDB][3])

Parquet는 Clean의 durable analytical representation으로 사용하며 DuckDB의 columnar scan과 predicate pushdown의 이점을 활용한다. ([DuckDB][4])

---

# 27. Storage Contract

```text
Raw
→ original ZIP / CSV / XLSX
→ immutable source manifest

Clean
→ partitioned Parquet
→ all successfully parsed canonical rows
→ DQ/trusted eligibility attached

Quarantine
→ Clean filtered side output

Warehouse
→ persistent DuckDB database

Marts
→ DuckDB tables/views generated by plain SQL
```

Iceberg/Delta 등 open table format은 v1에서 사용하지 않는다.

---

# 28. Execution Engine Decision

v1 execution engine은 **DuckDB**로 확정한다.

이유:

* 현재 검증된 규모는 single-node 처리가 가능하다.
* multi-GB workload를 처리할 수 있다.
* SQL 중심 transformation에 적합하다.
* larger-than-memory workload에서 disk spill을 지원한다. ([DuckDB][3])
* distributed system 자체가 요구사항이 아니다.

PySpark는 v1 non-goal이다.

---

# 29. SQL Transformation Decision

v1 transformation framework는 **plain SQL**이다.

SQL 파일에서 다음을 정의한다.

* trusted fact
* date dimension
* station history dimension
* station-day mart
* OD-day mart
* monthly quality mart
* reconciliation query

SQL execution은 DuckDB가 담당한다.

dbt는 v1 non-goal이다.

---

# 30. Technology Non-goals

v1에서는 다음을 사용하지 않는다.

* PySpark
* dbt
* Kafka
* Iceberg
* Delta Lake
* Kubernetes

해당 기술을 사용하지 않는 이유는 역량 부족이 아니라 **현재 요구사항을 만족시키는 데 필요하지 않기 때문**이다.

---

# 31. Other Non-goals

* streaming pipeline
* real-time bike availability
* ML demand forecasting
* recommendation
* user-facing REST API
* dashboard 자체를 핵심 deliverable로 삼기
* station actual physical change date 추론
* 모든 이상치 자동 수정
* 2015~2019 production mart 확장
* source website crawler 자체를 핵심 프로젝트로 만들기

---

# 32. Public Repository Data Policy

Git repository에는 대규모 raw source를 저장하지 않는다.

## 포함

* source URLs
* source/license attribution
* manifest schema/example
* Python pipeline code
* SQL models
* DQ rules
* synthetic fixtures
* small regression fixture
* aggregate DQ results
* architecture documentation
* runbook
* reproducibility instructions

## 제외

* 전체 raw ZIP/CSV/XLSX
* generated multi-GB Parquet
* generated DuckDB database
* local absolute paths
* credentials
* Airflow runtime metadata
* 불필요한 실제 individual trip raw sample

실제 발견한 edge case 테스트는 가능한 한 synthetic fixture로 재현한다.

Public documentation에는 서울특별시 source와 이용허락을 명시한다.

---

# 33. Constraints

C1. 개인 로컬 환경에서 재현 가능해야 한다.

C2. 유료 cloud infrastructure는 필수가 아니다.

C3. Airflow runtime은 Linux 환경이어야 한다.

C4. Windows host에서는 WSL2를 v1 reference environment로 사용한다.

C5. Raw source의 CP949 등 source encoding을 처리해야 한다.

C6. 전체 source를 RAM에 적재하는 것을 전제로 하지 않는다.

C7. Station source는 snapshot이며 exact change date를 제공하지 않는다.

C8. Stable source trip ID가 없다.

C9. Historical source artifact가 수정될 수 있다.

C10. 기술 수 증가 자체는 성공 기준이 아니다.

---

# 34. Validation Criteria

## Source / Schema

* `rental_v17` 정상 ingest
* `rental_v16` 정상 ingest
* unsupported schema hard fail
* source Korean text 정상 보존
* source month/rent month validation 통과

## Identity

* physical `raw_row_id` unique
* logical conflict detection 통과
* conflicting group 전체 trusted fact 제외

## Provenance

임의의 mart result에서 최소 다음 chain을 추적할 수 있어야 한다.

```text
mart
→ trusted fact / clean partition
→ source version
→ source artifact/member hash
→ official source
```

## Station

* same-snapshot duplicate station 처리
* future snapshot을 과거 fact에 사용하지 않음
* unmatched station을 silent fill하지 않음
* observed-history semantics 유지

## DQ

Due diligence에서 확인한 실제 문제를 검출할 수 있어야 한다.

* logical key conflict
* lower-case/noncanonical sex
* missing sex
* impossible birth year
* missing return information
* zero distance
* extreme distance
* station unmatched
* 17→16 schema drift

## Warehouse / Mart

* defined grain uniqueness
* row accounting
* fact↔mart reconciliation
* month-only rebuild
* unaffected partition stability

---

# 35. Definition of Done

프로젝트 완료는 코드 존재 여부가 아니라 다음 증거로 판단한다.

## Dataset

* 2020-01~2026-06 target source partition 전체를 처리한다.
* immutable source manifest/version/hash가 존재한다.
* 2015 compatibility regression test가 통과한다.

## Pipeline

* new month processing 가능.
* unchanged month skip 가능.
* historical revision detection 가능.
* changed month만 rebuild 가능.
* arbitrary month/range backfill 가능.
* same input retry가 idempotent함.

## Data Contract / Quality

* known schema version 처리.
* unknown schema hard fail.
* source rows와 Clean rows가 reconciliation됨.
* trusted/quarantine classification이 reconciliation됨.
* warning/hard-fail/quarantine rule이 구현됨.
* quarantine reason/provenance 조회 가능.
* 실제 due-diligence edge case가 regression test로 검증됨.

## Warehouse

* `fact_trip_trusted` 생성.
* `dim_date` 생성.
* `dim_station_history` 생성.
* `mart_station_day` 생성.
* `mart_od_day` 생성.
* `mart_source_month_quality` 생성.
* grain/reconciliation 검증 통과.

## Reliability

다음 failure injection을 모두 통과한다.

* transform failure
* pre-publish failure
* mart failure
* historical source revision
* partial backfill failure
* retry after failure

각 테스트에서 기존 published data가 계약에 맞게 보존되는 것을 증명한다.

## Orchestration

* Airflow DAG에서 month processing을 실행할 수 있다.
* month range backfill을 orchestrate할 수 있다.
* failed month만 retry 가능하다.
* DAG 외 CLI에서 동일 business logic을 실행할 수 있다.
* 테스트에서도 동일 business layer를 호출한다.

## Environment

* Windows host + WSL2 Linux reference environment에서 재현 가능하다.
* Airflow를 Windows native로 실행하지 않는다.
* Windows absolute path가 business logic에 hard-code되지 않는다.

## Reproducibility

Fresh environment에서 문서만 보고 다음 흐름을 재현할 수 있다.

```text
dependency setup
→ source configuration
→ source register
→ pipeline execution
→ DQ result
→ warehouse/mart query
→ backfill/retry
```

## Portfolio Explainability

사용자가 자신의 말로 설명할 수 있어야 한다.

* 왜 rental month partition인가
* 왜 `(bike_id, rent_at)`을 PK로 쓰지 않았는가
* historical revision을 어떻게 탐지하는가
* 어떤 month만 다시 계산하는가
* Raw/Clean/Quarantine/Trusted Mart의 책임 차이는 무엇인가
* station history가 왜 actual SCD change history가 아닌가
* hard fail / quarantine / warning의 차이는 무엇인가
* 왜 DuckDB를 선택했는가
* 왜 Spark를 사용하지 않았는가
* 왜 Airflow는 필요했는가
* 왜 dbt는 사용하지 않았는가
* 실패 시 partial publish를 어떻게 방지하는가

---

# 36. Final Architecture Baseline

```text
Official Seoul Files
        │
        ▼
Immutable Raw Artifacts
+ Source Version Manifest
        │
        ▼
Python Source / Contract Layer
        │
        ▼
DuckDB Canonical Processing
        │
        ▼
Partitioned Parquet Clean
 ├── DQ status
 └── Quarantine side output
        │
        ▼
DuckDB Warehouse
 ├── fact_trip_trusted
 ├── dim_date
 └── dim_station_history
        │
        ▼
Plain SQL Marts
 ├── mart_station_day
 ├── mart_od_day
 └── mart_source_month_quality
```

Control plane:

```text
Apache Airflow
```

Execution:

```text
Windows Host
└── WSL2 Linux
```

Responsibility rule:

```text
Airflow = orchestration
Python = control/source/DQ glue
DuckDB = analytical execution
Parquet = durable Clean storage
SQL = warehouse/mart transformation
```

---

# 37. Freeze Review

## Requirement ↔ Definition of Done

**PASS**

모든 핵심 requirement에 대응하는 검증 또는 DoD가 존재한다.

* incremental → new/unchanged/revised partition tests
* backfill → arbitrary month/range + partial-failure recovery
* DQ → severity rules + regression fixture
* provenance → source hash lineage
* station history → snapshot-observed dimension
* mart → grain + reconciliation
* failure/recovery → explicit fault-injection acceptance tests
* Airflow → DAG/CLI 동일 business logic contract

## Layer Consistency

**PASS**

Clean과 Quarantine의 책임을 명확히 했다.

Quarantine은 source row를 Clean에서 제거하는 별도 truth layer가 아니라 Clean canonical row의 상태 및 side output이다.

따라서 row accounting과 provenance가 모순되지 않는다.

## Runtime Contract

**PASS**

Airflow가 Linux/POSIX environment를 요구한다는 조건을 v1 environment contract에 반영했다. Windows에서는 WSL2를 reference runtime으로 사용한다. ([Apache Airflow][2])

## Technology Boundary

**PASS**

DuckDB, Parquet, SQL, Python, Airflow의 책임이 겹치지 않도록 정의됐다.

## Remaining Architecture/Product Decisions

**NONE**

구현을 막는 미확정 data/product/architecture decision은 없다.

다음은 repository bootstrap 또는 implementation detail이며 PRD decision이 아니다.

* exact package version pin
* directory/file naming
* Python package/module naming
* Airflow DAG ID/task ID naming
* DuckDB memory/thread configuration
* local filesystem paths
* logging format의 구체적 serialization
* test framework configuration

이 값들은 PRD의 계약을 변경하지 않는 범위에서 구현 단계에서 결정한다.

---

# 38. Freeze Declaration

**PRD v1.0 FINAL은 구현 가능한 상태로 freeze한다.**

다음 단계부터는 이 문서를 기준으로:

```text
repository bootstrap
→ implementation plan
→ implementation
→ verification
```

순서로 진행한다.

PRD를 변경해야 하는 새로운 사실이 발견되면 구현 편의를 위해 조용히 바꾸지 않고 명시적으로 PRD revision으로 다룬다.

[1]: https://airflow.apache.org/docs/apache-airflow/stable/best-practices.html?utm_source=chatgpt.com "Best Practices — Airflow 3.3.2 Documentation"
[2]: https://airflow.apache.org/docs/apache-airflow/stable/start.html?utm_source=chatgpt.com "Quick Start — Airflow 3.3.2 Documentation"
[3]: https://duckdb.org/docs/current/guides/performance/environment?utm_source=chatgpt.com "Environment – DuckDB"
[4]: https://duckdb.org/docs/current/data/parquet/tips?utm_source=chatgpt.com "Parquet Tips – DuckDB"
