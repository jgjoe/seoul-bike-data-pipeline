# 사용자 작업: Seoul Bike 클라우드 실행 준비

프로젝트 **`seoul-bike-dw`**, 리전 **`us-central1`** 기준입니다.
무료 체험 크레딧 및 **₩1,000 / 50·90·100% 예산 알림은 이미 설정**되었습니다.
유료 계정 전환은 하지 않습니다. 예산 알림은 자동 차단 장치가 아닙니다.
프로모션 크레딧을 예산 계산에서 제외해야 체험 크레딧 뒤에 사용 비용이 가려지지 않습니다.

아래 명령은 **사용자가 WSL Ubuntu 터미널에서** 실행합니다. `gcloud`가
있는 Google Cloud CLI가 필요합니다. `bq` CLI는 사용하지 않습니다. 미설치라면 공식 설치 안내
<https://cloud.google.com/sdk/docs/install>를 먼저 따릅니다.
계정 로그인·동의는 직접 합니다. 비밀번호·인증 코드·토큰·키 파일을 채팅에 보내지 않습니다.

## 1. 사용자 로그인, 프로젝트 선택, API 활성화

```bash
export PROJECT_ID="seoul-bike-dw"
export REGION="us-central1"
export BUCKET="seoul-bike-dw-representative"
export DATASET="bike_cloud"
export SA="bike-cloud-loader@${PROJECT_ID}.iam.gserviceaccount.com"

gcloud auth login --no-launch-browser
gcloud config set project "$PROJECT_ID"
export USER_EMAIL="$(gcloud config get-value account)"

gcloud services enable \
  bigquery.googleapis.com \
  storage.googleapis.com \
  iam.googleapis.com \
  iamcredentials.googleapis.com \
  serviceusage.googleapis.com \
  --project="$PROJECT_ID"
```

버킷 이름은 전 세계에서 유일합니다. 이미 사용 중이라는 오류가 나면 임의 재시도하지 말고
`BUCKET` 이름만 고유하게 바꿉니다. 최종 버킷 이름을 유지보수자에게 알려 주세요.

## 2. 전용 리소스 생성

**한 번만 실행합니다.** 기존 리소스를 삭제하거나 덮어쓰지 않습니다.
복구 가능한 원본은 로컬에 있으므로 새 데모 버킷은 soft delete를 끕니다.
이는 기존 객체 삭제 명령이 아니며, 향후 리소스 삭제는 별도 승인입니다.

```bash
gcloud storage buckets create "gs://${BUCKET}" \
  --project="$PROJECT_ID" \
  --location="$REGION" \
  --default-storage-class=STANDARD \
  --uniform-bucket-level-access \
  --public-access-prevention \
  --soft-delete-duration=0

gcloud iam service-accounts create bike-cloud-loader \
  --project="$PROJECT_ID" \
  --display-name="Seoul Bike bounded cloud publisher"
```

데이터 세트는 콘솔에서 생성합니다:

1. <https://console.cloud.google.com/bigquery?project=seoul-bike-dw> 접속.
2. 탐색기에서 `seoul-bike-dw`의 메뉴 → **데이터 세트 만들기**.
3. 데이터 세트 ID: **`bike_cloud`**.
4. 위치 유형: **리전**, 위치: **`us-central1`**.
5. 기본 테이블/파티션 만료는 설정하지 않고 **데이터 세트 만들기**.
6. 이미 생성했다면 재생성하지 말고 세부정보에서 위치와 만료 설정만 확인합니다.

BigQuery의 데이터 세트 기본 파티션 만료는 설정하지 않습니다. 2020년 데이터에 짧은
파티션 만료를 적용하면 적재 직후 사라질 수 있습니다. 테스트가 끝난 뒤 정리는
별도 승인 후 진행합니다. 자동 만료를 쓴다면 파티션 날짜가 아닌 테이블 생성 시각 기준입니다.

## 3. 실행 계정에 최소 권한 부여

버킷 전용 커스텀 역할은 **조회·새 객체 생성만** 허용합니다. 삭제·기존 객체 교체·
버킷 관리·IAM 변경은 허용하지 않습니다.

```bash
gcloud iam roles create bikeCloudPublisher \
  --project="$PROJECT_ID" \
  --title="Bike immutable object publisher" \
  --stage=GA \
  --permissions=storage.buckets.get,storage.objects.create,storage.objects.get,storage.objects.list

gcloud storage buckets add-iam-policy-binding "gs://${BUCKET}" \
  --member="serviceAccount:${SA}" \
  --role="projects/${PROJECT_ID}/roles/bikeCloudPublisher"

gcloud projects add-iam-policy-binding "$PROJECT_ID" \
  --member="serviceAccount:${SA}" \
  --role=roles/bigquery.jobUser

gcloud projects add-iam-policy-binding "$PROJECT_ID" \
  --member="serviceAccount:${SA}" \
  --role=roles/serviceusage.serviceUsageConsumer
```

**데이터 세트에만** Data Editor를 부여합니다:

1. <https://console.cloud.google.com/bigquery?project=seoul-bike-dw> 접속.
2. 탐색기에서 `seoul-bike-dw` → `bike_cloud` 데이터 세트 선택.
3. 공유/권한 → 주 구성원 추가.
4. 주 구성원: `bike-cloud-loader@seoul-bike-dw.iam.gserviceaccount.com`
5. 역할: **BigQuery Data Editor (`roles/bigquery.dataEditor`)** → 저장.

프로젝트 전체에 Data Editor/Admin을 부여하지 않습니다.
`bq add-iam-policy-binding`은 데이터 세트를 지원하지 않으므로 이 단계는 콘솔을 사용합니다.
기존 데이터 세트 ACL 전체를 교체하지 않습니다.

## 4. 사용자에게 이 서비스 계정의 impersonation만 허용

```bash
gcloud iam service-accounts add-iam-policy-binding "$SA" \
  --project="$PROJECT_ID" \
  --member="user:${USER_EMAIL}" \
  --role=roles/iam.serviceAccountTokenCreator
```

Token Creator는 **해당 서비스 계정 리소스에만** 부여합니다.
서비스 계정에는 Owner/Editor/Storage Admin/Billing 권한을 주지 않습니다.
사용자의 초기 프로젝트 관리자 권한은 리소스 설정용이고 파이프라인 실행 권한과 다릅니다.

## 5. WSL에서 키 없는 ADC 로그인

기존 `GOOGLE_APPLICATION_CREDENTIALS` 경로가 있으면 이 터미널에서 해제합니다.
서비스 계정 키 생성·다운로드는 하지 않습니다.

```bash
unset GOOGLE_APPLICATION_CREDENTIALS
gcloud auth application-default login --no-launch-browser \
  --impersonate-service-account="$SA"
```

Impersonated ADC에는 `set-quota-project`를 실행하지 않습니다.
Python Storage/BigQuery 클라이언트가 `project="seoul-bike-dw"`를 명시합니다.

ADC는 WSL 사용자 홈의 gcloud 설정에 저장되며 Git 밖에 둡니다.
Windows에서만 ADC 로그인하면 WSL Airflow에는 적용되지 않습니다.
로컬 사용자 ADC이므로 로그인 사용자 계정이 비활성화되면 재인증이 필요합니다.

## 6. 비용 제한 확인

콘솔 → BigQuery API → 할당량에서 프로젝트의 **Query usage per day**를
가능한 범위에서 **10 GiB/일**로 낮춥니다. 승인된 검증에 부족하면 먼저 검토합니다.
코드는 쿼리별 dry run, `maximum_bytes_billed` 기본 1 GiB와 필수 파티션 필터를 사용합니다.
GCS는 기존 객체를 포함한 live bytes와 업로드 합계 기본 4 GB 한도를 확인합니다.
스캔 한도는 저장 비용이나 모든 서비스 비용을 차단하는 전역 예산이 아닙니다.

기본 무료 제공량(계정 전체 사용량과 합산):
- 적격 US 리전 Standard GCS 5 GB-month, Class A 5,000회 / Class B 50,000회.
- BigQuery 저장 10 GiB, 쿼리 처리 1 TiB/월.

한도를 넘으면 체험 크레딧이 소비될 수 있습니다. 세 달 업로드를 보장하지 않고
실측 파일·저장 크기를 먼저 확인하여 1~3개월 안에서 선택합니다.

## 7. 사용자 검증 명령 — 토큰은 출력하지 않음

저장소 체크아웃에 설치된 Python을 사용합니다. 아래는 **사용자가 실행하는 실제
클라우드 조회**입니다. 데이터를 업로드하거나 삭제하지 않습니다. SELECT 1은
작은 쿼리 작업을 생성하며 데이터 테이블을 읽지 않습니다.
환경 변수는 1단계와 같은 WSL 터미널에 있어야 합니다.

```bash
cd <repository checkout>
.venv-airflow/bin/python - <<'PY'
import os
import google.auth
from google.cloud import bigquery, storage

project = "seoul-bike-dw"
region = "us-central1"
credentials, _ = google.auth.default()
assert getattr(credentials, "service_account_email", None) == (
    "bike-cloud-loader@seoul-bike-dw.iam.gserviceaccount.com"
), "ADC가 전용 서비스 계정을 impersonate하는지 확인하세요."
bucket = storage.Client(project=project, credentials=credentials).get_bucket(os.environ["BUCKET"])
assert bucket.location.lower() == region
assert bucket.iam_configuration.uniform_bucket_level_access_enabled
assert bucket.iam_configuration.public_access_prevention == "enforced"
bq = bigquery.Client(project=project, credentials=credentials, location=region)
dataset = bq.get_dataset(project + ".bike_cloud")
assert dataset.location.lower() == region
assert dataset.default_partition_expiration_ms is None
rows = list(bq.query(
    "SELECT 1 AS ok",
    job_config=bigquery.QueryJobConfig(maximum_bytes_billed=1024 * 1024),
    location=region,
).result())
assert rows[0]["ok"] == 1
print("SETUP_OK project=seoul-bike-dw region=us-central1 bucket=" + bucket.name)
PY
```

이 명령은 기본 인증·읽기·jobUser만 확인합니다. 객체 생성과 데이터 세트 쓰기 권한은
유지보수자가 준비 완료를 확인한 후 실제 적재 검증에서 확인합니다.
오류가 나면 키나 토큰 대신 **오류 메시지만** 전달합니다.

완료 후 유지보수자에게 **최종 버킷 이름 + SETUP_OK 결과**를 전달하세요.
그때까지 실제 적재 검증을 시작하지 않습니다.

## 실제 사용량 및 정리

적재 후 수집할 항목: 파일 hash/version, 월별 row/집계 비교,
재실행 skip 및 job ID, 수정 월만 reload, 실패·복구, bytes processed/billed,
GCS 객체/잔존 저장, BigQuery logical/physical/time-travel bytes.
사용자는 Billing Reports에서 프로젝트 필터로 비용·크레딧·최종 청구액을 각각 확인합니다.
처음에는 24~48시간 이상 보고 지연이 있을 수 있습니다. 청구 ₩0과 사용 비용 ₩0은 다릅니다.
TABLE_STORAGE 조회에 필요한 프로젝트 메타데이터 권한은 실행 계정에 넓혀 주지 않고,
사용자 관리자 콘솔에서 physical/time-travel 저장 증거를 확보합니다.

정리는 증거 보존 후 **정확한 전용 리소스 목록으로 별도 승인**을 받아 수행합니다.
예약 작업 중단 → 전용 테이블/객체/리소스 확인 → 승인된 대상만 삭제 →
impersonation/ADC 회수 → 잔존 저장 및 지연 청구 확인 순서입니다.
로컬 원본과 검증 결과는 삭제하지 않습니다.
