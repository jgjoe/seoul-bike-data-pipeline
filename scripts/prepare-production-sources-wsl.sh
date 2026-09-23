#!/usr/bin/env bash
set -euo pipefail

# Official Seoul file catalog observed on 2026-09-21/22.  Expected byte sizes
# are deliberate revision guards: if Seoul replaces an artifact, stop and
# review/register the new immutable source version instead of silently drifting.

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

OUTPUT_ROOT="${1:-$ROOT/data/production-sources}"
TRANSFORMATION_VERSION="${2:-}"
if [[ -z "$TRANSFORMATION_VERSION" ]]; then
  echo "usage: $0 [output-root] <transformation-version>" >&2
  echo "example: $0 data/production-sources git-<short-commit>" >&2
  exit 2
fi

PYTHON_BIN="${PYTHON_BIN:-$ROOT/.venv/bin/python}"
DOWNLOAD_URL="https://datafile.seoul.go.kr/bigfile/iot/inf/nio_download.do?useCache=false"
RENTAL_SOURCE_URL="https://data.seoul.go.kr/dataList/OA-15182/A/1/datasetView.do"
STATION_SOURCE_URL="https://data.seoul.go.kr/dataList/OA-13252/F/1/datasetView.do"
RENTAL_LICENSE="공공누리 1유형 : 출처표시 (상업적 이용 및 변경 가능)"
STATION_LICENSE="공공누리 제1유형"

SOURCE_MANIFEST="$OUTPUT_ROOT/manifest.json"
SOURCE_RAW="$OUTPUT_ROOT/raw"
RENTAL_DOWNLOADS="$OUTPUT_ROOT/downloads/rental"
STATION_DOWNLOADS="$OUTPUT_ROOT/downloads/station"
STATION_PUBLISHED="$OUTPUT_ROOT/station"

mkdir -p "$RENTAL_DOWNLOADS" "$STATION_DOWNLOADS" \
  "$SOURCE_RAW" "$STATION_PUBLISHED"

download_checked() {
  local inf_id="$1"
  local seq="$2"
  local destination="$3"
  local expected_size="$4"
  local expected_sha256="$5"
  local tmp="${destination}.part"

  rm -f "$tmp"
  curl --fail --location --silent --show-error \
    --retry 3 --retry-delay 2 --retry-all-errors \
    -X POST \
    --data-urlencode "infId=$inf_id" \
    --data-urlencode "seq=$seq" \
    --data-urlencode "infSeq=1" \
    "$DOWNLOAD_URL" \
    -o "$tmp"

  local actual_size
  actual_size="$(stat -c '%s' "$tmp")"
  if [[ "$actual_size" != "$expected_size" ]]; then
    echo "SIZE_MISMATCH infId=$inf_id seq=$seq expected=$expected_size actual=$actual_size" >&2
    exit 3
  fi
  local actual_sha256
  actual_sha256="$(sha256sum "$tmp" | awk '{print $1}')"
  if [[ "$actual_sha256" != "$expected_sha256" ]]; then
    echo "SHA256_MISMATCH infId=$inf_id seq=$seq expected=$expected_sha256 actual=$actual_sha256" >&2
    exit 3
  fi
  mv "$tmp" "$destination"
}

register_source() {
  local artifact="$1"
  local manifest="$2"
  local raw_root="$3"
  local dataset_id="$4"
  local logical_period="$5"
  local published_filename="$6"
  local source_url="$7"
  local license="$8"

  "$PYTHON_BIN" -m seoul_bike_pipeline source register "$artifact" \
    --manifest "$manifest" \
    --raw-root "$raw_root" \
    --dataset-id "$dataset_id" \
    --logical-period "$logical_period" \
    --published-filename "$published_filename" \
    --source-url "$source_url" \
    --license "$license"
}

rental_items=(
  "91|2020|2020.zip|1022004940|a8f3a38f2e73661c2bc0d2d01becd08712cc5226c433da9d5ff8348f9e1b3a91|서울특별시 공공자전거 대여이력 정보_2020.zip"
  "92|2021|2021.zip|1474363948|fdfafd4f83fd1b81046cac88686d60b6d3384e91f401119461547f089b1020b5|서울특별시 공공자전거 대여이력 정보_2021.zip"
  "93|2022|2022.zip|1905706824|fa2dc9e71d78821b78e5d15b5f51c8c8ea68c38a14f6c933235b98e4c6da18a8|서울특별시 공공자전거 대여이력 정보_2022.zip"
  "94|2023|2023.zip|2104571539|84c747156590551eae6d7fb2c852708cba3e70a29949456fa503b7e6cfd90e98|서울특별시 공공자전거 대여이력 정보_2023.zip"
  "95|2024|2024.zip|2068679477|f1385bffd913aa0b37a4914fd835621eea4f55d14665ffdd6fb2d76a0faae8cb|서울특별시 공공자전거 대여이력 정보_2024.zip"
  "96|2025|2025.zip|1778640879|05f01e9f27f33303b60171b06c858c8a166a664f89915aef6ab283bb1e63752e|서울특별시 공공자전거 대여이력 정보_2025.zip"
  "144|2026-01|2601.csv|287053827|ebbf0707e895bd2d147abe5e33eeb24664aa7b233e8768c678959932eafaeb73|서울특별시 공공자전거 대여이력 정보_2601.csv"
  "145|2026-02|2602.csv|314300382|de6015c0eca7658bc38e9efc9078429f98c90c46ecdfbc127070ba9f7f87fc38|서울특별시 공공자전거 대여이력 정보_2602.csv"
  "147|2026-03|2603.csv|512604494|761ee08f8b998114a7b3f60b8822190b09bd7a4dd75c312b4bb0c695abc3de6d|서울특별시 공공자전거 대여이력 정보_2603.csv"
  "148|2026-04|2604.csv|690141935|342ad77e7c3632a42911a1118e1eeda22e0f1eccb2683ff19d2864d82daa8feb|서울특별시 공공자전거 대여이력 정보_2604.csv"
  "149|2026-05|2605.csv|706488210|725caf7a7690db421d411fa431b4f9fbae5d29a5ad0ee5de81a3ddc3edae8c76|서울특별시 공공자전거 대여이력 정보_2605.csv"
  "150|2026-06|2606.csv|733525370|0743a5b520f3ad1a0bc9f3182c5eb081af8e39189e16ed636361e8d2bc5ea9dd|서울특별시 공공자전거 대여이력 정보_2606.csv"
)

for entry in "${rental_items[@]}"; do
  IFS='|' read -r seq logical_period local_name expected_size expected_sha256 published_filename <<<"$entry"
  artifact="$RENTAL_DOWNLOADS/$local_name"
  echo "RENTAL_DOWNLOAD logical_period=$logical_period seq=$seq"
  download_checked "OA-15182" "$seq" "$artifact" "$expected_size" "$expected_sha256"
  registration="$(register_source \
    "$artifact" "$SOURCE_MANIFEST" "$SOURCE_RAW" rental_history \
    "$logical_period" "$published_filename" "$RENTAL_SOURCE_URL" "$RENTAL_LICENSE")"
  echo "$registration"
  source_version_id="$(awk '{print $2}' <<<"$registration")"
  if [[ "$local_name" == *.zip ]]; then
    "$PYTHON_BIN" -m seoul_bike_pipeline source inventory-zip \
      --manifest "$SOURCE_MANIFEST" \
      --raw-root "$SOURCE_RAW" \
      --source-version-id "$source_version_id"
  fi
  rm -f "$artifact"
done

station_items=(
  "11|2021-01-31|2021-01-31.csv|206960|4adc277768ba0c4ddbf6eaa80784a48e3ed43a6e2d199b1e27e22416043314b5|공공자전거 대여소 정보(21.01.31 기준).csv"
  "14|2021-06|2021-06.xlsx|609907|bde14d7860aa6e265385aca1c5a9740e48f71cfb613b324eb6a04158d66ae34b|공공자전거 대여소 정보(21.06월 기준).xlsx"
  "15|2021-12|2021-12.xlsx|254790|9c7d44fcd4409945c7c876fbf4ad6470f60119cc1f4d837b2481fb453e76246a|공공자전거 대여소 정보(21.12월 기준).xlsx"
  "16|2022-06|2022-06.csv|265387|ecdba7d936e407125fd15a32f803e096f0494ca53b240366f68ff15d277f8962|3. 공공자전거 대여소 정보(22.06월 기준).csv"
  "17|2022-12|2022-12.xlsx|537310|2c740028e6e2e4057062ab301ec9b4ae9619da86f7cdcf88bbc8440cef961cfa|공공자전거 대여소 정보(22.12월 기준).xlsx"
  "18|2023-06|2023-06.xlsx|282741|b12f8a1b22346c0566665887c30ab57a5c72557915b72bb92c62aee09ce94845|공공자전거 대여소 정보(23.06월 기준).xlsx"
  "19|2023-12|2023-12.xlsx|295730|a74120e4b0b8284478d81ba36a9b68d184eb56a6a2f44dbcec0cdca8ef26643a|공공자전거 대여소 정보(23.12월 기준).xlsx"
  "20|2024-06|2024-06.xlsx|296324|d75e3ce0051f243f9fa55c2bee06dac5a868f45b2f80a41e1feb7ab21f41a1b0|공공자전거 대여소 정보(24.6월 기준).xlsx"
  "21|2024-12|2024-12.xlsx|290390|d6d50a6ba16bd26654e6dfe05b0c31316c94d7cde37f4d0501a40c6c58e1a650|공공자전거 대여소 정보(24.12월 기준).xlsx"
  "22|2025-06|2025-06.xlsx|292248|c1114647ac92eae48fedf9a3f425637b406b7d1562db185151fe3a012ade4523|공공자전거 대여소 정보(25.6월 기준).xlsx"
  "23|2025-12|2025-12.xlsx|291273|dd6877f4781cefcd6513a6610b9f3a217017f99097ac00c47077d90f0912c010|공공자전거 대여소 정보(25.12월 기준).xlsx"
  "24|2026-06|2026-06.xlsx|290529|a2dcee10a341ac9d6beaf14f83c1e46b38bc96ffc8289340508b01cac0d464f1|공공자전거 대여소 정보(26.6월 기준).xlsx"
)

for entry in "${station_items[@]}"; do
  IFS='|' read -r seq logical_period local_name expected_size expected_sha256 published_filename <<<"$entry"
  artifact="$STATION_DOWNLOADS/$local_name"
  echo "STATION_DOWNLOAD logical_period=$logical_period seq=$seq"
  download_checked "OA-13252" "$seq" "$artifact" "$expected_size" "$expected_sha256"
  registration="$(register_source \
    "$artifact" "$SOURCE_MANIFEST" "$SOURCE_RAW" station_snapshot \
    "$logical_period" "$published_filename" "$STATION_SOURCE_URL" "$STATION_LICENSE")"
  echo "$registration"
  source_version_id="$(awk '{print $2}' <<<"$registration")"
  "$PYTHON_BIN" -m seoul_bike_pipeline source publish-station-snapshot \
    --manifest "$SOURCE_MANIFEST" \
    --raw-root "$SOURCE_RAW" \
    --station-root "$STATION_PUBLISHED" \
    --source-version-id "$source_version_id" \
    --transformation-version "$TRANSFORMATION_VERSION" \
    --configuration-json '{}'
  rm -f "$artifact"
done

echo "PRODUCTION_SOURCES_READY root=$OUTPUT_ROOT"
echo "manifest=$SOURCE_MANIFEST"
echo "raw_root=$SOURCE_RAW"
echo "station_root=$STATION_PUBLISHED"
