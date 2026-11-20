#!/usr/bin/env bash
# P3-12 scripted evidence: update-pattern semantics on the live stack.
#
# Raw executions land in a dedicated bucket (ODP_P3_12_RAW_BUCKET, default
# odp-p3-12-raw) through the shipped ingestion code, completion markers
# included. Bronze and Silver then run for one dataset at a time with the
# shipped SQL builders and Spark scripts, and Trino checks the result.
#
# full_refresh (dummyjson_products.products):
#   A (3 products) -> B (2 products) leaves Silver with B only: the removed
#   product leaves Silver. C fails after page 0 (no marker): Silver keeps B.
#   D re-ingests B's snapshot: Silver payload checksum unchanged.
#   Only an incomplete execution in raw: the Silver write fails before the
#   replace and the table keeps its previous count.
# append_only (demo_storefront.clickstream):
#   batch A, the same events again, then a late batch with earlier
#   event_time: Silver deduplicates by event_id and keeps event_time.
#
# The Bronze and Silver tables of those two datasets are replaced with check
# data. Rebuild them from ODP_RAW_BUCKET afterwards (make live-bronze
# live-silver). Output (counts, checksums) goes to .local/p3-12/.
set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/.."
OUT=".local/p3-12"
mkdir -p "${OUT}"

set -a
# shellcheck disable=SC1091
[[ -f .env.example ]] && . ./.env.example
# shellcheck disable=SC1091
[[ -f .env ]] && . ./.env
set +a
export ODP_P3_12_RAW_BUCKET="${ODP_P3_12_RAW_BUCKET:-odp-p3-12-raw}"

COMPOSE=(docker compose)
"${COMPOSE[@]}" up -d --wait postgres minio >/dev/null
"${COMPOSE[@]}" --profile lakehouse up -d --wait trino >/dev/null

PRODUCTS_B="iceberg.bronze.dummyjson_products__products"
PRODUCTS_S="iceberg.silver.dummyjson_products__products"
CLICKS_B="iceberg.bronze.demo_storefront__clickstream"
CLICKS_S="iceberg.silver.demo_storefront__clickstream"
RESULTS="${OUT}/results.tsv"
printf 'check\texpected\tactual\n' >"${RESULTS}"
failures=0

trino() {
  "${COMPOSE[@]}" --profile lakehouse exec -T trino trino --output-format TSV --execute "$1"
}

land() { uv run python scripts/p3_12_land_fixtures.py "$1"; }

# Bronze then Silver for one dataset, reading the check bucket.
layers() {
  local label="$1" source="$2" dataset="$3"
  "${COMPOSE[@]}" --profile lakehouse run --rm -T \
    -e ODP_RAW_BUCKET="${ODP_P3_12_RAW_BUCKET}" spark bash -c '
    set -euo pipefail
    dir="/opt/odp/work/p3-12/$1"
    rm -rf "${dir}" && mkdir -p "${dir}/bronze" "${dir}/silver"
    python -m open_data_platform.lakehouse.bronze --dataset "$2/$3" --sql-out "${dir}/bronze"
    for f in "${dir}"/bronze/*.sql; do ODP_SPARK_SQL="${f}" bash /opt/odp/spark/run_bronze.sh; done
    python -m open_data_platform.lakehouse.silver.cli --source "$2" --dataset "$3" --sql-out "${dir}/silver"
    for f in "${dir}"/silver/*.sql; do ODP_SPARK_SQL="${f}" bash /opt/odp/spark/run_silver.sh; done
  ' _ "${label}" "${source}" "${dataset}" >"${OUT}/${label}.log" 2>&1
}

check() {
  local name="$1" expected="$2" actual
  actual="$(trino "$3")"
  printf '%s\t%s\t%s\n' "${name}" "${expected}" "${actual}" | tee -a "${RESULTS}"
  [[ "${actual}" == "${expected}" ]] || failures=$((failures + 1))
}

# Payload checksum of a Silver table: every column except ingestion metadata.
payload_checksum() {
  local table="$1" columns
  columns="$(trino "SELECT array_join(array_agg('\"' || column_name || '\"' ORDER BY ordinal_position), ', ') FROM iceberg.information_schema.columns WHERE table_schema = 'silver' AND table_name = '${table}' AND column_name NOT IN ('source_id', 'execution_id', '_ingestion_ts', 'raw_object_key', 'record_index', 'dt', '_execution_completed_ts')")"
  trino "SELECT to_hex(checksum(ROW(${columns}))) FROM iceberg.silver.\"${table}\""
}

echo "== full_refresh: dummyjson_products.products"
land reset
land products-a
layers products-a dummyjson_products products
check "a_silver_rows" "3" "SELECT count(*) FROM ${PRODUCTS_S}"

land products-b
layers products-b dummyjson_products products
check "b_silver_rows" "2" "SELECT count(*) FROM ${PRODUCTS_S}"
check "b_silver_rows_from_b" "2" "SELECT count(*) FROM ${PRODUCTS_S} WHERE execution_id = 'p312-b'"
check "b_bronze_keeps_a_for_audit" "3" "SELECT count(*) FROM ${PRODUCTS_B} WHERE execution_id = 'p312-a'"
check "b_removed_product_absent" "0" "SELECT count(*) FROM ${PRODUCTS_S} s WHERE NOT EXISTS (SELECT 1 FROM ${PRODUCTS_B} b WHERE b.execution_id = 'p312-b' AND CAST(b.id AS bigint) = s.id)"
checksum_b="$(payload_checksum dummyjson_products__products)"

land products-c
layers products-c dummyjson_products products
check "c_bronze_rows_incomplete" "2" "SELECT count(*) FROM ${PRODUCTS_B} WHERE execution_id = 'p312-c' AND _execution_completed_ts IS NULL"
check "c_silver_rows_still_b" "2" "SELECT count(*) FROM ${PRODUCTS_S} WHERE execution_id = 'p312-b'"

land products-d
layers products-d dummyjson_products products
check "d_silver_rows" "2" "SELECT count(*) FROM ${PRODUCTS_S}"
check "d_silver_rows_from_d" "2" "SELECT count(*) FROM ${PRODUCTS_S} WHERE execution_id = 'p312-d'"
checksum_d="$(payload_checksum dummyjson_products__products)"
printf 'd_payload_checksum_equals_b\t%s\t%s\n' "${checksum_b}" "${checksum_d}" | tee -a "${RESULTS}"
[[ "${checksum_b}" == "${checksum_d}" ]] || failures=$((failures + 1))

echo "== no complete execution: Silver write fails, previous snapshot stays"
land reset
land products-c
set +e
layers guard dummyjson_products products
guard_status=$?
set -e
printf 'guard_spark_exit_nonzero\t1\t%s\n' "$((guard_status != 0))" | tee -a "${RESULTS}"
[[ "${guard_status}" -ne 0 ]] || failures=$((failures + 1))
grep -q "no complete execution in Bronze for dummyjson_products.products" "${OUT}/guard.log" \
  || { echo "guard message missing from guard.log" >&2; failures=$((failures + 1)); }
check "guard_silver_rows_unchanged" "2" "SELECT count(*) FROM ${PRODUCTS_S} WHERE execution_id = 'p312-d'"

echo "== append_only: demo_storefront.clickstream"
land reset
land clicks-a
layers clicks-a demo_storefront clickstream
check "clicks_a_silver_rows" "50" "SELECT count(*) FROM ${CLICKS_S}"

land clicks-a-again
layers clicks-a-again demo_storefront clickstream
check "clicks_repeat_bronze_rows" "100" "SELECT count(*) FROM ${CLICKS_B}"
check "clicks_repeat_silver_rows" "50" "SELECT count(*) FROM ${CLICKS_S}"
check "clicks_repeat_keeps_first_ingested" "50" "SELECT count(*) FROM ${CLICKS_S} WHERE execution_id = 'p312-clicks-a'"

land clicks-late
layers clicks-late demo_storefront clickstream
check "clicks_late_silver_rows" "70" "SELECT count(*) FROM ${CLICKS_S}"
check "clicks_late_placed_by_event_time" "20" "SELECT count(*) FROM ${CLICKS_S} WHERE event_time < TIMESTAMP '2026-11-10 00:00:00'"
check "clicks_late_rows_from_late_batch" "20" "SELECT count(*) FROM ${CLICKS_S} WHERE execution_id = 'p312-clicks-late' AND event_time < TIMESTAMP '2026-11-10 00:00:00'"

echo "results: ${RESULTS}"
if [[ "${failures}" -ne 0 ]]; then
  echo "P3-12 refresh check failed: ${failures} check(s)" >&2
  exit 1
fi
echo "P3-12 refresh check passed"
