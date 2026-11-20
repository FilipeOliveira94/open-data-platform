#!/usr/bin/env bash
# P3-11 scripted evidence: lakehouse writers replace read tables atomically.
#
# 1. Forced failure: a Silver write that fails mid-run (raise_error injected
#    into a copy of the rendered SQL, in the work volume only) leaves the
#    Trino count and the snapshot count equal to the previous run.
# 2. Full rebuild under readers: a Trino count loop over Bronze, Silver,
#    quarantine staging, and Gold runs while live-bronze, live-silver,
#    stage-quarantine, and dbt-build rewrite those tables. Every query must
#    succeed and no count may drop to zero.
# 3. Snapshot history: each rewritten table gains snapshots; none is lost.
#
# Run from the repository root with the lakehouse stack available.
# Output (counts, durations, query totals) goes to .local/p3-11/. No row values.
set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/.."
OUT=".local/p3-11"
mkdir -p "${OUT}"

set -a
# shellcheck disable=SC1091
[[ -f .env.example ]] && . ./.env.example
# shellcheck disable=SC1091
[[ -f .env ]] && . ./.env
set +a

COMPOSE=(docker compose)
"${COMPOSE[@]}" up -d --wait postgres minio >/dev/null
"${COMPOSE[@]}" --profile lakehouse up -d --wait trino >/dev/null

BRONZE="iceberg.bronze.olist_brazilian_ecommerce__geolocation"
SILVER="iceberg.silver.olist_brazilian_ecommerce__geolocation"
QUARANTINE="iceberg.gold_staging.quarantine_findings"
GOLD="iceberg.marketplace_operations.fct_orders"
TABLES=("${BRONZE}" "${SILVER}" "${QUARANTINE}" "${GOLD}")

trino() {
  "${COMPOSE[@]}" --profile lakehouse exec -T trino trino --output-format TSV --execute "$1"
}

snapshots() {
  local schema="${1#iceberg.}"
  local table="${schema#*.}"
  schema="${schema%%.*}"
  trino "SELECT count(*) FROM iceberg.${schema}.\"${table}\$snapshots\""
}

now() { date +%s; }

echo "== 1. forced Silver failure"
before_count="$(trino "SELECT count(*) FROM ${SILVER}")"
before_snaps="$(snapshots "${SILVER}")"
set +e
"${COMPOSE[@]}" --profile lakehouse run --rm -T spark bash -c '
  set -euo pipefail
  dir=/opt/odp/work/p3-11/forced-failure
  rm -rf "${dir}" && mkdir -p "${dir}"
  python -m open_data_platform.lakehouse.silver.cli \
    --source olist_brazilian_ecommerce --dataset geolocation --sql-out "${dir}"
  file="$(ls "${dir}"/*.sql)"
  sed -i "s/^WHERE _silver_rn = 1;\$/WHERE _silver_rn = 1 AND raise_error('"'"'p3_11_forced_failure'"'"') IS NULL;/" "${file}"
  grep -q p3_11_forced_failure "${file}"
  ODP_SPARK_SQL="${file}" exec bash /opt/odp/spark/run_silver.sh
' >"${OUT}/forced-failure.log" 2>&1
forced_status=$?
set -e
after_count="$(trino "SELECT count(*) FROM ${SILVER}")"
after_snaps="$(snapshots "${SILVER}")"
grep -c "p3_11_forced_failure" "${OUT}/forced-failure.log" >/dev/null \
  || { echo "forced failure did not reach Spark" >&2; exit 1; }
{
  printf 'check\tvalue\n'
  printf 'spark_exit_status\t%s\n' "${forced_status}"
  printf 'count_before\t%s\n' "${before_count}"
  printf 'count_after\t%s\n' "${after_count}"
  printf 'snapshots_before\t%s\n' "${before_snaps}"
  printf 'snapshots_after\t%s\n' "${after_snaps}"
} | tee "${OUT}/forced-failure.tsv"
[[ "${forced_status}" -ne 0 ]] || { echo "forced write did not fail" >&2; exit 1; }
[[ "${after_count}" == "${before_count}" ]] || { echo "count changed" >&2; exit 1; }
[[ "${after_snaps}" == "${before_snaps}" ]] || { echo "snapshot added" >&2; exit 1; }

echo "== 2. full rebuild under a Trino count loop"
declare -A snaps_before
for table in "${TABLES[@]}"; do snaps_before["${table}"]="$(snapshots "${table}")"; done

query="SELECT"
sep=" "
for table in "${TABLES[@]}"; do
  query+="${sep}(SELECT count(*) FROM ${table})"
  sep=", "
done
stop_file="${OUT}/loop.stop"
rm -f "${stop_file}"
loop_log="${OUT}/count-loop.tsv"
: >"${loop_log}"
(
  while [[ ! -f "${stop_file}" ]]; do
    if row="$(trino "${query}" 2>/dev/null)"; then
      printf 'ok\t%s\n' "${row}" >>"${loop_log}"
    else
      printf 'error\n' >>"${loop_log}"
    fi
  done
) &
loop_pid=$!

durations="${OUT}/rebuild-durations.tsv"
printf 'step\tseconds\tstatus\n' >"${durations}"
rebuild_status=0
for target in live-bronze live-silver stage-quarantine dbt-build; do
  start="$(now)"
  set +e
  make "${target}" >"${OUT}/${target}.log" 2>&1
  status=$?
  set -e
  printf '%s\t%s\t%s\n' "${target}" "$(($(now) - start))" "${status}" >>"${durations}"
  if [[ "${status}" -ne 0 ]]; then rebuild_status="${status}"; break; fi
done
touch "${stop_file}"
wait "${loop_pid}"
cat "${durations}"

queries="$(wc -l <"${loop_log}")"
errors="$(grep -c '^error' "${loop_log}" || true)"
zeros="$(awk -F'\t' '$1 == "ok" { for (i = 2; i <= NF; i++) if ($i == "0") z++ } END { print z + 0 }' "${loop_log}")"
{
  printf 'check\tvalue\n'
  printf 'loop_queries\t%s\n' "${queries}"
  printf 'loop_errors\t%s\n' "${errors}"
  printf 'loop_zero_counts\t%s\n' "${zeros}"
} | tee "${OUT}/count-loop-summary.tsv"
[[ "${rebuild_status}" -eq 0 ]] || { echo "rebuild step failed" >&2; exit 1; }
[[ "${queries}" -gt 0 && "${errors}" -eq 0 && "${zeros}" -eq 0 ]] \
  || { echo "readers saw an error or an empty table" >&2; exit 1; }

echo "== 3. snapshot history"
history="${OUT}/snapshots.tsv"
printf 'table\tbefore\tafter\n' >"${history}"
for table in "${TABLES[@]}"; do
  after="$(snapshots "${table}")"
  printf '%s\t%s\t%s\n' "${table}" "${snaps_before[${table}]}" "${after}" >>"${history}"
  [[ "${after}" -gt "${snaps_before[${table}]}" ]] \
    || { echo "${table}: snapshot history did not grow" >&2; exit 1; }
done
cat "${history}"
echo "P3-11 replace check passed"
