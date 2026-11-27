#!/usr/bin/env bash
# P3-05 scripted evidence: the weekly PostgreSQL re-extract catches a late
# commit without duplicating rows the daily runs landed.
#
# Needs the Airflow profile and Trino up (make airflow-up). Steps:
#   1. Commit one synthetic seller on time in DAILY_DAY, backfill the daily
#      DAG for DAILY_DAY, and wait for the Asset-triggered
#      transform_lakehouse run. DAILY_DAY must not have a daily run yet.
#   2. Record Silver, Bronze, and PostgreSQL counts for that day.
#   3. Commit one synthetic seller whose updated_at falls in DAILY_DAY: a
#      late commit after its daily window was extracted.
#   4. Trigger ingest_olist_synth_cdc_weekly by hand. Its data interval is the
#      last complete Monday week before now; the extract window lags it by
#      one day and must contain DAILY_DAY. Wait for the extract and the
#      transform it schedules.
#   5. Silver sellers grow by one, Silver matches PostgreSQL for DAILY_DAY,
#      and every PostgreSQL Silver table has one row per key. Bronze holds
#      the on-time seller twice (daily and weekly executions), so the weekly
#      run re-read it and Silver kept one row.
#   6. Clear the weekly run (same run id) and check the counts again.
#
# The weekly DAG is unpaused (its first scheduled run is 2026-11-23). The
# planted sellers stay in PostgreSQL, raw, Silver, and Gold. Output (counts,
# run ids, durations) goes to .local/p3-05/.
set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/.."
OUT=".local/p3-05"
mkdir -p "${OUT}"

set -a
# shellcheck disable=SC1091
[[ -f .env.example ]] && . ./.env.example
# shellcheck disable=SC1091
[[ -f .env ]] && . ./.env
set +a

DAILY="ingest_olist_synth_cdc_daily"
WEEKLY="ingest_olist_synth_cdc_weekly"
TRANSFORM="transform_lakehouse"
DAILY_DAY="${P3_05_DAILY_DAY:-2026-11-13}"
DAY_START="${DAILY_DAY}T00:00:00+00:00"
DAY_END="$(date -u -d "${DAILY_DAY} + 1 day" +%Y-%m-%d)T00:00:00+00:00"
ON_TIME_TS="${DAILY_DAY}T06:00:00+00:00"
LATE_TS="${DAILY_DAY}T12:00:00+00:00"

COMPOSE=(docker compose)
airflow() {
  "${COMPOSE[@]}" exec -T airflow-scheduler airflow "$@" 2>/dev/null
}
trino() {
  "${COMPOSE[@]}" --profile lakehouse exec -T trino trino --output-format TSV --execute "$1"
}
log() { echo "[$(date -u +%H:%M:%S)] $*" | tee -a "${OUT}/check.log"; }
fail() { log "FAIL: $*"; exit 1; }

# The CLI prints log lines before the JSON array; keep only the array.
list_runs() {
  airflow dags list-runs "$1" -o json | grep -E '^\[(\{|\])' | tail -1
}

# Prints "<state>\t<start>\t<end>" for one run id ("missing" if absent).
run_state() {
  list_runs "$1" | python3 -c '
import json, sys
runs = json.load(sys.stdin)
match = [r for r in runs if r["run_id"] == sys.argv[1]]
print("\t".join([match[0]["state"], match[0]["start_date"] or "", match[0]["end_date"] or ""]) if match else "missing")
' "$2"
}

wait_run() {
  local dag="$1" run_id="$2" state
  for _ in $(seq 1 240); do
    state="$(run_state "${dag}" "${run_id}" | cut -f1)"
    case "${state}" in
      success) log "${dag} ${run_id} success $(run_state "${dag}" "${run_id}" | cut -f2-)"; return 0 ;;
      failed) fail "${dag} ${run_id} failed" ;;
    esac
    sleep 5
  done
  fail "${dag} ${run_id} did not finish"
}

transform_runs() {
  list_runs "${TRANSFORM}" | python3 -c '
import json, sys
runs = json.load(sys.stdin)
print(len(runs), sum(r["state"] in ("queued", "running") for r in runs))
'
}

# Waits until a transform run newer than the given total has finished and
# none is queued or running.
wait_transform() {
  local before="$1" total active
  for _ in $(seq 1 360); do
    read -r total active < <(transform_runs)
    if (( total > before && active == 0 )); then
      local latest
      latest="$(list_runs "${TRANSFORM}" | python3 -c '
import json, sys
runs = json.load(sys.stdin)
latest = max(runs, key=lambda r: r["start_date"] or "")
print(latest["run_id"], latest["state"], latest["start_date"], latest["end_date"], sep="\t")
')"
      log "transform settled: ${latest}"
      [[ "$(cut -f2 <<<"${latest}")" == "success" ]] || fail "transform did not succeed"
      return 0
    fi
    sleep 5
  done
  fail "transform did not settle"
}

silver_counts() {
  local label="$1"
  trino "
SELECT 'sellers', count(*), count(DISTINCT seller_id) FROM iceberg.silver.olist_synth_cdc__sellers
UNION ALL SELECT 'customers', count(*), count(DISTINCT customer_id) FROM iceberg.silver.olist_synth_cdc__customers
UNION ALL SELECT 'orders', count(*), count(DISTINCT order_id) FROM iceberg.silver.olist_synth_cdc__orders
UNION ALL SELECT 'order_items', count(*), count(DISTINCT (order_id, order_item_id)) FROM iceberg.silver.olist_synth_cdc__order_items
" | sort >"${OUT}/silver_${label}.tsv"
  awk -F'\t' '$2 != $3 { bad = 1 } END { exit bad }' "${OUT}/silver_${label}.tsv" \
    || fail "duplicate keys in Silver (${label})"
  trino "
SELECT 'sellers', count(*) FROM iceberg.silver.olist_synth_cdc__sellers WHERE updated_at >= from_iso8601_timestamp('${DAY_START}') AND updated_at < from_iso8601_timestamp('${DAY_END}')
UNION ALL SELECT 'customers', count(*) FROM iceberg.silver.olist_synth_cdc__customers WHERE updated_at >= from_iso8601_timestamp('${DAY_START}') AND updated_at < from_iso8601_timestamp('${DAY_END}')
UNION ALL SELECT 'orders', count(*) FROM iceberg.silver.olist_synth_cdc__orders WHERE updated_at >= from_iso8601_timestamp('${DAY_START}') AND updated_at < from_iso8601_timestamp('${DAY_END}')
UNION ALL SELECT 'order_items', count(*) FROM iceberg.silver.olist_synth_cdc__order_items WHERE updated_at >= from_iso8601_timestamp('${DAY_START}') AND updated_at < from_iso8601_timestamp('${DAY_END}')
" | sort >"${OUT}/silver_window_${label}.tsv"
  log "silver ${label}: $(tr '\t\n' ' ;' <"${OUT}/silver_${label}.tsv")"
  log "silver window ${label}: $(tr '\t\n' ' ;' <"${OUT}/silver_window_${label}.tsv")"
}

postgres_window() {
  uv run python scripts/p3_05_late_commit.py count "${DAY_START}" "${DAY_END}" \
    | sort >"${OUT}/postgres_window_$1.tsv"
  log "postgres window $1: $(tr '\t\n' ' ;' <"${OUT}/postgres_window_$1.tsv")"
}

bronze_sellers() {
  trino "SELECT count(*) FROM iceberg.bronze.olist_synth_cdc__sellers WHERE try(from_iso8601_timestamp(updated_at)) >= from_iso8601_timestamp('${DAY_START}') AND try(from_iso8601_timestamp(updated_at)) < from_iso8601_timestamp('${DAY_END}')" \
    >"${OUT}/bronze_sellers_$1.tsv"
  log "bronze sellers window $1: $(cat "${OUT}/bronze_sellers_$1.tsv")"
}

sellers() { awk -F'\t' '$1 == "sellers" { print $2 }' "$1"; }

: >"${OUT}/check.log"
log "daily day ${DAY_START} -> ${DAY_END}"
for _ in $(seq 1 24); do
  airflow dags list -o plain | grep -q "^${WEEKLY} " && break
  sleep 5
done
airflow dags list -o plain | grep -q "^${WEEKLY} " || fail "${WEEKLY} not parsed"
airflow dags unpause "${DAILY}" >/dev/null
airflow dags unpause "${TRANSFORM}" >/dev/null
airflow dags unpause "${WEEKLY}" >/dev/null

# 1. The daily window, extracted by the daily DAG.
daily_run="backfill__${DAY_END}"
read -r before _ < <(transform_runs)
[[ "$(run_state "${DAILY}" "${daily_run}" | cut -f1)" == "missing" ]] \
  || fail "${DAILY} ${daily_run} exists; set P3_05_DAILY_DAY to a day without a daily run"
uv run python scripts/p3_05_late_commit.py plant "${ON_TIME_TS}" | tee -a "${OUT}/check.log"
airflow backfill create --dag-id "${DAILY}" \
  --from-date "${DAY_START}" --to-date "${DAY_START}" >/dev/null
wait_run "${DAILY}" "${daily_run}"
wait_transform "${before}"

# 2. Counts before the late commit.
silver_counts before
postgres_window before
cmp -s "${OUT}/silver_window_before.tsv" "${OUT}/postgres_window_before.tsv" \
  || fail "Silver does not match PostgreSQL for the daily window before the late commit"
(( $(sellers "${OUT}/silver_window_before.tsv") >= 1 )) \
  || fail "the daily run landed no seller for DAILY_DAY"
bronze_sellers before

# 3. The late commit.
uv run python scripts/p3_05_late_commit.py plant "${LATE_TS}" | tee -a "${OUT}/check.log"
postgres_window late
(( $(sellers "${OUT}/postgres_window_late.tsv") == $(sellers "${OUT}/postgres_window_before.tsv") + 1 )) \
  || fail "late seller not committed"
silver_counts late
cmp -s "${OUT}/silver_window_late.tsv" "${OUT}/silver_window_before.tsv" \
  || fail "Silver changed without a run"
log "late seller is in PostgreSQL and not in Silver: the daily window missed it"

# 4. The weekly run.
logical="$(date -u +%Y-%m-%dT%H:%M:%S+00:00)"
read -r before _ < <(transform_runs)
airflow dags trigger "${WEEKLY}" -l "${logical}" >/dev/null
weekly_run="$(list_runs "${WEEKLY}" | python3 -c '
import json, sys
runs = json.load(sys.stdin)
print(max(runs, key=lambda r: r["run_after"])["run_id"])
')"
log "weekly run ${weekly_run} (logical date ${logical})"
wait_run "${WEEKLY}" "${weekly_run}"
wait_transform "${before}"

# 5. The late row is in Silver, once.
silver_counts weekly
postgres_window weekly
cmp -s "${OUT}/silver_window_weekly.tsv" "${OUT}/postgres_window_weekly.tsv" \
  || fail "Silver does not match PostgreSQL for the daily window after the weekly run"
(( $(sellers "${OUT}/silver_weekly.tsv") == $(sellers "${OUT}/silver_before.tsv") + 1 )) \
  || fail "Silver sellers did not grow by exactly one"
bronze_sellers weekly
# The weekly execution holds every seller of the day again, the on-time ones
# included: before + (before + 1).
(( $(cat "${OUT}/bronze_sellers_weekly.tsv") == 2 * $(cat "${OUT}/bronze_sellers_before.tsv") + 1 )) \
  || fail "Bronze does not hold the weekly re-read of the daily rows"
log "PASS weekly run: late seller in Silver, daily rows re-read without duplicates, window matches PostgreSQL"

# 6. Re-run the same weekly run.
read -r before _ < <(transform_runs)
airflow tasks clear "${WEEKLY}" -s "${logical}" -e "${logical}" -y >/dev/null
sleep 10
wait_run "${WEEKLY}" "${weekly_run}"
wait_transform "${before}"
silver_counts rerun
cmp -s "${OUT}/silver_rerun.tsv" "${OUT}/silver_weekly.tsv" \
  || fail "re-run changed Silver counts"
bronze_sellers rerun
cmp -s "${OUT}/bronze_sellers_rerun.tsv" "${OUT}/bronze_sellers_weekly.tsv" \
  || fail "re-run changed Bronze sellers"
log "PASS re-run: Silver and Bronze counts unchanged"
log "PASS P3-05 weekly check"
