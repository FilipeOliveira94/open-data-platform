#!/usr/bin/env bash
# P3-14 live run: the shipped daily and weekly PostgreSQL DAGs on local Airflow
# (make airflow-up, Trino up), with each landing compared to the source window
# and to the previous landing of that window. No fault is injected (P3-09 did).
#
# Usage: scripts/p3_14_live_run.sh <step>...
#   daily       Commit one synthetic on-time seller in a free day D, backfill the
#               daily DAG for D, compare the landing to PostgreSQL, and count
#               the four Silver olist_synth_cdc tables in Trino while the refresh
#               that follows runs (reader view). D is saved for `rerun`.
#   rerun       Clear the daily run of D (same run id), then extract D again
#               under a new execution id. Landings equal the first; Silver
#               counts unchanged; the daily gap report stays clean.
#   late        Commit one synthetic seller whose updated_at falls in an
#               already extracted daily window inside the next weekly window.
#   weekly      Trigger the weekly DAG with logical date now. Compare the
#               landing to PostgreSQL, to the previous weekly landing of the
#               same window, and to the daily landings inside it. Silver lands
#               the late seller once, and every key is unique.
#   no-new-raw  A manual refresh with nothing pending writes no table.
#   pause       Pause the daily DAG before 00:00Z (run after the steps above).
#   catchup     After 00:15Z: unpause the daily DAG. One scheduled run per
#               missed day, each succeeding and compared to PostgreSQL, one
#               refresh after, and an empty daily gap report.
#   report      Per refresh of this run: raw objects and rows read against the
#               total raw history at that time, and the Silver rows read.
#   all-day     daily rerun late weekly no-new-raw report
#
# Every check prints PASS or FAIL; the exit status is 1 when any check fails.
# Output (windows, states, counts, durations; no row values) goes to
# .local/p3-14/<step>.log. Refresh run ids go to .local/p3-14/refreshes.txt.
# The planted sellers stay in PostgreSQL, raw, Silver, and Gold (as in P3-05).
# `late` and `weekly` expect exactly one new seller, so run them once per late
# day: a second weekly run of the same window finds it already in Silver.
set -uo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/.."
OUT=".local/p3-14"
mkdir -p "${OUT}"

# shellcheck source=scripts/lib/live_check.sh
. scripts/lib/live_check.sh

STATE="${OUT}/state"
DATASETS=(sellers customers orders order_items)
declare -A KEYS=([sellers]="seller_id" [customers]="customer_id" [orders]="order_id"
                 [order_items]="order_id, order_item_id")
LATE_DAY="${P3_14_LATE_DAY:-2026-11-18}"
FIRST_DAY="${P3_14_FIRST_DAY:-2026-10-24}"

save() { grep -v "^$1=" "${STATE}" 2>/dev/null > "${STATE}.tmp"; echo "$1=$2" >> "${STATE}.tmp"; mv "${STATE}.tmp" "${STATE}"; }
load() { grep "^$1=" "${STATE}" 2>/dev/null | tail -1 | cut -d= -f2-; }
olist_synth() { uv run python scripts/p3_05_late_commit.py "$@"; }
day_after() { date -u -d "$1 + 1 day" +%F; }

# --- Landings -------------------------------------------------------------

# Window and per-dataset row count of one execution: "<dataset> <rows>" lines.
landed() {  # landed <execution id>
  pgq "${CATALOG_DB}" "SELECT dataset, row_count FROM ingestion_state.extract_coverage
       WHERE execution_id='$1' ORDER BY dataset"
}
window_of() {  # window_of <execution id>: "<start> <end>" as ISO instants
  pgq "${CATALOG_DB}" "SELECT DISTINCT to_char(window_start AT TIME ZONE 'UTC','YYYY-MM-DD\"T\"HH24:MI:SS+00:00'),
       to_char(window_end AT TIME ZONE 'UTC','YYYY-MM-DD\"T\"HH24:MI:SS+00:00')
       FROM ingestion_state.extract_coverage WHERE execution_id='$1'"
}
source_counts() {  # source_counts <start> <end>: PostgreSQL rows by updated_at
  olist_synth count "$1" "$2" | tr '\t' ' ' | sort
}

# Logs the window and compares the landing of an execution to PostgreSQL. Run
# right after the extract, with nothing else writing olist_synth.
compare_to_source() {  # compare_to_source <execution id> <label>
  local exec_id="$1" what="$2" start end
  read -r start end <<<"$(window_of "${exec_id}")"
  require "${what} coverage rows" "${start}" || return 1
  log "  ${what} window [${start}, ${end}) execution ${exec_id}"
  landed "${exec_id}" | sort > "${OUT}/.landed"
  source_counts "${start}" "${end}" > "${OUT}/.source"
  join "${OUT}/.landed" "${OUT}/.source" | while read -r d l s; do log "    ${d} landed ${l} source ${s}"; done
  check "${what}: landed counts equal the source window" diff -q "${OUT}/.landed" "${OUT}/.source"
  check "${what}: one coverage row per dataset" test "$(wc -l < "${OUT}/.landed")" == "${#DATASETS[@]}"
}

# --- Lakehouse ------------------------------------------------------------

# Silver rows whose updated_at falls in the window, per dataset, equal the
# PostgreSQL rows of that window (Silver keeps the latest version per key).
silver_matches_source() {  # silver_matches_source <start> <end> <label>
  local t n
  : > "${OUT}/.silver_window"
  for t in "${DATASETS[@]}"; do
    n="$(trino "SELECT count(*) FROM iceberg.silver.olist_synth_cdc__${t}
         WHERE updated_at >= from_iso8601_timestamp('$1') AND updated_at < from_iso8601_timestamp('$2')" | tr -d '"')"
    echo "${t} ${n:-ERROR}" >> "${OUT}/.silver_window"
  done
  sort -o "${OUT}/.silver_window" "${OUT}/.silver_window"
  source_counts "$1" "$2" > "${OUT}/.source"
  join "${OUT}/.silver_window" "${OUT}/.source" | while read -r d v s; do log "    ${d} Silver ${v} source ${s} in [$1, $2)"; done
  check "$3: Silver rows of the window equal the source window" diff -q "${OUT}/.silver_window" "${OUT}/.source"
}

silver_dupes() {  # rows minus distinct keys, summed over the four tables
  local t total=0 n
  for t in "${DATASETS[@]}"; do
    n="$(trino "SELECT count(*) - count(DISTINCT (${KEYS[$t]})) FROM iceberg.silver.olist_synth_cdc__${t}" | tr -d '"')"
    log "    silver.olist_synth_cdc__${t} duplicate keys ${n:-ERROR}"
    total=$((total + ${n:-1}))
  done
  echo "${total}" > "${OUT}/.dupes"
}

quarantine_counts() {  # quarantine and cast counts recorded by one refresh
  local label="${1//[^A-Za-z0-9._-]/_}"
  trino "SELECT metric, count(DISTINCT dataset), sum(row_count) FROM iceberg.silver_audit.quality_counts
         WHERE run_label = '${label}' GROUP BY metric ORDER BY metric" | tr -d '"' \
    | while read -r m n c; do log "    quality_counts ${m}: ${n} datasets, ${c} rows"; done
  trino "SELECT source_id || '.' || dataset, metric, sum(row_count) FROM iceberg.silver_audit.quality_counts
         WHERE run_label = '${label}' AND row_count > 0 GROUP BY 1, 2 ORDER BY 1, 2" | tr -d '"' \
    | while read -r d m c; do log "    quality_counts ${d} ${m} ${c}"; done
}

reader_start() {
  : > "${OUT}/reader-$1.txt"; rm -f "${OUT}/.reader_stop"
  (
    while [[ ! -f "${OUT}/.reader_stop" ]]; do
      silver_counts | while read -r t c; do echo "$(date -u +%H:%M:%S) ${t} ${c:-ERROR}" >> "${OUT}/reader-$1.txt"; done
      sleep 1
    done
  ) &
  READER_PID=$!
}
reader_stop() {  # reader_stop <name>
  touch "${OUT}/.reader_stop"; wait "${READER_PID}" 2>/dev/null; rm -f "${OUT}/.reader_stop"
  local reads errors
  reads="$(wc -l < "${OUT}/reader-$1.txt")"
  errors="$(grep -cE ' (ERROR|0)$' "${OUT}/reader-$1.txt" || true)"
  log "  reader queries ${reads}, failed or empty ${errors}"
  check "no Silver query failed or saw an empty table during the refresh" test "${errors}" == 0 -a "${reads}" -gt 0
}
trap 'touch "${OUT}/.reader_stop"' EXIT

# Waits for the refresh an extract scheduled, optionally with the reader loop.
follow_refresh() {  # follow_refresh <since> [reader name]; prints nothing
  local tr
  tr="$(wait_new_run "${TRANSFORM}" "$1" 600)"
  require "transform run after the extract" "${tr}" || return 1
  echo "${tr}" >> "${OUT}/refreshes.txt"
  [[ -n "${2:-}" ]] && reader_start "$2"
  wait_run "${TRANSFORM}" "${tr}"
  [[ -n "${2:-}" ]] && reader_stop "$2"
  check "the refresh ${tr} succeeds" test "$(run_state "${TRANSFORM}" "${tr}")" == success
  step_runs "${tr}"
  quarantine_counts "${tr}"
}

# --- Steps ----------------------------------------------------------------

step_daily() {
  local day run since
  wait_transform_idle
  day="$(free_backfill_day "${FIRST_DAY}")"
  require "a free backfill day" "${day}" || return
  [[ "${day}" != "$(date -u +%F)" ]] || { log "FAIL D must not be today (the window pause leaves missed)"; FAILURES=$((FAILURES + 1)); return; }
  log "  D = ${day}"
  olist_synth plant "${day}T06:00:00+00:00" | while read -r l; do log "  on-time seller ${l}"; done
  silver_counts > "${OUT}/daily-silver-before.txt"
  since="$(now)"
  run="$(backfill_daily "${day}")"
  require "daily backfill run" "${run}" || return
  wait_run "${DAILY}" "${run}"
  check "daily run succeeds" test "$(last_state)" == success
  save day "${day}"; save daily_run "${run}"
  compare_to_source "${DAILY}__${run}" daily
  follow_refresh "${since}" daily
  silver_counts > "${OUT}/daily-silver-after.txt"
  join "${OUT}/daily-silver-before.txt" "${OUT}/daily-silver-after.txt" | while read -r t b a; do log "    ${t} ${b} -> ${a}"; done
  silver_matches_source "${day}T00:00:00+00:00" "$(day_after "${day}")T00:00:00+00:00" daily
}

step_rerun() {
  local day run exec_id since manual="p314-rerun-manual-${TAG}" rc
  day="$(load day)"; run="$(load daily_run)"
  require "the daily run of the daily step" "${run}" || return
  exec_id="${DAILY}__${run}"
  wait_transform_idle
  landed "${exec_id}" > "${OUT}/rerun-first-landing.txt"
  silver_counts > "${OUT}/rerun-silver-before.txt"

  since="$(now)"
  clear_run "${DAILY}" "${run}" false
  wait_run "${DAILY}" "${run}"
  check "the cleared daily run succeeds under the same run id" test "$(last_state)" == success
  landed "${exec_id}" > "${OUT}/rerun-second-landing.txt"
  check "rerun of the same run: landing equals the first landing" diff -q "${OUT}/rerun-first-landing.txt" "${OUT}/rerun-second-landing.txt"
  compare_to_source "${exec_id}" "daily rerun"
  follow_refresh "${since}"

  wait_transform_idle
  since="$(now)"
  manual_extract "${manual}" "${day}" "$(day_after "${day}")"
  wait_run "${MANUAL}" "${manual}"
  check "a new extract of the same window succeeds" test "$(last_state)" == success
  landed "${manual}" > "${OUT}/rerun-manual-landing.txt"
  check "new execution of the window: landing equals the first landing" diff -q "${OUT}/rerun-first-landing.txt" "${OUT}/rerun-manual-landing.txt"
  compare_to_source "${manual}" "manual re-extract"
  follow_refresh "${since}"

  silver_counts > "${OUT}/rerun-silver-after.txt"
  join "${OUT}/rerun-silver-before.txt" "${OUT}/rerun-silver-after.txt" | while read -r t b a; do log "    ${t} ${b} -> ${a}"; done
  check "Silver counts unchanged after both reruns" diff -q "${OUT}/rerun-silver-before.txt" "${OUT}/rerun-silver-after.txt"
  silver_dupes
  check "no duplicate key in Silver" test "$(cat "${OUT}/.dupes")" == 0
  coverage_report > "${OUT}/rerun-gap-report.txt"; rc=$?
  sed 's/^/    report /' "${OUT}/rerun-gap-report.txt" | tee -a "${OUT}/${SCENARIO}.log" >/dev/null
  check "the daily gap report is clean" test "${rc}" == 0
}

step_late() {
  local start="${LATE_DAY}T00:00:00+00:00" end
  end="$(day_after "${LATE_DAY}")T00:00:00+00:00"
  local daily_exec
  daily_exec="$(pgq "${CATALOG_DB}" "SELECT execution_id FROM ingestion_state.extract_coverage
       WHERE source_id='olist_synth_cdc' AND frequency='daily' AND window_start='${start}' ORDER BY recorded_at DESC LIMIT 1")"
  require "a daily landing of ${LATE_DAY}" "${daily_exec}" || return
  log "  ${LATE_DAY} was landed by ${daily_exec}"
  silver_counts > "${OUT}/late-silver-before.txt"
  olist_synth plant "${LATE_DAY}T12:00:00+00:00" | while read -r l; do log "  late seller ${l}"; done
  source_counts "${start}" "${end}" > "${OUT}/late-source.txt"
  landed "${daily_exec}" | sort > "${OUT}/late-daily-landing.txt"
  join "${OUT}/late-daily-landing.txt" "${OUT}/late-source.txt" | while read -r d l s; do log "    ${d} daily landed ${l} source now ${s}"; done
  check "the source holds one seller more than the daily landing of ${LATE_DAY}" test \
    "$(awk '$1=="sellers"{print $2}' "${OUT}/late-source.txt")" == "$(( $(awk '$1=="sellers"{print $2}' "${OUT}/late-daily-landing.txt") + 1 ))"
  save late_day "${LATE_DAY}"
}

step_weekly() {
  local run="p314-weekly-${TAG}" exec_id since start end prev
  exec_id="${WEEKLY}__${run}"
  wait_transform_idle
  silver_counts > "${OUT}/weekly-silver-before.txt"
  since="$(now)"
  trigger_at_now "${WEEKLY}" "${run}"
  wait_run "${WEEKLY}" "${run}"
  check "weekly run succeeds" test "$(last_state)" == success
  log "  weekly data interval: $(afq "SELECT data_interval_start, data_interval_end FROM dag_run WHERE dag_id='${WEEKLY}' AND run_id='${run}'")"
  compare_to_source "${exec_id}" weekly
  read -r start end <<<"$(window_of "${exec_id}")"

  prev="$(pgq "${CATALOG_DB}" "SELECT execution_id FROM ingestion_state.extract_coverage
       WHERE source_id='olist_synth_cdc' AND frequency='weekly' AND window_start='${start}' AND execution_id <> '${exec_id}'
       ORDER BY recorded_at DESC LIMIT 1")"
  landed "${exec_id}" | sort > "${OUT}/weekly-landing.txt"
  if [[ -n "${prev}" ]]; then
    landed "${prev}" | sort > "${OUT}/weekly-previous.txt"
    join "${OUT}/weekly-previous.txt" "${OUT}/weekly-landing.txt" | while read -r d p l; do log "    ${d} previous weekly ${p} now ${l}"; done
  else
    log "  no previous weekly landing of this window"
  fi
  # Latest daily landing of each daily window inside the weekly window.
  pgq "${CATALOG_DB}" "SELECT dataset, sum(row_count) FROM (
       SELECT DISTINCT ON (dataset, window_start) dataset, row_count FROM ingestion_state.extract_coverage
       WHERE source_id='olist_synth_cdc' AND frequency='daily' AND window_start >= '${start}' AND window_end <= '${end}'
       ORDER BY dataset, window_start, recorded_at DESC) x GROUP BY dataset ORDER BY dataset" | sort > "${OUT}/weekly-daily-sum.txt"
  log "  daily windows inside: $(pgq "${CATALOG_DB}" "SELECT count(DISTINCT window_start) FROM ingestion_state.extract_coverage WHERE source_id='olist_synth_cdc' AND frequency='daily' AND window_start >= '${start}' AND window_end <= '${end}'") of 7"
  join "${OUT}/weekly-daily-sum.txt" "${OUT}/weekly-landing.txt" | while read -r d s l; do log "    ${d} daily sum ${s} weekly ${l} difference $((l - s))"; done

  local late_day late_ts; late_day="$(load late_day)"; late_ts="${late_day}T00:00:00+00:00"
  if [[ -n "${late_day}" && ! "${late_ts}" < "${start}" && "${late_ts}" < "${end}" ]]; then
    log "  the late seller day ${late_day} is inside the weekly window"
  else
    log "FAIL the late seller day ${late_day:-unset} is outside [${start}, ${end})"; FAILURES=$((FAILURES + 1))
  fi
  follow_refresh "${since}" weekly
  silver_counts > "${OUT}/weekly-silver-after.txt"
  join "${OUT}/weekly-silver-before.txt" "${OUT}/weekly-silver-after.txt" | while read -r t b a; do log "    ${t} ${b} -> ${a}"; done
  check "Silver sellers grow by the late seller only" test \
    "$(awk '/sellers/{print $2}' "${OUT}/weekly-silver-after.txt")" == "$(( $(awk '/sellers/{print $2}' "${OUT}/weekly-silver-before.txt") + 1 ))"
  check "the other Silver tables keep their counts" diff -q \
    <(grep -v sellers "${OUT}/weekly-silver-before.txt") <(grep -v sellers "${OUT}/weekly-silver-after.txt")
  silver_matches_source "${late_day}T00:00:00+00:00" "$(day_after "${late_day}")T00:00:00+00:00" "late day"
  silver_matches_source "${start}" "${end}" weekly
  silver_dupes
  check "no duplicate key in Silver" test "$(cat "${OUT}/.dupes")" == 0
}

step_no_new_raw() {
  no_new_raw_refresh "p314-no-new-raw-${TAG}"
  echo "p314-no-new-raw-${TAG}" >> "${OUT}/refreshes.txt"
}

step_pause() {
  airflow dags pause "${DAILY}" >/dev/null
  log "  paused ${DAILY} at $(now)"
  check "the daily DAG is paused" test "$(afq "SELECT is_paused FROM dag WHERE dag_id='${DAILY}'")" == t
  log "  last scheduled interval end $(afq "SELECT max(data_interval_end) FROM dag_run WHERE dag_id='${DAILY}' AND run_type='scheduled'")"
  save paused_at "$(now)"
}

step_catchup() {
  local last missed since runs run rc today
  check "the daily DAG is paused" test "$(afq "SELECT is_paused FROM dag WHERE dag_id='${DAILY}'")" == t
  last="$(afq "SELECT to_char(max(data_interval_end) AT TIME ZONE 'UTC', 'YYYY-MM-DD') FROM dag_run WHERE dag_id='${DAILY}' AND run_type='scheduled'")"
  today="$(date -u +%F)"
  missed=$(( ($(date -u -d "${today}" +%s) - $(date -u -d "${last}" +%s)) / 86400 ))
  log "  last scheduled interval end ${last}; missed days up to ${today}: ${missed}"
  (( missed >= 1 )) || { log "FAIL no midnight passed while the DAG was paused"; FAILURES=$((FAILURES + 1)); return; }
  wait_transform_idle
  since="$(now)"
  airflow dags unpause "${DAILY}" >/dev/null
  log "  unpaused ${DAILY} at ${since}"
  # max_active_runs=1: the scheduler creates the catch-up runs one at a time.
  local i n waited
  for (( i = 1; i <= missed; i++ )); do
    waited=0
    while n="$(afq "SELECT count(*) FROM dag_run WHERE dag_id='${DAILY}' AND created_at >= '${since}' AND run_type='scheduled'")"; (( n < i && waited < 1800 )); do
      sleep 10; waited=$((waited + 10))
    done
    run="$(afq "SELECT run_id FROM dag_run WHERE dag_id='${DAILY}' AND created_at >= '${since}' AND run_type='scheduled' ORDER BY logical_date OFFSET $((i - 1)) LIMIT 1")"
    require "catch-up run ${i} of ${missed}" "${run}" || return
    wait_run "${DAILY}" "${run}" 3600
    check "catch-up run ${run} succeeds" test "$(last_state)" == success
    compare_to_source "${DAILY}__${run}" "catch-up ${run}"
  done
  sleep 30
  check "one run per missed day, no other daily run" test "$(runs_since "${DAILY}" "${since}")" == "${missed}"
  local tr
  wait_new_run "${TRANSFORM}" "${since}" 600 >/dev/null
  wait_transform_idle; sleep 30; wait_transform_idle
  for tr in $(afq "SELECT run_id FROM dag_run WHERE dag_id='${TRANSFORM}' AND created_at >= '${since}' ORDER BY created_at"); do
    echo "${tr}" >> "${OUT}/refreshes.txt"
    log "  refresh ${tr} $(run_state "${TRANSFORM}" "${tr}")"
    check "the refresh ${tr} succeeds" test "$(run_state "${TRANSFORM}" "${tr}")" == success
    step_runs "${tr}"
    quarantine_counts "${tr}"
  done
  log "  transform runs since the unpause: $(runs_since "${TRANSFORM}" "${since}")"
  coverage_report > "${OUT}/catchup-gap-report.txt"; rc=$?
  sed 's/^/    report /' "${OUT}/catchup-gap-report.txt" | tee -a "${OUT}/${SCENARIO}.log" >/dev/null
  check "the daily gap report is empty after the catch-up" test "${rc}" == 0
}

step_report() {
  [[ -s "${OUT}/refreshes.txt" ]] || { log "no refreshes recorded"; return; }
  log "  run | bronze objects read / raw objects promoted so far | bronze rows read / raw rows so far | silver rows read / bronze rows | seconds"
  local run label
  sort -u "${OUT}/refreshes.txt" | while read -r run; do
    label="${run//[^A-Za-z0-9._-]/_}"
    pgq "${CATALOG_DB}" "WITH r AS (
         SELECT max(finished_at) AS finished, sum(seconds) AS seconds FROM lakehouse_state.step_runs WHERE run_label='${label}'),
       b AS (SELECT coalesce(sum(objects_read), 0) o, coalesce(sum(rows_read), 0) n, sum(table_rows) t
             FROM lakehouse_state.dataset_runs WHERE run_label='${label}' AND layer='bronze'),
       s AS (SELECT coalesce(sum(rows_read), 0) n FROM lakehouse_state.dataset_runs WHERE run_label='${label}' AND layer='silver'),
       h AS (SELECT count(*) o, coalesce(sum(row_count), 0) n FROM lakehouse_state.bronze_promotions, r WHERE promoted_at <= r.finished)
       SELECT b.o || '/' || h.o, b.n || '/' || h.n, s.n || '/' || coalesce(b.t::text, '-'), round(r.seconds::numeric, 1)
       FROM b, s, h, r" | while read -r o n s sec; do log "    ${run} | ${o} | ${n} | ${s} | ${sec}"; done
  done
}

[[ $# -gt 0 ]] || { sed -n '2,32p' "$0"; exit 2; }
steps=("$@")
[[ "${steps[*]}" == "all-day" ]] && steps=(daily rerun late weekly no-new-raw report)
for SCENARIO in "${steps[@]}"; do
  fn="step_${SCENARIO//-/_}"
  declare -F "${fn}" >/dev/null || { echo "unknown step: ${SCENARIO}"; exit 2; }
  : > "${OUT}/${SCENARIO}.log"
  log "=== ${SCENARIO} (tag ${TAG})"
  start=$(date +%s)
  "${fn}"
  log "=== ${SCENARIO} done in $(( $(date +%s) - start ))s, failures so far ${FAILURES}"
done
exit $(( FAILURES > 0 ))
