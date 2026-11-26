#!/usr/bin/env bash
# P3-09 scripted evidence: orchestration success and failure scenarios on the
# local Airflow (make airflow-up, Trino up). Each scenario drives the shipped
# DAGs through the Airflow CLI and reads task states and try numbers from the
# Airflow metadata database (read only), step results from
# lakehouse_state.step_runs (P3-08), and table versions from the Iceberg JDBC
# catalog (iceberg_tables.metadata_location).
#
# Usage: scripts/p3_09_orchestration_check.sh <scenario>...
#   catchup        The daily DAG was paused over a scheduled midnight. Unpause it
#                  with the OLTP database refusing connections: the missed run
#                  is created, the settle sensor holds it, the extract fails all
#                  tries, the run fails, and the gap report lists the window.
#                  Allow connections, clear the run: it succeeds, the gap closes,
#                  and the refresh follows. Run after D 00:00Z with the DAG paused.
#   happy          Manual PostgreSQL extract, daily backfill, manual weekly run,
#                  and reconcile_gold: every run succeeds and each extract is
#                  followed by a transform_lakehouse run.
#   retry          Daily backfill with the OLTP database refusing connections on
#                  try 1, allowed before try 2. Then Trino stopped while dbt_build
#                  runs its first try and started before the retry.
#   reconcile      reconcile_gold with Trino down on try 1 (retry succeeds), then
#                  with Trino down for every try (run fails), then cleared.
#   write-failure  SIGTERM to the Silver step's process group while it writes,
#                  with a Trino count loop on Silver. Readers keep the previous
#                  table; the retry finishes the write.
#   no-new-raw     A manual refresh with nothing pending writes no table.
#   coalesce       Two extracts land while a refresh runs: one follow-up run.
#   gate           A planted Bronze check mismatch fails bronze_checks; a refresh
#                  with no new raw object still does not build Silver; removing
#                  the plant lets the next refresh pass and build.
#
# Faults are reversible: ALTER DATABASE ... ALLOW_CONNECTIONS, docker compose
# stop/start trino, a SIGTERM to one task's process group, and a trigger on
# lakehouse_state.layer_checks that the gate scenario drops. PostgreSQL itself
# stays up because it also holds the Airflow metadata database. PostgreSQL
# windows land under new execution ids (p309-*); Silver deduplicates by key.
# Output (states, try numbers, counts, durations; no row values) goes to
# .local/p3-09/<scenario>.log.
set -uo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/.."
OUT=".local/p3-09"
mkdir -p "${OUT}"

set -a
# shellcheck disable=SC1091
[[ -f .env.example ]] && . ./.env.example
# shellcheck disable=SC1091
[[ -f .env ]] && . ./.env
set +a

AIRFLOW_DB="${AIRFLOW_DB:-airflow}"
CATALOG_DB="${ICEBERG_CATALOG_DB:-iceberg}"
OLTP_DB="${POSTGRES_DB}"
TAG="$(date -u +%Y%m%dT%H%M%S)"
COMPOSE=(docker compose)
DAILY="ingest_postgres_oltp_daily"
WEEKLY="ingest_postgres_oltp_weekly"
MANUAL="ingest_postgres_oltp"
TRANSFORM="transform_lakehouse"
RECONCILE="reconcile_gold"
SCENARIO="setup"
FAILURES=0

log() { echo "[$(date -u +%H:%M:%S)] $*" | tee -a "${OUT}/${SCENARIO}.log"; }
check() {  # check <description> <condition...>
  local what="$1"; shift
  if "$@"; then log "PASS ${what}"; else log "FAIL ${what}"; FAILURES=$((FAILURES + 1)); fi
}
airflow() { "${COMPOSE[@]}" exec -T airflow-scheduler airflow "$@" 2>/dev/null; }
pgq() { docker exec odp-postgres psql -U "${POSTGRES_USER}" -d "$1" -AtF ' ' -c "$2"; }
afq() { pgq "${AIRFLOW_DB}" "$1"; }
trino() {
  "${COMPOSE[@]}" --profile lakehouse exec -T trino trino --output-format TSV --execute "$1" 2>/dev/null
}
now() { date -u +%Y-%m-%dT%H:%M:%S+00:00; }
# Stops a scenario when a run it needs was not created.
require() {  # require <what> <value>
  [[ -n "$2" ]] && return 0
  log "FAIL $1 (not found)"; FAILURES=$((FAILURES + 1)); return 1
}

# --- Airflow state (metadata database, read only) -------------------------

run_state() { afq "SELECT state FROM dag_run WHERE dag_id='$1' AND run_id='$2'"; }

# Newest run of a DAG created at or after a timestamp, optionally of one type.
newest_run() {
  local type_filter=""
  [[ -n "${3:-}" ]] && type_filter="AND run_type='$3'"
  afq "SELECT run_id FROM dag_run WHERE dag_id='$1' AND created_at >= '$2' ${type_filter} ORDER BY created_at DESC LIMIT 1"
}

runs_since() { afq "SELECT count(*) FROM dag_run WHERE dag_id='$1' AND created_at >= '$2'"; }

# Airflow 3.4 keeps one task_instance row per try; the latest try is current.
ti_state() {
  afq "SELECT state, try_number FROM task_instance WHERE dag_id='$1' AND run_id='$2' AND task_id='$3'
       ORDER BY try_number DESC LIMIT 1"
}

# Task id, state, try number, and duration in seconds of every try in one run.
show_tis() {
  afq "SELECT task_id, coalesce(state, 'none'), try_number, round(coalesce(duration, 0))
       FROM task_instance WHERE dag_id='$1' AND run_id='$2' ORDER BY priority_weight DESC, task_id, try_number" \
    | while read -r line; do log "    $1 ${line}"; done
}

wait_run() {  # wait_run <dag> <run id> [timeout seconds]; prints the final state
  local dag="$1" run="$2" limit="${3:-3600}" state="" waited=0
  while (( waited < limit )); do
    state="$(run_state "${dag}" "${run}")"
    [[ "${state}" == "success" || "${state}" == "failed" ]] && break
    sleep 10; waited=$((waited + 10))
  done
  log "  ${dag} ${run} -> ${state:-missing} after ${waited}s"
  show_tis "${dag}" "${run}"
  echo "${state}" > "${OUT}/.last_state"
}
last_state() { cat "${OUT}/.last_state"; }

# Waits until a task instance reaches a state matching a regex.
wait_ti() {  # wait_ti <dag> <run> <task> <state regex> [timeout seconds]
  local limit="${5:-1800}" waited=0 state=""
  while (( waited < limit )); do
    state="$(ti_state "$1" "$2" "$3")"
    [[ "${state%% *}" =~ ^($4)$ ]] && { log "  $3 ${state}"; return 0; }
    sleep 3; waited=$((waited + 3))
  done
  log "  $3 did not reach $4 (last: ${state:-none})"
  return 1
}

# Waits for the first run of a DAG created at or after a timestamp.
wait_new_run() {  # wait_new_run <dag> <since> [timeout seconds]; prints run id
  local limit="${3:-600}" waited=0 run=""
  while (( waited < limit )); do
    run="$(newest_run "$1" "$2")"
    [[ -n "${run}" ]] && { echo "${run}"; return 0; }
    sleep 5; waited=$((waited + 5))
  done
  return 1
}

# Waits until no transform run is queued or running.
wait_transform_idle() {
  local waited=0
  while (( waited < 3600 )); do
    [[ "$(afq "SELECT count(*) FROM dag_run WHERE dag_id='${TRANSFORM}' AND state IN ('queued','running')")" == "0" ]] && return 0
    sleep 10; waited=$((waited + 10))
  done
  return 1
}

events_consumed() { afq "SELECT count(*) FROM dagrun_asset_event e JOIN dag_run r ON r.id = e.dag_run_id WHERE r.dag_id='${TRANSFORM}' AND r.run_id='$1'"; }

step_runs() {  # step results recorded by medallion_build for one run label
  # medallion_build replaces characters outside [A-Za-z0-9._-] in the label.
  local label="${1//[^A-Za-z0-9._-]/_}"
  pgq "${CATALOG_DB}" "SELECT step, status, round(seconds::numeric, 1) FROM lakehouse_state.step_runs WHERE run_label='${label}' ORDER BY started_at" \
    | while read -r line; do log "    step_runs ${line}"; done
}

trigger() {  # trigger <dag> <run id> [conf json]
  if [[ -n "${3:-}" ]]; then
    airflow dags trigger "$1" --run-id "$2" --conf "$3" >/dev/null
  else
    airflow dags trigger "$1" --run-id "$2" >/dev/null
  fi
  log "  triggered $1 $2"
}

# A scheduled-window DAG triggered without a logical date has no data interval
# in Airflow 3.4, and the extract rejects the empty bounds. With a logical
# date the timetable gives the run its interval (as P3-05 does).
trigger_at_now() {  # trigger_at_now <dag> <run id>
  airflow dags trigger "$1" --run-id "$2" -l "$(now)" >/dev/null
  log "  triggered $1 $2 with logical date now"
}

manual_extract() {  # manual_extract <run id> <window start day> <window end day>
  trigger "${MANUAL}" "$1" \
    "{\"window_start\": \"$2T00:00:00Z\", \"window_end\": \"$3T00:00:00Z\", \"execution_id\": \"$1\"}"
}

# Airflow 3.4 refused ("already exists") a one-day backfill of the day after an
# earlier one-day backfill, so a scenario takes the latest day at or before a
# start day that has no daily run on it or next to it.
free_backfill_day() {  # free_backfill_day <start day>
  local d="$1" prev next
  for _ in $(seq 1 60); do
    prev="$(date -u -d "${d} - 1 day" +%F)"; next="$(date -u -d "${d} + 1 day" +%F)"
    if [[ "$(afq "SELECT count(*) FROM dag_run WHERE dag_id='${DAILY}' AND ((logical_date AT TIME ZONE 'UTC')::date BETWEEN '${prev}' AND '${next}' OR run_id LIKE 'backfill\_\_${d}%' OR run_id LIKE 'backfill\_\_${next}%')")" == 0 ]]; then
      echo "${d}"; return 0
    fi
    d="$(date -u -d "${d} - 2 day" +%F)"
  done
  return 1
}

backfill_daily() {  # backfill_daily <day>; prints the backfill run id
  local since; since="$(now)"
  airflow backfill create --dag-id "${DAILY}" --from-date "$1" --to-date "$1" \
    --reprocess-behavior none >/dev/null
  wait_new_run "${DAILY}" "${since}"
}

# Clears the failed tasks of one run through the REST API (a manual run has no
# logical date, so `airflow tasks clear -s/-e` cannot select it). The admin
# password reaches the container as an environment variable, not an argument.
clear_run() {  # clear_run <dag> <run id>
  AF_USER="${AIRFLOW_ADMIN_USER:-airflow}" AF_PASSWORD="${AIRFLOW_ADMIN_PASSWORD:-odp}" \
    "${COMPOSE[@]}" exec -T -e AF_USER -e AF_PASSWORD airflow-scheduler python - "$1" "$2" <<'EOF'
import json, os, sys, urllib.request

base = "http://airflow-apiserver:8080"
def post(path, body, token=None):
    headers = {"Content-Type": "application/json"}
    if token:
        headers["Authorization"] = f"Bearer {token}"
    req = urllib.request.Request(base + path, data=json.dumps(body).encode(), headers=headers)
    with urllib.request.urlopen(req) as resp:
        return json.load(resp)
token = post("/auth/token", {"username": os.environ["AF_USER"], "password": os.environ["AF_PASSWORD"]})["access_token"]
result = post(f"/api/v2/dags/{sys.argv[1]}/clearTaskInstances",
              {"dry_run": False, "dag_run_id": sys.argv[2], "only_failed": True, "reset_dag_runs": True}, token)
print(len(result.get("task_instances", [])))
EOF
  log "  cleared $1 $2"
  local waited=0
  while [[ "$(run_state "$1" "$2")" == "failed" && ${waited} -lt 120 ]]; do sleep 5; waited=$((waited + 5)); done
}

# --- Faults ---------------------------------------------------------------

oltp_connections() {  # oltp_connections true|false
  pgq postgres "ALTER DATABASE \"${OLTP_DB}\" ALLOW_CONNECTIONS $1" >/dev/null
  log "  fault: ${OLTP_DB} ALLOW_CONNECTIONS $1"
}
trino_stop() { "${COMPOSE[@]}" --profile lakehouse stop trino >/dev/null 2>&1; log "  fault: trino stopped"; }
trino_start() {
  "${COMPOSE[@]}" --profile lakehouse up -d --wait trino >/dev/null 2>&1
  log "  trino started"
}

# Leaves no fault behind when a scenario stops early.
cleanup() {
  pgq postgres "ALTER DATABASE \"${OLTP_DB}\" ALLOW_CONNECTIONS true" >/dev/null 2>&1
  "${COMPOSE[@]}" --profile lakehouse up -d --wait trino >/dev/null 2>&1
  gate_unplant >/dev/null 2>&1
  rm -f "${OUT}/.reader_stop"
}
trap cleanup EXIT

# --- Lakehouse state ------------------------------------------------------

catalog_versions() {  # one line per Iceberg table: name and metadata location
  pgq "${CATALOG_DB}" "SELECT table_namespace || '.' || table_name, metadata_location FROM iceberg_tables ORDER BY 1"
}

silver_counts() {
  local t
  for t in customers order_items orders sellers; do
    echo "silver.postgres_oltp__${t} $(trino "SELECT count(*) FROM iceberg.silver.postgres_oltp__${t}" | tr -d '"')"
  done
}

coverage_report() {
  "${COMPOSE[@]}" --profile lakehouse run --rm -T --no-deps spark \
    python -m open_data_platform.ingestion.coverage report --frequency daily 2>/dev/null
}

# --- Scenarios ------------------------------------------------------------

scenario_catchup() {
  local paused last_run since run rc
  paused="$(afq "SELECT is_paused FROM dag WHERE dag_id='${DAILY}'")"
  last_run="$(afq "SELECT max(data_interval_end) FROM dag_run WHERE dag_id='${DAILY}' AND run_type='scheduled'")"
  log "daily paused=${paused}; last scheduled interval end ${last_run}"
  [[ "${paused}" == "t" ]] || { log "FAIL the daily DAG must be paused"; FAILURES=$((FAILURES + 1)); return; }

  oltp_connections false
  since="$(now)"
  airflow dags unpause "${DAILY}" >/dev/null
  log "  unpaused ${DAILY}"
  run="$(wait_new_run "${DAILY}" "${since}" 300)"
  require "daily catch-up run" "${run}" || return
  log "  created ${run} ($(afq "SELECT run_type, data_interval_start, data_interval_end FROM dag_run WHERE dag_id='${DAILY}' AND run_id='${run}'"))"
  check "the missed day gets a scheduled run" test "$(afq "SELECT run_type FROM dag_run WHERE dag_id='${DAILY}' AND run_id='${run}'")" == scheduled
  check "only one run is created for the missed day" test "$(runs_since "${DAILY}" "${since}")" == 1
  wait_ti "${DAILY}" "${run}" settle "success" 1800
  log "  settle finished at $(afq "SELECT end_date FROM task_instance WHERE dag_id='${DAILY}' AND run_id='${run}' AND task_id='settle'") (window end + 15 min)"
  wait_run "${DAILY}" "${run}" 1800
  check "the run fails once every extract try failed" test "$(last_state)" == failed
  check "the extract used three tries" test "$(ti_state "${DAILY}" "${run}" extract)" == "failed 3"
  local start end
  read -r start end <<<"$(afq "SELECT to_char(data_interval_start AT TIME ZONE 'UTC','YYYY-MM-DD\"T\"HH24:MI:SS'), to_char(data_interval_end AT TIME ZONE 'UTC','YYYY-MM-DD\"T\"HH24:MI:SS') FROM dag_run WHERE dag_id='${DAILY}' AND run_id='${run}'")"
  check "no coverage row for the failed window" test "$(pgq "${CATALOG_DB}" "SELECT count(*) FROM ingestion_state.extract_coverage WHERE frequency='daily' AND window_start='${start}+00' AND window_end='${end}+00'")" == 0
  check "the failed run emits no Asset event (no new transform run)" test "$(runs_since "${TRANSFORM}" "${since}")" == 0
  coverage_report > "${OUT}/catchup-gap-report.txt"; rc=$?
  sed 's/^/    report /' "${OUT}/catchup-gap-report.txt" | tee -a "${OUT}/${SCENARIO}.log" >/dev/null
  check "the gap report exits 1 and lists the failed window" test "${rc}" == 1 -a -n "$(grep -F "gap [${start%%T*}," "${OUT}/catchup-gap-report.txt")"

  oltp_connections true
  since="$(now)"
  clear_run "${DAILY}" "${run}"
  wait_run "${DAILY}" "${run}" 1800
  check "the cleared run succeeds under the same run id" test "$(last_state)" == success
  coverage_report > "${OUT}/catchup-report-after.txt"; rc=$?
  sed 's/^/    report /' "${OUT}/catchup-report-after.txt" | tee -a "${OUT}/${SCENARIO}.log" >/dev/null
  check "the gap report is clean after the clear" test "${rc}" == 0
  local tr; tr="$(wait_new_run "${TRANSFORM}" "${since}" 300)"
  require "transform run after the clear" "${tr}" || return
  wait_run "${TRANSFORM}" "${tr}"
  check "the extract is followed by one successful refresh" test "$(last_state)" == success
  step_runs "${tr}"
}

scenario_happy() {
  local since run tr logical
  wait_transform_idle
  since="$(now)"
  manual_extract "p309-happy-manual-${TAG}" 2026-10-19 2026-10-20
  wait_run "${MANUAL}" "p309-happy-manual-${TAG}"
  check "manual extract succeeds" test "$(last_state)" == success
  tr="$(wait_new_run "${TRANSFORM}" "${since}")"
  require "transform run after the extract" "${tr}" || return
  wait_run "${TRANSFORM}" "${tr}"
  check "the Asset event starts a refresh that succeeds" test "$(last_state)" == success
  step_runs "${tr}"

  wait_transform_idle
  since="$(now)"
  run="$(backfill_daily "$(free_backfill_day 2026-10-10)")"
  require "daily backfill run" "${run}" || return
  wait_run "${DAILY}" "${run}"
  check "daily backfill run succeeds" test "$(last_state)" == success
  tr="$(wait_new_run "${TRANSFORM}" "${since}")"
  require "transform run after the extract" "${tr}" || return
  wait_run "${TRANSFORM}" "${tr}"
  check "daily run is followed by a successful refresh" test "$(last_state)" == success

  wait_transform_idle
  since="$(now)"
  trigger_at_now "${WEEKLY}" "p309-happy-weekly-${TAG}"
  wait_run "${WEEKLY}" "p309-happy-weekly-${TAG}"
  check "manual weekly run succeeds" test "$(last_state)" == success
  log "  weekly window: $(afq "SELECT data_interval_start, data_interval_end FROM dag_run WHERE dag_id='${WEEKLY}' AND run_id='p309-happy-weekly-${TAG}'")"
  tr="$(wait_new_run "${TRANSFORM}" "${since}")"
  require "transform run after the extract" "${tr}" || return
  wait_run "${TRANSFORM}" "${tr}"
  check "weekly run is followed by a successful refresh" test "$(last_state)" == success

  wait_transform_idle
  trigger "${RECONCILE}" "p309-happy-reconcile-${TAG}"
  wait_run "${RECONCILE}" "p309-happy-reconcile-${TAG}"
  check "reconcile_gold succeeds" test "$(last_state)" == success
}

scenario_retry() {
  local since run tr
  wait_transform_idle
  oltp_connections false
  since="$(now)"
  run="$(backfill_daily "$(free_backfill_day 2026-10-06)")"
  require "daily backfill run" "${run}" || return
  wait_ti "${DAILY}" "${run}" extract "up_for_retry"
  oltp_connections true
  wait_run "${DAILY}" "${run}"
  check "daily extract succeeds on try 2" test "$(ti_state "${DAILY}" "${run}" extract)" == "success 2"
  local exec_id="${DAILY}__${run}"
  tr="$(wait_new_run "${TRANSFORM}" "${since}")"
  require "transform run after the extract" "${tr}" || return

  # Trino down during the first dbt_build try of the refresh that follows.
  wait_ti "${TRANSFORM}" "${tr}" dbt_build "running|skipped|upstream_failed" 3600
  if [[ "$(ti_state "${TRANSFORM}" "${tr}" dbt_build)" == running* ]]; then
    trino_stop
    wait_ti "${TRANSFORM}" "${tr}" dbt_build "up_for_retry|failed"
    trino_start
  fi
  wait_run "${TRANSFORM}" "${tr}"
  check "refresh succeeds after the dbt_build retry" test "$(last_state)" == success
  check "dbt_build succeeded on try 2" test "$(ti_state "${TRANSFORM}" "${tr}" dbt_build)" == "success 2"
  step_runs "${tr}"
  local prefixes
  prefixes="$(pgq "${CATALOG_DB}" "SELECT count(DISTINCT substring(raw_object_key from 'dt=[0-9-]+')) FROM lakehouse_state.bronze_promotions WHERE raw_object_key LIKE '%${exec_id}%'")"
  log "  raw dt= prefixes for ${exec_id}: ${prefixes}"
  check "both extract tries wrote under one dt= prefix" test "${prefixes}" == 1
}

scenario_reconcile() {
  local run="p309-reconcile-retry-${TAG}"
  wait_transform_idle
  trino_stop
  trigger "${RECONCILE}" "${run}"
  wait_ti "${RECONCILE}" "${run}" dbt_reconcile "up_for_retry"
  trino_start
  wait_run "${RECONCILE}" "${run}"
  check "reconcile succeeds on try 2" test "$(ti_state "${RECONCILE}" "${run}" dbt_reconcile)" == "success 2"

  run="p309-reconcile-fail-${TAG}"
  trino_stop
  trigger "${RECONCILE}" "${run}"
  wait_run "${RECONCILE}" "${run}"
  check "reconcile run fails after three tries" test "$(ti_state "${RECONCILE}" "${run}" dbt_reconcile)" == "failed 3"
  trino_start
  clear_run "${RECONCILE}" "${run}"
  wait_run "${RECONCILE}" "${run}"
  check "the cleared reconcile run succeeds" test "$(last_state)" == success
}

# PID of the Silver step's python child in the scheduler container (it leads
# its own process group), or empty.
silver_pid() {
  "${COMPOSE[@]}" exec -T airflow-scheduler bash -c '
    for p in /proc/[0-9]*; do
      cmd=$(tr "\0" " " < "$p/cmdline" 2>/dev/null) || continue
      case "$cmd" in python*"medallion_build silver --run-label"*) echo "${p#/proc/}"; exit ;; esac
    done'
}
group_has_java() {
  "${COMPOSE[@]}" exec -T airflow-scheduler bash -c '
    for p in /proc/[0-9]*; do
      read -r -a st < "$p/stat" 2>/dev/null || continue
      [ "${st[4]}" = "'"$1"'" ] && grep -q java "$p/cmdline" 2>/dev/null && exit 0
    done; exit 1'
}

scenario_write_failure() {
  local since tr pid before after reader_pid
  wait_transform_idle
  catalog_versions > "${OUT}/write-before.txt"
  silver_counts > "${OUT}/write-counts-before.txt"
  since="$(now)"
  manual_extract "p309-write-${TAG}" 2026-10-19 2026-10-20
  tr="$(wait_new_run "${TRANSFORM}" "${since}")"
  require "transform run after the extract" "${tr}" || return
  wait_ti "${TRANSFORM}" "${tr}" silver "running" 3600

  # Readers: count every Silver PostgreSQL table until the write finishes.
  : > "${OUT}/write-reader.txt"
  (
    while [[ ! -f "${OUT}/.reader_stop" ]]; do
      silver_counts | while read -r t c; do
        echo "$(date -u +%H:%M:%S) ${t} ${c:-ERROR}" >> "${OUT}/write-reader.txt"
      done
      sleep 1
    done
  ) &
  reader_pid=$!

  pid=""
  for _ in $(seq 1 120); do
    pid="$(silver_pid)"
    [[ -n "${pid}" ]] && group_has_java "${pid}" && break
    sleep 1
  done
  require "silver process group with a Spark JVM" "${pid}" || { touch "${OUT}/.reader_stop"; return; }
  sleep 5
  catalog_versions > "${OUT}/write-at-kill.txt"
  "${COMPOSE[@]}" exec -T airflow-scheduler bash -c "kill -TERM -- -${pid}"
  log "  fault: SIGTERM to the silver process group ${pid}"
  wait_ti "${TRANSFORM}" "${tr}" silver "up_for_retry|failed"
  catalog_versions > "${OUT}/write-after-kill.txt"
  silver_counts > "${OUT}/write-counts-after-kill.txt"
  local changed
  changed="$(diff "${OUT}/write-before.txt" "${OUT}/write-after-kill.txt" | grep -c '^>' || true)"
  log "  Iceberg tables with a new version after the kill: ${changed}"
  grep '^silver.postgres' "${OUT}/write-after-kill.txt" | while read -r t loc; do
    if grep -qF "${t} ${loc}" "${OUT}/write-before.txt"; then
      log "    ${t} unchanged (previous version)"
    else
      log "    ${t} new version committed before the kill"
    fi
  done
  check "Silver counts after the failed write equal the previous counts" \
    diff -q "${OUT}/write-counts-before.txt" "${OUT}/write-counts-after-kill.txt"

  wait_run "${TRANSFORM}" "${tr}"
  touch "${OUT}/.reader_stop"; wait "${reader_pid}" 2>/dev/null; rm -f "${OUT}/.reader_stop"
  check "the refresh succeeds after the Silver retry" test "$(last_state)" == success
  check "silver succeeded on try 2" test "$(ti_state "${TRANSFORM}" "${tr}" silver)" == "success 2"
  step_runs "${tr}"
  local reads errors
  reads="$(wc -l < "${OUT}/write-reader.txt")"
  errors="$(grep -cE ' (ERROR|0)$' "${OUT}/write-reader.txt" || true)"
  log "  reader queries ${reads}, failed or empty ${errors}"
  check "no reader query failed or saw an empty table" test "${errors}" == 0 -a "${reads}" -gt 0
}

scenario_no_new_raw() {
  local run="p309-no-new-raw-${TAG}"
  wait_transform_idle
  catalog_versions > "${OUT}/no-new-raw-before.txt"
  trigger "${TRANSFORM}" "${run}"
  wait_run "${TRANSFORM}" "${run}"
  catalog_versions > "${OUT}/no-new-raw-after.txt"
  check "the refresh run succeeds" test "$(last_state)" == success
  check "every write step is skipped" test "$(afq "SELECT count(*) FROM task_instance WHERE dag_id='${TRANSFORM}' AND run_id='${run}' AND task_id IN ('bronze','silver','stage_quarantine','dbt_build') AND state='skipped'")" == 4
  check "no Iceberg table gets a new version" diff -q "${OUT}/no-new-raw-before.txt" "${OUT}/no-new-raw-after.txt"
  log "  tables compared: $(wc -l < "${OUT}/no-new-raw-after.txt")"
  step_runs "${run}"
}

scenario_coalesce() {
  local since first follow n
  wait_transform_idle
  since="$(now)"
  manual_extract "p309-coalesce-a-${TAG}" 2026-10-19 2026-10-20
  first="$(wait_new_run "${TRANSFORM}" "${since}")"
  require "first transform run" "${first}" || return
  log "  first refresh ${first} created"
  manual_extract "p309-coalesce-b-${TAG}" 2026-10-17 2026-10-18
  manual_extract "p309-coalesce-c-${TAG}" 2026-10-19 2026-10-20
  wait_run "${MANUAL}" "p309-coalesce-b-${TAG}"
  wait_run "${MANUAL}" "p309-coalesce-c-${TAG}"
  local first_state; first_state="$(run_state "${TRANSFORM}" "${first}")"
  log "  first refresh ${first_state} when both extracts finished"
  check "both extracts landed while the first refresh was queued or running" \
    test "${first_state}" == running -o "${first_state}" == queued
  wait_run "${TRANSFORM}" "${first}"
  wait_transform_idle
  sleep 30
  wait_transform_idle
  n="$(runs_since "${TRANSFORM}" "${since}")"
  follow="$(newest_run "${TRANSFORM}" "${since}")"
  log "  transform runs since the first extract: ${n}"
  check "two extracts during one refresh give one follow-up run" test "${n}" == 2
  check "the follow-up consumed both Asset events" test "$(events_consumed "${follow}")" == 2
  check "the follow-up refresh succeeds" test "$(run_state "${TRANSFORM}" "${follow}")" == success
  step_runs "${follow}"
}

GATE_WHERE="layer='bronze' AND source_id='postgres_oltp' AND dataset='orders'"
gate_plant() {
  pgq "${CATALOG_DB}" "CREATE OR REPLACE FUNCTION lakehouse_state.p309_plant_mismatch() RETURNS trigger
    LANGUAGE plpgsql AS \$\$ BEGIN NEW.expected_rows := NEW.expected_rows + 1; RETURN NEW; END \$\$" >/dev/null
  pgq "${CATALOG_DB}" "DROP TRIGGER IF EXISTS p309_plant_mismatch ON lakehouse_state.layer_checks" >/dev/null
  # BEFORE UPDATE only: the mark is an INSERT ... ON CONFLICT DO UPDATE, which
  # fires a BEFORE INSERT trigger and then a BEFORE UPDATE trigger on EXCLUDED,
  # so a trigger on both adds two rows. The orders row exists (checked below).
  pgq "${CATALOG_DB}" "CREATE TRIGGER p309_plant_mismatch BEFORE UPDATE ON lakehouse_state.layer_checks
    FOR EACH ROW WHEN (NEW.layer='bronze' AND NEW.source_id='postgres_oltp' AND NEW.dataset='orders' AND NEW.status='pending')
    EXECUTE FUNCTION lakehouse_state.p309_plant_mismatch()" >/dev/null
  log "  fault: Bronze orders check expects one row more than written"
}
gate_unplant() {
  pgq "${CATALOG_DB}" "DROP TRIGGER IF EXISTS p309_plant_mismatch ON lakehouse_state.layer_checks" >/dev/null
  pgq "${CATALOG_DB}" "DROP FUNCTION IF EXISTS lakehouse_state.p309_plant_mismatch()" >/dev/null
}
gate_state() { pgq "${CATALOG_DB}" "SELECT status, marked_by_run, coalesce(checked_by_run, '-') FROM lakehouse_state.layer_checks WHERE ${GATE_WHERE}"; }

scenario_gate() {
  local since r1 r2="p309-gate-r2-${TAG}" r3="p309-gate-r3-${TAG}" silver_before
  wait_transform_idle
  require "an existing Bronze orders check row" "$(gate_state)" || return
  gate_plant
  since="$(now)"
  manual_extract "p309-gate-${TAG}" 2026-10-19 2026-10-20
  r1="$(wait_new_run "${TRANSFORM}" "${since}")"
  require "transform run r1" "${r1}" || return
  wait_run "${TRANSFORM}" "${r1}"
  check "r1 fails" test "$(last_state)" == failed
  check "r1 bronze_checks failed on its only try" test "$(ti_state "${TRANSFORM}" "${r1}" bronze_checks)" == "failed 1"
  check "r1 silver is upstream_failed" test "$(ti_state "${TRANSFORM}" "${r1}" silver | cut -d' ' -f1)" == upstream_failed
  log "  check: $(gate_state)"
  step_runs "${r1}"

  silver_before="$(pgq "${CATALOG_DB}" "SELECT metadata_location FROM iceberg_tables WHERE table_namespace='silver' AND table_name='postgres_oltp__orders'")"
  trigger "${TRANSFORM}" "${r2}"
  wait_run "${TRANSFORM}" "${r2}"
  check "r2 (no new raw) fails again on the open check" test "$(ti_state "${TRANSFORM}" "${r2}" bronze_checks)" == "failed 1"
  check "r2 does not build Silver" test "$(ti_state "${TRANSFORM}" "${r2}" silver | cut -d' ' -f1)" == upstream_failed
  check "Silver orders keeps its version" test "$(pgq "${CATALOG_DB}" "SELECT metadata_location FROM iceberg_tables WHERE table_namespace='silver' AND table_name='postgres_oltp__orders'")" == "${silver_before}"
  log "  check: $(gate_state)"
  step_runs "${r2}"

  gate_unplant
  pgq "${CATALOG_DB}" "UPDATE lakehouse_state.layer_checks SET expected_rows = expected_rows - 1 WHERE ${GATE_WHERE} AND status='failed'" >/dev/null
  log "  plant removed"
  trigger "${TRANSFORM}" "${r3}"
  wait_run "${TRANSFORM}" "${r3}"
  check "r3 succeeds" test "$(last_state)" == success
  check "r3 bronze_checks passes" test "$(ti_state "${TRANSFORM}" "${r3}" bronze_checks | cut -d' ' -f1)" == success
  check "r3 builds Silver" test "$(ti_state "${TRANSFORM}" "${r3}" silver | cut -d' ' -f1)" == success
  log "  check: $(gate_state)"
  step_runs "${r3}"
}

[[ $# -gt 0 ]] || { sed -n '2,32p' "$0"; exit 2; }
for SCENARIO in "$@"; do
  fn="scenario_${SCENARIO//-/_}"
  declare -F "${fn}" >/dev/null || { echo "unknown scenario: ${SCENARIO}"; exit 2; }
  : > "${OUT}/${SCENARIO}.log"
  log "=== ${SCENARIO} (tag ${TAG})"
  start=$(date +%s)
  "${fn}"
  log "=== ${SCENARIO} done in $(( $(date +%s) - start ))s, failures so far ${FAILURES}"
done
exit $(( FAILURES > 0 ))
