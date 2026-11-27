# Shared helpers for the live orchestration checks (P3-09, P3-14).
# Source it after setting OUT (output directory) from the repository root:
#
#   cd "$(dirname "${BASH_SOURCE[0]}")/.."; OUT=.local/<issue>; mkdir -p "${OUT}"
#   . scripts/lib/live_check.sh
#
# The caller sets SCENARIO before each scenario; log() appends to
# ${OUT}/${SCENARIO}.log. check() counts failures in FAILURES. The helpers read
# the Airflow metadata database, the Iceberg JDBC catalog, and Trino, and print
# states, counts, names, and durations only, never row values.
# shellcheck shell=bash

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
SCENARIO="${SCENARIO:-setup}"
FAILURES="${FAILURES:-0}"

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
clear_run() {  # clear_run <dag> <run id> [only failed: true|false, default true]
  AF_USER="${AIRFLOW_ADMIN_USER:-airflow}" AF_PASSWORD="${AIRFLOW_ADMIN_PASSWORD:-odp}" \
    "${COMPOSE[@]}" exec -T -e AF_USER -e AF_PASSWORD airflow-scheduler python - "$1" "$2" "${3:-true}" <<'EOF'
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
              {"dry_run": False, "dag_run_id": sys.argv[2], "only_failed": sys.argv[3] == "true", "reset_dag_runs": True}, token)
print(len(result.get("task_instances", [])))
EOF
  log "  cleared $1 $2"
  local waited=0
  while [[ "$(run_state "$1" "$2")" =~ ^(failed|success)$ && ${waited} -lt 120 ]]; do sleep 5; waited=$((waited + 5)); done
}

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

# A manual refresh with nothing pending: every write step skips and no Iceberg
# table gets a new version.
no_new_raw_refresh() {  # no_new_raw_refresh <run id>
  local run="$1"
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
