#!/usr/bin/env bash
# P3-16 live evidence: trigger guard, then a refresh pair with a planted
# Bronze check failure, then recovery. Prints step status, durations, and
# registry rows (names, statuses, counts only).
# Local stack only: it runs apply-schema on POSTGRES_DB, lands a manual
# PostgreSQL window [2026-10-19, 2026-10-20) under a new execution id, and
# edits one expected count in lakehouse_state.layer_checks, then restores it.
# Usage: scripts/p3_16_gate_check.sh [log dir]
set -u
cd "$(dirname "$0")/.."
set -a; . ./.env.example; [ -f .env ] && . ./.env; set +a
LOG_DIR=${1:-.local/p3-16-logs}
mkdir -p "$LOG_DIR"
RUN="docker compose --profile lakehouse run --rm -T spark python -m"
TAG=$(date -u +%Y%m%dT%H%M%S)
EXEC="p316-live-$TAG"

psqlc() { docker exec odp-postgres psql -U "$POSTGRES_USER" -d "$1" -Atc "$2"; }
checks() {
  echo "  layer_checks:"
  psqlc iceberg "select layer, source_id||'.'||dataset, status, coalesce(expected_rows::text,'-'), marked_by_run, coalesce(checked_by_run,'-') from lakehouse_state.layer_checks where source_id='olist_synth_cdc' order by 1,2" | sed 's/^/    /'
  echo "  pending_layers: $(psqlc iceberg "select string_agg(layer||':'||source_id||'.'||dataset, ' ' order by 1) from lakehouse_state.pending_layers")"
}
run() {  # run <name> <module> <args...>
  local name=$1; shift
  local start=$(date +%s)
  $RUN "$@" > "$LOG_DIR/$name.log" 2>&1
  local rc=$?
  echo "== $name rc=$rc seconds=$(( $(date +%s) - start ))"
  grep -E "^(bronze|silver|stage-quarantine|dbt-build)|check|blocked|stage=(extract_finished|schema_applied|failed)|error=" "$LOG_DIR/$name.log" \
    | grep -vE "^SELECT|-> " | tail -6 | sed 's/^/  /'
}
step() {  # step <run-label> <medallion step>
  run "$1-$2" open_data_platform.lakehouse.medallion_build "$2" --run-label "$1"
}
extract() {
  run "extract-$1" open_data_platform.ingestion.olist_synth_cdc extract \
    --window-start 2026-10-19T00:00:00Z --window-end 2026-10-20T00:00:00Z \
    --execution-id "$EXEC" --ingestion-ts 2026-11-22T00:00:00Z
}

docker compose --profile lakehouse up -d --wait trino > /dev/null 2>&1

echo "### 0. extract on the old schema (no triggers)"
extract before-schema
run apply-schema open_data_platform.ingestion.olist_synth_cdc apply-schema
echo "  triggers: $(psqlc "$POSTGRES_DB" "select count(*) from pg_trigger where tgname='set_updated_at'")"
extract after-schema

echo "### 1. run r1: Bronze writes; planted failure on orders"
step "p316-r1-$TAG" bronze
psqlc iceberg "update lakehouse_state.layer_checks set expected_rows = expected_rows + 1 where layer='bronze' and source_id='olist_synth_cdc' and dataset='orders' and status='pending'" > /dev/null
step "p316-r1-$TAG" bronze-checks
checks

echo "### 2. run r2: no new raw object"
step "p316-r2-$TAG" bronze
step "p316-r2-$TAG" bronze-checks
step "p316-r2-$TAG" silver
step "p316-r2-$TAG" dbt-build
checks

echo "### 3. run r3: planted value removed"
psqlc iceberg "update lakehouse_state.layer_checks set expected_rows = expected_rows - 1 where layer='bronze' and source_id='olist_synth_cdc' and dataset='orders'" > /dev/null
for s in bronze bronze-checks silver silver-checks stage-quarantine dbt-build; do
  step "p316-r3-$TAG" "$s"
done
checks

echo "### 4. run r4: nothing new, nothing open"
step "p316-r4-$TAG" bronze
step "p316-r4-$TAG" bronze-checks
