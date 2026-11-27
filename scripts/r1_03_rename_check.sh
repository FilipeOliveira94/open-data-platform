#!/usr/bin/env bash
# R1-03 live migration check: postgres_oltp -> olist_synth_cdc.
#
#   bash scripts/r1_03_rename_check.sh counts old   # before the rename
#   bash scripts/r1_03_rename_check.sh rename-pg-schema  # oltp -> olist_synth
#   (then apply-schema, scripts/r1_03_copy_raw.py, and a transform_lakehouse run)
#   bash scripts/r1_03_rename_check.sh counts new after-copy  # after the copy
#   bash scripts/r1_03_rename_check.sh compare copy           # compare-copy.tsv
#   bash scripts/r1_03_rename_check.sh counts new   # after the re-extract
#   bash scripts/r1_03_rename_check.sh compare      # writes compare.tsv
#   bash scripts/r1_03_rename_check.sh delete --dry-run
#   bash scripts/r1_03_rename_check.sh delete       # only after compare passed
#   bash scripts/r1_03_rename_check.sh leftovers    # what still has the old name
#
# Output goes to .local/r1-03/ and holds counts, names, and paths only, never
# row values (AGENTS.md §8). The MinIO credentials reach `mc` through the
# MC_HOST_odp environment variable, not through an argument.
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."
OUT="${R1_03_OUT:-.local/r1-03}"
mkdir -p "${OUT}"
# shellcheck disable=SC1091
. scripts/lib/live_check.sh

OLD_SOURCE=postgres_oltp
NEW_SOURCE=olist_synth_cdc
OLD_PG_SCHEMA=oltp
NEW_PG_SCHEMA=olist_synth
OLD_GOLD=oltp_operations
NEW_GOLD=olist_synth_operations
OLD_DAGS=(ingest_postgres_oltp ingest_postgres_oltp_daily ingest_postgres_oltp_weekly seed_postgres_oltp)
DATASETS=(customers order_items orders sellers)
RAW_BUCKET="${ODP_RAW_BUCKET:-odp-raw}"
WAREHOUSE_BUCKET="${ODP_WAREHOUSE_BUCKET:-odp-warehouse}"

export MC_HOST_odp="http://${MINIO_ROOT_USER}:${MINIO_ROOT_PASSWORD}@localhost:${MINIO_API_PORT:-9000}"
mcx() { docker exec -e MC_HOST_odp odp-minio mc "$@"; }

# Counts objects under one prefix; an absent prefix counts 0.
objects() {  # objects <bucket/prefix> [name regex]
  { mcx ls -r "odp/$1" 2>/dev/null || true; } | awk '{print $NF}' | { grep -cE "${2:-.}" || true; }
}

# One Gold table name on the other side of the rename.
gold_name() {  # gold_name <old|new> <table>
  if [[ "$1" == new ]]; then echo "${2//_oltp_/_olist_synth_}"; else echo "$2"; fi
}

side_names() {  # sets SRC PG GOLD for a side
  case "$1" in
    old) SRC="${OLD_SOURCE}"; PG="${OLD_PG_SCHEMA}"; GOLD="${OLD_GOLD}" ;;
    new) SRC="${NEW_SOURCE}"; PG="${NEW_PG_SCHEMA}"; GOLD="${NEW_GOLD}" ;;
    *) echo "side must be old or new" >&2; exit 2 ;;
  esac
}

# Prints "<kind>\t<name>\t<count>". Names are written with the old source id,
# schema, and Gold names on both sides, so compare can join the two files.
counts() {
  side_names "$1"
  local ds t tables
  for ds in "${DATASETS[@]}"; do
    printf 'postgres\t%s\t%s\n' "${ds}" "$(pgq "${OLIST_SYNTH_DB}" "SELECT count(*) FROM ${PG}.${ds}" 2>/dev/null || echo absent)"
    printf 'raw_windows\t%s\t%s\n' "${ds}" "$(objects "${RAW_BUCKET}/raw/${SRC}/${ds}/" '\.json$')"
    printf 'raw_complete\t%s\t%s\n' "${ds}" "$(objects "${RAW_BUCKET}/raw/${SRC}/${ds}/" '_COMPLETE$')"
    printf 'quarantine_objects\t%s\t%s\n' "${ds}" "$(objects "${RAW_BUCKET}/quarantine/${SRC}/${ds}/")"
    for t in bronze silver; do
      printf '%s\t%s\t%s\n' "${t}" "${ds}" "$(trino "SELECT count(*) FROM iceberg.${t}.${SRC}__${ds}" | tr -d '"' || echo absent)"
    done
    printf 'silver_audit\t%s\t%s\n' "${ds}" "$(trino "SELECT count(*) FROM iceberg.silver_audit.quality_counts WHERE source_id = '${SRC}' AND dataset = '${ds}'" | tr -d '"')"
    printf 'promotions\t%s\t%s\n' "${ds}" "$(pgq "${CATALOG_DB}" "SELECT count(*) FROM lakehouse_state.bronze_promotions WHERE source_id = '${SRC}' AND dataset = '${ds}'")"
    for t in pending_layers bronze_refreshes layer_checks; do
      printf '%s\t%s\t%s\n' "${t}" "${ds}" "$(pgq "${CATALOG_DB}" "SELECT count(*) FROM lakehouse_state.${t} WHERE source_id = '${SRC}' AND dataset = '${ds}'")"
    done
    printf 'layer_checks_passed\t%s\t%s\n' "${ds}" "$(pgq "${CATALOG_DB}" "SELECT count(*) FROM lakehouse_state.layer_checks WHERE source_id = '${SRC}' AND dataset = '${ds}' AND status = 'passed'")"
    for t in daily weekly manual; do
      printf 'coverage_%s\t%s\t%s\n' "${t}" "${ds}" "$(pgq "${CATALOG_DB}" "SELECT count(DISTINCT (window_start, window_end)) FROM ingestion_state.extract_coverage WHERE source_id = '${SRC}' AND dataset = '${ds}' AND frequency = '${t}'")"
    done
  done
  tables="$(pgq "${CATALOG_DB}" "SELECT table_name FROM iceberg_tables WHERE table_namespace = '${GOLD}' ORDER BY 1")"
  for t in ${tables}; do
    printf 'gold\t%s\t%s\n' "$(gold_name old "${t/_olist_synth_/_oltp_}")" "$(trino "SELECT count(*) FROM iceberg.${GOLD}.${t}" | tr -d '"')"
  done
  printf 'gold_tables\t%s\t%s\n' "${OLD_GOLD}" "$(echo "${tables}" | grep -c . || true)"
  printf 'warehouse_objects\tbronze\t%s\n' "$(objects "${WAREHOUSE_BUCKET}/bronze/${SRC}/")"
  printf 'warehouse_objects\tsilver\t%s\n' "$(objects "${WAREHOUSE_BUCKET}/silver/${SRC}/")"
  printf 'warehouse_objects\tgold\t%s\n' "$(objects "${WAREHOUSE_BUCKET}/gold/${GOLD}/")"
}

# Pass rules. Equal: the source, Silver, Gold, and the passed quality checks.
# Present: the new side has raw windows, a complete marker per window, Bronze
# rows, promotions, and a daily coverage ledger. In copy mode (after the raw
# copy, before any new extract) raw windows, markers, and Bronze rows must
# also be equal, and the ledger is not yet expected.
compare() {
  local mode="${1:-final}" old="${OUT}/before.tsv" new="${OUT}/after.tsv" result="${OUT}/compare.tsv"
  if [[ "${mode}" == copy ]]; then new="${OUT}/after-copy.tsv"; result="${OUT}/compare-copy.tsv"; fi
  [[ -s "${old}" && -s "${new}" ]] || { echo "before.tsv and after.tsv are required" >&2; exit 1; }
  join -t $'\t' -a1 -a2 -e missing -o 0,1.2,2.2 \
    <(awk -F'\t' '{print $1":"$2"\t"$3}' "${old}" | sort) \
    <(awk -F'\t' '{print $1":"$2"\t"$3}' "${new}" | sort) \
    | awk -F'\t' -v mode="${mode}" 'BEGIN { OFS = "\t" }
        {
          split($1, k, ":"); kind = k[1]; rule = "report"; status = "info"
          equal = "^(postgres|silver|gold|gold_tables|layer_checks_passed)$"
          present = "^(raw_windows|raw_complete|coverage_daily|bronze|promotions)$"
          if (mode == "copy") {
            equal = "^(postgres|silver|gold|gold_tables|layer_checks_passed|raw_windows|raw_complete|bronze)$"
            present = "^(promotions)$"
          }
          if (kind ~ equal) {
            rule = "equal"; status = ($2 == $3 && $3 != "missing" && $3 != "absent") ? "PASS" : "FAIL"
          } else if (kind ~ present) {
            rule = "present"; status = ($3 + 0 > 0) ? "PASS" : "FAIL"
          }
          print $1, $2, $3, rule, status
        }' > "${result}"
  column -t -s $'\t' "${result}"
  if grep -q $'\tFAIL$' "${result}"; then echo "compare: FAIL"; return 1; fi
  echo "compare: PASS"
}

run() {  # run <description> <command...>; prints the step, runs it unless dry
  echo "  ${1}"
  shift
  [[ "${DRY_RUN}" == 1 ]] || "$@"
}

# Removes a prefix only when it holds objects, so a second run is a no-op.
remove_prefix() {  # remove_prefix <bucket/prefix>
  if [[ "$(objects "$1")" != 0 ]]; then
    run "remove $1" mcx rm -r --force "odp/$1"
  fi
}

# Renames the PostgreSQL schema in place: rows, keys, and triggers move with
# it. A second run finds no old schema and does nothing.
rename_pg_schema() {
  if [[ "$(pgq "${OLIST_SYNTH_DB}" "SELECT count(*) FROM information_schema.schemata WHERE schema_name = '${OLD_PG_SCHEMA}'")" == 0 ]]; then
    echo "schema ${OLD_PG_SCHEMA} is absent; nothing to rename"
    return 0
  fi
  pgq "${OLIST_SYNTH_DB}" "ALTER SCHEMA ${OLD_PG_SCHEMA} RENAME TO ${NEW_PG_SCHEMA}"
  echo "schema ${OLD_PG_SCHEMA} renamed to ${NEW_PG_SCHEMA}"
}

delete_old() {
  DRY_RUN=0
  [[ "${1:-}" == --dry-run ]] && DRY_RUN=1
  if [[ "${DRY_RUN}" == 0 ]]; then
    [[ -s "${OUT}/compare.tsv" ]] || { echo "refusing: ${OUT}/compare.tsv is missing" >&2; exit 1; }
    if grep -q $'\tFAIL$' "${OUT}/compare.tsv"; then echo "refusing: compare.tsv has a FAIL line" >&2; exit 1; fi
    # A stale PASS must not unlock the delete: compare.tsv is newer than the
    # after counts, and those are newer than the before counts.
    if [[ ! "${OUT}/compare.tsv" -nt "${OUT}/after.tsv" || ! "${OUT}/after.tsv" -nt "${OUT}/before.tsv" ]]; then
      echo "refusing: compare.tsv is older than after.tsv, or after.tsv older than before.tsv" >&2; exit 1
    fi
  fi
  local ds t tables
  echo "Iceberg tables"
  for ds in "${DATASETS[@]}"; do
    for t in bronze silver; do
      run "DROP TABLE IF EXISTS iceberg.${t}.${OLD_SOURCE}__${ds}" trino "DROP TABLE IF EXISTS iceberg.${t}.${OLD_SOURCE}__${ds}"
    done
  done
  tables="$(pgq "${CATALOG_DB}" "SELECT table_name FROM iceberg_tables WHERE table_namespace = '${OLD_GOLD}' ORDER BY 1")"
  for t in ${tables}; do
    run "DROP TABLE IF EXISTS iceberg.${OLD_GOLD}.${t}" trino "DROP TABLE IF EXISTS iceberg.${OLD_GOLD}.${t}"
  done
  run "DROP SCHEMA IF EXISTS iceberg.${OLD_GOLD}" trino "DROP SCHEMA IF EXISTS iceberg.${OLD_GOLD}"
  echo "Rows keyed by the old source id"
  run "DELETE silver_audit.quality_counts rows" trino "DELETE FROM iceberg.silver_audit.quality_counts WHERE source_id = '${OLD_SOURCE}'"
  for t in bronze_promotions pending_layers bronze_refreshes layer_checks; do
    run "DELETE lakehouse_state.${t} rows" pgq "${CATALOG_DB}" "DELETE FROM lakehouse_state.${t} WHERE source_id = '${OLD_SOURCE}'"
  done
  run "DELETE ingestion_state.extract_coverage rows" pgq "${CATALOG_DB}" "DELETE FROM ingestion_state.extract_coverage WHERE source_id = '${OLD_SOURCE}'"
  echo "Object prefixes"
  remove_prefix "${RAW_BUCKET}/raw/${OLD_SOURCE}/"
  remove_prefix "${RAW_BUCKET}/quarantine/${OLD_SOURCE}/"
  for t in "bronze/${OLD_SOURCE}" "silver/${OLD_SOURCE}" "gold/${OLD_GOLD}" "bronze/_staging/${OLD_SOURCE}"; do
    remove_prefix "${WAREHOUSE_BUCKET}/${t}/"
  done
  for t in $({ mcx ls "odp/${WAREHOUSE_BUCKET}/bronze/_staging/" 2>/dev/null || true; } | awk '{print $NF}'); do
    remove_prefix "${WAREHOUSE_BUCKET}/bronze/_staging/${t}${OLD_SOURCE}/"
  done
  echo "Airflow DAG history"
  for t in "${OLD_DAGS[@]}"; do
    if [[ "$(afq "SELECT count(*) FROM dag WHERE dag_id = '${t}'")" != 0 ]]; then
      run "airflow dags delete ${t}" airflow dags delete "${t}" --yes
    fi
  done
  [[ "${DRY_RUN}" == 1 ]] || leftovers
}

# Everything that still carries an old name. Empty sections are the end state.
leftovers() {
  echo "Iceberg tables:"
  pgq "${CATALOG_DB}" "SELECT table_namespace || '.' || table_name FROM iceberg_tables WHERE table_name LIKE '${OLD_SOURCE}%' OR table_namespace = '${OLD_GOLD}' ORDER BY 1"
  echo "Rows (table, count):"
  for t in bronze_promotions pending_layers bronze_refreshes layer_checks; do
    echo "lakehouse_state.${t} $(pgq "${CATALOG_DB}" "SELECT count(*) FROM lakehouse_state.${t} WHERE source_id = '${OLD_SOURCE}'")"
  done
  echo "ingestion_state.extract_coverage $(pgq "${CATALOG_DB}" "SELECT count(*) FROM ingestion_state.extract_coverage WHERE source_id = '${OLD_SOURCE}'")"
  echo "silver_audit.quality_counts $(trino "SELECT count(*) FROM iceberg.silver_audit.quality_counts WHERE source_id = '${OLD_SOURCE}'" | tr -d '"')"
  echo "Objects (prefix, count):"
  echo "${RAW_BUCKET}/raw/${OLD_SOURCE}/ $(objects "${RAW_BUCKET}/raw/${OLD_SOURCE}/")"
  echo "${RAW_BUCKET}/quarantine/${OLD_SOURCE}/ $(objects "${RAW_BUCKET}/quarantine/${OLD_SOURCE}/")"
  echo "${WAREHOUSE_BUCKET} keys naming ${OLD_SOURCE} or ${OLD_GOLD}: $(objects "${WAREHOUSE_BUCKET}/" "${OLD_SOURCE}|/${OLD_GOLD}/")"
  echo "Postgres schema ${OLD_PG_SCHEMA}: $(pgq "${OLIST_SYNTH_DB}" "SELECT count(*) FROM information_schema.schemata WHERE schema_name = '${OLD_PG_SCHEMA}'")"
  echo "Airflow DAG rows: $(afq "SELECT count(*) FROM dag WHERE dag_id IN ('${OLD_DAGS[0]}', '${OLD_DAGS[1]}', '${OLD_DAGS[2]}', '${OLD_DAGS[3]}')")"
}

case "${1:-}" in
  counts) counts "${2:?old or new}" | tee "${OUT}/${3:-$([[ "$2" == old ]] && echo before || echo after)}.tsv" ;;
  compare) compare "${2:-final}" ;;
  delete) delete_old "${2:-}" ;;
  leftovers) leftovers ;;
  rename-pg-schema) rename_pg_schema ;;
  *) sed -n '2,16p' "$0"; exit 2 ;;
esac
