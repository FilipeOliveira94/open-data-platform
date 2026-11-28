#!/usr/bin/env bash
# P11-04 drill: Spark Structured Streaming on Kafka, stop and restart.
# Needs `make stream-up` (Kafka and the telemetry producer publishing).
#
# 1. run r1: stream for RUN_SECONDS, stop between micro-batches.
# 2. rewind-commit: delete the newest commit-log entry (a crash after the
#    sink commit), so r2 replays a batch Iceberg already holds.
# 3. run r2 --kill-in-flight: SIGKILL the driver JVM while a new batch is
#    planned and not committed.
# 4. drain r3: AvailableNow from the checkpoint.
# 5. check (Spark) and count (Trino) after r1, after the kill and after r3,
#    then drop and confirm Trino no longer finds the schema.
#
# Prints counts, offsets and batch ids only (kafka_probe.py), no payloads.
# Exits 1 when a step or a check fails; the next run starts with drop.
set -euo pipefail

run_seconds=${1:-120}
filter='kafka_probe:|Skipping epoch'

# stderr: fail may run inside $(...), whose stdout is captured.
fail() {
  echo "drill FAIL: $*" >&2
  exit 1
}

# Run one probe mode. Prints the probe lines; on failure also the last
# lines of Spark's output, so the cause is visible.
probe() {
  local out rc=0
  out=$(make --no-print-directory kafka-spark-probe ARGS="$*" 2>&1) || rc=$?
  grep -E "$filter" <<<"$out" || true
  if ((rc != 0)); then
    echo "--- last lines of: $* (exit $rc)"
    grep -vE "$filter" <<<"$out" | tail -15
  fi
  return "$rc"
}

trino_count() {
  local out
  out=$(make --no-print-directory kafka-spark-probe-trino 2>&1) || {
    echo "$out" | tail -5
    fail "Trino count failed"
  }
  tail -1 <<<"$out"
}

spark_rows() { awk '/kafka_probe: rows /{ gsub(",", "", $3); print $3 }' <<<"$1"; }

# check, compare with Trino, and print the Spark row count last.
checked_rows() {
  local label=$1 runs=$2 report rows trino
  report=$(probe check --runs "$runs") || { echo "$report" >&2; fail "check $label"; }
  echo "$report" >&2
  rows=$(spark_rows "$report")
  trino=$(trino_count)
  echo "$label: Spark rows $rows, Trino rows $trino" >&2
  [[ -n "$rows" && "$rows" == "$trino" ]] || fail "$label: counts differ"
  echo "$rows"
}

probe drop
probe run --run-label r1 --max-seconds "$run_seconds" || fail "r1"
r1_rows=$(checked_rows "after r1" r1)
((r1_rows > 0)) || fail "r1 read no rows; is the producer publishing?"

probe rewind-commit || fail "rewind-commit"
r2_out=$(probe run --run-label r2 --max-seconds 600 --kill-in-flight) && fail "r2 exited cleanly: $r2_out"
echo "$r2_out"
grep -q 'Skipping epoch' <<<"$r2_out" || fail "r2 did not replay a committed epoch"
grep -q 'killing the driver JVM' <<<"$r2_out" || fail "r2 was not killed mid-batch"
killed_rows=$(checked_rows "after the kill" r1,r2)

probe drain --run-label r3 || fail "r3"
r3_rows=$(checked_rows "after r3" r1,r2,r3)
((r3_rows > killed_rows)) || fail "r3 read no new rows"

probe drop || fail "drop"
gone=$(make --no-print-directory kafka-spark-probe-trino 2>&1 || true)
grep -q "Schema 'scratch_p11_04' does not exist" <<<"$gone" || fail "Trino still finds the probe: $(tail -1 <<<"$gone")"
echo "drill PASS: r1 $r1_rows rows, after the kill $killed_rows, after r3 $r3_rows; probe dropped"
