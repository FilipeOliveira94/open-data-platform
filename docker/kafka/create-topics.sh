#!/usr/bin/env bash
# Create the BioGuardia telemetry topics (P11-03, ADR-0028). Idempotent: an
# existing topic keeps its partitions and gets the retention applied again.
set -euo pipefail

bin=/opt/kafka/bin
server=kafka:9092
retention_ms=$((RETENTION_HOURS * 3600 * 1000))

topic() {
  local name=$1 partitions=$2 bytes=$3
  "$bin/kafka-topics.sh" --bootstrap-server "$server" --create --if-not-exists \
    --topic "$name" --partitions "$partitions" --replication-factor 1 \
    --config "retention.ms=$retention_ms" --config "retention.bytes=$bytes" \
    --config segment.ms=3600000 --config compression.type=producer
  "$bin/kafka-configs.sh" --bootstrap-server "$server" --alter --entity-type topics \
    --entity-name "$name" --add-config "retention.ms=$retention_ms,retention.bytes=$bytes"
}

# retention.bytes is per partition.
topic bioguardia.telemetry.readings.v1 6 4294967296
topic bioguardia.telemetry.health.v1 3 4294967296
topic bioguardia.telemetry.alerts.v1 3 1073741824
topic bioguardia.telemetry.gateway.v1 1 1073741824
"$bin/kafka-topics.sh" --bootstrap-server "$server" --describe --topic bioguardia.telemetry.readings.v1
