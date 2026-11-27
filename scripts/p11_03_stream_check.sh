#!/usr/bin/env bash
# P11-03 stream check: measure what the BioGuardia producer delivers to Kafka
# over a window of MINUTES (default 60) and compare it with the model.
# Records end-offset deltas per topic, producer and broker CPU and memory,
# and the broker's log growth. Prints counts only (no payloads).
set -euo pipefail

minutes=${1:-60}
topics=(bioguardia.telemetry.readings.v1 bioguardia.telemetry.health.v1 bioguardia.telemetry.alerts.v1)

offsets() {
  # Sum of end offsets of one topic.
  docker compose exec -T kafka /opt/kafka/bin/kafka-get-offsets.sh \
    --bootstrap-server localhost:9092 --topic "$1" --time -1 \
    | awk -F: '{ total += $3 } END { print total + 0 }'
}

log_bytes() {
  # Segment data only: index files are preallocated sparse files, so their
  # apparent size says nothing about growth.
  docker compose exec -T kafka sh -c \
    'find /var/lib/kafka/data -path "*bioguardia.telemetry.*" -name "*.log" -exec stat -c %s {} +' \
    | awk '{ total += $1 } END { print total + 0 }'
}

start=$(date -u +%s)
declare -A first
for topic in "${topics[@]}"; do first[$topic]=$(offsets "$topic"); done
bytes_first=$(log_bytes)
echo "start $(date -u -d "@$start" +%FT%TZ), window ${minutes} min"

samples=0
cpu_producer=0
cpu_broker=0
mem_producer=0
mem_broker=0
deadline=$((start + minutes * 60))
while [ "$(date -u +%s)" -lt "$deadline" ]; do
  sleep 60
  stats=$(docker stats --no-stream --format '{{.Name}} {{.CPUPerc}} {{.MemUsage}}' odp-kafka "$(docker compose ps -q bioguardia-telemetry)")
  read -r p_cpu p_mem < <(echo "$stats" | grep -v odp-kafka | awk '{ gsub("%", "", $2); print $2, $3 }')
  read -r b_cpu b_mem < <(echo "$stats" | grep odp-kafka | awk '{ gsub("%", "", $2); print $2, $3 }')
  samples=$((samples + 1))
  cpu_producer=$(awk -v a="$cpu_producer" -v b="$p_cpu" 'BEGIN { print a + b }')
  cpu_broker=$(awk -v a="$cpu_broker" -v b="$b_cpu" 'BEGIN { print a + b }')
  mem_producer=$p_mem
  mem_broker=$b_mem
done
end=$(date -u +%s)
bytes_last=$(log_bytes)

total=0
for topic in "${topics[@]}"; do
  last=$(offsets "$topic")
  delta=$((last - first[$topic]))
  total=$((total + delta))
  echo "$topic: $delta messages"
done
seconds=$((end - start))
echo "window: $(date -u -d "@$start" +%FT%TZ) to $(date -u -d "@$end" +%FT%TZ) ($seconds s)"
echo "delivered: $total messages, $((total * 3600 / seconds)) per hour"
docker compose run --rm -T --no-deps bioguardia-telemetry telemetry model \
  --from "$(date -u -d "@$start" +%FT%TZ)" --to "$(date -u -d "@$end" +%FT%TZ)"
echo "producer CPU mean $(awk -v a="$cpu_producer" -v n="$samples" 'BEGIN { printf "%.1f", a / n }')%, memory $mem_producer"
echo "broker CPU mean $(awk -v a="$cpu_broker" -v n="$samples" 'BEGIN { printf "%.1f", a / n }')%, memory $mem_broker"
echo "broker segment growth: $((bytes_last - bytes_first)) bytes ($(( (bytes_last - bytes_first) * 3600 / seconds )) per hour)"
