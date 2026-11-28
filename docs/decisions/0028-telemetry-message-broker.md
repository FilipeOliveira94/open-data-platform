# ADR-0028: Message broker for the telemetry stream

| Field | Value |
| --- | --- |
| Status | `approved` |
| Date opened | 2026-11-26 |
| Date decided | 2026-11-27 |
| Owner | Platform architect |
| Index ID | D-028 |
| Related issues | P11-03, P11-04, P11-09; P4-02, P4-04 |
| Supersedes | — |
| Superseded by | — |

## Context / problem

The BioGuardia telemetry source ([ADR-0027](0027-bioguardia-synthetic-sources.md)) publishes about 1M events per hour, 24 hours a day. It must keep publishing while Airflow or the stream consumer is down, and the consumer must resume where it stopped. That needs a durable log with retention, consumer offsets, and a first-party Spark Structured Streaming source.

[ADR-0008](0008-streaming-cdc.md) already names Kafka for the Phase 4 CDC path, and P4-02 planned to add it with Debezium. The telemetry source needs a broker before Phase 4 starts. AGENTS.md section 10 asks to avoid two tools for the same job.

## Decision

Run one Apache Kafka broker in Compose, added by P11-03 on the `streaming` profile:

- Image `apache/kafka:4.3.2` (Apache License 2.0), KRaft combined mode (broker and controller in one node, no ZooKeeper), PLAINTEXT on the Compose network, a host listener on `ODP_KAFKA_HOST_PORT` (default 9094), a 1 GB heap, volume `odp_kafka_data`.
- Automatic topic creation is off. An init service creates the topics with their partitions, `retention.ms` from `ODP_KAFKA_RETENTION_HOURS` (default 72) and a `retention.bytes` cap per partition.
- Producer client: `confluent-kafka` 2.16.1 (Apache License 2.0, librdkafka) in the non-default uv group `sources`, with `acks=all`, idempotence on, and zstd compression.
- One broker means replication factor 1: losing the volume loses the log. That is a documented local limit.

This ADR governs the broker deployment (version, mode, listeners, retention, topics) for every Kafka user. ADR-0008 keeps the CDC decision (logical replication, Debezium, Spark Structured Streaming). P4-02 adds Debezium Connect on this broker instead of a second Kafka. Spark 3.5.8's Kafka source with this broker is proven once, in P11-04; P11-09 and P4-04 depend on that proof.

Retention is 72 hours so that a weekend with Airflow or the consumer stopped is still recoverable from the broker.

## Alternatives considered

| Option | Pros | Cons | Why not (if rejected) |
| --- | --- | --- | --- |
| Apache Kafka 4.3.2, KRaft, one node (chosen) | Already chosen for CDC (ADR-0008); first-party Spark source; Apache 2.0; no ZooKeeper | JVM memory; replication factor 1 locally | — |
| Redpanda | Kafka API; lighter | Community edition is BSL 1.1, not open source; a second implementation next to the Debezium path | Rejected |
| RabbitMQ Streams | Light; open source | No first-party Spark source; a different API from the CDC path | Rejected |
| NATS JetStream | Light; open source | No first-party Spark source | Rejected |
| Amazon MSK or Kinesis | Managed | Managed cloud service; the platform is local and open source | Rejected (ADR-0029) |

## Consequences and trade-offs

- Phase 4 depends on P11-01 to P11-04: P4-02 runs on this broker and P4-04 reuses the P11-04 proof.
- At about 1M events per hour the log grows by 1.24 GB per day (measured, P11-03); 72 hours of retention is about 3.7 GB.
- PLAINTEXT without authentication is acceptable on the local Compose network only.

## Evidence / validation

Versions checked on 2026-11-26:

| Artifact | Version | Where checked | Published |
| --- | --- | --- | --- |
| Kafka image | `apache/kafka:4.3.2` | Docker Hub tags of `apache/kafka` | 2026-10-20 |
| Python base image for the producer | `python:3.12.15-slim-bookworm` | Docker Hub tags of `python` | 2026-10-21 |
| Producer client | `confluent-kafka` 2.16.1 | PyPI release history | 2026-10-28 (2.16.2 of 2026-11-18 was under two weeks old) |

Spark consumer combination, proven by P11-04 on 2026-11-28:

| Component | Version | Source |
| --- | --- | --- |
| Spark | 3.5.8 (Scala 2.12, Temurin 17, PySpark on Python 3.12.16) | Runtime image `odp-runtime:airflow3.4.0-spark3.5.8` |
| Kafka source | `org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.8` | Pinned in `docker/spark/warm_ivy.sh`, cached in `.local/spark-ivy` |
| Kafka client | `kafka-clients` 3.4.1 (transitive), with `spark-token-provider-kafka-0-10_2.12` 3.5.8, `commons-pool2` 2.11.1, `hadoop-client-runtime` 3.3.4 | Ivy resolution of the Kafka source |
| Table format and catalog | `iceberg-spark-runtime-3.5_2.12` 1.12.0, JDBC catalog on PostgreSQL | Same pins as the batch path (P2-08) |
| Broker | `apache/kafka:4.3.2` | This ADR |
| Query engine | Trino 484 | ADR-0004 |

P11-03 (2026-11-26): the topics were created with the documented partitions, `retention.ms=259200000` and `retention.bytes` caps; over a measured hour the broker took 1,019,332 messages (99.97% of the model) at 23.9% CPU and 1.03 GiB, and the segment files grew about 52 MB per hour (about 51 bytes per message with zstd), so 72 hours of retention is about 3.7 GB. Detail: [`docs/sources/bioguardia-telemetry.md`](../sources/bioguardia-telemetry.md#measured-volume). P11-04 (2026-11-28): the combination above is compatible. `make kafka-spark-probe-drill` read `bioguardia.telemetry.health.v1` (3 partitions) into the scratch table `local.scratch_p11_04.telemetry_probe`, with the checkpoint on the `odp_lakehouse_work` volume. The drill passed on each run; the last run, on the final code:

| Step | Result |
| --- | --- |
| r1, 120 s, 10 s trigger, stopped between batches | 14 batches (0 to 13), 17,121 rows; Trino 17,121 |
| Commit-log entry 13 removed (a crash between the sink commit and the commit log) | r2 replayed batch 13; Iceberg logged `Skipping epoch 13 ... as it was already committed` and added no rows |
| r2 driver JVM killed with SIGKILL while batch 14 was planned and not committed | After the kill: 17,121 rows, epochs 0 to 13; batch 14 had not reached the sink commit |
| r3, `AvailableNow` from the same checkpoint | Replayed batch 14, then 15; 26,958 rows; Trino 26,958 |
| Checks after each step | Rows = distinct `(partition, offset)`; each partition gapless; r3 starts at r1's last offset + 1 in every partition; the table's last offset + 1 = the newest committed batch's end offset; one Iceberg snapshot per streaming epoch |
| `drop` | Table, namespace, checkpoint, and every object under `scratch/p11_04/` removed; Trino reports the schema does not exist |

So, on this setup, a restart resumes from the checkpoint and committed offsets are not written twice. Delivery stays at-least-once at the source (a killed batch is read again); the Iceberg sink skips an epoch it already committed for the same query id. That is not an end-to-end exactly-once claim. Two operating notes for P11-09: a replayed epoch that Iceberg skips leaves its data files behind as orphans (the probe deleted three), so the stream tables need orphan-file cleanup; and the driver JVM must not be PID 1 in its container, or a SIGKILL from inside the container is ignored. Commands: [`docs/sources/bioguardia-telemetry.md`](../sources/bioguardia-telemetry.md#spark-consumer-proof-p11-04).

## Revisit when

- Spark's Kafka source or its `kafka-clients` changes version, or the broker leaves Kafka 4.3 (P11-04 proved Spark 3.5.8 with `kafka-clients` 3.4.1 on Kafka 4.3.2).
- Debezium cannot run on it (P4-02).
- Retention or disk use exceeds the local budget.
