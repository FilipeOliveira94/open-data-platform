# ADR-0028: Message broker for the telemetry stream

| Field | Value |
| --- | --- |
| Status | `proposed` |
| Date opened | 2026-11-26 |
| Date decided | — |
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
| Amazon MSK or Kinesis | Managed | Cloud cost; local development must not need AWS (AGENTS.md section 7) | Recorded as the AWS counterpart for Phase 7 |

## Consequences and trade-offs

- Phase 4 depends on P11-01 to P11-04: P4-02 runs on this broker and P4-04 reuses the P11-04 proof.
- At about 1M events per hour the log grows by 1.24 GB per day (measured, P11-03); 72 hours of retention is about 3.7 GB.
- PLAINTEXT without authentication is acceptable on the local Compose network only. Phase 7 maps the broker to MSK with IAM authentication.

## Evidence / validation

Versions checked on 2026-11-26:

| Artifact | Version | Where checked | Published |
| --- | --- | --- | --- |
| Kafka image | `apache/kafka:4.3.2` | Docker Hub tags of `apache/kafka` | 2026-10-20 |
| Python base image for the producer | `python:3.12.15-slim-bookworm` | Docker Hub tags of `python` | 2026-10-21 |
| Producer client | `confluent-kafka` 2.16.1 | PyPI release history | 2026-10-28 (2.16.2 of 2026-11-18 was under two weeks old) |

## Revisit when

- Spark 3.5.8's Kafka source cannot read this broker (P11-04).
- Debezium cannot run on it (P4-02).
- Retention or disk use exceeds the local budget.
