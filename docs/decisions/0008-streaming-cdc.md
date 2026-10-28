# ADR-0008: Streaming CDC

| Field | Value |
| --- | --- |
| Status | `proposed` |
| Date opened | 2026-10-15 |
| Date decided | — |
| Owner | Platform architect |
| Index ID | D-008 |
| Related issues | Phase 4 (`P4-01`–`P4-09`) |
| Supersedes | — |
| Superseded by | — |

## Context / problem

Transactional systems change continuously. Inserts, updates and deletes must flow through a CDC pipeline with **explicit** delivery semantics—including duplicates, ordering, checkpoints and replay.

## Decision

In Phase 4, capture changes via **PostgreSQL logical replication**, **Debezium**, **Kafka**, and consume with **Spark Structured Streaming** into the lakehouse. Document semantics explicitly. **Do not claim exactly-once end-to-end** without a specific technical proof. Keep validated daily/weekly batch paths working (P4-07).

## Alternatives considered

| Option | Pros | Cons | Why not (if rejected) |
| --- | --- | --- | --- |
| Logical replication → Debezium → Kafka → Spark SS (chosen) | True CDC; aligns with Spark compute decision | Higher Compose resource use | — |
| Polling-based “CDC” | Simpler | Not true CDC; conflates with batch | Rejected for Phase 4 goal |
| Kafka Connect sink without Spark SS | Fewer moving parts | Weaker processing/checkpoint story | Possible interim; full path preferred |
| Replace batch with CDC only | Fewer pipelines | Loses required batch use cases | Rejected (see D-007, D-014) |

## Consequences and trade-offs

- Higher Compose resource use in Phase 4.
- Documentation quality matters as much as the happy-path demo.

## Evidence / validation

None yet — Phase 4 acceptance and P4-05 semantics doc.

## Revisit when

Version incompatibility or resource limits force a smaller CDC path; record reduced scope here.
