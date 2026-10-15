# ADR-0002: Object storage

| Field | Value |
| --- | --- |
| Status | `proposed` |
| Date opened | 2026-10-15 |
| Date decided | — |
| Owner | Platform architect |
| Index ID | D-002 |
| Related issues | P0-10, P1-02, P7-01 |
| Supersedes | — |
| Superseded by | — |

## Context / problem

Lakehouse layers need durable object storage with the same architectural role locally and in AWS so path conventions and configuration stay portable.

## Decision

Use **MinIO** locally and **Amazon S3** on the AWS path as the object store for raw and table data. Treat them as the same role in docs and configuration (endpoint/credentials abstracted).

## Alternatives considered

| Option | Pros | Cons | Why not (if rejected) |
| --- | --- | --- | --- |
| MinIO ↔ Amazon S3 (chosen) | S3 API locally; clear AWS mapping | Must validate MinIO feature gaps if any | — |
| Local filesystem only | Zero extra services | Diverges from cloud path; weak object-store semantics | Rejected for lakehouse layers |
| HDFS | Familiar big-data store | Heavy ops; poor fit for S3-centric lakehouse + Athena path | Rejected |
| Google Cloud Storage | Strong object store | Out of AWS target architecture for this program | Rejected (wrong cloud) |
| Azure Blob Storage | Strong object store | Out of AWS target architecture | Rejected (wrong cloud) |
| Ceph / RGW | S3-compatible self-hosted | Heavier than MinIO for laptop Compose | Rejected for local path |
| Real S3 from day one | Production-like | Credentials and cost for every clone | Rejected as default |

## Consequences and trade-offs

- Never hard-code secrets; use env/config for endpoints.
- Phase 7 IAM focuses on S3 bucket layout and least privilege.

## Evidence / validation

None yet — MinIO write/read smoke test is a Phase 0 acceptance criterion.

## Revisit when

API incompatibility between MinIO and required S3 features appears in a PoC.
