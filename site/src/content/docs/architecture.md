---
title: Architecture
description: Verified local runtime, verified lakehouse, and the phases still ahead.
---

What is verified locally today, and what remains planned until later phases land with evidence.

## Verified local runtime

**Verified.** Docker Compose runs the Phase 0 services. Business logic lives in the Python package; Compose is the development and verification environment ([D-001](https://github.com/FilipeOliveira94/open-data-platform/blob/main/docs/decisions/0001-runtime-compose-local-aws-iac.md)).

- **PostgreSQL 16** — `postgres:16.15-bookworm`, health-checked
- **MinIO** — community image pin `coollabsio/minio:RELEASE.2025-10-15T17-29-55Z` (AGPLv3 community server; [D-002](https://github.com/FilipeOliveira94/open-data-platform/blob/main/docs/decisions/0002-object-storage-minio-s3.md))
- **Toolchain** — Python 3.12, `uv` 0.13.1, Ruff, pytest
- **Airflow 3.4** — `apache/airflow:3.4.0-python3.12` on the `orchestration` profile. UI on port 8081. Finished task logs in MinIO. ([D-006](https://github.com/FilipeOliveira94/open-data-platform/blob/main/docs/decisions/0006-orchestration.md))
- **Raw ingestion** — five sources write auditable objects to MinIO. Contracts and the quarantine sidecar are in the repository ([D-013](https://github.com/FilipeOliveira94/open-data-platform/blob/main/docs/decisions/0013-data-contracts.md), [D-020](https://github.com/FilipeOliveira94/open-data-platform/blob/main/docs/decisions/0020-raw-storage-layout.md))

## Sketch

```text
[ Developer / WSL2 ]
        │
        ▼
[ Docker Compose ]
   ├── PostgreSQL 16     ◄── verified
   └── MinIO (S3 API)    ◄── verified
        ▲
        │ raw/{source}/{dataset}/dt=…/execution_id=…
[ Ingestion package ]    ◄── verified
        │
        ▼
[ CI: lint + unit tests ] ◄── verified

Verified in the repository, not yet drawn on this page:
  Bronze / Silver / Gold on Iceberg, Spark writes, Trino reads, dbt builds Gold
  Charts from JupyterLab and Superset are Phase 9

Later (not started):
  Airflow daily and weekly schedules, streaming CDC, Grafana
```

## Lakehouse and later phases

Bronze, Silver, and Gold are verified in the repository and are not drawn on this page yet. The Airflow scheduler runs on the orchestration profile. Daily and weekly schedules, CDC, and observability are not running platform components.

- Bronze, Silver, and Gold tables are verified in the repository. Spark writes Bronze and Silver. Trino reads. dbt-trino builds Gold. This page does not yet show those diagrams or the Phase 9 charts. Index: [Gold layer](https://github.com/FilipeOliveira94/open-data-platform/blob/main/docs/architecture/gold-layer.md)
- Daily batch, weekly batch, and streaming CDC stay separate. The PostgreSQL windowed extract is verified. Airflow schedules and CDC are not (D-007, D-008, D-014, all `proposed`)
- Every component is open source and runs on the local Compose stack; there is no cloud deployment path ([D-029](https://github.com/FilipeOliveira94/open-data-platform/blob/main/docs/decisions/0029-open-source-only-platform.md))

## Decisions

- [D-001 — Docker Compose runtime](https://github.com/FilipeOliveira94/open-data-platform/blob/main/docs/decisions/0001-runtime-compose-local-aws-iac.md) (approved)
- [D-006 — Airflow locally](https://github.com/FilipeOliveira94/open-data-platform/blob/main/docs/decisions/0006-orchestration.md) (approved)
- [D-002 — MinIO object storage (S3 API)](https://github.com/FilipeOliveira94/open-data-platform/blob/main/docs/decisions/0002-object-storage-minio-s3.md) (approved)
- [D-029 — Open-source-only platform](https://github.com/FilipeOliveira94/open-data-platform/blob/main/docs/decisions/0029-open-source-only-platform.md) (approved)
- [D-013 — Data contracts](https://github.com/FilipeOliveira94/open-data-platform/blob/main/docs/decisions/0013-data-contracts.md) (approved)
- [D-020 — Raw storage layout](https://github.com/FilipeOliveira94/open-data-platform/blob/main/docs/decisions/0020-raw-storage-layout.md) (approved)
- Index: [DECISIONS.md](https://github.com/FilipeOliveira94/open-data-platform/blob/main/DECISIONS.md)
