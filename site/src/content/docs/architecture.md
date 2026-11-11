---
title: Architecture
description: Verified local runtime, verified lakehouse, and the planned AWS path.
---

What is verified locally today, and what remains planned until later phases land with evidence.

## Verified local runtime

**Verified.** Docker Compose runs the Phase 0 services. Business logic lives in the Python package; Compose is the development and verification environment ([D-001](https://github.com/FilipeOliveira94/open-data-platform/blob/main/docs/decisions/0001-runtime-compose-local-aws-iac.md)).

- **PostgreSQL 16** — `postgres:16.15-bookworm`, health-checked
- **MinIO** — community image pin `coollabsio/minio:RELEASE.2025-10-15T17-29-55Z` (AGPLv3 community server; [D-002](https://github.com/FilipeOliveira94/open-data-platform/blob/main/docs/decisions/0002-object-storage-minio-s3.md))
- **Toolchain** — Python 3.12, `uv` 0.13.1, Ruff, pytest
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
  Airflow schedules, streaming CDC, Grafana
  AWS path: MinIO→S3, catalog→Glue, SQL→Athena (Terraform, Phase 7)
```

## Lakehouse and later phases

Bronze, Silver, and Gold are verified in the repository and are not drawn on this page yet. Airflow schedules, CDC, observability, and the AWS Terraform modules are not running platform components.

- Bronze, Silver, and Gold tables are verified in the repository. Spark writes Bronze and Silver. Trino reads. dbt-trino builds Gold. This page does not yet show those diagrams or the Phase 9 charts. Index: [Gold layer](https://github.com/FilipeOliveira94/open-data-platform/blob/main/docs/architecture/gold-layer.md)
- Daily batch, weekly batch, and streaming CDC stay separate. The PostgreSQL windowed extract is verified. Airflow schedules and CDC are not (D-007, D-008, D-014, all `proposed`)
- AWS as Infrastructure as Code — Phase 7; local Compose remains the verification environment

## Decisions

- [D-001 — Compose local; AWS as Terraform IaC](https://github.com/FilipeOliveira94/open-data-platform/blob/main/docs/decisions/0001-runtime-compose-local-aws-iac.md) (approved)
- [D-002 — MinIO local; S3 on AWS](https://github.com/FilipeOliveira94/open-data-platform/blob/main/docs/decisions/0002-object-storage-minio-s3.md) (approved)
- [D-013 — Data contracts](https://github.com/FilipeOliveira94/open-data-platform/blob/main/docs/decisions/0013-data-contracts.md) (approved)
- [D-020 — Raw storage layout](https://github.com/FilipeOliveira94/open-data-platform/blob/main/docs/decisions/0020-raw-storage-layout.md) (approved)
- Index: [DECISIONS.md](https://github.com/FilipeOliveira94/open-data-platform/blob/main/DECISIONS.md)
