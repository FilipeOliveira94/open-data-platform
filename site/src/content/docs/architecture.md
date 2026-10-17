---
title: Architecture
description: Verified local runtime and planned AWS path for the Open Data Platform.
---

What is verified locally today, and what remains planned until later phases land with evidence.

## Verified local runtime

**Verified.** Docker Compose runs the Phase 0 services. Business logic lives in the Python package; Compose is the development and verification environment ([D-001](https://github.com/FilipeOliveira94/open-data-platform/blob/main/docs/decisions/0001-runtime-compose-local-aws-iac.md)).

- **PostgreSQL 16** — `postgres:16.15-bookworm`, health-checked
- **MinIO** — community image pin `coollabsio/minio:RELEASE.2025-10-15T17-29-55Z` (AGPLv3 community server; [D-002](https://github.com/FilipeOliveira94/open-data-platform/blob/main/docs/decisions/0002-object-storage-minio-s3.md))
- **Toolchain** — Python 3.12, `uv` 0.13.1, Ruff, pytest

## Sketch

```text
[ Developer / WSL2 ]
        │
        ▼
[ Docker Compose ]
   ├── PostgreSQL 16     ◄── verified
   └── MinIO (S3 API)    ◄── verified
        │
        ▼
[ Python package + CI ]  ◄── verified

Later (not verified):
  Ingestion → Bronze / Silver / Gold → query / dbt
  AWS path: MinIO→S3, catalog→Glue, SQL→Athena (Terraform, Phase 7)
```

## Planned (not verified)

**Planned.** Table format, catalog, Spark, Airflow, CDC, observability, and the AWS Terraform modules are documented as proposed decisions. They are not presented as running platform components until their phases are verified in `PROJECT_STATUS.md`.

- Medallion layers, contracts, batch vs streaming — proposed ADRs D-012–D-014
- AWS as Infrastructure as Code — Phase 7; local Compose remains the verification environment

## Decisions

- [D-001 — Compose local; AWS as Terraform IaC](https://github.com/FilipeOliveira94/open-data-platform/blob/main/docs/decisions/0001-runtime-compose-local-aws-iac.md) (approved)
- [D-002 — MinIO local; S3 on AWS](https://github.com/FilipeOliveira94/open-data-platform/blob/main/docs/decisions/0002-object-storage-minio-s3.md) (approved)
- Index: [DECISIONS.md](https://github.com/FilipeOliveira94/open-data-platform/blob/main/DECISIONS.md)
