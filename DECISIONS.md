# Architecture Decision Log

Short index of architecture choices for the Open Data Platform. **Rationale, alternatives, consequences and evidence live in ADRs** under [`docs/decisions/`](docs/decisions/).

| | |
| --- | --- |
| Owns | Decision index and status |
| Detail | [`docs/decisions/`](docs/decisions/) (one ADR per decision) |
| Related | `PROJECT_STATUS.md`, `ROADMAP.md`, `docs/project-context-for-cursor.md`, `AGENTS.md` |

**How to use:** skim the table for direction and implementation status; open the ADR for *why*. **Proposed** is the planned stack; **Implemented** is what is verified in the repository (update when evidence lands in `PROJECT_STATUS.md` / `docs/status-log.md`).

## Lifecycle

| State | Meaning |
| --- | --- |
| `proposed` | Preferred for planning; not binding until approved |
| `approved` | Accepted (with PoC evidence when the roadmap requires it) |
| `superseded` | Replaced by a later ADR |
| `rejected` | Declined; kept for auditability |

**Rules:** prove stack compatibility before approving format/catalog/engine combos; never claim end-to-end exactly-once without a written proof; when status or implementation changes, update this index and the ADR together.

## Index

| ID | Topic | Proposed stack | Implemented stack | State | ADR |
| --- | --- | --- | --- | --- | --- |
| D-001 | Runtime | Docker Compose (local); Terraform IaC for AWS (Phase 7) | Compose baseline: PostgreSQL 16 + MinIO (Phase 0); AWS IaC still Phase 7 | `approved` | [ADR-0001](docs/decisions/0001-runtime-compose-local-aws-iac.md) |
| D-002 | Object storage | MinIO (local, AGPLv3); Amazon S3 (AWS) | `coollabsio/minio:RELEASE.2025-10-15T17-29-55Z` (community rebuild; official images pulled for AIStor); S3 still Phase 7 | `approved` | [ADR-0002](docs/decisions/0002-object-storage-minio-s3.md) |
| D-003 | Table format | Parquet + Apache Iceberg (after PoC) | — | `proposed` | [ADR-0003](docs/decisions/0003-table-format-parquet-iceberg.md) |
| D-004 | Table catalog / SQL | Local table catalog + SQL after PoC; Glue Catalog + Athena (AWS) | — | `proposed` | [ADR-0004](docs/decisions/0004-table-catalog-and-sql-engines.md) |
| D-005 | Compute / transform | Apache Spark; dbt for Gold | — | `proposed` | [ADR-0005](docs/decisions/0005-compute-and-transform.md) |
| D-006 | Orchestration | Apache Airflow (OSS) locally; MWAA / Step Functions as AWS counterparts | — | `proposed` | [ADR-0006](docs/decisions/0006-orchestration.md) |
| D-007 | Daily / weekly batch | Windowed PostgreSQL extract; Airflow daily and weekly schedules | — | `proposed` | [ADR-0007](docs/decisions/0007-daily-weekly-batch.md) |
| D-008 | Streaming CDC | PostgreSQL logical replication, Debezium, Kafka, Spark Structured Streaming | — | `proposed` | [ADR-0008](docs/decisions/0008-streaming-cdc.md) |
| D-009 | Observability | Prometheus + Grafana locally; CloudWatch (+ optional Datadog/New Relic) on AWS | — | `proposed` | [ADR-0009](docs/decisions/0009-observability.md) |
| D-010 | Lineage | OpenLineage where verified; dbt lineage | — | `proposed` | [ADR-0010](docs/decisions/0010-lineage.md) |
| D-011 | IaC / CI | Terraform (`fmt`/`validate` in CI; apply gated); GitHub Actions; OIDC | — | `proposed` | [ADR-0011](docs/decisions/0011-iac-and-ci-credentials.md) |
| D-012 | Medallion layers | Bronze / Silver / Gold; Gold domains `olist_analysis` + `product_analysis` | — | `proposed` | [ADR-0012](docs/decisions/0012-medallion-layers.md) |
| D-013 | Data contracts | Contracts under `config/contracts/`; explicit quarantine/fail (no silent drops) | YAML contracts, structural loader (P1-03), and quarantine sidecar through the shared object writer (P1-10) | `approved` | [ADR-0013](docs/decisions/0013-data-contracts.md) |
| D-014 | Batch vs streaming | Daily batch, weekly batch, and CDC coexist; CDC does not replace batch | — | `proposed` | [ADR-0014](docs/decisions/0014-batch-vs-streaming-coexistence.md) |
| D-015 | Monorepo boundaries | Package owns business logic; thin DAGs and IaC | — | `proposed` | [ADR-0015](docs/decisions/0015-monorepo-package-boundaries.md) |
| D-016 | Discovery / metadata | Deferred; evaluate DataHub / Amundsen / OpenMetadata later | — | `proposed` | [ADR-0016](docs/decisions/0016-data-discovery.md) |
| D-017 | Dataviz | Deferred until Gold; OSS BI locally; QuickSight as AWS counterpart | — | `proposed` | [ADR-0017](docs/decisions/0017-dataviz.md) |
| D-018 | Historical Olist datasets | CC BY-NC-SA 4.0 e-commerce + marketing funnel; no vendor; fetch-from-source | Provenance under `docs/sources/`; no dataset binaries in git (P1-01) | `approved` | [ADR-0018](docs/decisions/0018-historical-dataset-redistribution.md) |
| D-019 | HTTP API sources | P1-05 DummyJSON `/products`; IBGE Localidades (P1-08). BrasilAPI CEP withdrawn | DummyJSON products client; IBGE `municipios` client. No CEP dataset | `approved` | [ADR-0019](docs/decisions/0019-public-api-ingestion-source.md) |
| D-020 | Raw storage layout | Object-store `raw/{source_id}/{dataset}/dt=…/execution_id=…/`; metadata `source_id`, `execution_id`, `_ingestion_ts`; `data/raw/` is local staging only | Package helpers + unit tests (P1-02) | `approved` | [ADR-0020](docs/decisions/0020-raw-storage-layout.md) |
| D-021 | BrasilAPI CEP | Not a platform source. Geography for `olist_analysis` is Olist geolocation plus IBGE | No CEP dataset (P1-09 withdrawn) | `approved` | [ADR-0021](docs/decisions/0021-withdraw-brasilapi-cep.md) |

`—` means not yet implemented or not yet verified in the repository.

## Recording a new decision

1. Copy [`docs/decisions/_template.md`](docs/decisions/_template.md) to `docs/decisions/NNNN-short-slug.md`.
2. Fill context, decision, alternatives, consequences, evidence, revisit triggers.
3. Add one row to the index above (proposed stack, implemented stack, state, ADR link).
4. In `PROJECT_STATUS.md` → **Decisions (active)**, list only decisions that **gate the current phase** or were **approved in this phase**. Do not copy this full index.
5. When the choice is built and verified, set **Implemented stack** to the concrete versions/services (not a copy of Proposed until evidence exists).

## Changelog

| Date | Change |
| --- | --- |
| 2026-10-26 | D-013 approved. Invalid-record classification and the `quarantine/` sidecar are in the repository. The sidecar put goes through `write_raw_object` |
| 2026-10-25 | D-021 approved: BrasilAPI CEP withdrawn as obsolete. D-019 CEP clause superseded; DummyJSON and IBGE stay |
| 2026-10-18 | D-013: P1-03 landed YAML contracts and a structural loader; state stays `proposed` until approval |
| 2026-10-18 | D-020 approved: raw object-store layout + ingestion metadata conventions (P1-02) |
| 2026-10-17 | D-019 approved: DummyJSON (P1-05), IBGE Localidades planned, BrasilAPI CEP optional; D-012 Gold domains `olist_analysis` / `product_analysis` |
| 2026-10-17 | Recording rule: mirror only active/current-phase decisions into `PROJECT_STATUS.md` (full index stays here) |
| 2026-10-17 | D-018: approved — Olist Brazilian e-commerce + marketing funnel (CC BY-NC-SA 4.0); do not vendor; fetch-from-source (P1-01) |
| 2026-10-16 | D-002: noted MinIO official images removed for AIStor; community AGPLv3 builds remain OK for local use; Compose runtime verified |
| 2026-10-16 | P0-13: approved D-001 and D-002; recorded Compose baseline and MinIO community image pin |
| 2026-10-15 | Restructured to D-001–D-017: removed phase-0/K8s/placeholder ADRs; split compute vs orchestration and observability vs lineage; added medallion, contracts, coexistence, monorepo, discovery, dataviz |
| 2026-10-15 | Index columns: Proposed stack vs Implemented stack |
| 2026-10-15 | Initial decision log (P0-07) |
