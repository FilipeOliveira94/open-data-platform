# Project Status

| | |
| --- | --- |
| Repository | [FilipeOliveira94/open-data-platform](https://github.com/FilipeOliveira94/open-data-platform) |
| Started | 2026-10-14 |
| Last updated | 2026-10-15 (P0-07) |
| Current phase | Phase 0 — Repository and environment foundation |
| Phase 0 | In progress |
| Platform (ingestion through lakehouse; AWS path as IaC in Phase 7) | Not started |

This ledger records **delivered work and executed validation**. Planned architecture is not treated as complete.

**Status values:** not started · in progress · delivered · verified · blocked

**Verified** is used only when the acceptance check has been run and the result is recorded below.

## Current objective

Establish Phase 0: a reproducible local baseline (Python 3.12, lint/tests, PostgreSQL 16, MinIO, CI) and remaining program docs (`README.md`, P0-13 decision note). `ROADMAP.md` and `DECISIONS.md` exist.

Out of scope for Phase 0: Airflow, Kafka, Spark, Trino, Iceberg, Kubernetes, and the AWS Terraform path (Phase 7).

## Verified

None. Phase 0 acceptance checks have not been run (no Compose stack, no lint/test command, no CI execution).

## Delivered

| ID | Item | Evidence |
| --- | --- | --- |
| P0-01 | Public GitHub monorepo, MIT license | `ab3c9a6` — `LICENSE` |
| P0-02 | Agent and contributor operating contract | `dc49fd8` — `AGENTS.md`, `docs/agent-workflow.md` |
| P0-03 | Project engineering rules (architecture, Python/testing, data engineering, infrastructure/security, workflow) | `71e96c6` — `.cursor/rules/` |
| P0-04 | Project status ledger | `PROJECT_STATUS.md` (2026-10-14) |
| P0-05 | Program context: objectives, constraints, target architecture, and phase backlog | `docs/` |
| P0-06 | Program roadmap: phases 0–7, issue IDs, dependencies, acceptance criteria | `ROADMAP.md` (2026-10-15) |
| P0-07 | Decision log: summary index + ADRs for proposed stack D-001–D-017 | `DECISIONS.md`, `docs/decisions/` (2026-10-15) |

## Phase 0 remaining

| ID | Item |
| --- | --- |
| P0-08 | Python 3.12 package with `uv` and a committed lockfile (`src/open_data_platform/`) |
| P0-09 | Ruff and pytest, including a toolchain smoke test |
| P0-10 | Docker Compose: PostgreSQL 16 and MinIO, health checks, pinned image tags |
| P0-11 | `.gitignore`, `.env.example` (names and placeholders only), Makefile, `README.md` |
| P0-12 | GitHub Actions: lint and tests (no cloud deploy) |
| P0-13 | Initial decision note: Compose for local development/verification; AWS target architecture as Terraform IaC in Phase 7; Phase 0 limited to PostgreSQL and MinIO |

Do not create empty directories for later phases until those phases start.

### Phase 0 acceptance criteria

Phase 0 is complete only when all of the following are true and recorded:

- [ ] A clean clone can follow the documented setup.
- [ ] PostgreSQL is healthy and reachable.
- [ ] A smoke test can write and read an object in MinIO.
- [ ] Lint and tests run via documented commands.
- [ ] No secrets, live `.env` files, or private datasets are in version control.
- [ ] CI runs the same lint and test checks.

## Required update patterns

Three PostgreSQL refresh patterns are in scope (see `ROADMAP.md`). CDC does **not** cover daily/weekly batch.

| Use case | Pattern | Phases | Status |
| --- | --- | --- | --- |
| Daily batch | Windowed incremental extract + Airflow daily schedule | 1 (P1-06), 3 (P3-08) | Not started |
| Weekly batch | Windowed extract + Airflow weekly schedule (distinct from daily) | 1 (P1-06), 3 (P3-09) | Not started |
| Streaming | Logical replication → Debezium → Kafka → Spark Structured Streaming | 4 | Not started |

## Program roadmap

Issue-level detail belongs in `ROADMAP.md`.

| Phase | Scope | Window | Status |
| --- | --- | --- | --- |
| 0 | Repository, Python toolchain, PostgreSQL 16, MinIO, CI | Sprint 1 | In progress |
| 1 | Multi-source ingestion: historical dataset, API, PostgreSQL incremental extract (daily/weekly windows), synthetic events | Sprint 2 | Not started |
| 2 | Lakehouse PoC (Spark, Iceberg, catalog, query engine), Bronze / Silver / Gold, initial dbt models | Sprint 3 | Not started |
| 3 | Airflow: daily + weekly batch schedules, idempotent backfills, quality checks and quarantine | Sprint 4 | Not started |
| 4 | Streaming CDC: PostgreSQL logical replication, Debezium, Kafka, Spark Structured Streaming | Sprint 5 | Not started |
| 5 | Observability, lineage, and cost visibility (Prometheus, Grafana, OpenLineage where supported) | Sprint 6 | Not started |
| 6 | CI/CD expansion, resilience tests, security checks | Sprint 6 | Not started |
| 7 | AWS deployment path as IaC (Terraform, local→AWS mapping), case-study documentation | Sprint 6 | Not started |

## Open decisions

Target stack, pending proof of concept and explicit approval. Summary: **`DECISIONS.md`**. Detail: **`docs/decisions/`** (ADR-0001–0017).

| ID | Topic | Direction | State |
| --- | --- | --- | --- |
| D-001 | Runtime | Docker Compose locally; Terraform AWS IaC (Phase 7); K8s rejected in ADR-0001 | Proposed |
| D-002 | Object storage | MinIO locally; Amazon S3 on AWS | Proposed |
| D-003 | Table format | Parquet + Iceberg after PoC | Proposed |
| D-004 | Table catalog / SQL | Local table catalog + SQL after PoC; Glue + Athena on AWS | Proposed |
| D-005 | Compute / transform | Spark + dbt | Proposed |
| D-006 | Orchestration | Airflow OSS locally; MWAA / Step Functions as AWS counterparts | Proposed |
| D-007 | Daily / weekly batch | Shared windowed extract; distinct Airflow daily and weekly schedules | Proposed |
| D-008 | Streaming CDC | Logical replication → Debezium → Kafka → Spark Structured Streaming | Proposed |
| D-009 | Observability | Prometheus + Grafana locally; CloudWatch (+ optional Datadog/New Relic) on AWS | Proposed |
| D-010 | Lineage | OpenLineage where verified; dbt lineage | Proposed |
| D-011 | IaC / CI | Terraform fmt/validate in CI; apply gated; GitHub Actions; OIDC | Proposed |
| D-012 | Medallion layers | Bronze / Silver / Gold | Proposed |
| D-013 | Data contracts | Explicit contracts; quarantine/fail; no silent drops | Proposed |
| D-014 | Batch vs streaming | Three update patterns coexist; CDC does not replace batch | Proposed |
| D-015 | Monorepo boundaries | Package owns logic; thin DAGs/IaC | Proposed |
| D-016 | Discovery / metadata | Deferred; evaluate DataHub / Amundsen / OpenMetadata later | Proposed |
| D-017 | Dataviz | Deferred until Gold; OSS BI locally; QuickSight as AWS counterpart | Proposed |

Analytical model names (`fct_*`, `dim_*`, `mart_*`) remain roadmap placeholders until P2-05 (not a standalone ADR).

## Risks and limitations

- The local stack and Python toolchain are not in the repository yet; they are not operational.
- Spark, Iceberg, catalog, and query-engine compatibility is unproven. That combination will not be expanded until a minimal create/write/query PoC succeeds.
- The historical e-commerce dataset will not be vendored until license and redistribution terms are confirmed.
- Exactly-once processing will not be claimed without a specific technical proof.
- Cloud provisioning (`terraform apply`/`destroy`) requires explicit approval; Phase 7 focuses on a complete AWS IaC artifact and local→AWS mapping validated by CI.
- Daily and weekly batch vs streaming CDC must stay clearly separated in implementation and demos; conflating them would weaken the technical narrative.

## Blocked

None.

## Next work

1. Phase 0 implementation plan for **P0-08–P0-13** (Python/`uv`, Ruff/pytest, Compose PostgreSQL+MinIO, hygiene/README, CI, initial decision note), then implementation in small slices after approval.
2. Promote D-001/D-002 to approved via **P0-13** once Compose baseline exists.

## Latest validation

| | |
| --- | --- |
| Date | 2026-10-14 |
| Environment | WSL2 / Ubuntu, repository root |
| Runtime checks | Not applicable — no application, Compose file, or test runner yet |
| Result | Documentation and repository baseline only |

## Log

### 2026-10-15 — Decision log restructure (D-001–D-017)

Restructured ADRs: removed Phase-0-services, Kubernetes-standalone, and analytical-placeholder ADRs; split compute vs orchestration and observability vs lineage; added medallion, contracts, batch/streaming coexistence, monorepo, discovery, and dataviz. Index in `DECISIONS.md`; detail in `docs/decisions/` ADR-0001–0017. All remain `proposed`.

### 2026-10-15 — P0-07 decision log (summary + ADRs)

`DECISIONS.md` is a short index (status + one-line summary). Full rationale lives in `docs/decisions/`, with `_template.md` and README. No decisions approved yet; evidence fields remain empty until PoCs and Phase 0 checks run.

### 2026-10-15 — AWS path as Infrastructure as Code

Scope alignment: Phase 7 delivers the **AWS deployment path as Terraform IaC**—service mapping (MinIO→S3, catalog→Glue, Trino→Athena), modular resources, OIDC/IAM, cost model and case study—with CI `fmt`/`validate`. Local Compose remains the environment for pipeline development and verification. Cloud apply/destroy stay approval-gated. Updated `ROADMAP.md`, this ledger, `docs/project-context-for-cursor.md`, `AGENTS.md` and infrastructure rules.

### 2026-10-15 — Three update patterns (batch + stream)

Scope review: CDC (Phase 4) covers **streaming** only, not daily/weekly PostgreSQL loads. Updated `ROADMAP.md` and this ledger to require three use cases—daily batch (P1-06 + P3-08), weekly batch (P1-06 + P3-09), streaming CDC (Phase 4)—and added P4-07 batch regression after CDC.

### 2026-10-15 — P0-06 roadmap

Added `ROADMAP.md` with issue-level scope for phases 0–7 (IDs `P0-xx` … `P7-xx`), dependencies, phase and issue acceptance criteria, and out-of-scope notes. Phase 0 issue statuses aligned with this ledger; later phases remain not started.

### 2026-10-14 — Phase 0 started

Initialized the public repository and the operating model for the program.

| Commit | Description |
| --- | --- |
| `ab3c9a6` | MIT license |
| `dc49fd8` | `AGENTS.md` and contributor workflow |
| `71e96c6` | Engineering rules by concern |

Also delivered: this status ledger and the program context under `docs/`.
