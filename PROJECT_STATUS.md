# Project Status

| | |
| --- | --- |
| Repository | [FilipeOliveira94/open-data-platform](https://github.com/FilipeOliveira94/open-data-platform) |
| Started | 2026-10-14 |
| Last updated | 2026-10-15 |
| Current phase | Phase 0 — Repository and environment foundation |
| Phase 0 | In progress |
| Platform (ingestion through AWS) | Not started |

This ledger records **delivered work and executed validation**. Planned architecture is not treated as complete.

**Status values:** not started · in progress · delivered · verified · blocked

**Verified** is used only when the acceptance check has been run and the result is recorded below.

## Current objective

Establish Phase 0: a reproducible local baseline (Python 3.12, lint/tests, PostgreSQL 16, MinIO, CI) and the program documents that govern later phases (`ROADMAP.md`, `DECISIONS.md`, README).

Out of scope for Phase 0: Airflow, Kafka, Spark, Trino, Iceberg, Kubernetes, and AWS deployment.

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

## Phase 0 remaining

| ID | Item |
| --- | --- |
| P0-06 | `ROADMAP.md` — phases, dependencies, and acceptance criteria |
| P0-07 | `DECISIONS.md` — architecture decisions (proposed vs approved); ADRs when a decision is taken |
| P0-08 | Python 3.12 package with `uv` and a committed lockfile (`src/open_data_platform/`) |
| P0-09 | Ruff and pytest, including a toolchain smoke test |
| P0-10 | Docker Compose: PostgreSQL 16 and MinIO, health checks, pinned image tags |
| P0-11 | `.gitignore`, `.env.example` (names and placeholders only), Makefile, `README.md` |
| P0-12 | GitHub Actions: lint and tests (no cloud deploy) |
| P0-13 | Initial decision note: local Compose vs later AWS; Phase 0 limited to PostgreSQL and MinIO |

Do not create empty directories for later phases until those phases start.

### Phase 0 acceptance criteria

Phase 0 is complete only when all of the following are true and recorded:

- [ ] A clean clone can follow the documented setup.
- [ ] PostgreSQL is healthy and reachable.
- [ ] A smoke test can write and read an object in MinIO.
- [ ] Lint and tests run via documented commands.
- [ ] No secrets, live `.env` files, or private datasets are in version control.
- [ ] CI runs the same lint and test checks.

## Program roadmap

Issue-level detail belongs in `ROADMAP.md`.

| Phase | Scope | Window | Status |
| --- | --- | --- | --- |
| 0 | Repository, Python toolchain, PostgreSQL 16, MinIO, CI | Sprint 1 | In progress |
| 1 | Multi-source ingestion: historical dataset (license-checked), paginated public API, PostgreSQL extract, synthetic events | Sprint 2 | Not started |
| 2 | Lakehouse PoC (Spark, Iceberg, catalog, query engine), Bronze / Silver / Gold, initial dbt models | Sprint 3 | Not started |
| 3 | Airflow orchestration, idempotent backfills, quality checks and quarantine | Sprint 4 | Not started |
| 4 | CDC and streaming: PostgreSQL logical replication, Debezium, Kafka, Spark Structured Streaming | Sprint 5 | Not started |
| 5 | Observability, lineage, and cost visibility (Prometheus, Grafana, OpenLineage where supported) | Sprint 6 | Not started |
| 6 | CI/CD expansion, resilience tests, security checks | Sprint 6 | Not started |
| 7 | Minimal AWS path (Terraform, S3, Glue, Athena, IAM/OIDC), measured cost, case-study documentation | Sprint 6 | Not started |

## Open decisions

Target stack, pending proof of concept and explicit approval:

| Topic | Direction | State |
| --- | --- | --- |
| Runtime | Local Docker Compose for development; AWS for a controlled demonstration | Proposed |
| Phase 0 services | PostgreSQL 16 and MinIO only | Proposed |
| Object storage | MinIO locally; Amazon S3 on AWS | Proposed |
| Table format | Parquet and Apache Iceberg after version compatibility is proven | Proposed |
| Catalog / SQL | Glue Catalog and Athena on AWS; local catalog chosen after the Phase 2 PoC | Proposed |
| Batch compute / transform | Spark, Airflow, dbt | Proposed |
| CDC | PostgreSQL logical replication, Debezium, Kafka, Spark Structured Streaming (Phase 4) | Proposed |
| Observability | Prometheus, Grafana; OpenLineage only on components with verified support | Proposed |
| IaC / CI | Terraform; GitHub Actions; OIDC in preference to long-lived cloud keys | Proposed |
| Kubernetes | Out of scope unless a later requirement justifies it | Proposed |

Analytical models (`fct_orders`, `fct_order_items`, `fct_payments`, `dim_customers`, `dim_products`, `dim_date`, `mart_daily_sales`, `mart_customer_retention`) are placeholders until grain, keys, and update rules are documented.

## Risks and limitations

- The local stack and Python toolchain are not in the repository yet; they are not operational.
- Spark, Iceberg, catalog, and query-engine compatibility is unproven. That combination will not be expanded until a minimal create/write/query PoC succeeds.
- The historical e-commerce dataset will not be vendored until license and redistribution terms are confirmed.
- Exactly-once processing will not be claimed without a specific technical proof.
- Cloud apply/destroy and other cost-incurring operations require explicit approval.

## Blocked

None.

## Next work

1. **P0-06** — `ROADMAP.md` (phases 0–7, acceptance criteria, out-of-scope notes).
2. **P0-07** — `DECISIONS.md` with the target stack marked proposed.
3. Phase 0 implementation plan for P0-08–P0-13, then implementation in small slices after approval.

## Latest validation

| | |
| --- | --- |
| Date | 2026-10-14 |
| Environment | WSL2 / Ubuntu, repository root |
| Runtime checks | Not applicable — no application, Compose file, or test runner yet |
| Result | Documentation and repository baseline only |

## Log

### 2026-10-14 — Phase 0 started

Initialized the public repository and the operating model for the program.

| Commit | Description |
| --- | --- |
| `ab3c9a6` | MIT license |
| `dc49fd8` | `AGENTS.md` and contributor workflow |
| `71e96c6` | Engineering rules by concern |

Also delivered: this status ledger and the program context under `docs/`.
