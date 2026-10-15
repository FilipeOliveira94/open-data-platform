# Roadmap

Issue-level plan for the Open Data Platform. This file is the source of truth for **scope, dependencies and acceptance criteria**. `PROJECT_STATUS.md` records what has been **delivered and verified**.

**Estimated window:** ~16 weeks / 240–320 hours at 15+ h/week. Windows are planning guides, not delivery promises.

**Status values:** `not started` · `in progress` · `delivered` · `verified` · `blocked`

- Mark an issue **delivered** only when the change exists in the repository.
- Mark an issue **verified** only when its acceptance checks have been run and the result is recorded in `PROJECT_STATUS.md`.
- Do not start a phase until its listed dependencies are satisfied (or an explicit waiver is recorded in `DECISIONS.md`).
- Do not create empty directories for later phases until that phase starts.

---

## Phase overview

| Phase | Scope | Window | Depends on | Status |
| --- | --- | --- | --- | --- |
| 0 | Repository, Python toolchain, PostgreSQL 16, MinIO, CI | Sprint 1 (week 1) | — | In progress |
| 1 | Multi-source ingestion (historical, API, PostgreSQL, synthetic) | Sprint 2 (weeks 1–2) | Phase 0 verified | Not started |
| 2 | Lakehouse PoC, Bronze / Silver / Gold, initial dbt | Sprint 3 (weeks 3–4) | Phase 1 verified; stack PoC | Not started |
| 3 | Airflow, idempotent backfills, quality and quarantine | Sprint 4 (weeks 5–6) | Phase 2 verified | Not started |
| 4 | CDC and streaming (logical replication, Debezium, Kafka, Spark SS) | Sprint 5 (weeks 7–9) | Phase 3 verified | Not started |
| 5 | Observability, lineage, cost visibility | Sprint 6 (weeks 10–11) | Phase 3 verified (Phase 4 optional for stream metrics) | Not started |
| 6 | CI/CD expansion, resilience tests, security checks | Sprint 6 (weeks 12–13) | Phases 1–3 verified; Phase 4/5 as applicable | Not started |
| 7 | Minimal AWS path, measured cost, case-study docs | Sprint 6 (weeks 14–16) | Phase 6 verified; explicit cloud approval | Not started |

---

## Phase 0 — Repository and environment foundation

**Goal:** Reproducible local baseline and program documents that govern later work.

**Out of scope:** Airflow, Kafka, Spark, Trino, Iceberg, Kubernetes, AWS deployment.

**Phase acceptance (all required):**

- [ ] A clean clone can follow the documented setup.
- [ ] PostgreSQL is healthy and reachable.
- [ ] A smoke test can write and read an object in MinIO.
- [ ] Lint and tests run via documented commands.
- [ ] No secrets, live `.env` files, or private datasets are in version control.
- [ ] CI runs the same lint and test checks.

### Issues

| ID | Title | Depends on | Status | Acceptance |
| --- | --- | --- | --- | --- |
| P0-01 | Public GitHub monorepo, MIT license | — | Delivered | `LICENSE` present; repo public under MIT |
| P0-02 | Agent and contributor operating contract | — | Delivered | `AGENTS.md` and `docs/agent-workflow.md` define workflow |
| P0-03 | Project engineering rules | P0-02 | Delivered | `.cursor/rules/` covers principles, Python/testing, data engineering, infra/security, workflow |
| P0-04 | Project status ledger | — | Delivered | `PROJECT_STATUS.md` distinguishes delivered vs verified |
| P0-05 | Program context document | — | Delivered | `docs/project-context-for-cursor.md` captures objectives, constraints, target architecture |
| P0-06 | Program roadmap | P0-04, P0-05 | Delivered | This file: phases 0–7, issue IDs, dependencies, acceptance, out-of-scope |
| P0-07 | Decision log scaffold | P0-06 | Not started | `DECISIONS.md` lists target stack as proposed vs approved; ADR path documented |
| P0-08 | Python 3.12 package with `uv` | — | Not started | Package under `src/open_data_platform/`; `pyproject.toml` + committed lockfile; installable via documented command |
| P0-09 | Ruff and pytest smoke | P0-08 | Not started | Lint and a toolchain smoke test pass via documented commands |
| P0-10 | Docker Compose: PostgreSQL 16 and MinIO | — | Not started | Pinned image tags; health checks; documented start/stop; no unverified tags |
| P0-11 | Repo hygiene and README | P0-08, P0-10 | Not started | `.gitignore`, `.env.example` (names/placeholders only), Makefile, `README.md` with setup |
| P0-12 | GitHub Actions: lint and tests | P0-09 | Not started | Workflow runs lint + tests; no cloud deploy |
| P0-13 | Initial decision note (Compose vs AWS; Phase 0 services) | P0-07 | Not started | Decision recorded: local Compose for dev; Phase 0 limited to PostgreSQL + MinIO |

**Suggested order:** P0-06 → P0-07 → P0-08 → P0-09 → P0-10 → P0-11 → P0-12 → P0-13 (P0-08 and P0-10 may proceed in parallel after P0-07).

---

## Phase 1 — Multi-source ingestion

**Goal:** Idempotent batch ingestion from four source types into auditable raw storage, with contracts and deterministic tests.

**Depends on:** Phase 0 verified.

**Out of scope:** Streaming, Iceberg, Airflow, lakehouse transforms.

**Phase acceptance (all required):**

- [ ] Ingestions can be repeated without undue duplication or corruption.
- [ ] Unit tests do not require the public API to be online.
- [ ] Raw data is traceable by source and execution.
- [ ] Contracts and expected fields are documented.
- [ ] Invalid records and errors have explicit, documented behavior.

### Issues

| ID | Title | Depends on | Status | Acceptance |
| --- | --- | --- | --- | --- |
| P1-01 | Historical dataset license and redistribution check | Phase 0 | Not started | License/terms recorded; no dataset vendored until redistribution is allowed (or fetch-from-source documented) |
| P1-02 | Raw storage layout and ingestion metadata conventions | Phase 0 | Not started | Paths, partition conventions, `_ingestion_ts`, source id, execution id documented and used by writers |
| P1-03 | Data contracts for planned sources | P1-02 | Not started | Contracts under `config/contracts/` (or agreed path); schema/fields/null rules explicit |
| P1-04 | Historical e-commerce dataset ingestion | P1-01, P1-02, P1-03 | Not started | Repeatable ingest to raw layout; idempotent re-run; unit/integration coverage without private data |
| P1-05 | Paginated public API client | P1-02, P1-03 | Not started | Timeouts, retries, rate-limit handling; local fixtures; no live-API dependency in unit tests |
| P1-06 | PostgreSQL source extract | P0-10, P1-02, P1-03 | Not started | Parameterized extract; documented grain/keys; repeatable without corrupting raw |
| P1-07 | Synthetic event generator | P1-02 | Not started | Deterministic or seedable generator for later streaming scenarios; schema documented |
| P1-08 | Failure and invalid-record behavior | P1-04–P1-07 | Not started | Explicit handling for nulls, duplicate records, invalid schemas; tests for failure paths |
| P1-09 | Phase 1 documentation and status | P1-08 | Not started | README/docs updated; `PROJECT_STATUS.md` records verification evidence |

**Suggested order:** P1-01 ∥ P1-02 → P1-03 → P1-04 ∥ P1-05 ∥ P1-06 ∥ P1-07 → P1-08 → P1-09.

---

## Phase 2 — Lakehouse and analytics

**Goal:** Prove Spark + Iceberg + catalog + query-engine compatibility, then implement Bronze / Silver / Gold and initial dbt models.

**Depends on:** Phase 1 verified. **Hard gate:** P2-01 must succeed before scaling transforms.

**Out of scope:** Airflow orchestration, CDC/streaming, AWS lakehouse services (those belong to Phase 7).

**Phase acceptance (all required):**

- [ ] A table can be created, written and queried in the chosen local stack.
- [ ] Queries and transforms are reproducible via documented commands.
- [ ] Quality and reconciliation rules have tests.
- [ ] Documentation lists component versions and known limitations.

### Issues

| ID | Title | Depends on | Status | Acceptance |
| --- | --- | --- | --- | --- |
| P2-01 | Minimal Spark + Iceberg + catalog + query PoC | Phase 1 | Not started | Create / write / catalog / query one table; versions pinned; result recorded; incompatible combos rejected with a decision note |
| P2-02 | Local catalog and query-engine choice | P2-01 | Not started | Local catalog (and Trino or alternative) chosen after PoC; recorded in `DECISIONS.md` |
| P2-03 | Bronze layer from raw ingestion | P2-01, P1-02 | Not started | Bronze preserves source payload + ingestion metadata; grain and PK documented |
| P2-04 | Silver normalization and deduplication | P2-03 | Not started | Typed, deduplicated Silver tables; null/duplicate/invalid handling intentional |
| P2-05 | Analytical model design (grain, keys, SCD) | P2-04 | Not started | Document grain/keys/update rules for facts, dims and marts before implementing models |
| P2-06 | dbt project and initial Gold models | P2-05 | Not started | dbt project builds; placeholders replaced only for models with documented contracts (`fct_*`, `dim_*`, `mart_*` as approved) |
| P2-07 | Data quality and reconciliation tests | P2-06 | Not started | dbt/Python tests cover key metrics vs source; failures are visible |
| P2-08 | Phase 2 docs (versions, partitions, limitations) | P2-01, P2-07 | Not started | Partitioning, file sizing, incremental strategy and stack limitations documented |

**Suggested order:** P2-01 → P2-02 → P2-03 → P2-04 → P2-05 → P2-06 → P2-07 → P2-08.

**Analytical models (placeholders until P2-05):** `fct_orders`, `fct_order_items`, `fct_payments`, `dim_customers`, `dim_products`, `dim_date`, `mart_daily_sales`, `mart_customer_retention`.

---

## Phase 3 — Orchestration and data quality

**Goal:** Orchestrate validated batch flows with Airflow; prove idempotent backfills and quarantine.

**Depends on:** Phase 2 verified.

**Out of scope:** CDC, Kafka, streaming.

**Phase acceptance (all required):**

- [ ] DAGs have clear dependencies and verifiable failure behavior.
- [ ] Backfills do not corrupt data or create undue duplication.
- [ ] Invalid records are identified and handled per documented rules.
- [ ] Success and failure scenarios have tests or reproducible evidence.

### Issues

| ID | Title | Depends on | Status | Acceptance |
| --- | --- | --- | --- | --- |
| P3-01 | Airflow in local Compose | Phase 2 | Not started | Airflow runs locally; pinned images; documented UI/CLI access; business logic stays out of DAG bodies |
| P3-02 | Ingestion DAGs (thin wrappers) | P3-01, Phase 1 | Not started | DAGs call package entrypoints; retries/timeouts configured; no duplicated business logic |
| P3-03 | Transform / dbt DAGs | P3-01, Phase 2 | Not started | Orchestrates Bronze→Silver→Gold/dbt with clear task dependencies |
| P3-04 | Idempotent backfill support | P3-02, P3-03 | Not started | Documented backfill parameters; re-run evidence shows no corruption/undue duplicates |
| P3-05 | Quality checks and quarantine | P3-03, P2-07 | Not started | Invalid records routed per contract; quarantine location and reprocess path documented |
| P3-06 | Execution metadata and failure logging | P3-02–P3-05 | Not started | Execution IDs, failure reasons and run metadata are inspectable |
| P3-07 | Orchestration success/failure tests | P3-04, P3-05 | Not started | Automated or scripted evidence for happy path, retry and failed-run handling |

**Suggested order:** P3-01 → P3-02 ∥ P3-03 → P3-04 → P3-05 → P3-06 → P3-07.

---

## Phase 4 — CDC and streaming

**Goal:** End-to-end change capture from PostgreSQL through Debezium/Kafka into a streaming processor, with honest delivery semantics.

**Depends on:** Phase 3 verified; PostgreSQL from Phase 0/1 available for logical replication.

**Out of scope:** Claiming exactly-once end-to-end without a specific technical proof; breaking the validated batch path.

**Phase acceptance (all required):**

- [ ] Inserts, updates and deletes are handled correctly.
- [ ] Duplicates and out-of-order events have explicit handling.
- [ ] Restart and replay do not corrupt sinking state.
- [ ] Docs do not claim “exactly once” without proof.

### Issues

| ID | Title | Depends on | Status | Acceptance |
| --- | --- | --- | --- | --- |
| P4-01 | PostgreSQL logical replication prerequisites | P0-10, Phase 3 | Not started | WAL level / publications documented; safe local config; no secrets committed |
| P4-02 | Debezium and Kafka Compose stack | P4-01 | Not started | Pinned versions; topic/connector config documented; health checks where supported |
| P4-03 | CDC path for insert / update / delete | P4-02 | Not started | Reproducible demo of I/U/D landing in Kafka (or agreed sink) with schema documented |
| P4-04 | Spark Structured Streaming consumer | P4-03, P2-01 | Not started | Consumer processes CDC events into lakehouse tables; checkpoints documented |
| P4-05 | Semantics documentation | P4-03, P4-04 | Not started | Snapshots, duplicates, ordering, late events, deletes, checkpoints, replay and recovery documented |
| P4-06 | Streaming resilience scenarios | P4-04, P4-05 | Not started | Tests or runbooks for duplicates, out-of-order, restart/replay and deletes |

**Suggested order:** P4-01 → P4-02 → P4-03 → P4-04 → P4-05 → P4-06.

---

## Phase 5 — Observability, lineage and FinOps

**Goal:** Diagnose failures/latency with real metrics; show lineage from real runs; separate estimated vs observed cost.

**Depends on:** Phase 3 verified. Stream-specific metrics may wait for Phase 4.

**Out of scope:** Invented metrics, benchmarks, lineage graphs or cost figures; cloud spend without approval.

**Phase acceptance (all required):**

- [ ] Relevant failures and delays can be diagnosed from logs/metrics.
- [ ] Demonstrated lineage matches actual executions.
- [ ] Metrics are measured, not invented.
- [ ] Cost estimates state assumptions; observed AWS costs (if any) are separate.

### Issues

| ID | Title | Depends on | Status | Acceptance |
| --- | --- | --- | --- | --- |
| P5-01 | Structured logging conventions | Phase 3 | Not started | Shared fields (execution id, source, layer) documented and used in key paths |
| P5-02 | Prometheus and Grafana local stack | P5-01 | Not started | Services in Compose; pinned images; documented access |
| P5-03 | Pipeline metrics and dashboards | P5-02 | Not started | Useful dashboards for lag, failures, throughput where instrumented |
| P5-04 | OpenLineage where supported | Phase 3 (and 4 if streaming) | Not started | Lineage emitted only for components with verified support; demo from a real run |
| P5-05 | Cost visibility (estimates vs observed) | — | Not started | Local/AWS cost assumptions documented; observed figures only after real usage |
| P5-06 | Alerts and runbooks | P5-03 | Not started | Alert thresholds and at least one failure runbook under `docs/runbooks/` |

**Suggested order:** P5-01 → P5-02 → P5-03 → P5-04 ∥ P5-05 → P5-06.

---

## Phase 6 — CI/CD, resilience and security

**Goal:** Automate quality gates beyond Phase 0; prove recovery paths; keep secrets out of logs and artifacts.

**Depends on:** Phases 1–3 verified; include Phase 4/5 checks when those components exist.

**Out of scope:** AWS deploy (Phase 7); weakening tests to force green CI.

**Phase acceptance (all required):**

- [ ] CI fails when a required check fails.
- [ ] Integration tests are reproducible and isolated.
- [ ] Recovery scenarios have recorded evidence.
- [ ] Secrets and credentials are not exposed in logs or artifacts.

### Issues

| ID | Title | Depends on | Status | Acceptance |
| --- | --- | --- | --- | --- |
| P6-01 | CI: Python lint/tests hardening | P0-12, Phase 1+ | Not started | Required checks fail the workflow on regression |
| P6-02 | CI: dbt checks | P2-06, P6-01 | Not started | dbt parse/build/test (or agreed subset) runs in CI |
| P6-03 | CI: Terraform validate/fmt | Phase 7 scaffold or early `infrastructure/` | Not started | `terraform fmt -check` and `validate` in CI without apply |
| P6-04 | Isolated integration test job | P0-10, Phase 1+ | Not started | Integration tests run in CI or documented Compose profile; deterministic fixtures |
| P6-05 | Resilience test evidence | P3-07; P4-06 if present | Not started | Failure/retry/recovery scenarios executed and linked from `PROJECT_STATUS.md` |
| P6-06 | Security checks | P6-01 | Not started | Appropriate secret scanning / dependency audit; no credential leakage in CI logs |

**Suggested order:** P6-01 → P6-02 ∥ P6-04 → P6-03 (when Terraform exists) → P6-05 → P6-06.

---

## Phase 7 — AWS, documentation and demonstration

**Goal:** Minimal, economical AWS demonstration path with Terraform, measured cost and a portfolio-ready case study.

**Depends on:** Phase 6 verified. **Hard gate:** no `terraform apply` / destroy / paid usage without explicit user approval.

**Out of scope:** Kubernetes unless a later approved requirement; broad multi-account enterprise landing zones.

**Phase acceptance (all required):**

- [ ] Deployment is reproducible and documented.
- [ ] IAM follows least privilege.
- [ ] CI uses OIDC when applicable (no long-lived AWS keys).
- [ ] Costs, limitations and teardown are clear.
- [ ] Cloud operations run only after explicit approval.

### Issues

| ID | Title | Depends on | Status | Acceptance |
| --- | --- | --- | --- | --- |
| P7-01 | Minimal AWS architecture and cost assumptions | Phase 6 | Not started | Written proposal (S3, Glue, Athena, IAM/OIDC); cost assumptions reviewed; **approval required before apply** |
| P7-02 | Terraform remote state (versioning + lock) | P7-01 approved | Not started | Backend with versioning and locking compatible with chosen Terraform version; no secrets in state docs |
| P7-03 | Core AWS resources (S3, Glue Catalog, Athena, IAM) | P7-02 | Not started | Modules/resources match approved architecture; least-privilege IAM documented |
| P7-04 | GitHub Actions OIDC to AWS | P7-03 | Not started | Short-lived creds via OIDC; no long-lived access keys in repo or CI secrets for deploy role |
| P7-05 | Controlled deploy, verification and cost measurement | P7-04, explicit approval | Not started | Apply after approval; smoke verification; observed cost recorded separately from estimates |
| P7-06 | Teardown runbook | P7-05 | Not started | Documented destroy/cleanup path; approval required before destroy |
| P7-07 | Case study: README, diagrams, runbooks, demo path | P7-05, P7-06 | Not started | Portfolio narrative: architecture, limitations, how to reproduce, what was measured |

**Suggested order:** P7-01 (approve) → P7-02 → P7-03 → P7-04 → P7-05 → P7-06 → P7-07.

---

## Cross-cutting rules

| Topic | Rule |
| --- | --- |
| Exactly-once | Never claim end-to-end exactly-once without a written technical proof for the specific path |
| New technology | State purpose and operational cost; prefer proving compatibility before expanding |
| Kubernetes | Out of scope unless an explicit approved requirement |
| Secrets | Never commit credentials, tokens, live `.env`, or private datasets |
| Cloud cost | Ask before apply, destroy, or any command likely to incur charges |
| Empty dirs | Do not scaffold later-phase trees until that phase starts |
| Language | Code/docs in English; agent↔user communication in Brazilian Portuguese |

---

## How to use this roadmap

1. Pick the next issue from the current phase (see `PROJECT_STATUS.md` → Next work).
2. Plan with the issue ID (see `docs/agent-workflow.md`); wait for approval on non-trivial changes.
3. Implement one coherent issue; run its acceptance checks.
4. Update `PROJECT_STATUS.md` with evidence; mark the issue verified only with evidence.
5. Record architecture choices in `DECISIONS.md` / `docs/decisions/` when a proposed item is approved or replaced.

---

## Document control

| | |
| --- | --- |
| Created | 2026-10-15 |
| Owns | Program scope and issue acceptance |
| Related | `PROJECT_STATUS.md`, `DECISIONS.md`, `docs/project-context-for-cursor.md`, `AGENTS.md` |
