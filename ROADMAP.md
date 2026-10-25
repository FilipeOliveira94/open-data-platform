# Roadmap

Issue-level plan for the Open Data Platform. This file is the source of truth for **scope, dependencies, issue status and acceptance criteria**. `PROJECT_STATUS.md` is the **current-phase** operational ledger (progress, latest validation, active gates). Longer history lives in `docs/status-log.md`.

**Estimated window:** ~16 weeks / 240–320 hours at 15+ h/week. Windows are planning guides, not delivery promises.

**Status values:** `not started` · `in progress` · `delivered` · `verified` · `blocked`

- Mark an issue **delivered** only when the change exists in the repository.
- Mark an issue **verified** only when its acceptance checks have been run and the result is recorded in `PROJECT_STATUS.md`.
- Do not start a phase until its listed dependencies are satisfied (or an explicit waiver is recorded in `DECISIONS.md`).
- Do not create empty directories for later phases until that phase starts.
- Within a phase, keep **feature / source issues before phase-closing** validation and docs issues (failure tests, regression checks, “Phase N documentation”). New work must not be appended after those closing IDs.

---

## Target update patterns (required use cases)

The platform must demonstrate **three distinct ways** to refresh data from the transactional PostgreSQL source (and related batch sources). CDC does **not** replace scheduled batch loads.

| Use case | Pattern | Where it lives | What “done” means |
| --- | --- | --- | --- |
| Daily batch | Scheduled incremental extract → lakehouse | Phase 1 extract + Phase 3 Airflow (`@daily` or cron) | Idempotent daily window; watermark / partition documented; re-run safe |
| Weekly batch | Scheduled incremental or snapshot extract on a weekly cadence | Phase 1 extract + Phase 3 Airflow (`@weekly` or cron) | Distinct weekly DAG (or schedule); grain and why-weekly documented; re-run safe |
| Streaming | Continuous CDC (logical replication → Debezium → Kafka → Spark Structured Streaming) | Phase 4 | Inserts, updates and deletes handled with explicit delivery semantics |

**Clarification:** Phase 4 CDC is near-real-time change capture. Daily and weekly refreshes are **batch** pipelines orchestrated by Airflow, typically driven by watermarks (`updated_at`, business date, or agreed high-water mark)—not by Debezium schedules.

---

## Phase overview

| Phase | Scope | Window | Depends on | Status |
| --- | --- | --- | --- | --- |
| 0 | Repository, Python toolchain, PostgreSQL 16, MinIO, CI | Sprint 1 (week 1) | — | Verified |
| 1 | Multi-source ingestion (historical, API, PostgreSQL incremental extract, synthetic) | Sprint 2 (weeks 1–2) | Phase 0 verified | In progress (P1-01, P1-02, P1-03, P1-04, P1-05, P1-06 verified) |
| 2 | Lakehouse PoC, Bronze / Silver / Gold, initial dbt | Sprint 3 (weeks 3–4) | Phase 1 verified; stack PoC | Not started |
| 3 | Airflow: daily + weekly batch schedules, backfills, quality and quarantine | Sprint 4 (weeks 5–6) | Phase 2 verified | Not started |
| 4 | Streaming CDC (logical replication, Debezium, Kafka, Spark SS) | Sprint 5 (weeks 7–9) | Phase 3 verified | Not started |
| 5 | Observability, lineage, cost visibility | Sprint 6 (weeks 10–11) | Phase 3 verified (Phase 4 optional for stream metrics) | Not started |
| 6 | CI/CD expansion, resilience tests, security checks | Sprint 6 (weeks 12–13) | Phases 1–3 verified; Phase 4/5 as applicable | Not started |
| 7 | AWS deployment path as IaC (Terraform, local→AWS mapping), case-study docs | Sprint 6 (weeks 14–16) | Phase 6 verified | Not started |
| 8 | Platform report site: multi-page static report | Sprint 7 (weeks 16–18) | Phase 7 verified | Not started |

### Platform report site — growth scheme

Public-facing **platform report** (not a separate product): a small static hub that presents the verified platform. Tone stays technical and factual.

| Page | Role | Primary sources |
| --- | --- | --- |
| **Home** | What the platform is, current status, one architecture sketch | `README.md`, `PROJECT_STATUS.md` |
| **Architecture** | Stack, medallion layers, contracts, batch vs streaming coexistence | `docs/architecture/`, ADRs, `DECISIONS.md` |
| **Pipeline** | Ingestion → Bronze / Silver / Gold, orchestration, CDC | Phase 1–4 docs and diagrams |
| **Operations** | Quality, observability, lineage, CI, local→AWS IaC path | Phases 5–7 docs; P7 case study |
| **Reference** | Links to repository, ADRs, roadmap, setup | Repo root docs |

**Incremental rule (Phases 1–7):** a scaffold under `site/` may exist early (Astro Starlight + Catppuccin theme). Content updates must use only facts already **verified** in `PROJECT_STATUS.md`. Do **not** complete public hosting or final copy polish until Phase 8.

**Phase 7 vs Phase 8:** P7-07 delivers the **in-repo case study** (markdown, diagrams, reproduction steps). Phase 8 turns that material into the **deployed multi-page report** and sets the GitHub repository Website URL.

---

## Phase 0 — Repository and environment foundation

**Goal:** Reproducible local baseline and program documents that govern later work.

**Out of scope:** Airflow, Kafka, Spark, Trino, Iceberg, Kubernetes, AWS Terraform path.

**Phase acceptance (all required):**

- [x] A clean clone can follow the documented setup.
- [x] PostgreSQL is healthy and reachable.
- [x] A smoke test can write and read an object in MinIO.
- [x] Lint and tests run via documented commands.
- [x] No secrets, live `.env` files, or private datasets are in version control.
- [x] CI runs the same lint and test checks.

### Issues

| ID | Title | Depends on | Status | Acceptance |
| --- | --- | --- | --- | --- |
| P0-01 | Public GitHub monorepo, MIT license | — | Delivered | `LICENSE` present; repo public under MIT |
| P0-02 | Agent and contributor operating contract | — | Delivered | `AGENTS.md` and `docs/agent-workflow.md` define workflow |
| P0-03 | Project engineering rules | P0-02 | Delivered | `.cursor/rules/` covers principles, Python/testing, data engineering, infra/security, workflow |
| P0-04 | Project status ledger | — | Delivered | `PROJECT_STATUS.md` distinguishes delivered vs verified |
| P0-05 | Program context document | — | Delivered | `docs/project-context-for-cursor.md` captures objectives, constraints, target architecture |
| P0-06 | Program roadmap | P0-04, P0-05 | Delivered | This file: phases 0–8, issue IDs, dependencies, acceptance, out-of-scope |
| P0-07 | Decision log scaffold | P0-06 | Delivered | `DECISIONS.md` is a summary index; ADRs under `docs/decisions/` hold rationale; proposed vs approved visible |
| P0-08 | Python 3.12 package with `uv` | — | Delivered | Package under `src/open_data_platform/`; `pyproject.toml` + committed lockfile; installable via documented command |
| P0-09 | Ruff and pytest smoke | P0-08 | Verified | Lint and a toolchain smoke test pass via documented commands |
| P0-10 | Docker Compose: PostgreSQL 16 and MinIO | — | Verified | Pinned image tags; health checks; documented start/stop; no unverified tags |
| P0-11 | Repo hygiene and README | P0-08, P0-10 | Delivered | `.gitignore`, `.env.example` (names/placeholders only), Makefile, `README.md` with setup |
| P0-12 | GitHub Actions: lint and tests | P0-09 | Verified | Workflow runs lint + tests; no cloud deploy |
| P0-13 | Initial decision note (Compose vs AWS; Phase 0 services) | P0-07 | Delivered | Approve/update D-001 (and D-002 as applicable) in `DECISIONS.md`/ADRs: Compose for local verification; AWS as Terraform IaC in Phase 7 |

**Suggested order:** P0-06 → P0-07 → P0-08 → P0-09 → P0-10 → P0-11 → P0-12 → P0-13 (P0-08 and P0-10 may proceed in parallel after P0-07).

---

## Phase 1 — Multi-source ingestion

**Goal:** Idempotent batch ingestion from four source types into auditable raw storage, with contracts and deterministic tests. PostgreSQL extract must support **windowed incremental loads** usable by daily and weekly Airflow schedules (scheduling itself is Phase 3).

**Depends on:** Phase 0 verified.

**Out of scope:** Streaming/CDC, Iceberg, Airflow schedules, lakehouse transforms.

**Phase acceptance (all required):**

- [ ] Ingestions can be repeated without undue duplication or corruption.
- [x] Unit tests do not require the public API to be online.
- [ ] Raw data is traceable by source and execution.
- [x] Contracts and expected fields are documented.
- [ ] Invalid records and errors have explicit, documented behavior.
- [x] PostgreSQL extract accepts an explicit time/watermark window suitable for daily and weekly batch runs.

### Issues

| ID | Title | Depends on | Status | Acceptance |
| --- | --- | --- | --- | --- |
| P1-01 | Historical dataset license and redistribution check | Phase 0 | Verified | License/terms recorded for Olist e-commerce + marketing funnel (CC BY-NC-SA 4.0); no dataset vendored; fetch-from-source documented (`docs/sources/`, ADR-0018) |
| P1-02 | Raw storage layout and ingestion metadata conventions | Phase 0 | Verified | Paths, partition conventions, `_ingestion_ts`, source id, execution id documented and used by writers |
| P1-03 | Data contracts for planned sources | P1-02 | Verified | Contracts under `config/contracts/` (or agreed path); schema/fields/null rules explicit |
| P1-04 | Historical e-commerce dataset ingestion | P1-01, P1-02, P1-03 | Verified | Repeatable ingest of Olist e-commerce (+ marketing funnel join on `seller_id`) to raw layout; idempotent re-run; unit/integration coverage without private/vendored Olist data |
| P1-05 | Paginated public API client (DummyJSON products) | P1-02, P1-03 | Verified | DummyJSON `/products` (`limit`/`skip`); timeouts, retries, rate-limit handling; fixtures; no live API in unit tests; lands in Gold `product_analysis` (D-019, D-012) |
| P1-06 | PostgreSQL incremental batch extract | P0-10, P1-02, P1-03 | Verified | Parameterized extract by watermark/window (`start`/`end` or high-water mark); grain/keys documented; same code path supports daily and weekly windows; idempotent re-run without corrupting raw; OLTP seed per `docs/sources/postgres-oltp.md` |
| P1-07 | Synthetic event generator | P1-02 | Not started | Deterministic or seedable generator for later streaming scenarios; schema documented |
| P1-08 | IBGE Localidades ingest | P1-02, P1-03 | Not started | Ingest IBGE localities into raw; contracts; geo reference for `olist_analysis` (D-019) |
| P1-09 | BrasilAPI CEP enrichment (optional) | P1-02, P1-04 | Not started | Optional: small cached CEP lookups only; no CEP crawl; enrich Olist zip prefixes when scheduled |
| P1-10 | Failure and invalid-record behavior | P1-04–P1-08 | Not started | Explicit handling for nulls, duplicates, invalid schemas; tests for failure paths |
| P1-11 | Phase 1 documentation and status | P1-10 | Not started | README/docs updated; three update patterns (daily batch, weekly batch, streaming) referenced; `PROJECT_STATUS.md` records verification evidence |

**Suggested order:** P1-01 ∥ P1-02 → P1-03 → P1-04 ∥ P1-05 ∥ P1-06 ∥ P1-07 ∥ P1-08 → P1-10 → P1-11; P1-09 optional after P1-04 layout exists.

---

## Phase 2 — Lakehouse and analytics

**Goal:** Prove Spark + Iceberg + catalog + query-engine compatibility, then implement Bronze / Silver / Gold and initial dbt models.

**Depends on:** Phase 1 verified. **Hard gate:** P2-01 must succeed before scaling transforms.

**Out of scope:** Airflow orchestration, CDC/streaming, AWS lakehouse modules (Phase 7 IaC).

**Phase acceptance (all required):**

- [ ] A table can be created, written and queried in the chosen local stack.
- [ ] Queries and transforms are reproducible via documented commands.
- [ ] Quality and reconciliation rules have tests.
- [ ] Documentation lists component versions and known limitations.

### Issues

| ID | Title | Depends on | Status | Acceptance |
| --- | --- | --- | --- | --- |
| P2-01 | Minimal Spark + Iceberg + catalog + query PoC | Phase 1 | Not started | Create / write / catalog / query one table; versions pinned; result recorded; incompatible combos rejected with a decision note |
| P2-02 | Local catalog and query-engine choice | P2-01 | Not started | Local table catalog (and Trino or alternative) chosen after PoC; recorded in D-004 / `DECISIONS.md` |
| P2-03 | Bronze layer from raw ingestion | P2-01, P1-02 | Not started | Bronze preserves source payload + ingestion metadata; grain and PK documented |
| P2-04 | Silver normalization and deduplication | P2-03 | Not started | Typed, deduplicated Silver tables; null/duplicate/invalid handling intentional |
| P2-05 | Analytical model design (grain, keys, SCD) | P2-04 | Not started | Document grain/keys/update rules for facts, dims and marts before implementing models |
| P2-06 | dbt project and initial Gold models | P2-05 | Not started | dbt project builds; placeholders replaced only for models with documented contracts (`fct_*`, `dim_*`, `mart_*` as approved) |
| P2-07 | Data quality and reconciliation tests | P2-06 | Not started | dbt/Python tests cover key metrics vs source; failures are visible |
| P2-08 | Phase 2 docs (versions, partitions, limitations) | P2-01, P2-07 | Not started | Partitioning, file sizing, incremental strategy and stack limitations documented |

**Suggested order:** P2-01 → P2-02 → P2-03 → P2-04 → P2-05 → P2-06 → P2-07 → P2-08.

**Analytical models (placeholders until P2-05), by Gold domain:**

- `olist_analysis`: `fct_orders`, `fct_order_items`, `fct_payments`, `dim_customers`, `dim_products`, `dim_date`, `mart_daily_sales`, `mart_customer_retention` (plus funnel/geo as approved).
- `product_analysis`: DummyJSON product marts (names locked in P2-05); separate schema/folder from Olist.

---

## Phase 3 — Orchestration and data quality

**Goal:** Orchestrate validated batch flows with Airflow—including **daily** and **weekly** PostgreSQL (and related) schedules—prove idempotent backfills and quarantine. This phase delivers the two batch use cases; streaming remains Phase 4.

**Depends on:** Phase 2 verified.

**Out of scope:** CDC, Kafka, streaming.

**Phase acceptance (all required):**

- [ ] DAGs have clear dependencies and verifiable failure behavior.
- [ ] A **daily** scheduled batch path exists for PostgreSQL incremental extract (and downstream transforms as designed).
- [ ] A **weekly** scheduled batch path exists (distinct schedule and documented purpose/grain vs daily).
- [ ] Backfills do not corrupt data or create undue duplication.
- [ ] Invalid records are identified and handled per documented rules.
- [ ] Success and failure scenarios have tests or reproducible evidence.

### Issues

| ID | Title | Depends on | Status | Acceptance |
| --- | --- | --- | --- | --- |
| P3-01 | Airflow in local Compose | Phase 2 | Not started | Airflow runs locally; pinned images; documented UI/CLI access; business logic stays out of DAG bodies |
| P3-02 | Ingestion DAGs (thin wrappers) | P3-01, Phase 1 | Not started | DAGs call package entrypoints; retries/timeouts configured; no duplicated business logic |
| P3-03 | Transform / dbt DAGs | P3-01, Phase 2 | Not started | Orchestrates Bronze→Silver→Gold/dbt with clear task dependencies |
| P3-04 | Daily batch schedule (PostgreSQL) | P3-02, P1-06 | Not started | Airflow DAG with daily schedule passes watermark/window into P1-06 extract; documented cadence, catchup policy and idempotency; manual trigger proves one successful daily-shaped run |
| P3-05 | Weekly batch schedule (PostgreSQL) | P3-02, P1-06 | Not started | Distinct weekly schedule (separate DAG or explicit weekly timetable); purpose/grain vs daily documented; watermark/window for a week; manual trigger proves one successful weekly-shaped run |
| P3-06 | Idempotent backfill support | P3-02, P3-03 | Not started | Documented backfill parameters for daily and weekly windows; re-run evidence shows no corruption/undue duplicates |
| P3-07 | Quality checks and quarantine | P3-03, P2-07 | Not started | Invalid records routed per contract; quarantine location and reprocess path documented |
| P3-08 | Execution metadata and failure logging | P3-02–P3-07 | Not started | Execution IDs, failure reasons and run metadata are inspectable |
| P3-09 | Orchestration success/failure tests | P3-04, P3-05, P3-06, P3-07 | Not started | Automated or scripted evidence for happy path, retry and failed-run handling for daily and weekly DAGs |

**Suggested order:** P3-01 → P3-02 ∥ P3-03 → P3-04 ∥ P3-05 → P3-06 → P3-07 → P3-08 → P3-09.

---

## Phase 4 — Streaming CDC

**Goal:** Deliver the **streaming** use case: continuous change capture from PostgreSQL through Debezium/Kafka into a streaming processor, with honest delivery semantics. Complements—does not replace—daily and weekly batch (Phase 3).

**Depends on:** Phase 3 verified (daily and weekly batch paths exist); PostgreSQL from Phase 0/1 available for logical replication.

**Out of scope:** Using CDC as a substitute for daily/weekly scheduled batch loads; claiming exactly-once end-to-end without a specific technical proof; breaking the validated batch path.

**Phase acceptance (all required):**

- [ ] Inserts, updates and deletes are handled correctly on the streaming path.
- [ ] Duplicates and out-of-order events have explicit handling.
- [ ] Restart and replay do not corrupt sinking state.
- [ ] Docs contrast streaming CDC vs daily/weekly batch and do not claim “exactly once” without proof.
- [ ] Validated daily and weekly batch paths still work after CDC is introduced.

### Issues

| ID | Title | Depends on | Status | Acceptance |
| --- | --- | --- | --- | --- |
| P4-01 | PostgreSQL logical replication prerequisites | P0-10, Phase 3 | Not started | WAL level / publications documented; safe local config; no secrets committed |
| P4-02 | Debezium and Kafka Compose stack | P4-01 | Not started | Pinned versions; topic/connector config documented; health checks where supported |
| P4-03 | CDC path for insert / update / delete | P4-02 | Not started | Reproducible demo of I/U/D landing in Kafka (or agreed sink) with schema documented |
| P4-04 | Spark Structured Streaming consumer | P4-03, P2-01 | Not started | Consumer processes CDC events into lakehouse tables; checkpoints documented |
| P4-05 | Semantics documentation (batch vs stream) | P4-03, P4-04, P3-04, P3-05 | Not started | Snapshots, duplicates, ordering, late events, deletes, checkpoints, replay and recovery documented; explicit comparison of daily batch, weekly batch and streaming CDC |
| P4-06 | Streaming resilience scenarios | P4-04, P4-05 | Not started | Tests or runbooks for duplicates, out-of-order, restart/replay and deletes |
| P4-07 | Batch path regression check | P3-04, P3-05, P4-04 | Not started | Evidence that daily and weekly batch extracts still succeed alongside the streaming path |

**Suggested order:** P4-01 → P4-02 → P4-03 → P4-04 → P4-05 → P4-06 → P4-07.

---

## Phase 5 — Observability, lineage and FinOps

**Goal:** Diagnose failures/latency with real metrics; show lineage from real runs; document a FinOps cost model for the AWS target architecture with explicit assumptions.

**Depends on:** Phase 3 verified. Stream-specific metrics may wait for Phase 4.

**Out of scope:** Invented metrics, benchmarks, lineage graphs or cost figures.

**Phase acceptance (all required):**

- [ ] Relevant failures and delays can be diagnosed from logs/metrics.
- [ ] Demonstrated lineage matches actual executions.
- [ ] Metrics are measured, not invented.
- [ ] Cost model documents assumptions, unit drivers and estimated monthly ranges for the AWS target design.

### Issues

| ID | Title | Depends on | Status | Acceptance |
| --- | --- | --- | --- | --- |
| P5-01 | Structured logging conventions | Phase 3 | Not started | Shared fields (execution id, source, layer) documented and used in key paths |
| P5-02 | Prometheus and Grafana local stack | P5-01 | Not started | Services in Compose; pinned images; documented access |
| P5-03 | Pipeline metrics and dashboards | P5-02 | Not started | Useful dashboards for lag, failures, throughput where instrumented |
| P5-04 | OpenLineage where supported | Phase 3 (and 4 if streaming) | Not started | Lineage emitted only for components with verified support; demo from a real run |
| P5-05 | Cost model for AWS target architecture | — | Not started | Documented cost model with assumptions, drivers and estimated ranges aligned to the Phase 7 AWS design |
| P5-06 | Alerts and runbooks | P5-03 | Not started | Alert thresholds and at least one failure runbook under `docs/runbooks/` |

**Suggested order:** P5-01 → P5-02 → P5-03 → P5-04 ∥ P5-05 → P5-06.

---

## Phase 6 — CI/CD, resilience and security

**Goal:** Automate quality gates beyond Phase 0; prove recovery paths; keep secrets out of logs and artifacts.

**Depends on:** Phases 1–3 verified; include Phase 4/5 checks when those components exist.

**Out of scope:** Cloud provisioning workflows (Phase 7 focuses on Terraform IaC and CI validation); weakening tests to force green CI.

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
| P6-05 | Resilience test evidence | P3-09; P4-06 if present | Not started | Failure/retry/recovery scenarios executed and linked from `PROJECT_STATUS.md` |
| P6-06 | Security checks | P6-01 | Not started | Appropriate secret scanning / dependency audit; no credential leakage in CI logs |

**Suggested order:** P6-01 → P6-02 ∥ P6-04 → P6-03 (when Terraform exists) → P6-05 → P6-06.

---

## Phase 7 — AWS deployment path as Infrastructure as Code

**Goal:** Deliver a clear AWS target architecture and Terraform modules that show how the verified local lakehouse maps to cloud services (S3, Glue Catalog, Athena, IAM/OIDC)—reproducible Infrastructure as Code for provisioning.

**Depends on:** Phase 6 verified. Cloud `apply`/`destroy` require explicit user approval (see `AGENTS.md`); CI validates Terraform with `fmt` / `validate`.

**Out of scope:** Multi-account enterprise landing zones; Kubernetes unless a later approved requirement; embedding managed EMR/MWAA/MSK as required core modules (document as optional AWS counterparts to local Spark/Airflow/Kafka).

**Phase acceptance (all required):**

- [ ] Local→AWS service mapping is documented (MinIO→S3, local catalog→Glue, Trino→Athena; compute/orchestration counterparts noted).
- [ ] Terraform modules are modular, readable and aligned with the target architecture.
- [ ] IAM follows least privilege in code and documentation.
- [ ] GitHub Actions OIDC to AWS is defined as Terraform and documented.
- [ ] CI runs `terraform fmt -check` and `validate`.
- [ ] Cost model states assumptions and estimated ranges for the AWS design.
- [ ] Case study presents the local platform and the AWS IaC path with diagrams and reproduction steps.

### Issues

| ID | Title | Depends on | Status | Acceptance |
| --- | --- | --- | --- | --- |
| P7-01 | Local→AWS architecture and service mapping | Phase 6 | Not started | Written mapping: MinIO→S3, local catalog→Glue, Trino→Athena; optional AWS counterparts for Airflow/Spark/Kafka documented; diagrams under `docs/architecture/` |
| P7-02 | Terraform scaffold under `infrastructure/terraform/` | P7-01 | Not started | Module layout, variables, outputs, README; backend configuration documented (local for CI; remote S3+DynamoDB lock as the recommended production pattern) |
| P7-03 | Core AWS resources as code (S3, Glue, Athena, IAM) | P7-02 | Not started | Modules for lakehouse buckets, Glue catalog/database, Athena workgroup, least-privilege IAM; `terraform validate` succeeds in CI |
| P7-04 | GitHub Actions OIDC to AWS | P7-03 | Not started | OIDC provider and deploy role as Terraform + docs; no long-lived access keys in the repository |
| P7-05 | CI: Terraform fmt and validate | P7-02, P6-03 | Not started | Workflow runs `terraform fmt -check` and `validate` on the IaC tree |
| P7-06 | AWS cost model | P7-01 | Not started | Cost model with assumptions, drivers and monthly estimate ranges for the documented AWS design |
| P7-07 | Case study: README, diagrams, demo path | P7-03, P7-04, P7-05, P7-06 | Not started | In-repo case study: local platform + AWS IaC path, architecture diagrams, setup and provisioning guidance; content ready for Phase 8 site consumption |

**Suggested order:** P7-01 → P7-02 → P7-03 → P7-04 ∥ P7-05 ∥ P7-06 → P7-07.

---

## Phase 8 — Platform report site

**Goal:** Finish the program with a small, polished **multi-page static report** that documents the verified platform (home, architecture, pipeline, operations, reference). Host it and link it from the repository Website field and the README.

**Depends on:** Phase 7 verified (in-repo case study and AWS IaC path available as source material).

**Out of scope:** Backend, CMS, authentication, analytics pixels, interactive demos that require a live cluster, duplicating the full `ROADMAP.md` issue backlog on the site.

**Phase acceptance (all required):**

- [ ] Static site under `site/` (or equivalent) with the five pages populated from verified platform material.
- [ ] Content matches delivered/verified work; no planned-but-unbuilt capabilities presented as complete.
- [ ] Site builds locally via a documented command; CI builds (and optionally deploys) the static output.
- [ ] Public URL is set on the GitHub repository Website field and linked from the README Platform report block.
- [ ] Navigation and page roles match the growth scheme above; reference page links back to repo docs/ADRs.

### Issues

| ID | Title | Depends on | Status | Acceptance |
| --- | --- | --- | --- | --- |
| P8-01 | Site scaffold and page map | Phase 7 | In progress | `site/` as Astro Starlight + `@catppuccin/starlight`; five routes (Home, Architecture, Pipeline, Operations, Reference); local preview via `npm run dev`. Early scaffold allowed before Phase 7; Phase 8 completes content depth and hosting |
| P8-02 | Home and Architecture pages | P8-01, P7-07 | Not started | Home states platform purpose and verified status; Architecture covers stack, medallion, contracts, batch vs stream with diagrams sourced from `docs/` |
| P8-03 | Pipeline and Operations pages | P8-01, P7-07 | Not started | Pipeline covers ingestion through Gold, orchestration and CDC; Operations covers quality, observability, lineage, CI and local→AWS IaC path |
| P8-04 | Reference page and repo cross-links | P8-02, P8-03 | Not started | Reference links to GitHub, ADRs, roadmap, setup; README Platform report block points to the live URL (or staging URL if deploy is gated) |
| P8-05 | Static hosting and GitHub Website field | P8-04 | Not started | Hosting documented (e.g. GitHub Pages or equivalent); CI build green; repository About → Website set to the report URL |
| P8-06 | Final copy and consistency pass | P8-05 | Not started | Tone is factual platform documentation; status/claims aligned with `PROJECT_STATUS.md`; no unverified metrics or cost figures |


**Suggested order:** P8-01 → P8-02 ∥ P8-03 → P8-04 → P8-05 → P8-06.

---

## Cross-cutting rules

| Topic | Rule |
| --- | --- |
| Three update patterns | Daily batch, weekly batch and streaming CDC are all in scope; each must remain distinguishable in docs and demos |
| Batch vs CDC | Scheduled batch uses windowed extracts + Airflow; CDC uses logical replication—do not conflate the two |
| Exactly-once | Never claim end-to-end exactly-once without a written technical proof for the specific path |
| New technology | State purpose and operational cost; prefer proving compatibility before expanding |
| Kubernetes | Out of scope unless an explicit approved requirement |
| Secrets | Never commit credentials, tokens, live `.env`, or private datasets |
| Cloud / AWS IaC | Phase 7 delivers the AWS path as Terraform IaC and local→AWS mapping. CI validates with `fmt`/`validate`. Cost models must state assumptions. Never invent metrics or spend figures. Do not run `terraform apply`/`destroy` without explicit user approval |
| Platform report site | Scaffold may land early; full content, design and hosting finalized in Phase 8. Earlier phases may add verified facts only. Site summarizes the platform; it does not replace `PROJECT_STATUS.md` / ADRs as source of truth |
| Empty dirs | Do not scaffold later-phase trees until that phase starts |
| Language | Code/docs in English; agent↔user communication in Brazilian Portuguese |

---

## How to use this roadmap

1. Pick the next issue from the current phase (see `PROJECT_STATUS.md` → Next work).
2. Plan with the issue ID (see `docs/agent-workflow.md`); wait for approval on non-trivial changes.
3. Implement one coherent issue; run its acceptance checks.
4. Update `PROJECT_STATUS.md` (current-phase row + latest validation); mark the issue verified only with evidence; append older narrative to `docs/status-log.md` when trimming.
5. Record architecture choices as ADRs in `docs/decisions/` and update the one-line row in `DECISIONS.md` when a proposed item is approved or replaced.

---

## Document control

| | |
| --- | --- |
| Created | 2026-10-15 |
| Last updated | 2026-10-18 |
| Owns | Program scope and issue acceptance |
| Related | `PROJECT_STATUS.md`, `docs/status-log.md`, `DECISIONS.md`, `docs/project-context-for-cursor.md`, `AGENTS.md` |
| Changelog | 2026-10-18: P1-02 verified (D-020 raw layout + metadata helpers). 2026-10-17: Source metadata standardized; P1-08 IBGE / P1-09 CEP before closing P1-10/P1-11; Phase 3 daily/weekly → P3-04/P3-05 (tests P3-09). D-019 approved; Gold domains D-012; status ledger scoped; P1-01 verified. 2026-10-16: site scaffold; Phase 8; Phase 0 verified. 2026-10-15: ADRs; AWS IaC; three update patterns |
