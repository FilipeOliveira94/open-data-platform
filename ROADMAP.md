# Roadmap

Issue-level plan for the Open Data Platform. This file is the source of truth for **scope, dependencies, issue status and acceptance criteria**. `PROJECT_STATUS.md` is the **current-phase** operational ledger (progress, latest validation, active gates). Longer history lives in `docs/status-log.md`.

**Estimated window:** ~16 weeks / 240–320 hours at 15+ h/week, set before Phase 8 existed. Windows are planning guides, not delivery promises.

**Status values:** `not started` · `in progress` · `delivered` · `verified` · `blocked`

**Expected/Seen Risk and Size:** each cell is `expected/seen`. Risk is `Lo`, `Med` or `Hi`; Size is `Sm`, `Md` or `Lg` (scale in `~/.agents/claude/workflow.md`, repo `FilipeOliveira94/agent-config`; Hi risk here is listed in `AGENTS.md` §3). An open issue has only the expected half (`Med/-`, `Md/-`); the seen half is filled when it closes. Issues closed before 2026-11-24 have only the seen half (`-/Hi`, `-/Lg`), rated afterwards from the hand-written lines of the commits that cite the issue (docs and lockfiles excluded), or from the scope when no commit cites it. A withdrawn issue has `-/-`.

- Mark an issue **delivered** only when the change exists in the repository.
- Mark an issue **verified** only when its acceptance checks have been run and the result is recorded in the architecture doc or ADR and in `docs/status-log.md`; `PROJECT_STATUS.md` summarizes it.
- Do not start a phase until its listed dependencies are satisfied (or an explicit waiver is recorded in `DECISIONS.md`).
- Do not create empty directories for later phases until that phase starts.
- From **Phase 2** on, keep **feature / source issues before the phase close**. The last two issues are always, in this order:
  1. **Live run, result, and consistency check** — run the phase path on the real local stack, and on an external source only when that phase's contract requires it. Record the result. Compare it to the phase contracts and to the last verified baseline. Record mismatches. Unit tests alone do not close this issue.
  2. **Phase N documentation and status** — depends on that live-run issue. Docs and `PROJECT_STATUS.md` cite the recorded result.
- New work must not be appended after those two closing IDs.
- From Phase 2 on, a phase is not verified, and the next phase does not start, until both closing issues are verified.
- Phases 0 and 1 stay verified on the results already recorded in `docs/status-log.md`. Do not reopen them for this close pattern.

---

## Target update patterns (required use cases)

The platform must demonstrate **three distinct ways** to refresh data from the transactional PostgreSQL source (and related batch sources). CDC does **not** replace scheduled batch loads.

| Use case | Pattern | Where it lives | What “done” means |
| --- | --- | --- | --- |
| Daily batch | Scheduled incremental extract → lakehouse | Phase 1 extract + Phase 3 Airflow (`@daily` or cron) | Idempotent daily window; watermark / partition documented; re-run safe; a missed window is caught up or reported as a gap; the lakehouse refresh reads the new data, not the whole history |
| Weekly batch | Trailing seven-day re-extract on a weekly cadence | Phase 1 extract + Phase 3 Airflow (`@weekly` or cron) | Distinct weekly DAG; it re-reads the last seven daily windows and lands rows the daily run missed (late commits); grain and why-weekly documented; re-run safe |
| Streaming | Continuous CDC (logical replication → Debezium → Kafka → Spark Structured Streaming) | Phase 4 | Inserts, updates and deletes handled with explicit delivery semantics |

**Deletes:** a windowed batch extract filters on `updated_at` and never sees a hard delete. A deleted row stays in Silver and Gold on the batch path. P3-12 documents that limit; the CDC path (Phase 4) is the one that handles deletes.

**Clarification:** Phase 4 CDC is near-real-time change capture. Daily and weekly refreshes are **batch** pipelines orchestrated by Airflow, typically driven by watermarks (`updated_at`, business date, or agreed high-water mark)—not by Debezium schedules.

---

## Pipeline design checklist

Every issue that adds or changes a pipeline step (ingestion, Bronze, Silver, Gold, a schedule, a trigger, a backfill, a stream) answers each row in its plan and in its acceptance, before code. "Not applicable" needs a reason. A plan that skips a row is not ready for approval. The 2026-11-20 review found four defects that these rows would have caught: a daily schedule that silently skipped missed days, a refresh that re-read all raw history on every run, a Silver writer that dropped the table readers were using, and `update_pattern` labels with no downstream behavior.

| Question | What the issue must state |
| --- | --- |
| Change type | The contract `update_pattern` and what it means for this step: what a new run of the same data does, and what replaces or adds to what |
| Window and watermark | Where the window comes from, what happens to a window that was missed, late data, and overlapping windows |
| Re-run and backfill | The result of re-running the same run, a new run of the same window, and a full rebuild. Which one is idempotent and how it is proven |
| Deletes | Whether this path sees a deleted source row, and what Silver and Gold do with it |
| What a run reads | Whether the work of one run grows with new data or with total history. A scheduled step does not re-read all history unless the issue says so, with the cost |
| Reader view | Whether a reader (Trino, dbt, a check) can see a missing, empty, or half-written table during or after a failed write. Replacing a table that readers use is atomic |
| Ownership and concurrency | Which process writes each table, what may run at the same time, and which paths (staging, work dirs, caches) are shared |
| Trigger | What starts the step, what it starts downstream, and what happens when N upstream runs land close together |
| Failure | The state a failure leaves: raw, tables, markers, and what a retry does |
| Evidence | The live check exercises the gap, re-run, failure, and no-new-data paths that apply, not only the happy path. Durations and row counts are measured, not estimated |

## Phase overview

| Phase | Scope | Window | Depends on | Status |
| --- | --- | --- | --- | --- |
| 0 | Repository, Python toolchain, PostgreSQL 16, MinIO, CI | Sprint 1 (week 1) | — | Verified |
| 1 | Multi-source ingestion (historical, API, PostgreSQL incremental extract, demo storefront clickstream) | Sprint 2 (weeks 1–2) | Phase 0 verified | Verified (P1-09 withdrawn) |
| 2 | Lakehouse PoC, Bronze / Silver / Gold, initial dbt | Sprint 3 (weeks 3–4) | Phase 1 verified; stack PoC | Verified |
| 3 | Airflow: daily + weekly batch schedules, backfills, quality and quarantine | Sprint 4 (weeks 5–6) | Phase 2 verified | In progress (P3-01–P3-08, P3-11–P3-13, P3-16–P3-18 verified; P3-16–P3-18 added by the 2026-11-21 review) |
| 4 | Streaming CDC (logical replication, Debezium, Kafka, Spark SS) | Sprint 5 (weeks 7–9) | Phase 3 verified; P11-01 to P11-04 (shared Kafka broker, Spark–Kafka proof) | Not started |
| 5 | Observability, lineage, cost visibility | Sprint 6 (weeks 10–11) | Phase 3 verified (Phase 4 optional for stream metrics) | Not started |
| 6 | CI/CD expansion, resilience tests, security checks | Sprint 6 (weeks 12–13) | Phases 1–3 verified; Phase 4/5 as applicable | Not started |
| 7 | AWS deployment path as IaC (Terraform, local→AWS mapping), case-study docs | Sprint 6 (weeks 14–16) | Phase 6 verified | Not started |
| 8 | JupyterLab exploration and Superset charts over Gold, read-only via Trino | After Phase 2 (parallel with 3–7) | Phase 2 verified | Not started |
| 9 | Platform report site: multi-page static report | After Phases 7 and 8 | Phase 7 and Phase 8 verified | Not started |
| 10 | Table contracts and quality observability (ODCS contracts, Data Contract CLI, BCB SGS source) | Position not fixed; toward the end, decided by Filipe during execution | Phase 3 verified | Not started |
| 11 | BioGuardia Atlantic operational sources at scale: daily export (about 24M rows a day), telemetry stream (1M events an hour), PII masking, data skew, SCD Type 2, reconciliation | Position not fixed, decided by Filipe; P11-01 to P11-03 run now under a waiver | P11-01 to P11-03: none (waiver, `DECISIONS.md` 2026-11-26); P11-04 onward: Phase 3 verified; P11-08 and P11-15: P5-07 | In progress (P11-01 to P11-03) |

### Platform report site — growth scheme

Public-facing **platform report** (not a separate product): a small static hub that presents the verified platform. Tone stays technical and factual.

| Page | Role | Primary sources |
| --- | --- | --- |
| **Home** | What the platform is, current status, and the on-time delivery chart | `README.md`, `PROJECT_STATUS.md`, figure 1 |
| **Architecture** | Stack, medallion, contracts, batch vs stream, and the read path through Trino to JupyterLab and Superset | `docs/architecture/`, ADRs, figure 10 |
| **Pipeline** | Ingestion → Bronze / Silver / Gold, one chart per Gold domain, orchestration and CDC when those phases are verified | Phase 1–4 docs; figures 2–9 |
| **Operations** | Quality, lineage, CI, local→AWS path; a Grafana image when Phase 5 is verified | Phases 5–7 docs; P7 case study |
| **Reference** | Links to the repository, ADRs, roadmap, setup, and the figure manifest | Repo root docs |

**Figure caption.** Each caption records the question, the value, the grain, what the rate excludes, the Gold table, the tool, the version, and the run id. The number in the caption is the number in the image and the number recorded for that run.

**Site figures.** Phase 8 draws them. Phase 9 publishes them as static images under `site/src/assets/figures/`, with a caption manifest. The site does not embed Jupyter or Superset. `.ipynb` files stay free of outputs. An image shows an aggregate. It does not show an order id, a review text, an email, a customer name, or a single-person location. Olist figures are aggregates of the CC BY-NC-SA source, the same class of fact already written in the Gold docs, and the caption says so. A Grafana image is added on Operations only when Phase 5 is verified. P9-06 compares every caption to the manifest and to that run.

| # | Page | Question | Mart | What the image shows | Tool |
| --- | --- | --- | --- | --- | --- |
| 1 | Home | Did delivery meet the promised date? | `mart_delivery_performance` | On-time rate | Superset |
| 2 | Pipeline | How late are the missed promises? | `mart_late_orders` | Days-late buckets or a count, not order rows | Superset |
| 3 | Pipeline | How does marketplace GMV move? | `mart_platform_sales` | Item GMV by purchase date | Superset |
| 4 | Pipeline | Does delay change the review score? | `mart_review_drivers` | Mean score by delay bucket | Superset |
| 5 | Pipeline | Which lead origins become sellers that sell? | `mart_marketing_acquisition` | Win rate and matched item GMV by origin | Superset |
| 6 | Pipeline | Where are the buyers, by state? | `mart_customer_geography` | Buyer counts by UF | Superset |
| 7 | Pipeline | How are catalog prices spread? | `mart_catalog_prices` | Median price by category | Superset |
| 8 | Pipeline | How often does a session purchase? | `mart_storefront_session` | Session counts by cart and purchase | Superset |
| 9 | Pipeline | Does the OLTP sample keep the same promise? | `mart_oltp_delivery_performance` | On-time rate, labeled as a different population from Olist | Superset |
| 10 | Architecture | Does the notebook reach the same rate? | `mart_delivery_performance` | The on-time query, same value as figure 1 | JupyterLab |

**Incremental rule (Phases 1–8):** a scaffold under `site/` may exist early (Astro Starlight + Catppuccin theme). Content updates must use only facts already **verified** in `PROJECT_STATUS.md`. Do **not** complete public hosting, the figure gallery, or final copy polish until Phase 9. Phase 8 may add the exported images and the manifest once P8-06 has checked them. It does not publish the hosted report.

**Phase 7 vs Phase 9:** P7-07 delivers the **in-repo case study** (markdown, diagrams, reproduction steps). P7-08 checks that case study against a recorded IaC result, and P7-09 records status. Phase 9 turns that material, plus verified Phase 8 exploration notes, into the **deployed multi-page report** and sets the GitHub repository Website URL.

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

| ID | Title | Depends on | Expected/Seen Risk | Expected/Seen Size | Status | Acceptance |
| --- | --- | --- | --- | --- | --- | --- |
| P0-01 | Public GitHub monorepo, MIT license | — | -/Lo | -/Sm | Delivered | `LICENSE` present; repo public under MIT |
| P0-02 | Agent and contributor operating contract | — | -/Lo | -/Sm | Delivered | `AGENTS.md` and `docs/agent-workflow.md` define workflow |
| P0-03 | Project engineering rules | P0-02 | -/Lo | -/Sm | Delivered | Rules in `AGENTS.md` (imported by `CLAUDE.md`) cover principles, Python/testing, data engineering, infra/security, workflow. Rules consolidated into `AGENTS.md` (previously `.cursor/rules/`, removed on 2026-11-14 when the project moved from Cursor to Claude Code) |
| P0-04 | Project status ledger | — | -/Lo | -/Sm | Delivered | `PROJECT_STATUS.md` distinguishes delivered vs verified |
| P0-05 | Program context document | — | -/Lo | -/Sm | Delivered | `docs/project-genesis.md` (previously `docs/project-context-for-cursor.md`) captures objectives, constraints, target architecture |
| P0-06 | Program roadmap | P0-04, P0-05 | -/Lo | -/Sm | Delivered | This file: all phases, issue IDs, dependencies, acceptance, out-of-scope |
| P0-07 | Decision log scaffold | P0-06 | -/Lo | -/Sm | Delivered | `DECISIONS.md` is a summary index; ADRs under `docs/decisions/` hold rationale; proposed vs approved visible |
| P0-08 | Python 3.12 package with `uv` | — | -/Lo | -/Sm | Delivered | Package under `src/open_data_platform/`; `pyproject.toml` + committed lockfile; installable via documented command |
| P0-09 | Ruff and pytest smoke | P0-08 | -/Lo | -/Sm | Verified | Lint and a toolchain smoke test pass via documented commands |
| P0-10 | Docker Compose: PostgreSQL 16 and MinIO | — | -/Med | -/Sm | Verified | Pinned image tags; health checks; documented start/stop; no unverified tags |
| P0-11 | Repo hygiene and README | P0-08, P0-10 | -/Lo | -/Sm | Delivered | `.gitignore`, `.env.example` (names/placeholders only), Makefile, `README.md` with setup |
| P0-12 | GitHub Actions: lint and tests | P0-09 | -/Med | -/Sm | Verified | Workflow runs lint + tests; no cloud deploy |
| P0-13 | Initial decision note (Compose vs AWS; Phase 0 services) | P0-07 | -/Lo | -/Sm | Delivered | Approve/update D-001 (and D-002 as applicable) in `DECISIONS.md`/ADRs: Compose for local verification; AWS as Terraform IaC in Phase 7 |

**Suggested order:** P0-06 → P0-07 → P0-08 → P0-09 → P0-10 → P0-11 → P0-12 → P0-13 (P0-08 and P0-10 may proceed in parallel after P0-07). Live Compose, MinIO put/get, lint/tests, and CI are recorded in `docs/status-log.md` (2026-10-16). README and the status ledger are the phase documentation. The Phase 2 close pattern is not applied here.

---

## Phase 1 — Multi-source ingestion

**Goal:** Idempotent batch ingestion from four source types into auditable raw storage, with contracts and deterministic tests. PostgreSQL extract must support **windowed incremental loads** usable by daily and weekly Airflow schedules (scheduling itself is Phase 3).

**Depends on:** Phase 0 verified.

**Out of scope:** Streaming/CDC, Iceberg, Airflow schedules, lakehouse transforms.

**Phase acceptance (all required):**

- [x] Ingestions can be repeated without undue duplication or corruption.
- [x] Unit tests do not require the public API to be online.
- [x] Raw data is traceable by source and execution.
- [x] Contracts and expected fields are documented.
- [x] Invalid records and errors have explicit, documented behavior.
- [x] PostgreSQL extract accepts an explicit time/watermark window suitable for daily and weekly batch runs.

### Issues

| ID | Title | Depends on | Expected/Seen Risk | Expected/Seen Size | Status | Acceptance |
| --- | --- | --- | --- | --- | --- | --- |
| P1-01 | Historical dataset license and redistribution check | Phase 0 | -/Hi | -/Sm | Verified | License/terms recorded for Olist e-commerce + marketing funnel (CC BY-NC-SA 4.0); no dataset vendored; fetch-from-source documented (`docs/sources/`, ADR-0018) |
| P1-02 | Raw storage layout and ingestion metadata conventions | Phase 0 | -/Med | -/Md | Verified | Paths, partition conventions, `_ingestion_ts`, source id, execution id documented and used by writers |
| P1-03 | Data contracts for planned sources | P1-02 | -/Med | -/Lg | Verified | Contracts under `config/contracts/` (or agreed path); schema/fields/null rules explicit |
| P1-04 | Historical e-commerce dataset ingestion | P1-01, P1-02, P1-03 | -/Med | -/Lg | Verified | Repeatable ingest of Olist e-commerce (+ marketing funnel join on `seller_id`) to raw layout; idempotent re-run; unit/integration coverage without private/vendored Olist data |
| P1-05 | Paginated public API client (DummyJSON products) | P1-02, P1-03 | -/Med | -/Lg | Verified | DummyJSON `/products` (`limit`/`skip`); timeouts, retries, rate-limit handling; fixtures; no live API in unit tests; lands in Gold `product_analysis` (D-019, D-012) |
| P1-06 | PostgreSQL incremental batch extract | P0-10, P1-02, P1-03 | -/Hi | -/Lg | Verified | Parameterized extract by watermark/window (`start`/`end` or high-water mark); grain/keys documented; same code path supports daily and weekly windows; idempotent re-run without corrupting raw; OLTP seed per `docs/sources/postgres-oltp.md` |
| P1-07 | Demo storefront clickstream | P1-02 | -/Lo | -/Md | Verified | Seedable JSONL clickstream for a fictional retail website (`demo_storefront` / `clickstream`); schema documented; event time on the record. Bronze, Silver, and Gold placement is noted for P2-03, P2-04, and P2-05 and is not built here. CDC remains the PostgreSQL path |
| P1-08 | IBGE Localidades ingest | P1-02, P1-03 | -/Med | -/Md | Verified | IBGE `municipios` (`view=nivelado`, `orderBy=nome`) into one raw `municipios.json`; contract v1; geo reference for `olist_analysis` (D-019). Invalid rows are copied to the P1-10 quarantine sidecar |
| P1-09 | BrasilAPI CEP enrichment (optional) | P1-02, P1-04 | -/- | -/- | Withdrawn | Withdrawn (ADR-0021 / D-021). An 8-digit CEP lookup does not join to the Olist zip prefix. IBGE and Olist geolocation cover that geography |
| P1-10 | Failure and invalid-record behavior | P1-04–P1-08 | -/Hi | -/Md | Verified | Nulls, duplicate keys, and wrong types are explicit. Raw bytes stay in place. Findings land in `quarantine/…/{object_name}.jsonl` in the same bucket. Structural errors still fail the batch. Unit tests cover the failure paths |
| P1-11 | Phase 1 documentation and status | P1-10 | -/Lo | -/Sm | Verified | README/docs updated; three update patterns (daily batch, weekly batch, streaming) referenced; `PROJECT_STATUS.md` records verification evidence |

**Suggested order:** P1-01 ∥ P1-02 → P1-03 → P1-04 ∥ P1-05 ∥ P1-06 ∥ P1-07 ∥ P1-08 → P1-10 → P1-11. P1-09 is withdrawn (ADR-0021). Live results already logged: DummyJSON `p105-live-20261024` (7 pages / 194 products), IBGE `p108-live-20261025` (5_571 municipalities), P1-10 MinIO matrix (40 passed, including the 1_603 duplicate `review_id` rows). P1-11 is the documentation close. The Phase 2 close pattern is not applied here.

---

## Phase 2 — Lakehouse and analytics

**Goal:** Prove Spark + Iceberg + catalog + query-engine compatibility, then implement Bronze / Silver / Gold and initial dbt models.

**Depends on:** Phase 1 verified. **Hard gate:** P2-01 must succeed before scaling transforms.

**Out of scope:** Airflow orchestration, CDC/streaming, AWS lakehouse modules (Phase 7 IaC), notebooks and business BI (Phase 8).

**Phase acceptance (all required):**

- [x] A table can be created, written and queried in the chosen local stack.
- [x] Queries and transforms are reproducible via documented commands.
- [x] Quality and reconciliation rules have tests.
- [x] Documentation lists component versions and known limitations.
- [x] A live run result is recorded and compared to the Phase 2 contracts (P2-09).
- [x] Documentation and `PROJECT_STATUS.md` cite that result (P2-10).

### Issues

| ID | Title | Depends on | Expected/Seen Risk | Expected/Seen Size | Status | Acceptance |
| --- | --- | --- | --- | --- | --- | --- |
| P2-01 | Minimal Spark + Iceberg + catalog + query PoC | Phase 1 | -/Med | -/Sm | Verified | Create / write / catalog / query one table; versions pinned; result recorded; incompatible combos rejected with a decision note |
| P2-02 | Local catalog and query-engine choice | P2-01 | -/Lo | -/Sm | Verified | Local table catalog (and Trino or alternative) chosen after PoC; recorded in D-004 / `DECISIONS.md` |
| P2-03 | Bronze layer from raw ingestion | P2-01, P1-02 | -/Hi | -/Md | Verified | Bronze preserves source payload + ingestion metadata; grain and PK documented |
| P2-04 | Silver normalization and deduplication | P2-03 | -/Hi | -/Md | Verified | Typed, deduplicated Silver tables; null/duplicate/invalid handling intentional |
| P2-05 | Analytical model design (grain, keys, SCD) | P2-04 | -/Med | -/Sm | Verified | Grain, keys, and update rules for facts, dims, and marts are in [`docs/architecture/gold-model-design.md`](docs/architecture/gold-model-design.md). Type 1 for P2-06. Type 2 applications are specified there and implemented only in P4-08 |
| P2-06 | dbt project and initial Gold models | P2-05 | -/Hi | -/Lg | Verified | dbt-trino builds the facts, dims, and marts named in [`docs/architecture/gold-model-design.md`](docs/architecture/gold-model-design.md). Silver stays the Spark writer. Evidence: [`docs/architecture/gold-layer.md`](docs/architecture/gold-layer.md) |
| P2-07 | Data quality and reconciliation tests | P2-06 | -/Med | -/Md | Verified | dbt/Python tests cover key metrics vs source; failures are visible. Replace hardcoded `live_counts` snapshot sizes (current `odp-raw` / `run-001` pin, including DummyJSON 194 and growing `full_refresh` / `incremental_window` sources) with counts derived from Bronze/source for that run |
| P2-08 | Spark lakehouse jar / Ivy cache | P2-03 | -/Lo | -/Sm | Verified | Persist or bake Iceberg/Hadoop/JDBC jars so each `docker compose run spark` does not re-download Maven artifacts; document the local cache path; keep pins unchanged. May run any time after P2-03; does not block P2-05 |
| P2-09 | Live run, result, and consistency check | P2-01, P2-06, P2-07 | -/Med | -/Sm | Verified | On the local stack, run the documented create/write/query path and the Bronze → Silver → Gold/dbt path for the approved models. Record component versions, row counts, and the quality-test result. Compare Gold metrics to the source or Silver counts named in the contracts. Record mismatches. Unit tests alone do not close this issue. Persist evidence in architecture docs / `docs/status-log.md` (not only `PROJECT_STATUS.md`). Evidence: [`docs/architecture/phase-2-live-run.md`](docs/architecture/phase-2-live-run.md) |
| P2-10 | Phase 2 documentation and status | P2-09 | -/Lo | -/Sm | Verified | Document component versions, partitioning, file sizing, incremental strategy (vs current full-overwrite Bronze/Silver), known limitations, and the P2-09 result. `PROJECT_STATUS.md` cites that evidence. Evidence: [`docs/architecture/phase-2-lakehouse.md`](docs/architecture/phase-2-lakehouse.md) |

**Suggested order:** P2-01 → P2-02 → P2-03 → P2-04 → P2-05 → P2-06 → P2-07 → P2-08 → P2-09 → P2-10. Phase close is P2-09 then P2-10; P2-08 may run earlier after P2-03. Phase 2 is verified (2026-11-11). Index: [`docs/architecture/phase-2-lakehouse.md`](docs/architecture/phase-2-lakehouse.md). Live result: [`docs/architecture/phase-2-live-run.md`](docs/architecture/phase-2-live-run.md).

**Analytical models** are specified in [`docs/architecture/gold-model-design.md`](docs/architecture/gold-model-design.md). Gold schemas: `marketplace_operations`, `customer_crm`, `seller_acquisition`, `brazil_geography`, `catalog_commerce`, `storefront_behavior`, `oltp_operations`, plus shared `dim_date`. Contract `governance.source_family` is the ingest label (`olist_analysis` or `product_analysis`), not the Gold schema. `mart_platform_sales` replaces the earlier `mart_daily_sales` placeholder. On-time rate and `mart_late_orders` are the delay analysis. Each schema has a quality-findings fact.

---

## Phase 3 — Orchestration and data quality

**Goal:** Orchestrate validated batch flows with Airflow—including **daily** and **weekly** PostgreSQL (and related) schedules—prove idempotent backfills and quarantine. This phase delivers the two batch use cases; streaming remains Phase 4.

**Depends on:** Phase 2 verified.

**Out of scope:** CDC, Kafka, streaming.

**Phase acceptance (all required):**

- [ ] DAGs have clear dependencies and verifiable failure behavior.
- [ ] A **daily** scheduled batch path exists for PostgreSQL incremental extract (and downstream transforms as designed).
- [x] A **weekly** scheduled batch path exists (distinct schedule and documented purpose/grain vs daily) (P3-05).
- [ ] Backfills do not corrupt data or create undue duplication.
- [x] A missed window is caught up or reported as a gap; covered windows are queryable (P3-04, P3-06). Reopened 2026-11-21 for trailing gaps; since P3-16 the report runs to the last window due at report time, so a stopped DAG shows its gaps.
- [x] A failed Bronze or Silver check blocks the next layer until it passes, including on a later refresh with no new raw objects (P3-16).
- [x] Gold metrics keep their documented grain: no fan-out, rate denominators count only eligible rows, and money is exact decimal (P3-17, P3-18).
- [ ] Readers never see a missing, empty, or half-written table, including after a failed write (P3-11).
- [x] Each contract `update_pattern` has defined and enforced Bronze and Silver behavior, and every source has a documented refresh policy (P3-12).
- [x] A lakehouse refresh reads new raw objects and, for snapshot sources, the latest complete execution only. Its work does not grow with total raw history (P3-13). Raw object reads do not; the raw key listing, Silver rebuilds of `incremental_window`/`append_only` datasets (ADR-0024), still do. The Bronze `MERGE` scans only the `dt` partitions it writes since P3-08 (2026-11-21 review R5).
- [x] Invalid records are identified and handled per documented rules (P3-07).
- [ ] Success and failure scenarios have tests or reproducible evidence.
- [x] A live run result is recorded and compared to the source windows (P3-14).
- [ ] Documentation and `PROJECT_STATUS.md` cite that result (P3-15).

### Issues

| ID | Title | Depends on | Expected/Seen Risk | Expected/Seen Size | Status | Acceptance |
| --- | --- | --- | --- | --- | --- | --- |
| P3-01 | Airflow in local Compose | Phase 2 | -/Med | -/Md | Verified | Airflow runs locally; pinned images; documented UI/CLI access; business logic stays out of DAG bodies. Evidence: [`docs/architecture/airflow-local.md`](docs/architecture/airflow-local.md) |
| P3-02 | Ingestion DAGs (thin wrappers) | P3-01, Phase 1 | -/Med | -/Sm | Verified | DAGs call package entrypoints; retries/timeouts configured; no duplicated business logic. Evidence: [`docs/architecture/airflow-local.md`](docs/architecture/airflow-local.md) |
| P3-03 | Transform / dbt DAGs | P3-01, Phase 2 | -/Med | -/Sm | Verified | `transform_lakehouse` orchestrates Bronze→Silver→Gold/dbt with clear task dependencies, ends at `dbt_build`, and calls the same entrypoint as the `live-*` Make targets. `reconcile_gold` runs reconciliation weekly. P3-02 retries/timeouts. Airflow, Spark, and dbt share one runtime image. `DagBag` unit tests show zero import errors. A layer-by-layer run matches P2-09. Evidence: [`docs/architecture/airflow-local.md`](docs/architecture/airflow-local.md) |
| P3-04 | Daily batch schedule (PostgreSQL) | P3-02, P3-03, P1-06 | -/Hi | -/Lg | Verified | Airflow DAG with daily schedule passes watermark/window into P1-06 extract; documented cadence, catchup policy and idempotency; manual trigger proves one successful daily-shaped run. Missed days are caught up (`catchup=True`, corrected on 2026-11-20 after review). After a successful extract the daily DAG triggers `transform_lakehouse` (manual `ingest_postgres_oltp` runs do not); the manual trigger proves that transform run completed. `reconcile_gold` never runs while a `transform_lakehouse` dbt build is running (shared 1-slot pool or Asset emitted by `dbt_build`); every DAG is created with `is_paused_upon_creation=True`; DagBag test covers both. `run_python_args` starts the child with `start_new_session=True` and kills the whole process group (`os.killpg`) on timeout or exception; a unit test proves no child process survives. End-to-end retry scenario stays in P3-09. Evidence: [`docs/architecture/airflow-local.md`](docs/architecture/airflow-local.md) |
| P3-05 | Weekly batch schedule (PostgreSQL) | P3-02, P1-06, P3-13 | -/Hi | -/Md | Verified | A distinct weekly DAG runs Monday 00:00 UTC with catchup and re-extracts the trailing seven days `[W-8, W-1)` (the `[W-7, W)` data interval shifted back one day so Sunday does not share the 00:00Z edge; approved by Filipe 2026-11-21) through the same P1-06 extract and `postgres_extract_task`, under its own execution id. Purpose, documented against the daily grain: the safety net for rows the daily window missed because they committed after its run (late commits). A scripted check commits a row whose `updated_at` falls in an already extracted daily window, runs the weekly DAG, and finds the row in Silver; the same check shows the weekly run does not duplicate rows the daily runs already landed. It states that a hard delete is still invisible (P3-12). The weekly run requests a lakehouse refresh the way P3-13 defines. Created paused. A manual trigger proves one successful weekly-shaped run |
| P3-06 | Window coverage, gap report, and backfill | P3-02, P3-03, P3-13 | -/Hi | -/Md | Verified | Each successful incremental extract records its coverage: source, dataset, window start and end, execution id, Airflow run id, and row count, in a queryable table or manifest (not only the Airflow metadata database). A gap report lists missing and overlapping windows between a DAG's `start_date` and its last window, for the daily and weekly DAGs; a unit test plants a gap and an overlap. Backfill parameters for daily and weekly windows are documented (`airflow backfill create` and the window it gives, since `airflow dags trigger -l` does not select a window in Airflow 3.4). A backfill of N windows requests lakehouse refreshes the way P3-13 coalesces them, not N full rebuilds. Re-run evidence: re-running a covered window leaves Silver counts unchanged and the gap report empty. Evidence: [`docs/architecture/airflow-local.md`](docs/architecture/airflow-local.md#window-coverage-gap-report-and-backfill-p3-06), [ADR-0025](docs/decisions/0025-extract-window-coverage-ledger.md) |
| P3-07 | Quality checks and quarantine | P3-03, P2-07, P3-16 | -/Hi | -/Lg | Verified | Invalid records routed per contract; quarantine location and reprocess path documented. Silver counts, per dataset and column, the values a typed cast turned into NULL and the rows dropped for a null key, and records those counts with the run; no invalid value disappears without a count ([review](docs/reviews/2026-11-21-verified-issues-review.md) R9). The contract `on_invalid` decides whether a quarantined record is excluded from Silver or kept with a flag; the plan states which per source, and the Gold quality facts follow it. A `snapshot` or `full_refresh` execution whose row count falls below a contract threshold relative to the previous complete execution does not replace Silver and fails visibly (R10) |
| P3-08 | Execution metadata and failure logging | P3-02–P3-07 | -/Med | -/Lg | Verified | Execution IDs, failure reasons and run metadata are inspectable. Each lakehouse refresh records, per dataset and layer, the raw objects read, the rows read and written, and the duration of each task, kept across runs so growth over time can be queried (Phase 5 charts it). Trino client errors report `errorName`, `errorType` and `errorCode`; the `nextUri` polling loop has a total timeout. Bronze tables get a partition spec (`dt`, or a documented alternative) and the incremental `MERGE` restricts the target to the staged partitions; the bytes or files a `MERGE` scans are recorded, and a refresh after one daily window scans no more than the partitions it writes (review R5). Evidence: [`docs/architecture/airflow-local.md`](docs/architecture/airflow-local.md#execution-metadata-p3-08), [ADR-0026](docs/decisions/0026-bronze-partitions-and-run-metadata.md) |
| P3-09 | Orchestration success/failure tests | P3-03, P3-04, P3-05, P3-06, P3-07, P3-11, P3-13, P3-16 | Med/Med | Lg/Md | Verified | Automated or scripted evidence for happy path, retry and failed-run handling for daily and weekly DAGs, `transform_lakehouse`, and `reconcile_gold`. Also: a missed day caught up after a pause; a failed Silver or Bronze write that leaves the previous table readable with its previous count; a refresh with no new raw objects that writes nothing; two upstream runs landing during one refresh that produce one follow-up refresh, not two; and a failed Bronze or Silver check followed by a refresh with no new raw objects that does not build the next layer (P3-16). Evidence: [`docs/architecture/airflow-local.md`](docs/architecture/airflow-local.md#orchestration-success-and-failure-scenarios-p3-09), `scripts/p3_09_orchestration_check.sh` |
| P3-10 | Orchestration runtime hardening | P3-03, P3-09 | Med/Med | Md/Md | Verified | Spark driver memory and cores inside the Airflow runtime are capped by environment variables and recorded as a known limit in the ADR-0005 addendum; Airflow moves from the uv `dev` group to an `airflow` group outside the default, and `test_dagbag` uses `pytest.importorskip`; documented retention for `odp_lakehouse_work` (per-run_id folders are cleaned); `.env.example` uses `ODP_TRINO_*` and `profiles.yml.example` matches whether `dbt/profiles.yml` is versioned. Every Spark script sets `spark.sql.session.timeZone` to UTC, and a unit test fails without it (review R16) |
| P3-11 | Atomic table replace for readers | P2-03, P2-04, P3-03 | -/Hi | -/Sm | Verified | Every writer of a table that Trino, dbt, or a check reads is listed with its replace statement: Bronze, Silver, quarantine staging, and dbt Gold. None uses `DROP TABLE` followed by `CREATE` on a table readers use. Silver moves from `DROP` → `CREATE` → `INSERT OVERWRITE` to one atomic Iceberg commit (`CREATE OR REPLACE TABLE … AS SELECT`, or `INSERT OVERWRITE` on the existing table), so the table never disappears and a failed write leaves the previous snapshot. A schema change from a new contract version has a documented path (Iceberg schema evolution or an explicit rebuild step), not a silent drop. Snapshot history is kept until P5-07 expires it. Unit tests: no rendered writer SQL contains `DROP TABLE` for a read table. Scripted evidence: a Silver write forced to fail mid-run leaves the Trino count equal to the previous run; a Trino count loop during a full rebuild never errors. Evidence: [`docs/architecture/phase-2-lakehouse.md`](docs/architecture/phase-2-lakehouse.md#atomic-table-replace-p3-11) |
| P3-12 | Update-pattern semantics and source refresh policy | P2-03, P2-04, P3-11 | -/Hi | -/Lg | Verified | [`docs/architecture/data-contracts.md`](docs/architecture/data-contracts.md) defines each `update_pattern` by its downstream behavior: what Bronze keeps, which rows Silver selects survivors from, what a re-ingest means, and what happens to a record that disappears from the source. `snapshot` and `full_refresh`: Bronze keeps every execution for audit; Silver takes survivors only from the latest complete execution of the dataset, so a record absent from it leaves Silver, and re-ingesting the same Olist snapshot does not change Silver. An execution is complete only when its ingestion writes a completion marker after the last object; Silver ignores an execution without one, so a partial DummyJSON page set never replaces a full one. `incremental_window`: Silver keeps the union of windows, latest `updated_at` per key; hard deletes are invisible on this path and the Gold docs say so. A new `append_only` value covers event streams: `demo_storefront.clickstream` moves to it in a new contract version (it is labelled `snapshot` today), Silver deduplicates by event key, and event time, not ingestion time, places a late event. Unit tests per pattern, including a record removed between two `full_refresh` executions that leaves Silver, and an incomplete execution that is ignored. The source refresh matrix below is filled with the decided cadence, trigger, and DAG for every source, or marked manual-only with the reason; each scheduled source has its DAG, created paused, with catchup where the window comes from the schedule. Evidence: [`docs/architecture/data-contracts.md`](docs/architecture/data-contracts.md#update-patterns-p3-12) |
| P3-13 | Incremental lakehouse refresh | P3-11, P3-12 | -/Hi | -/Lg | Verified | ADR-0024 records the refresh design and how it changes the `FULL_OVERWRITE` default of [`write_mode.py`](src/open_data_platform/lakehouse/write_mode.py). Bronze promotes only raw objects not promoted before, tracked by raw object key and ETag in a promotion registry, and appends their rows. Promotion streams one object at a time; no step holds a whole dataset in Python memory. Staging and work paths are per run label, so a `make` target and a DAG run do not overwrite each other's staging. Silver rebuilds only datasets with new Bronze rows, reading what P3-12 selects (the latest complete execution for snapshot sources). A documented `--full-rebuild` mode rebuilds from all raw objects, for a contract version change or recovery, and is never the scheduled path. Every ingestion DAG, not only PostgreSQL, emits an Airflow Asset on success, and `transform_lakehouse` is scheduled on those Assets: N upstream runs that land during one refresh produce at most one follow-up refresh. This replaces the P3-04 `TriggerDagRunOperator`. A refresh with no new raw objects writes no table and records that. Gold stays a dbt full table build unless the ADR names models to make incremental; the decision cites the measured `dbt_build` duration. Evidence: rows read and written and task durations for a refresh with no new objects, after one new daily window, and for `--full-rebuild`, all measured on the local stack. Evidence: [`docs/architecture/bronze-layer.md`](docs/architecture/bronze-layer.md#result-2026-11-20-incremental-refresh-p3-13), [ADR-0024](docs/decisions/0024-incremental-lakehouse-refresh.md) |
| P3-16 | Quality gate state and extract-window corrections | P3-06, P3-13 | -/Hi | -/Lg | Verified | From the [2026-11-21 review](docs/reviews/2026-11-21-verified-issues-review.md) (R1, R7, R8, R13, R14, T3). The plan answers the pipeline design checklist. **Gate:** the promotion registry records the result of `bronze_checks` and `silver_checks` per dataset; a refresh with no new raw objects runs the pending checks again before Silver or Gold, and no write step builds on a layer whose last check failed or never ran. Check steps have `retries=0`. The strict xfail test `test_a_failed_bronze_check_is_checked_again_before_silver_builds` passes and loses its marker, a unit test shows the second of two refreshes with a failed check between them builds neither Silver nor Gold, and the two tests that assert today's skip change with it. **Gap report:** expected windows run up to the last interval that should have run at report time, so a stopped DAG shows trailing gaps; the strict xfail test `test_a_stopped_dag_reports_its_trailing_gaps` passes and loses its marker. **Watermark:** a `BEFORE UPDATE` trigger on each `oltp` table sets `updated_at` to `clock_timestamp()` when the statement does not change it; the daily run reads its window only after a documented settle lag past the window end, with UTC-midnight bounds kept; the docs state the residual late-commit case. **Extract:** the datasets of one window are read in one `REPEATABLE READ` read-only transaction. A task retry keeps the raw `dt` of its first try (a run-stable ingestion timestamp), so a retry after midnight does not add a second complete prefix. Evidence: unit tests, `pytest tests/integration` on the local stack, and one scripted refresh pair with a planted check failure |
| P3-17 | Gold metric grain corrections | P2-06, P2-07 | -/Hi | -/Md | Verified | From the [2026-11-21 review](docs/reviews/2026-11-21-verified-issues-review.md) (R2, R3, R19, T7). `mart_review_drivers` counts each `(review_id, order_id)` once: the category is resolved per order by a rule written in [`docs/architecture/gold-model-design.md`](docs/architecture/gold-model-design.md), and its `review_count` reconciles to `fct_reviews`. `on_time_handoff_rate` in `mart_delivery_performance`, `mart_seller_performance`, and the two OLTP marts divides by the orders eligible for a handoff (a carrier date and a shipping limit), and each mart exposes that eligible count. The `fct_oltp_orders` docs and column descriptions say the customer location is the current one (Type 1) until P4-08. dbt unit tests (dbt-core 1.12) build each corrected model from a small fixture: an order with two items and one review, and a cancelled order with no carrier date. A unit-test rule in [`docs/architecture/gold-layer.md`](docs/architecture/gold-layer.md#unit-test-rule) says which Gold models need a unit test (fan-out join, tie-break, rate on a subset, bucket or threshold, null-key row). `dbt/unit_test_policy.yml` lists every Gold model as covered, exempt with a reason, or pending with an issue, and a pytest fails on an unlisted model or a covered model with no test. Every mart and dimension the rule applies to has a unit test that fails when its rule is broken; the quality-findings models may stay pending, named in the policy file. The live run records each changed metric before and after, and the Gold docs cite the new values |
| P3-18 | Exact decimal type for money fields | P1-03, P2-04, P3-12 | -/Hi | -/Md | Verified | From the [2026-11-21 review](docs/reviews/2026-11-21-verified-issues-review.md) (R4). The contract vocabulary gains `decimal` with precision and scale. Money fields (Olist item price and freight, payment value, OLTP item price and freight, DummyJSON price, and the marketing-funnel `declared_monthly_revenue`, added by Filipe on 2026-11-22) move to it in a new contract version. Each money field names its currency in its description, and the data-contracts doc states the currency rules. The PostgreSQL extract writes `numeric` values without a float conversion; record validation accepts them; Silver casts to `DECIMAL(p, s)`; Gold sums stay decimal. Reconciliation of money totals uses exact equality, and the `scalars_near` checks on money are removed. The contract version change follows the documented rebuild path (P3-11, `make live-rebuild`). The plan answers the pipeline design checklist. Unit tests: a planted amount survives extract, Bronze, and Silver unchanged, and a sum of planted cents is exact. Evidence: [`docs/architecture/data-contracts.md`](docs/architecture/data-contracts.md#money-p3-18), [`docs/architecture/gold-model-design.md`](docs/architecture/gold-model-design.md#p3-18-exact-money-2026-11-22) |
| P3-14 | Live run, result, and consistency check | P3-08, P3-09, P3-10, P3-11, P3-12, P3-13, P3-16, P3-17, P3-18 | Med/Med | Md/Md | Verified | On local Airflow, trigger one daily-shaped run and one weekly-shaped run, then rerun one of those windows. Record watermarks, task states, landing counts, and quarantine counts. Compare the counts to the source window and to the previous landing for that window. Pause the daily DAG before a 00:00Z boundary and unpause it after: one caught-up run per missed day, and an empty gap report. Commit one late row and show the weekly run lands it. Run one refresh with no new raw objects and show it writes nothing. Query a Silver table during a refresh and show it never errors. Record, per refresh, raw objects and rows read against total raw history. Record mismatches. Unit tests alone do not close this issue. Evidence: [`docs/architecture/phase-3-live-run.md`](docs/architecture/phase-3-live-run.md) |
| P3-15 | Phase 3 documentation and status | P3-14 | Lo/- | Sm/- | Not started | Document schedules, catchup, backfill, quarantine, the review findings closed in Phase 3, that `make` targets and Airflow DAGs must not run concurrently against the same Iceberg tables and Ivy cache, and the P3-14 result. `PROJECT_STATUS.md` cites that evidence |

**Suggested order:** P3-01 → P3-02 ∥ P3-03 → P3-04 → P3-11 → P3-12 → P3-13 → P3-05 → P3-06 → P3-16 → P3-17 → P3-18 → P3-07 → P3-08 → P3-09 → P3-10 → P3-14 → P3-15. P3-11 to P3-13 come before the weekly schedule so that P3-05 and P3-06 build on the incremental refresh, not on the full rebuild. P3-16 to P3-18 come from the [2026-11-21 review](docs/reviews/2026-11-21-verified-issues-review.md) and are numbered after P3-15 but run before the close pair: P3-16 first, because P3-07 builds on the check gate, and P3-17 and P3-18 before P3-14, so the live run records corrected metrics.

**Source refresh matrix.** Decided and verified in P3-12 (2026-11-20), except the rows marked as a later issue. Pattern semantics: [`docs/architecture/data-contracts.md`](docs/architecture/data-contracts.md#update-patterns-p3-12). Since P3-13 every ingestion DAG below emits its raw Asset on success, and `transform_lakehouse` is scheduled on those Assets.

| Source | Datasets | `update_pattern` | Cadence, trigger, DAG | A new run of the same data | Deletes | Time fields |
| --- | --- | --- | --- | --- | --- | --- |
| `olist_brazilian_ecommerce`, `olist_marketing_funnel` | 9 + 2 CSV | `snapshot` | Manual only (`ingest_olist`, no schedule): a fixed historical release | Silver reads the latest complete execution; same rows and counts | None (static) | Order and review dates are event time |
| `dummyjson_products` | `products` | `full_refresh` | Weekly, Monday 03:00 UTC (`ingest_dummyjson_products`), `catchup=False`: no window, the next full fetch covers a missed week | Latest complete execution replaces the previous one in Silver; a partial page set is ignored | Absent product leaves Silver | Ingestion time only |
| `ibge_localidades` | `municipios` | `full_refresh` | Monthly, day 1 at 03:00 UTC (`ingest_ibge_localidades`), `catchup=False`: no window, the next full fetch covers a missed month | Latest complete execution replaces the previous one in Silver | Absent municipality leaves Silver | Ingestion time only |
| `demo_storefront` | `clickstream` | `append_only` (contract v3; was `snapshot`) | Manual only (`ingest_demo_storefront`, no schedule): a demo generator | Every complete batch appends; a repeated `event_id` keeps the first ingested row | None (immutable events) | `event_time` is event time; a late event keeps it and Gold groups by it |
| `postgres_oltp` | 4 tables | `incremental_window` | Daily 00:00 UTC with catchup (`ingest_postgres_oltp_daily`, P3-04); weekly re-extract is P3-05; `ingest_postgres_oltp` stays manual | Window union; latest `updated_at` per key | Hard deletes invisible on batch; CDC in Phase 4 | `updated_at` watermark; order dates are event time |
| `bcb_sgs` (Phase 10) | Series 1 and 11 | `incremental_window` by date | Daily on business days (P10-04, not built) | Window union per `(codigo_serie, data)` | Revisions overwrite by key | `data` is the business date |
| `bioguardia_ops` (Phase 11) | 22 tables | `snapshot` (6 master tables), `incremental_window` on `updated_at` (7 change tables), `append_only` (9) | Daily drop released 02:00 BRT; ingestion DAG with catchup (P11-05, not built) | Version 2 of a date supersedes version 1 | Absence from a snapshot; `deleted_records` for change tables | `-03:00` local times; the export date is the local business day |
| `bioguardia_finance` (Phase 11) | `fine_payments` | `append_only` | Daily drop released 06:00 BRT (P11-05, not built) | Same as `bioguardia_ops` | None | `posted_on` decides the drop; `paid_on` is the business date |
| `bioguardia_telemetry` (Phase 11) | 4 Kafka topics | `append_only` events, dedupe on `event_id` | Continuous streaming service (P11-09, not built) | A replay duplicates events after the checkpoint | None (immutable events) | `event_time` is the device clock; `gateway_time` is arrival |

---

## Phase 4 — Streaming CDC

**Goal:** Deliver the **streaming** use case: continuous change capture from PostgreSQL through Debezium/Kafka into a streaming processor, with honest delivery semantics. Complements—does not replace—daily and weekly batch (Phase 3).

**Depends on:** Phase 3 verified (daily and weekly batch paths exist); PostgreSQL from Phase 0/1 available for logical replication; P11-01 to P11-04 (the Kafka broker arrives with P11-03 under [ADR-0028](docs/decisions/0028-telemetry-message-broker.md), and P11-04 proves Spark's Kafka source with it).

**Out of scope:** Using CDC as a substitute for daily/weekly scheduled batch loads; claiming exactly-once end-to-end without a specific technical proof; breaking the validated batch path.

**Phase acceptance (all required):**

- [ ] Inserts, updates and deletes are handled correctly on the streaming path.
- [ ] Duplicates and out-of-order events have explicit handling.
- [ ] Restart and replay do not corrupt sinking state.
- [ ] Docs contrast streaming CDC vs daily/weekly batch and do not claim “exactly once” without proof.
- [ ] Validated daily and weekly batch paths still work after CDC is introduced.
- [ ] A live run result is recorded and compared to the PostgreSQL source and the batch baseline (P4-09).
- [ ] Documentation and `PROJECT_STATUS.md` cite that result (P4-10).

### Issues

| ID | Title | Depends on | Expected/Seen Risk | Expected/Seen Size | Status | Acceptance |
| --- | --- | --- | --- | --- | --- | --- |
| P4-01 | PostgreSQL logical replication prerequisites | P0-10, Phase 3 | Med/- | Sm/- | Not started | WAL level / publications documented; safe local config; no secrets committed. The replicated source is separate from platform state: Airflow metadata, the Iceberg catalog, the promotion registry, and the coverage ledger are not on the replicated database, and the batch extract and Debezium each use their own least-privilege role (read-only and replication), not the platform's `POSTGRES_USER` ([review](docs/reviews/2026-11-21-verified-issues-review.md) R12) |
| P4-02 | Debezium on the shared Kafka broker | P4-01, P11-03 | Med/- | Md/- | Not started | Debezium Connect (pinned) runs on the Kafka broker of P11-03 ([ADR-0028](docs/decisions/0028-telemetry-message-broker.md)), not on a second Kafka; Debezium's compatibility with that broker version is recorded; topic and connector config documented; health checks where supported. Amended 2026-11-26: the broker moved to P11-03 |
| P4-03 | CDC path for insert / update / delete | P4-02 | Hi/- | Md/- | Not started | Reproducible demo of I/U/D landing in Kafka (or agreed sink) with schema documented |
| P4-04 | Spark Structured Streaming consumer | P4-03, P2-01, P11-04 | Hi/- | Lg/- | Not started | Consumer processes CDC events into lakehouse tables; checkpoints documented. It reuses the Spark–Kafka–Iceberg proof of P11-04. The stream writes its own named tables, never a table the batch refresh replaces, and the batch refresh never writes a stream table; ADR-0014 records which process owns each table and how Gold reads both. The `MERGE` writer the sink uses also replaces the full rebuild of `incremental_window` Silver datasets: a daily refresh merges new Bronze rows by primary key with an `updated_at` guard instead of re-reading the whole Bronze table ([review](docs/reviews/2026-11-21-verified-issues-review.md) R11). The `WriteMode` placeholders `INCREMENTAL_MERGE` and `APPEND_ONLY` are implemented or removed with their tests (T5) |
| P4-05 | Semantics documentation (batch vs stream) | P4-03, P4-04, P3-04, P3-05 | Lo/- | Sm/- | Not started | Snapshots, duplicates, ordering, late events, deletes, checkpoints, replay and recovery documented; explicit comparison of daily batch, weekly batch and streaming CDC |
| P4-06 | Streaming resilience scenarios | P4-04, P4-05 | Hi/- | Md/- | Not started | Tests or runbooks for duplicates, out-of-order, restart/replay and deletes |
| P4-07 | Batch path regression check | P3-04, P3-05, P4-04 | Med/- | Sm/- | Not started | Evidence that daily and weekly batch extracts still succeed alongside the streaming path |
| P4-08 | SCD Type 2 history | P4-04, P4-05 | Hi/- | Lg/- | Not started | Implement the type 2 applications in [`docs/architecture/gold-model-design.md`](docs/architecture/gold-model-design.md). First history is PostgreSQL CDC for customers, sellers, orders, and order items: `valid_from`, `valid_to`, and `is_current`, including deletes that close the current row. Purchase-time city, UF, and ZIP stay copied on the order fact. Do not invent history for a single snapshot. Clickstream events and reviews stay immutable. This issue is the only CDC-based SCD implementation; P11-07 builds Type 2 from daily snapshots for the BioGuardia dimensions (amended 2026-11-26). P4-05 stays the batch-vs-stream semantics note |
| P4-09 | Live run, result, and consistency check | P4-04, P4-06, P4-07 | Med/- | Md/- | Not started | Run inserts, updates, and deletes through the local CDC path into the sink. Restart the consumer once and replay. Record event counts and the sink row set before and after replay. Compare the sink to the PostgreSQL source for those keys. Run the daily and weekly batch paths and compare them to their pre-CDC baseline. Record mismatches. Do not claim exactly-once. Unit tests alone do not close this issue |
| P4-10 | Phase 4 documentation and status | P4-09 | Lo/- | Sm/- | Not started | Document batch vs streaming from the P4-09 result. P4-05 remains the semantics write-up. `PROJECT_STATUS.md` cites the live-run evidence |

**Suggested order:** P4-01 → P4-02 → P4-03 → P4-04 → P4-05 → P4-06 → P4-07 → P4-08 → P4-09 → P4-10. Phase close is P4-09 then P4-10.

---

## Phase 5 — Observability, lineage and FinOps

**Goal:** Diagnose failures/latency with real metrics; show lineage from real runs; document a FinOps cost model for the AWS target architecture with explicit assumptions, including the Iceberg snapshot retention the local warehouse applies.

**Depends on:** Phase 3 verified. Stream-specific metrics may wait for Phase 4.

**Out of scope:** Invented metrics, benchmarks, lineage graphs or cost figures.

**Phase acceptance (all required):**

- [ ] Relevant failures and delays can be diagnosed from logs/metrics.
- [ ] Demonstrated lineage matches actual executions.
- [ ] Metrics are measured, not invented.
- [ ] Cost model documents assumptions, unit drivers and estimated monthly ranges for the AWS target design.
- [ ] Iceberg snapshots past the retention named in that model are expired and orphan Parquet files are removed, with counts recorded (P5-07).
- [ ] A live run result is recorded and compared to that run's logs, metrics, lineage, and the snapshot-expiration counts (P5-08).
- [ ] Documentation and `PROJECT_STATUS.md` cite that result (P5-09).

### Issues

| ID | Title | Depends on | Expected/Seen Risk | Expected/Seen Size | Status | Acceptance |
| --- | --- | --- | --- | --- | --- | --- |
| P5-01 | Structured logging conventions | Phase 3 | Hi/- | Md/- | Not started | Shared fields (execution id, source, layer) documented and used in key paths |
| P5-02 | Prometheus and Grafana local stack | P5-01 | Med/- | Md/- | Not started | Services in Compose; pinned images; documented access |
| P5-03 | Pipeline metrics and dashboards | P5-02 | Med/- | Md/- | Not started | Useful dashboards for lag, failures, throughput where instrumented. One panel charts refresh duration and rows read per layer over time from the P3-08 records, so work that grows with history is visible |
| P5-04 | OpenLineage where supported | Phase 3 (and 4 if streaming) | Med/- | Md/- | Not started | Lineage emitted only for components with verified support; demo from a real run |
| P5-05 | Cost model for AWS target architecture | — | Med/- | Sm/- | Not started | Documented cost model with assumptions, drivers and estimated ranges aligned to the Phase 7 AWS design |
| P5-06 | Alerts and runbooks | P5-03 | Med/- | Sm/- | Not started | Alert thresholds and at least one failure runbook under `docs/runbooks/` |
| P5-07 | Expire Iceberg snapshots and delete orphan Parquet | P2-03, P5-05 | Hi/- | Md/- | Not started | A maintenance command expires snapshots older than the retention named in the P5-05 cost model and deletes orphan data files. It keeps the current snapshot. A recorded run for one table lists the snapshots and files removed as counts. It does not run on `docker compose up` and does not invent a spend figure. The same command compacts the tables the daily refresh appends to (`rewrite_data_files`, `rewrite_manifests`) and records file counts before and after ([review](docs/reviews/2026-11-21-verified-issues-review.md) R15). The command is shared: P11-08 and P11-15 run it on the BioGuardia tables (amended 2026-11-26) |
| P5-08 | Live run, result, and consistency check | P5-03, P5-04, P5-05, P5-06, P5-07 | Med/- | Md/- | Not started | Run one real local pipeline execution that emits logs, metrics, and lineage. Record the dashboard values and the lineage edges. Compare them to that run's execution id, counts, and failure state. Check the cost model against its own drivers and assumptions. Record the P5-07 before/after object counts. It is not a measured cloud bill. Record mismatches. Do not invent metrics or spend figures |
| P5-09 | Phase 5 documentation and status | P5-08 | Lo/- | Sm/- | Not started | Document the dashboards, lineage demo, cost-model assumptions, snapshot retention, and the P5-08 result. `PROJECT_STATUS.md` cites that evidence |

**Suggested order:** P5-01 → P5-02 → P5-03 → P5-04 ∥ P5-05 → P5-06 → P5-07 → P5-08 → P5-09.

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
- [ ] A live run result is recorded and compared to the local checks (P6-07).
- [ ] Documentation and `PROJECT_STATUS.md` cite that result (P6-08).

### Issues

| ID | Title | Depends on | Expected/Seen Risk | Expected/Seen Size | Status | Acceptance |
| --- | --- | --- | --- | --- | --- | --- |
| P6-01 | CI: Python lint/tests hardening | P0-12, Phase 1+ | Med/- | Sm/- | Not started | Required checks fail the workflow on regression |
| P6-02 | CI: dbt checks | P2-06, P6-01 | Med/- | Md/- | Not started | dbt parse/build/test (or agreed subset) runs in CI |
| P6-03 | CI: Terraform validate/fmt | Phase 7 scaffold or early `infrastructure/` | Med/- | Sm/- | Not started | `terraform fmt -check` and `validate` in CI without apply |
| P6-04 | Isolated integration test job | P0-10, Phase 1+ | Med/- | Md/- | Not started | Integration tests run in CI or documented Compose profile; deterministic fixtures. A skipped integration test fails the CI job, so a broken test cannot sit unseen ([review](docs/reviews/2026-11-21-verified-issues-review.md) T1) |
| P6-05 | Resilience test evidence | P3-09; P4-06 if present | Med/- | Md/- | Not started | Failure/retry/recovery scenarios executed and linked from `PROJECT_STATUS.md` |
| P6-06 | Security checks | P6-01 | Hi/- | Md/- | Not started | Appropriate secret scanning / dependency audit; no credential leakage in CI logs. The Olist download checks a recorded SHA-256 per file and fails on a mismatch (review R18) |
| P6-09 | Write-audit-publish for Silver and Gold | P3-11, P3-16 | Hi/- | Lg/- | Not started | From the [2026-11-21 review](docs/reviews/2026-11-21-verified-issues-review.md) (R6). Silver writes to an Iceberg branch or staging table and is published only after its checks pass; Gold builds into a staging schema and readers switch only after `dbt build` tests pass. A failed check leaves readers on the previous version of every table in the set. The plan proves Spark, the JDBC catalog, Trino 484, and dbt-trino support the chosen mechanism before adopting it. Scripted evidence: a planted check failure leaves the published Silver and Gold counts unchanged, and a Trino loop during a publish never joins a new fact to an old mart |
| P6-07 | Live run, result, and consistency check | P6-01, P6-02, P6-04, P6-05, P6-06, P6-09 | Med/- | Sm/- | Not started | Run the documented CI workflow and one recovery scenario. Record pass/fail. Compare CI results to the local results for the same checks. Confirm logs and artifacts from that run do not contain credentials. Record mismatches. Include P6-03 when that issue is verified; do not block this issue on Terraform that does not exist yet |
| P6-08 | Phase 6 documentation and status | P6-07 | Lo/- | Sm/- | Not started | Document the CI and recovery evidence, including any check still waiting on P6-03, and cite the P6-07 result in `PROJECT_STATUS.md` |

**Suggested order:** P6-01 → P6-02 ∥ P6-04 → P6-03 (when Terraform exists) → P6-05 → P6-06 → P6-09 → P6-07 → P6-08. P6-09 comes from the 2026-11-21 review and runs before the close pair.

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
- [ ] A live IaC check result is recorded and compared to the mapping and the cost model (P7-08).
- [ ] Documentation and `PROJECT_STATUS.md` cite that result (P7-09).

### Issues

| ID | Title | Depends on | Expected/Seen Risk | Expected/Seen Size | Status | Acceptance |
| --- | --- | --- | --- | --- | --- | --- |
| P7-01 | Local→AWS architecture and service mapping | Phase 6 | Lo/- | Sm/- | Not started | Written mapping: MinIO→S3, local catalog→Glue, Trino→Athena; optional AWS counterparts for Airflow/Spark/Kafka documented; diagrams under `docs/architecture/` |
| P7-02 | Terraform scaffold under `infrastructure/terraform/` | P7-01 | Med/- | Md/- | Not started | Module layout, variables, outputs, README; backend configuration documented (local for CI; remote S3+DynamoDB lock as the recommended production pattern) |
| P7-03 | Core AWS resources as code (S3, Glue, Athena, IAM) | P7-02 | Hi/- | Lg/- | Not started | Modules for lakehouse buckets, Glue catalog/database, Athena workgroup, least-privilege IAM; `terraform validate` succeeds in CI |
| P7-04 | GitHub Actions OIDC to AWS | P7-03 | Hi/- | Md/- | Not started | OIDC provider and deploy role as Terraform + docs; no long-lived access keys in the repository |
| P7-05 | CI: Terraform fmt and validate | P7-02, P6-03 | Med/- | Sm/- | Not started | Workflow runs `terraform fmt -check` and `validate` on the IaC tree |
| P7-06 | AWS cost model | P7-01 | Med/- | Sm/- | Not started | Cost model with assumptions, drivers and monthly estimate ranges for the documented AWS design |
| P7-07 | Case study: README, diagrams, demo path | P7-03, P7-04, P7-05, P7-06 | Med/- | Md/- | Not started | In-repo case study: local platform + AWS IaC path, architecture diagrams, setup and provisioning guidance; content ready for Phase 9 site consumption |
| P7-08 | Live run, result, and consistency check | P7-03, P7-04, P7-05, P7-06, P7-07 | Med/- | Sm/- | Not started | Run `terraform fmt -check` and `terraform validate` on the IaC tree. Record the result. Compare module resources to the local→AWS mapping and to the cost-model drivers. `terraform plan` only with explicit approval. `apply` and `destroy` are outside this issue. Record mismatches. Do not invent spend figures |
| P7-09 | Phase 7 documentation and status | P7-08 | Lo/- | Sm/- | Not started | README, the case study, and `PROJECT_STATUS.md` cite the P7-08 result. P7-07 remains the case-study content issue |

**Suggested order:** P7-01 → P7-02 → P7-03 → P7-04 ∥ P7-05 ∥ P7-06 → P7-07 → P7-08 → P7-09.

---

## Phase 8 — Interactive exploration and data visualization

**Goal:** Give two read-only surfaces over verified Gold marts, both through Trino. JupyterLab is for iterative exploration. Apache Superset is for saved business charts. Spark stays the Bronze/Silver writer. dbt-trino stays the Gold writer.

**Depends on:** Phase 2 verified. This phase may run in parallel with Phases 3–7. It does not wait for Airflow, CDC, or the AWS path.

**Out of scope:** Writing Iceberg or PostgreSQL from notebooks or Superset. Using Superset SQL Lab as the exploration product. Using Grafana as business BI (Phase 5 keeps Grafana for operations). Replacing the static site or embedding a live BI session in it. SSO, public embedding, and row-level security as a product. Building Amazon QuickSight (document it as the AWS counterpart only). Kubernetes. New business metrics. Committing `.ipynb` cell outputs, row extracts, or Olist source rows. Creating `notebooks/` before this phase starts. The reviewed aggregate images for the site figure table are in scope at P8-06.

**Phase acceptance (all required):**

- [ ] P8-01 records pinned versions and a successful read-only Trino 484 query from JupyterLab plus one Superset chart on a verified Gold mart, or it records a rejection in ADR-0017.
- [ ] Notebooks query Gold by table and column name, do not write, and are committed without outputs.
- [ ] Superset runs in Compose with its metadata in a PostgreSQL database that is neither `odp` nor `iceberg`.
- [ ] Saved charts use only metrics already defined for Gold. They do not invent a figure.
- [ ] Secrets stay out of argv, SQL text, notebook output, and logs.
- [ ] Docs name four surfaces: JupyterLab, Superset, Grafana, and the Phase 9 site.
- [ ] P8-06 exports one static image per row of the site figure table, plus a caption manifest a later page can publish.
- [ ] A live run result is recorded and compared to Trino or the Phase 2 result for that run (P8-06).
- [ ] Documentation and `PROJECT_STATUS.md` cite that result (P8-07).

### Issues

| ID | Title | Depends on | Expected/Seen Risk | Expected/Seen Size | Status | Acceptance |
| --- | --- | --- | --- | --- | --- | --- |
| P8-01 | Notebook and BI compatibility PoC | Phase 2 | Med/- | Md/- | Not started | Pin JupyterLab (host `uv` dependency) and the Superset image. From JupyterLab, run one read-only query against a verified Gold mart on Trino 484. From Superset, render one chart on that same mart. Record versions, licenses, the Superset metadata database name, and every extra service the pinned image actually requires. Do not add Redis, a Celery worker, or a Jupyter Compose service unless this run shows they are required; state the purpose of each. Update ADR-0017 with the result. D-017 stays `proposed` until this evidence exists. No dashboard suite in this issue |
| P8-02 | JupyterLab exploration notebooks | P8-01 | Med/- | Md/- | Not started | Notebooks live under `notebooks/` and start with a documented host command against the published Trino port, same pattern as dbt. Cover at least two verified marts, including on-time rate and late orders. Connection secrets, if any, come from the environment or a mode-600 file, not from argv. A test fails if a code cell contains `INSERT`, `UPDATE`, `DELETE`, `MERGE`, `CREATE`, `DROP`, or `ALTER`. A test fails if a committed notebook has non-empty outputs. Cells do not print record values |
| P8-03 | Superset in local Compose | P8-01 | Med/- | Md/- | Not started | Pinned image, health check, and a documented UI URL. Metadata database on the existing PostgreSQL, separate from `odp` and `iceberg`. Admin and database passwords come from the environment, not argv. Compose adds only the services named in P8-01. The saved Trino connection is read-only: a repo check rejects DDL and DML in the committed Superset assets |
| P8-04 | Dashboards as code on verified Gold marts | P8-03 | Hi/- | Md/- | Not started | Commit a re-importable Superset export with the nine Superset charts in the site figure table (growth scheme). Each chart uses that row's mart and a measure already named in [`docs/architecture/gold-model-design.md`](docs/architecture/gold-model-design.md). No new metric. Charts read Gold through Trino. Re-import does not duplicate the dashboard. A chart does not render an order id, review text, email, or a single-customer location |
| P8-05 | Exploration, BI, ops, and site boundaries | P8-02, P8-04 | Lo/- | Sm/- | Not started | Document the four surfaces and the AWS counterpart: JupyterLab explores, Superset saves business charts, Grafana (Phase 5) is operations, the Phase 9 site publishes the static figures from those tools. QuickSight on Athena is the unbuilt AWS counterpart of Superset. State that SQL Lab is not a second notebook product. State the Olist CC BY-NC-SA limit: aggregate figures may be published with the caption; row extracts stay out of git and off the site |
| P8-06 | Live run, result, and consistency check | P8-02, P8-04, P8-05 | Med/- | Md/- | Not started | On the local stack, run the documented notebooks and open the Superset charts. Record component versions, the Gold tables queried, and the chart values. Compare those values to the same metrics from Trino, or to the Phase 2 result for that run. Export one static image per row of the site figure table, including the Jupyter image of the same on-time query, and a caption manifest (question, value, grain, Gold table, tool, version, run id). The image shows the caption value. Images are aggregates only. Confirm committed notebooks have empty outputs and that logs from this run do not contain credentials or row values. Record mismatches. Unit tests alone do not close this issue. Persist evidence in architecture docs / `docs/status-log.md` (not only `PROJECT_STATUS.md`) |
| P8-07 | Phase 8 documentation and status | P8-06 | Lo/- | Sm/- | Not started | Document versions, start commands, the four surfaces, the QuickSight counterpart, licenses, known limitations, and the P8-06 result. `PROJECT_STATUS.md` cites that evidence |

**Suggested order:** P8-01 → P8-02 ∥ P8-03 → P8-04 → P8-05 → P8-06 → P8-07. Phase close is P8-06 then P8-07.

---

## Phase 9 — Platform report site

**Goal:** Publish a small, polished **multi-page static report** of the verified platform, including the site figures. Host it and link it from the repository Website field and the README.

**Depends on:** Phase 7 verified (in-repo case study and AWS IaC path available as source material) and Phase 8 verified (exploration and Superset evidence available to cite).

**Out of scope:** Backend, CMS, authentication, analytics pixels, interactive demos that require a live cluster, duplicating the full `ROADMAP.md` issue backlog on the site, embedding a live Superset or Jupyter service.

**Phase acceptance (all required):**

- [ ] Static site under `site/` (or equivalent) with the five pages populated from verified platform material.
- [ ] Content matches delivered/verified work; no planned-but-unbuilt capabilities presented as complete.
- [ ] Site builds locally via a documented command; CI builds (and optionally deploys) the static output.
- [ ] Public URL is set on the GitHub repository Website field and linked from the README Platform report block.
- [ ] Navigation and page roles match the growth scheme above; reference page links back to repo docs/ADRs.
- [ ] Every row of the site figure table is a static image on the named page, with its caption. Figure 1 and figure 10 show the same on-time value.
- [ ] A live site result is recorded and compared to `PROJECT_STATUS.md` (P9-06).
- [ ] Documentation and `PROJECT_STATUS.md` cite that result (P9-07).

### Issues

| ID | Title | Depends on | Expected/Seen Risk | Expected/Seen Size | Status | Acceptance |
| --- | --- | --- | --- | --- | --- | --- |
| P9-01 | Site scaffold and page map | Phase 7 | Lo/- | Sm/- | In progress | `site/` as Astro Starlight + `@catppuccin/starlight`; five routes (Home, Architecture, Pipeline, Operations, Reference); local preview via `npm run dev`. Early scaffold allowed before Phase 7 and Phase 8; Phase 9 completes content depth and hosting |
| P9-02 | Home and Architecture pages | P9-01, P7-07, Phase 8 | Hi/- | Md/- | Not started | Home states platform purpose, verified status, and figure 1 (on-time Superset chart) with its caption. Architecture covers stack, medallion, contracts, batch vs stream, and the read path Gold → Trino → JupyterLab and Superset, with figure 10 beside figure 1's value. Diagrams come from `docs/` and from the P8-06 export |
| P9-03 | Pipeline and Operations pages | P9-01, P7-07, Phase 8 | Hi/- | Md/- | Not started | Pipeline covers ingestion through Gold, orchestration and CDC, and places figures 2–9 on the domain each question belongs to. Operations covers quality, observability, lineage, CI, and the local→AWS IaC path. It includes a Grafana image only when Phase 5 is verified, and it says so when that image is absent |
| P9-04 | Reference page and repo cross-links | P9-02, P9-03 | Lo/- | Sm/- | Not started | Reference links to GitHub, ADRs, roadmap, setup, and the figure manifest; README Platform report block points to the live URL (or staging URL if deploy is gated) |
| P9-05 | Static hosting and GitHub Website field | P9-04 | Hi/- | Sm/- | Not started | Hosting documented (e.g. GitHub Pages or equivalent); CI build green; repository About → Website set to the report URL |
| P9-06 | Live run, result, and consistency check | P9-04, P9-05 | Med/- | Sm/- | Not started | Build the site and open the published URL, or the production build when the URL is not up yet. Walk Home, Architecture, Pipeline, Operations, and Reference. Open each figure. Compare status, counts, caption values, and any metric or cost claim to `PROJECT_STATUS.md`, the phase docs, and the P8-06 manifest. Figure 1 and figure 10 match. Record broken links, missing images, and mismatches. Do not treat a single screenshot of the site as this check |
| P9-07 | Phase 9 documentation and status | P9-06 | Med/- | Sm/- | Not started | Site copy, README, and `PROJECT_STATUS.md` match the P9-06 findings. Unresolved mismatches are listed as limitations. Tone stays factual. No unverified metrics or cost figures |


**Suggested order:** P9-01 → P9-02 ∥ P9-03 → P9-04 → P9-05 → P9-06 → P9-07. Phase close is P9-06 then P9-07.

---

## Phase 10 — Table contracts and quality observability

**Goal:** Give Silver and Gold tables an executable contract. One ODCS v3.2.0 contract per table becomes schema, quality, freshness, and volume checks run by [Data Contract CLI](https://github.com/datacontract/datacontract-cli) (`datacontract-cli`, MIT) against Trino, with a queryable result history and alerts deduplicated by state transition. A frequently updated, openly licensed source (BCB SGS daily series) gives those SLAs data that arrives on a schedule.

**Depends on:** Phase 3 verified. No external release blocks the phase. `datacontract-cli` is a pinned PyPI dependency in the uv `contracts` group from P10-06 onward; the P10-01 proof of concept fixes the version. The source part (P10-02 to P10-05) does not use the library.

**Position:** not fixed. The number 10 is an identifier, not an order. The phase runs toward the end of the roadmap, and Filipe decides its exact position during execution.

**Out of scope:** Building a contract engine in this repository: the library runs lint and the checks, and this repository adds only the calendar table, the result writer, and the alert state. `datacontract dbt sync`, `datacontract publish`, and `--include-failed-samples`. Migrating the 19 existing source contracts; `postgres_oltp` is eligible by the migration rule and follows outside this phase. Changing the contract loader, record validation, the quarantine flow, the `transform_lakehouse` tasks, `medallion_build`, reconciliation, the `fct_*_quality_findings` facts, or `demo_storefront`. Replacing dbt tests. dbt `store_failures`. Storing failed rows: results are counts and names only. Changing the Bronze and Silver write mode or the update-pattern semantics: this phase follows ADR-0024 (P3-13) and P3-12. Prometheus, Grafana, and the P5-06 operational alerts. A real Slack workspace. Any API extract in version control. A catalog or discovery UI, lineage, anomaly detection, and checks on streams.

**Phase acceptance (all required):**

- [ ] ADR-0022 and ADR-0023 are approved with evidence from verified issues.
- [ ] BCB SGS series 1 and 11 are ingested by date window under an ODCS source contract, and the 19 existing contracts load unchanged.
- [ ] No extract of the source is in version control. The source note records ODbL 1.0 with the check date and the attribution to Banco Central do Brasil.
- [ ] Table contracts for the new source and for the named OLTP tables pass `datacontract lint` in CI and declare owner, freshness, and volume.
- [ ] Freshness and volume for the BCB SGS tables count business days from the `br_banking` calendar table: Friday's data is on time on Monday, and a missing business day is a gap.
- [ ] `data_quality.check_results` holds the result history as counts and names, with no row values.
- [ ] Alerts reach the terminal, e-mail through Mailpit, and a fictional Slack payload, and a repeated failure does not notify twice.
- [ ] Each failure scenario in P10-10 has recorded evidence with the expected status.
- [ ] A live run that crosses a weekend is recorded and compared to Trino and to the source window (P10-11).
- [ ] Documentation and `PROJECT_STATUS.md` cite that result (P10-12).

### Issues

| ID | Title | Depends on | Expected/Seen Risk | Expected/Seen Size | Status | Acceptance |
| --- | --- | --- | --- | --- | --- | --- |
| P10-01 | ADR-0022: Adopt Data Contract CLI and ODCS v3.2.0 for table contracts | Phase 3 | Med/- | Sm/- | Not started | ADR compares `datacontract-cli`, `soda-core`, and `great_expectations` by version, exact license, release activity, ODCS support, and fit with Trino, Spark, Iceberg, and dbt, each with its source link and check date. A proof of concept on the local stack records: the pinned version; `uv lock` resolving the `contracts` group next to the existing groups; `datacontract lint` passing with no server reachable; `datacontract test` against one Iceberg table through Trino over the Compose connection; and the JSON output of one failing check holding counts and names only. The ADR states what the library does not cover (business-day calendar, result history, alert deduplication) and where this repository covers it. It records the migration rule: a source contract under `config/contracts/` migrates to ODCS only when the source supports incremental load; `snapshot` and `full_refresh` sources keep the current vocabulary. The issue adds D-022 to `DECISIONS.md` as `approved` only with that evidence |
| P10-02 | ADR-0023: Frequently updated open-licensed source for contract SLAs (BCB SGS daily series) | Phase 3 | Hi/- | Sm/- | Not started | ADR follows ADR-0018. It compares the candidate sources by exact license, official link, attribution, and redistribution, each with the check date. A source with no explicit license or terms is rejected, and the rejected ones are named. The source is BCB SGS series 1 (US dollar, selling rate) and 11 (Selic) under ODbL 1.0, checked on the official page of each series with the date. Redistribution: fetch from the source, no extract in version control, synthetic fixtures. The ADR gives the attribution text to Banco Central do Brasil for the docs and the report site, the Gold domain (ADR-0012), the cadence, and freshness by business-day calendar. It fixes the origin of the banking-holiday list: the BCB official publication, recorded with its URL and check date, not a generic holiday library, because banking holidays differ from national holidays. It also fixes the publication lag of each series, measured on one real response. That response is recorded as counts and field names only. API limits are checked at the source with the date. Open-Meteo is named as the fallback, with its license and usage conditions. "Revisit when": BCB changes the license or the API limits. The issue adds D-023 to `DECISIONS.md` as `approved` only with that evidence |
| P10-03 | Source contract in ODCS and adapter | P10-01, P10-02 | Med/- | Md/- | Not started | The BCB SGS source contract is ODCS v3.2.0. An adapter in `src/open_data_platform/contracts/` reads the ODCS YAML with this repository's own code, with no dependency on `datacontract-cli`, and returns a `DataContract` equal to the one the current vocabulary would give, so record validation and quarantine do not change. Fields with no ODCS equivalent (`on_invalid`, `on_structural_error`, `update_pattern`, `payload_format`, `record_locator`) go in `customProperties`, documented in [`docs/architecture/data-contracts.md`](docs/architecture/data-contracts.md). The 19 existing contracts load unchanged. Unit tests cover both |
| P10-04 | Incremental ingestion of BCB SGS | P10-03 | Hi/- | Md/- | Not started | The extract takes a date window (`dataInicial` / `dataFinal`). The same `execution_id` overwrites. A repeated window does not duplicate. Invalid records are quarantined per the contract. Tests use synthetic fixtures and do not call the API. The thin DAG is created paused, catches up missed windows, and emits the ingestion Asset P3-13 defines. `docs/sources/bcb-sgs.md` has the same fields as the Olist note, with `License` as "ODbL 1.0 (checked YYYY-MM-DD)" and its link, the attribution to Banco Central do Brasil, and a row in `docs/sources/README.md`. No API extract is in version control: a unit test fails when a data file of this source exists outside the synthetic `tests/fixtures/` |
| P10-05 | Bronze, Silver, and Gold for BCB SGS | P10-04 | Hi/- | Md/- | Not started | Bronze and Silver checks return `ok`. Silver deduplicates by `(codigo_serie, data)`. One Gold model lands in the domain ADR-0023 names. Grain and key are documented. Bronze and Silver use the refresh from P3-13 and the `incremental_window` semantics from P3-12 |
| P10-06 | Table contracts | P10-01, P10-05 | Med/- | Md/- | Not started | uv group `contracts` holds `datacontract-cli[trino]` at the exact version ADR-0022 names, outside the default groups, locked, and exported to the runtime image the way `docker/runtime/dbt-requirements.txt` is; the image builds and `datacontract --version` prints that version inside it. ODCS v3.2.0 contracts under `config/table_contracts/{schema}/{table}.yaml`, each with a `trino` server, for the Silver and Gold tables of the BCB SGS source, Silver `postgres_oltp` `orders` and `order_items`, and Gold `oltp_operations.fct_oltp_orders` and `mart_oltp_platform_sales`. Each declares owner, freshness, and volume. `config/calendars/br_banking.yaml` holds the time zone, the working weekdays, the holiday list, and `valid_until`, and records its origin and check date. It generates the Iceberg table `data_quality.business_days`; a unit test fails when the calendar is past `valid_until`. The BCB SGS contracts name the calendar in `customProperties` and declare freshness and missing-day checks as `type: sql` rules that count business days in that table; the OLTP contracts use the library's `freshness` service level. A unit test fails when a contract has no owner, no freshness rule, or no volume rule. A `contracts` job in `.github/workflows/ci.yml` installs only the uv `contracts` group and runs `datacontract lint --all-errors` over every file in `config/table_contracts/`; it passes, and it fails on a contract with a planted schema error |
| P10-07 | `data_quality.check_results` table | P10-01, P10-06 | Med/- | Md/- | Not started | Append-only Iceberg table created through the JDBC catalog and queryable in Trino. One row per check per run. Columns come from the library's `Run` and `Check` models at the pinned version: run id, contract id and version, server, start and end time, check id, category, type, name, model, field, engine, result, library version, plus `calendar_id` and `execution_id`. `failedSamples`, `diagnostics`, `implementation`, and `logs` are not stored. `reason` is stored only if the P10-01 evidence shows counts and names; otherwise it is dropped. The writer reads the file `datacontract test --output-format json` produces and appends through Spark; no result goes into SQL text. Loading the same run file twice does not duplicate rows. A unit test plants a value in `failedSamples`, `diagnostics`, and `reason` of a result file and finds it in no written row. A unit test fails if any call passes `--include-failed-samples`. Grain is documented |
| P10-08 | `check_contracts` DAG | P10-06, P10-07, P3-04 | Med/- | Md/- | Not started | Thin wrapper: one task runs `datacontract test` per contract with JSON output to a work file, the next appends that file to `data_quality.check_results`. A failed check does not fail the write task, so the history records failures. Trino settings come from the environment only. Created paused. A DagBag test covers it. Its test task takes the 1-slot `lakehouse_gold` pool that P3-04 defines, so it never runs while a lakehouse table is written or reconciled |
| P10-09 | Alerts and runbook | P10-08 | Med/- | Md/- | Not started | Alert state is computed in this repository's package from `data_quality.check_results`: a notification goes out only when a check's result differs from its previous run. The terminal alert always prints. E-mail goes through Mailpit in Compose (pinned image), with SMTP settings from the environment only. The fictional Slack alert is a Block Kit payload written to a file; there is no real workspace, and the docs say so. A runbook is under `docs/runbooks/`. Two consecutive runs prove deduplication: the second failing run does not notify again. Messages carry counts and thresholds, never values |
| P10-10 | Failure scenarios | P10-08 | Med/- | Md/- | Not started | Recorded evidence, each with the expected status, for: a removed column, a late window, a missing series, a Monday with no false alarm (Friday's data is on time), and a missing business day reported as a gap |
| P10-11 | Live run, result, and consistency check | P10-09, P10-10 | Med/- | Md/- | Not started | On the local stack, ingest two consecutive real windows from the BCB SGS API and repeat one of them. The run crosses a weekend. Compare the history counts to Trino and to the source window. Record mismatches. Unit tests alone do not close this issue |
| P10-12 | Phase 10 documentation and status | P10-11 | Lo/- | Sm/- | Not started | Docs and `PROJECT_STATUS.md` cite the P10-11 result |

**Suggested order:** P10-01 ∥ P10-02 → P10-03 → P10-04 → P10-05 → P10-06 → P10-07 → P10-08 → P10-09 → P10-10 → P10-11 → P10-12. Phase close is P10-11 then P10-12.

---

## Phase 11 — BioGuardia Atlantic: operational sources at scale

**Goal:** Generate and process two big synthetic feeds of a fictitious environmental agency in the Atlantic Forest: a daily operational export of about 24M rows (357 MB of gzip; at most 2 GB) and a telemetry stream of about 1M events per hour that runs 24/7 without Airflow. On them, build the cases earlier phases do not exercise at volume: PII masking, data skew, SCD Type 2 from daily snapshots, and reconciliation between systems and between batch and stream. Domain model: [`docs/architecture/bioguardia-domain.md`](docs/architecture/bioguardia-domain.md). Decisions: [ADR-0027](docs/decisions/0027-bioguardia-synthetic-sources.md) (sources) and [ADR-0028](docs/decisions/0028-telemetry-message-broker.md) (broker).

**Depends on:** P11-01 to P11-03 depend on nothing and start before Phase 3 closes (waiver in `DECISIONS.md`, 2026-11-26): they change no platform table, DAG, or contract. P11-04 onward depend on Phase 3 verified. P11-08 and P11-15 depend on P5-07, so P5-05 and P5-07 move forward if this phase's processing comes before Phase 5. Phase 4 depends on P11-01 to P11-04.

**Position:** not fixed, like Phase 10. Filipe decides the order of Phases 4, 5, and 11 during execution.

**Out of scope:** binary media (audio, images, rasters, LiDAR point clouds; see Candidates); Schema Registry and Avro; Kafka authentication, TLS, and multi-broker replication; real personal data; real species or IUCN data; AWS resources (Phase 7 maps them); an exactly-once claim without a written proof.

**Rules for every pipeline issue of this phase (P11-05 to P11-16):** its acceptance answers every row of the [pipeline design checklist](#pipeline-design-checklist), each with its evidence, or "not applicable" with a reason. The answers below are the minimum; the issue's plan completes them before code. Ingestion never reads the `odp-evaluation` bucket; tests and evidence compare findings to it.

**Phase acceptance (all required):**

- [ ] The ops and finance exports release one complete drop per local day with a manifest, and the stream sustains at least 1.0M events per hour, both without Airflow (P11-02, P11-03).
- [ ] A drop lands in raw only after its manifest checks pass; a defective drop is reported and lands nothing; a version 2 supersedes version 1 (P11-05).
- [ ] Bronze and Silver apply each table's change type, including deletes, and a refresh's work does not grow with total history (P11-06, P11-10).
- [ ] SCD Type 2 dimensions use business effective times, and stream readings use the calibration valid at event time (P11-07, P11-10).
- [ ] No direct identifier, free-text PII, single-person location, or erased person's data reaches Gold, logs, or SQL text (P11-08).
- [ ] The stream lands 24/7 independently of Airflow, resumes after a stop, and every missing event is classified (P11-09, P11-12, P11-16).
- [ ] Skew is measured and its mitigation recorded with before and after figures (P11-11).
- [ ] Reconciliation reports every planted defect, with exact decimal totals (P11-12).
- [ ] Gold answers the four use cases and field operations with documented grain (P11-13, P11-14).
- [ ] Telemetry storage stays inside a documented, measured budget (P11-15).
- [ ] A live run result is recorded and compared to the planted defects and the source counts (P11-17).
- [ ] Documentation and `PROJECT_STATUS.md` cite that result (P11-18).

### Issues

| ID | Title | Depends on | Expected/Seen Risk | Expected/Seen Size | Status | Acceptance |
| --- | --- | --- | --- | --- | --- | --- |
| P11-01 | Domain model, ADR-0027 and ADR-0028, Phase 11 issues | — | Lo/Lo | Sm/Sm | Verified | [`docs/architecture/bioguardia-domain.md`](docs/architecture/bioguardia-domain.md) describes the organization, use cases, sources, table and topic catalog, world model, the four cases, synthetic-data safety, planted defects, and the storage budget. ADR-0027 and ADR-0028 are `proposed`, with D-027, D-028, and the waiver in `DECISIONS.md`. This section lists every issue with its ratings. P4-02, P4-04, P4-08, and P5-07 are amended; ADR-0008 has an addendum; the SCD and PII notes of `gold-model-design.md` and `data-contracts.md` point to P11-07 and P11-08 |
| P11-02 | Daily operations export generator | P11-01 | Med/Med | Lg/Lg | Verified | Package `open_data_platform.simulators.bioguardia` and the Compose service `bioguardia-ops-export` (profile `sources`, image `odp-sources`) write, for each local day, an ops drop of 21 tables (22 on the first day of a month) and a finance drop of `fine_payments` to `odp-landing`, each with `_manifest.json` (row counts, bytes, SHA-256, columns, exact decimal control totals) and `_SUCCESS` written last; the planted list goes only to `odp-evaluation`. At scale 1 an ops drop holds at least 20M rows and at most 2 GB of gzip files; a larger drop fails (3.2 GB uncompressed at scale 1, measured). The service releases the ops drop after 02:00 BRT and the finance drop after 06:00 BRT, catches up missed days in date order, releases each scheduled version 2 three days later, and never releases a date early; the CLI writes later dates only with `--allow-future`. Unit tests: re-generating a (date, version) gives identical content; a date generated alone equals it inside a range; generating day D does not slow down with history; manifests match files except planted file defects; SCD2 signals (old state on D-1 and new on D, late-entered and future-dated changes, deletes); every CPF and CNPJ fails its checksum, every email is under `.example`, every phone uses an unassigned area code; skew shares within documented ranges; referential integrity except planted orphans; no row value in logs; a generated column missing from its source note fails. Live: the drops from 2026-11-26 to 2026-12-31 generated into MinIO with rows, bytes, and duration per drop recorded; one date regenerated with equal SHA-256; a drop killed mid-write completed by the next run |
| P11-03 | Kafka broker and telemetry producer | P11-02 | Med/Med | Md/Lg | Verified | Kafka `apache/kafka:4.3.2` (KRaft, one node) on the `streaming` profile, with the four topics of the domain model created by an init service (partitions, 72-hour `retention.ms`, `retention.bytes` cap, automatic creation off). The `bioguardia-telemetry` service publishes the world model's events with `acks=all`, idempotence, and zstd, keyed by `device_id`, with no Airflow dependency, and checkpoints the last fully delivered second every 5 s. Unit tests: the modeled baseline at scale 1 is at least 1.0M events per hour; per device and UTC day the producer's events equal `generated - dropped` in `device_daily_counters`; `event_id` is unique; a restart replays from the checkpoint and skips what is older than the 12-hour buffer, writing one gateway record whose skipped count equals the missing events; payload fields match the source note; no payload in logs. Live: a 60-minute run crossing 00:00 UTC with Airflow stopped delivers at least 1.0M events and at least 99% of the modeled count (end-offset deltas), with producer CPU and memory and broker disk growth recorded; a 10-minute stop is replayed, and `telemetry verify` finds every modeled `event_id` of the window, with duplicates bounded by the checkpoint interval. 24 hours of 24/7 operation are recorded in P11-17 |
| P11-04 | Spark Structured Streaming on Kafka: compatibility proof | P11-03, Phase 3 | Med/- | Sm/- | Not started | Spark 3.5.8 in the runtime image, with `spark-sql-kafka-0-10_2.12:3.5.8` pinned and cached like the Iceberg jars (P2-08), reads one telemetry topic and writes one Iceberg table in a scratch namespace through the JDBC catalog; Trino 484 reads its count; a stop and restart resumes from the checkpoint without re-reading committed offsets. Versions and the result go into ADR-0028; an incompatible combination is rejected with a note, as P2-01 did. Not a pipeline step: the probe table is dropped after the check. P11-09 and P4-04 depend on this proof |
| P11-05 | Contracts and landing ingestion of the daily exports | P11-02, Phase 3 | Hi/- | Lg/- | Not started | Contracts under `config/contracts/bioguardia_ops/` and `config/contracts/bioguardia_finance/` for the 23 datasets, with a new `source_family` `conservation_analysis`, `pii` on every identifier and free-text field, and each table's change type (`snapshot`, `incremental_window` on `updated_at`, or `append_only`); gzip CSV and JSONL payloads are read. One daily DAG per source, created paused, with catchup, waits for its date's `_SUCCESS` (a sensor with a timeout) and reports a missing drop as a gap in the coverage ledger (ADR-0025). It ignores a drop whose manifest `released_at` is later than the run (drops written in advance with `--allow-future`, as the 2026-11-26 to 2026-12-31 drops were). Before anything lands in raw it checks each file's SHA-256, bytes, and row count against the manifest; a truncated or missing file fails the drop visibly and lands nothing. A version 2 lands as a new execution that supersedes version 1 for its date. Invalid records go to quarantine per contract. The DAG emits the source's raw Asset, lists only its own prefix in `odp-landing`, and never reads `odp-evaluation`. Checklist: change type per table as above; window = the export date of the DAG's data interval, a missed date is caught up, a version 2 is the only overlap; re-running a date overwrites the same execution and a full rebuild re-reads every drop; deletes = `deleted_records` lands as data; a run reads only its date's drop; readers see an execution only with `_COMPLETE`; the DAG is the only writer of these raw prefixes; trigger = the schedule after the release time, plus catchup; failure = no `_COMPLETE`, and a retry re-lands the date; evidence = the planted truncated and missing files fail exactly their drops, a version 2 supersedes, a re-run changes no count, durations measured at scale 1 |
| P11-06 | Bronze and Silver for the daily exports | P11-05 | Hi/- | Lg/- | Not started | Bronze keeps every execution of every table. Silver per change type: snapshot tables take the latest complete execution (a row absent from it leaves Silver); change tables keep the latest `updated_at` per key and apply `deleted_records`, the first batch path of the platform that sees deletes; append tables deduplicate by key and place records by event time. The `-03:00` timestamps become UTC. The `devices` schema v2 lands through a new contract version on the documented path (P3-11). Promotion of a 1 GB drop holds no table in Python memory; if the P3-13 promotion cannot carry 21M rows a day, the plan names the Spark-native path for this source and measures both. Checklist: re-running a drop changes no Silver count; a refresh reads only new raw objects plus, for snapshots, the latest execution, and its work does not grow with history (measured over at least five drops); readers never see a half-written table (P3-11); the refresh is the only Silver writer of these tables; a failure leaves the previous snapshot; evidence = counts per table against the manifest for five drops, including a defect day and a version 2 |
| P11-07 | SCD Type 2 dimensions from daily snapshots | P11-06 | Hi/- | Md/- | Not started | Type 2 tables for `devices`, `staff`, `species`, `reserves`, and `zones` with `valid_from`, `valid_to`, `is_current`, and the source `updated_at`. `valid_from` is the business effective time (`calibrated_at`, `effective_from`, `assessed_on`) where the table has one, not the load date. A late-entered change opens its version at its effective time and shortens the previous one; a future-dated change is stored and becomes current on its date; a row absent from a snapshot closes its current version; a version 2 or a repeated snapshot adds no version; two changes on one day keep only the end-of-day state (documented limit). The staff dimension tracks only non-identifying attributes (team, reserve, role, status). An as-of join returns the version valid at a timestamp; a test shows a reading whose calibrated value differs between the as-of row and the current row. Checklist: derived from snapshots; re-running is idempotent (unit test); a run reads the new snapshot and the current versions only; atomic replace; evidence = unit tests per case and a live comparison over the P11-06 drops |
| P11-08 | PII protection | P11-06, P11-07, P5-07 | Hi/- | Lg/- | Not started | Direct identifiers (names, CPF, emails, phones, birth dates, home municipality) move from Silver to a restricted namespace that only a named Trino user can read (file-based access control); Silver and Gold keep keyed pseudonyms (HMAC-SHA256 with a secret read from a mode-600 file, never from SQL text, argv, or logs). Free text in `incidents.notes` and `citizen_reports.description` is scrubbed before Silver; recall and precision against the planted list in `odp-evaluation` are recorded. `patrol_track_points` reach Gold only as zone-hour coverage, and reporter coordinates are generalized to the zone. Sensitive-species locations are generalized to a 10 km grid in Gold. Camera-trap images with people and `human_voice` detections are excluded from Gold. An erasure request removes the person's identifiers and pseudonym mapping from the restricted namespace and Silver history, and the snapshots that still hold them are expired with the P5-07 command. Tests: a planted canary value is absent from every Gold table, every rendered SQL statement, and the task logs; a statement-builder test per AGENTS.md section 8. The plan answers the checklist per table |
| P11-09 | Telemetry stream to Bronze | P11-04 | Hi/- | Lg/- | Not started | A Spark Structured Streaming service (Compose, `restart: unless-stopped`, outside Airflow) reads the four topics and appends to Bronze Iceberg tables partitioned by event day, with checkpoints on a named volume and a trigger interval set from measurement. Each row keeps topic, partition, offset, Kafka timestamp, `event_id`, and the raw payload. A consumer stop and start resumes from the checkpoint with no loss while the backlog is inside retention; Airflow down has no effect on landing. An hourly Airflow DAG, created paused, reports consumer lag, freshness, and files written per hour, and emits the Asset the downstream hourly work is scheduled on. Delivery is at-least-once with `event_id` as the dedupe key; no exactly-once claim. Checklist: `append_only`; window = Kafka offsets, late events land when they arrive; a replay from a checkpoint duplicates only rows after it (deduplicated in P11-10); no deletes; a micro-batch reads only new offsets; readers see Iceberg commits per micro-batch; the service is the only writer of these tables; trigger = continuous; a failed micro-batch is retried from the checkpoint; evidence = stop and restart drills with counts per topic |
| P11-10 | Telemetry Silver and hourly aggregates | P11-07, P11-09 | Hi/- | Lg/- | Not started | Silver readings and health deduplicated by `event_id`; `event_time` places an event, and a 12-hour watermark (the device buffer) lets late uploads land in their hour; events with a skewed clock (outside a documented window around `gateway_time`) are quarantined with a count. Calibrated values come from the as-of join to the device version valid at `event_time` (P11-07). Hourly aggregates per device and per zone (count, minimum, maximum, mean of calibrated values). Each hour is replaced atomically, so re-running an hour gives the same rows; a run reads only Bronze rows added since the last processed snapshot plus the hours that late data or a late calibration change touches, which it recomputes. The plan answers the rest of the checklist. Evidence: late-event, duplicate, clock-skew, and late-calibration cases on the live stream, with counts |
| P11-11 | Data skew: measure and mitigate | P11-06, P11-10 | Med/- | Md/- | Not started | From the Spark event log (`lakehouse/spark_metrics.py`): task skew (maximum and median task duration, shuffle bytes per task) for detections joined to species and aggregated by species and zone, telemetry aggregated by reserve during a fire burst, and a chattering device's hour. Each is measured with AQE skew handling off and on, and with salting where AQE does not help; durations and task figures recorded before and after. The telemetry partition specs are chosen from these measurements and recorded in an ADR addendum. It changes the jobs of P11-06 and P11-10; the plan states any checklist answer that changes |
| P11-12 | Reconciliation | P11-06, P11-10 | Hi/- | Md/- | Not started | Gold reconciliation tables and dbt tests. R1: per drop and table, Silver counts and exact decimal sums against the manifest, with no tolerance. R2: per device, UTC day, and stream, landed distinct events against `generated - dropped` in `device_daily_counters`, each missing event classified as device-dropped, gateway-skipped (gateway topic), or unexplained, and duplicates counted. R3: infraction notices against finance payments in exact BRL, classed matched, partial, overpaid, unmatched payment, paid after cancellation, and in transit. Every planted defect in `odp-evaluation` appears in the findings; outside a drill, unexplained loss is zero. Findings hold keys and counts, no row values. Checklist: rebuilds per day only the days whose inputs changed; evidence = the findings of the P11-06 drops compared to their planted lists |
| P11-13 | Gold: wildfire and watershed | P11-08, P11-10, P11-12 | Hi/- | Lg/- | Not started | Marts with documented grain: detection lead time per fire (first ground alert, first satellite hotspot, first incident report); zone fire risk per day (calibrated smoke and heat, wind, hotspots); deforestation alert response time (detection, field validation, infraction); river level and flood alerts per gauge-hour; a soil-moisture drought index per zone-day. Each model is a full table build or incremental by day, as the plan decides from the measured `dbt_build` duration; re-running gives the same rows; an incremental run reads only the days that changed. dbt unit tests by the P3-17 rule and entries in `dbt/unit_test_policy.yml` |
| P11-14 | Gold: biodiversity, restoration and operations | P11-06, P11-08 | Hi/- | Lg/- | Not started | Marts with documented grain: species activity and diversity (Shannon index) per zone-week from acoustic and camera-trap detections; camera-trap occupancy per species and zone; planting survival by species and zone; carbon stock per zone from canopy surveys, wood density, and soil carbon, updated when late lab results arrive; patrol coverage per zone-hour without individual tracks; incident lifecycle and enforcement outcome. Same build, re-run, and unit-test rules as P11-13. No mart puts a pseudonym next to a location at single-person grain |
| P11-15 | Telemetry storage maintenance | P11-09, P5-07 | Hi/- | Md/- | Not started | The P5-07 maintenance command compacts the telemetry Bronze and Silver tables, expires their snapshots, and deletes Bronze partitions older than a retention chosen from measured growth; file counts, bytes, and snapshot counts before and after are recorded. Kafka retention and the storage budget in the domain model are updated with the measured growth per day and layer. A daily DAG runs it outside the stream's write path; a commit conflict with the stream retries. Checklist: deletes only past the retention; compaction is one atomic commit for readers; evidence = a recorded run |
| P11-16 | Orchestration and outage drills | P11-05 to P11-15 | Hi/- | Md/- | Not started | Every Phase 11 DAG is wired with Assets: the daily ingestion triggers the Bronze and Silver refresh, the stream monitor triggers the hourly telemetry work, and reconciliation and Gold follow them; DagBag tests cover them. Drills, each with counts: Airflow stopped for at least six hours while the producer and the stream keep landing, after which every DAG catches up with no gap and no duplicate; the stream consumer stopped for an hour while Kafka holds the backlog, after which it resumes; the producer stopped longer than a short test buffer, after which R2 reports gateway-skipped equal to the gateway record; the consumer stopped longer than a short test retention, after which R2 reports unexplained loss for exactly the expired range |
| P11-17 | Live run, result, and consistency check | P11-16 | Med/- | Md/- | Not started | On the local stack, at least three consecutive daily drops at scale 1, including a planted defect day and a version 2, and 24 hours of stream at the full rate, with one Airflow outage drill. Record rows and bytes per drop, the stream rate per hour, task durations, storage growth per layer, SCD2 version counts, the reconciliation findings compared to the planted lists and drill records, a PII scan of Gold and logs (no planted canary found), and the skew figures. Record mismatches. Unit tests alone do not close this issue |
| P11-18 | Phase 11 documentation and status | P11-17 | Lo/- | Sm/- | Not started | The domain model, the source notes, ADR-0027, ADR-0028, and `PROJECT_STATUS.md` cite the P11-17 result |

**Suggested order:** P11-01 → P11-02 → P11-03 (now, under the waiver). After Phase 3: P11-04 → P11-05 → P11-06 → P11-07 → P11-08, with P11-09 → P11-10 beside P11-06 to P11-08 → P11-11 → P11-12 → P11-13 ∥ P11-14 → P11-15 → P11-16 → P11-17 → P11-18. Phase close is P11-17 then P11-18. D-027 moves to `approved` when P11-02 is verified; D-028 when P11-03 and P11-04 are.

---

## Candidates, not issues

Not scheduled. None of these belong to a phase objective. Do not assign an ID until the roadmap is revised to include the work.

| Candidate | Current behavior | Why it waits |
| --- | --- | --- |
| Binary media for BioGuardia (audio clips, camera-trap images, satellite rasters, LiDAR point clouds) | The Phase 11 sources carry media metadata and derived products only ([domain model](docs/architecture/bioguardia-domain.md)) | Adds storage and an unstructured-data path without exercising the Phase 11 cases; revisit when an issue needs media processing |
| Iceberg partitions for Silver and Gold | Tables have no partition spec. File size is the engine default. Recorded in [`docs/architecture/phase-2-lakehouse.md`](docs/architecture/phase-2-lakehouse.md) | A per-table spec is a new design. Phases 3–9 do not ask for it for Silver and Gold. Bronze is no longer a candidate: P3-08 partitions it so the incremental `MERGE` does not scan all history (2026-11-21 review R5) |

---

## Cross-cutting rules

| Topic | Rule |
| --- | --- |
| Three update patterns | Daily batch, weekly batch and streaming CDC are all in scope; each must remain distinguishable in docs and demos |
| Batch vs CDC | Scheduled batch uses windowed extracts + Airflow; CDC uses logical replication—do not conflate the two |
| Pipeline design | Every pipeline issue answers the [pipeline design checklist](#pipeline-design-checklist) in its plan and acceptance |
| Exactly-once | Never claim end-to-end exactly-once without a written technical proof for the specific path |
| New technology | State purpose and operational cost; prefer proving compatibility before expanding |
| Kubernetes | Out of scope unless an explicit approved requirement |
| Secrets | Never commit credentials, tokens, live `.env`, or private datasets |
| Cloud / AWS IaC | Phase 7 delivers the AWS path as Terraform IaC and local→AWS mapping. CI validates with `fmt`/`validate`. Cost models must state assumptions. Never invent metrics or spend figures. Do not run `terraform apply`/`destroy` without explicit user approval |
| Phase close | From Phase 2 on, every phase ends with a live run, result, and consistency check, then a phase documentation and status issue. The next phase waits until both are verified. Phases 0 and 1 stay verified on results already in `docs/status-log.md` |
| Exploration vs BI | JupyterLab explores iteratively and is read-only. Superset holds saved charts over verified Gold metrics. Grafana (Phase 5) is operations. The static site (Phase 9) publishes the checked images from those tools and does not embed them |
| Platform report site | Scaffold may land early; figures, full content, design, and hosting are finalized in Phase 9. Earlier phases may add verified facts only. Phase 8 may add the exported images once P8-06 has checked them. The site summarizes the platform; it does not replace `PROJECT_STATUS.md` / ADRs as source of truth |
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

| Document | Reference |
| --- | --- |
| Created | 2026-10-15 |
| Last updated | 2026-11-27 (P3-14; P11-01 to P11-03) |
| Owns | Program scope and issue acceptance |
| Related | `PROJECT_STATUS.md`, `docs/status-log.md`, `DECISIONS.md`, `docs/project-genesis.md` (previously `docs/project-context-for-cursor.md`), `AGENTS.md`, `CLAUDE.md` |
| Changelog | [Changelog](#changelog) below; detail in `docs/status-log.md` |

---

## Changelog

- 2026-11-27: P3-14 verified. The live-run Phase 3 acceptance item is checked.
- 2026-11-27: P11-03 verified. Kafka 4.3.2 (KRaft, one node) on the `streaming` profile with four topics, and the `bioguardia-telemetry` producer with checkpoint, replay, and gateway records; `telemetry model` and `telemetry verify`. Seen size Lg: the restart, replay, and verify paths grew past the estimate.
- 2026-11-27: P11-02 verified. The `open_data_platform.simulators.bioguardia` package and the `bioguardia-ops-export` service publish the daily ops and finance drops to `odp-landing`; the drops from 2026-11-26 to 2026-12-31 are generated.
- 2026-11-27: P11-01 verified. Domain model, ADR-0027, ADR-0028, and the Phase 11 issues.
- 2026-11-26: Phase 11 added (P11-01 to P11-18): BioGuardia Atlantic synthetic sources (daily ops and finance exports, a 1M events/hour telemetry stream) and their processing. P11-01 to P11-03 start before Phase 3 closes (waiver in `DECISIONS.md`). P4-02 runs Debezium on the P11-03 broker, P4-04 depends on P11-04, P4-08 is the only CDC-based SCD issue, and P5-07's command is shared with P11-08 and P11-15. Phase 4 depends on P11-01 to P11-04.
- 2026-11-26: P3-10 verified. Spark cores and driver memory capped by `ODP_SPARK_CORES` and `ODP_SPARK_DRIVER_MEMORY`; every Spark script sets the UTC session time zone; Airflow moves to the uv `airflow` group; `prune-work` and the `prune_work` task delete run scratch folders past `ODP_WORK_RETENTION_DAYS`; `.env.example` uses `ODP_TRINO_HOST_PORT` and `ODP_TRINO_USER` (a `.env` that still sets `TRINO_PORT` falls back to 8080); `dbt/profiles.yml.example` removed, `dbt/profiles.yml` is the versioned profile.
- 2026-11-24: Expected/Seen Risk and Size columns on every issue: the expected half for open issues, the seen half for closed ones.
- 2026-11-24: P3-09 verified. `scripts/p3_09_orchestration_check.sh` runs eight success and failure scenarios on the local Airflow (catch-up after a pause, happy path, retries, failed runs, failed Silver write, no-new-raw refresh, coalesced refresh, quality gate); all checks passed.
- 2026-11-23: P3-08 verified. ADR-0026 approved: Bronze partitioned by `dt`, `MERGE` limited to the staged partitions, run log in `lakehouse_state` (`step_runs`, `dataset_runs`, `table_io`). Scan counts come from the Spark event log instead of the planned Iceberg scan report. At Filipe's request the Silver `quality_counts` `MERGE` was reviewed and no longer rewrites the table; `bronze_refreshes` kept.
- 2026-11-23: P3-07 verified. `on_invalid` splits into `quarantine_exclude` (PostgreSQL, clickstream) and `quarantine_flag` (Olist, funnel, DummyJSON, IBGE), approved by Filipe; `snapshot` and `full_refresh` contracts declare `volume_guard.min_row_ratio`; the refresh takes `--allow-volume-drop`. The invalid-records Phase 3 acceptance item is checked
One line per roadmap change, newest first. Detail and evidence are in [`docs/status-log.md`](docs/status-log.md) under the same date; do not repeat them here.

Renumbering: lines before 2026-11-14 that say P3-10/P3-11, and lines before 2026-11-20 that say P3-11/P3-12, mean the Phase 3 close pair, now P3-14/P3-15. Lines before 2026-11-11 that say Phase 8 or P8-06/P8-07 mean the platform report site, now Phase 9.

- 2026-11-22: P3-18 verified. Acceptance widened by Filipe: `closed_deals.declared_monthly_revenue` is also decimal, each money field names its currency, and the data-contracts doc states the currency rules. The Gold-metrics Phase 3 acceptance item is checked
- 2026-11-22: P3-17 verified. Acceptance widened by Filipe: a written unit-test rule and a policy file covering every Gold model. The review mart category for an order spanning categories is `mixed`; dbt unit tests run with `make dbt-unit`, outside `dbt build`.
- 2026-11-22: P3-16 verified. The missed-window and check-gate Phase 3 acceptance items are checked.
- 2026-11-21: Review of verified issues ([review](docs/reviews/2026-11-21-verified-issues-review.md)). New P3-16, P3-17, P3-18, and P6-09; P3-14 depends on P3-16 to P3-18. P3-07 to P3-10, P3-15, P4-01, P4-04, P5-07, P6-04, P6-06, and P6-07 amended. Missed-window acceptance reopened. No status change.
- 2026-11-21: P3-06 verified. The PostgreSQL execution id becomes `<dag_id>__<run_id>`.
- 2026-11-21: P3-05 verified. Window `[W-8, W-1)` instead of `[W-7, W)`, approved by Filipe.
- 2026-11-20: P3-13 verified (ADR-0024 approved). The raw Asset schedule replaces the P3-04 `TriggerDagRunOperator`.
- 2026-11-20: P3-12 verified. Clickstream contract v3 is `append_only`; the source refresh matrix is filled.
- 2026-11-20: P3-11 verified.
- 2026-11-20: Pipeline review after P3-04. New pipeline design checklist and P3-11 to P3-13; P3-04, P3-05, P3-06, P3-08, P3-09, P4-04, P5-03, and Phase 10 amended. The close pair moves to P3-14/P3-15.
- 2026-11-20: P3-04 verified. Acceptance gains the transform trigger; the `lakehouse_gold` pool holds every transform step that writes tables, and P10-08 joins it.
- 2026-11-20: Development flow simplified (`AGENTS.md` §3 and §13); Filipe decides the Phase 10 position. No status change.
- 2026-11-15: Phase 10 uses Data Contract CLI instead of contract-watcher (cancelled). P10-01 and P10-06 to P10-09 amended. No status change.
- 2026-11-15: Phase 10 added (P10-01 to P10-12). No status change.
- 2026-11-14: Git policy moved into `AGENTS.md` §13; verified-status wording names the evidence docs. No status change.
- 2026-11-14: Cursor rules consolidated into `AGENTS.md`; `CLAUDE.md` imports it; context doc renamed `docs/project-genesis.md`. No status change.
- 2026-11-14: P3-03 review findings. P3-04 and P3-08 amended; new P3-10 runtime hardening; the close pair is renumbered.
- 2026-11-13: P3-03 verified. P3-09 also covers `transform_lakehouse` and `reconcile_gold`.
- 2026-11-11: Iceberg partitions for Silver and Gold noted as a candidate, not an issue.
- 2026-11-11: P2-10 verified. Phase 2 verified.
- 2026-11-11: P2-09 verified.
- 2026-11-11: Site figure table added.
- 2026-11-11: Phase 8 is JupyterLab + Superset (P8-01 to P8-07); the platform report site moves to Phase 9 (P9-01 to P9-07).
- 2026-11-06: P2-06 verified (55 Gold tables).
- 2026-11-06: P2-05 verified. Phase 4 close pair is P4-09/P4-10; P4-08 is SCD Type 2 before it.
- 2026-11-05: Phase 2 close pair is P2-09/P2-10; Spark Ivy cache inserted as P2-08.
- 2026-11-01: Iceberg snapshot expiration is P5-07; Phase 5 close pair is P5-08/P5-09.
- 2026-10-28: Phase close pair applies from Phase 2 on. Phases 0 and 1 stay verified; P0-14/P0-15 and P1-12/P1-13 removed.
- 2026-10-28: P1-11 verified.
- 2026-10-26: P1-10 verified.
- 2026-10-25: P1-09 withdrawn (ADR-0021).
- 2026-10-25: P1-08 verified.
- 2026-10-18: P1-02 verified.
- 2026-10-17: Source metadata standardized; D-019 approved; status ledger scoped; P1-01 verified.
- 2026-10-16: Site scaffold; Phase 8 (site) added; Phase 0 verified.
- 2026-10-15: ADRs; AWS IaC path; three update patterns.
