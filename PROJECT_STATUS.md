# Project Status

Operational ledger for the **current phase**. Issue backlog and acceptance text live in [`ROADMAP.md`](ROADMAP.md). Full decision index: [`DECISIONS.md`](DECISIONS.md). Chronological archive: [`docs/status-log.md`](docs/status-log.md).

| Project | Current |
| --- | --- |
| Repository | [FilipeOliveira94/open-data-platform](https://github.com/FilipeOliveira94/open-data-platform) |
| Started | 2026-10-14 |
| Last updated | 2026-11-28 (P9-05 delivered; GitHub Pages workflow, live check after merge) |
| Current phase | Phase 4 — Streaming CDC |
| Phase status | Not started |

**Status values:** not started · in progress · delivered · verified · blocked · withdrawn

**Verified** means acceptance checks were run and evidence is recorded (here and/or in `docs/status-log.md`). Do not treat planned architecture as complete.

**What belongs here:** current objective, current-phase issue progress, phase acceptance checklist, latest validation, short foundation baseline, decisions that gate *this* work, active risks, blockers, next work.

**What does not:** cumulative issue inventories (use `ROADMAP.md`), full ADR tables (use `DECISIONS.md`), long history (use `docs/status-log.md`).

---

## Current objective

Phase 3 is **verified** (2026-11-27). Phase 4 starts. It depends on Phase 3 and on P11-01 to P11-04; P11-01 to P11-03 are verified, and P11-04 was waiting for Phase 3 (waiver in [`DECISIONS.md`](DECISIONS.md)). The project-level refactor R1 runs first: R1-01 removed the AWS path, so the core is open source only ([ADR-0029](docs/decisions/0029-open-source-only-platform.md)) and Phase 7 is withdrawn. R1-02 moved Site v1 (Phase 9) before Phase 4 and gave every later phase a site update issue. R1-03 renamed the PostgreSQL source to olist-synth-cdc (`olist_synth_cdc`, schema `olist_synth`, Gold `olist_synth_operations`); Silver and Gold counts match the pre-rename run and the old objects are deleted. Refactor R1 is complete. Site v1 (Phase 9) is in progress: P9-01 verified the `site/` scaffold and its five routes, P9-02 filled Home and Architecture, P9-03 filled Pipeline and Operations, P9-04 filled Reference and linked the GitHub Pages URL from the README, and P9-05 delivered the GitHub Pages workflow (`pages.yml`); it is verified once the first deploy from `main` serves the site and About → Website is set.

## Next work

1. **Phase 9 (Site v1)** — P9-05 post-merge check (first deploy and About → Website), then P9-06 and P9-07. Acceptance: `ROADMAP.md` → Phase 9.
2. **P11-04** — Spark Structured Streaming on Kafka: compatibility proof. Acceptance: `ROADMAP.md` → P11-04.
3. **P4-01** — PostgreSQL logical replication prerequisites. Acceptance: `ROADMAP.md` → P4-01.

---

## Phase 4 progress

| ID | Title | Status | Evidence |
| --- | --- | --- | --- |
| P4-01 | PostgreSQL logical replication prerequisites | Not started | — |
| P4-02 | Debezium on the shared Kafka broker | Not started | — |
| P4-03 | CDC path for insert / update / delete | Not started | — |
| P4-04 | Spark Structured Streaming consumer | Not started | — |
| P4-05 | Semantics documentation (batch vs stream) | Not started | — |
| P4-06 | Streaming resilience scenarios | Not started | — |
| P4-07 | Batch path regression check | Not started | — |
| P4-08 | SCD Type 2 history | Not started | — |
| P4-09 | Live run, result, and consistency check | Not started | — |
| P4-10 | Phase 4 documentation and status | Not started | — |

Acceptance detail: `ROADMAP.md` → Phase 4.

---

## Latest validation

| Validation | Detail |
| --- | --- |
| Date | 2026-11-27 |
| Environment | WSL2, repository checks only (P3-15 is documentation) |
| Checks | `make lint` |
| Result | PASS. The phase result it cites is the P3-14 live run: [`docs/architecture/phase-3-live-run.md`](docs/architecture/phase-3-live-run.md) |

---

## Phase 3 baseline

Phase 3 is **verified**. Index: [`docs/architecture/phase-3-orchestration.md`](docs/architecture/phase-3-orchestration.md). Live result: [`docs/architecture/phase-3-live-run.md`](docs/architecture/phase-3-live-run.md) (2026-11-26 to 2026-11-27).

Airflow 3.4.0 (LocalExecutor) runs the daily PostgreSQL extract at 00:00 UTC (`[D-1, D)`, 15 min settle lag, catchup) and the weekly re-extract on Monday (`[W-8, W-1)`). Each successful extract records its window in the coverage ledger, and a gap report lists missed windows. Every ingestion DAG emits a raw Asset that schedules `transform_lakehouse`, which promotes only new raw objects. Failed Bronze or Silver checks block the next layer. Invalid records are quarantined or flagged per contract and counted. Each table replace is one Iceberg commit. Money is exact decimal. In the P3-14 run every landing matched its source window, reruns left Silver unchanged, 152 reads during refreshes all succeeded, and one missed midnight gave one catch-up run. `make live-*` targets and the DAGs must not run at the same time.

Closed Phase 3 narrative: [`ROADMAP.md`](ROADMAP.md) (Phase 3), [`docs/status-log.md`](docs/status-log.md).

---

## Phase 2 baseline

Phase 2 is **verified**. Index: [`docs/architecture/phase-2-lakehouse.md`](docs/architecture/phase-2-lakehouse.md). Live result: [`docs/architecture/phase-2-live-run.md`](docs/architecture/phase-2-live-run.md) (2026-11-11).

Spark writes Bronze and Silver through the Iceberg JDBC catalog. Trino reads them. dbt-trino builds Gold. That run: catalog probe count 2, Bronze 18/18, Silver 20/20 `ok`, `dbt build` PASS=287, reconciliation PASS=56. Headline anchors match the 2026-11-05 / 2026-11-06 baseline. At Phase 2 close Bronze and Silver rebuilt the whole table (`FULL_OVERWRITE`); P3-13 made the refresh incremental. Gold replaces each table. There is no Iceberg partition spec and no target file size. `INSERT OVERWRITE` leaves previous Parquet files (P5-07).

Closed Phase 2 narrative: [`ROADMAP.md`](ROADMAP.md) (Phase 2), [`docs/status-log.md`](docs/status-log.md).

---

## Phase 1 baseline

Phase 1 is **verified** (P1-09 withdrawn). Index: [`docs/architecture/phase-1-ingestion.md`](docs/architecture/phase-1-ingestion.md). Live results already logged: DummyJSON 194 products, IBGE 5_571 municipalities, and the P1-10 MinIO matrix (40 passed).

Five sources land raw objects: Olist e-commerce and marketing funnel, DummyJSON products, PostgreSQL windowed extract, demo storefront clickstream, and IBGE `municipios`. The same execution identity overwrites an object. A new `execution_id` keeps the previous one. Keys and user metadata carry `source_id`, `execution_id`, and `_ingestion_ts`. Invalid rows stay in the raw object; findings go to `quarantine/…/{object_name}.jsonl`.

Daily and weekly batch share `extract_postgres_oltp` (one-day and seven-day UTC-midnight windows). Airflow schedules are Phase 3. Streaming CDC is Phase 4 and does not replace batch.

`order_reviews` contract v2 uses primary key `(review_id, order_id)`. The P1-10 matrix recorded 1_603 duplicate findings under contract v1 (`review_id` alone). A shorter DummyJSON rerun on the same `execution_id` does not delete later page keys it does not rewrite.

---

## Foundation baseline (Phase 0)

Phase 0 is **verified**. Re-check when touching toolchain or Compose:

```bash
uv run ruff check src tests
uv run pytest tests/unit
docker compose up -d && docker compose exec postgres pg_isready -U odp -d odp
set -a && source .env.example && set +a && uv run pytest tests/integration/infrastructure/test_minio_smoke.py
```

Closed Phase 0 issues and narrative: [`ROADMAP.md`](ROADMAP.md) (Phase 0), [`docs/status-log.md`](docs/status-log.md).

---

## Decisions (active for this phase)

Full index: [`DECISIONS.md`](DECISIONS.md). List here only gates for current/near work.

| ID | Relevance | State |
| --- | --- | --- |
| D-006 | Airflow 3.4 LocalExecutor on `odp-runtime:airflow3.4.0-spark3.5.8`; task logs in MinIO; Celery and Flower are not a follow-up | `approved` |
| D-007 | Daily and weekly windows. The extract (P1-06), the daily schedule (P3-04), and the weekly re-extract (P3-05) exist | `approved` |
| D-008 | Streaming CDC: logical replication, Debezium, Kafka, Spark Structured Streaming | `proposed` |
| D-014 | CDC does not replace daily or weekly batch | `proposed` |
| D-028 | Kafka 4.3.2 broker shared by telemetry and the Phase 4 CDC path | `approved` |
| D-029 | Open source only; no cloud deployment path; FinOps is the local resource footprint (R1-01) | `approved` |
| D-005 | DAGs call Spark Bronze/Silver and dbt-trino Gold. They do not reimplement them. Addendum: one runtime image runs Airflow, Spark, and dbt | Approved (addendum 2026-11-13) |
| D-013 | Invalid rows stay visible. Reprocessing quarantined source rows is P3-07 | Approved |

---

## Required update patterns

Three PostgreSQL refresh patterns remain in scope (`ROADMAP.md`). CDC does **not** replace daily/weekly batch.

| Use case | Pattern | Phases | Status |
| --- | --- | --- | --- |
| Daily batch | Windowed incremental extract + Airflow daily schedule | 1 (P1-06), 3 (P3-04) | Verified |
| Weekly batch | Windowed extract + Airflow weekly re-extract `[W-8, W-1)` for late commits | 1 (P1-06), 3 (P3-05) | Verified |
| Streaming | Logical replication → Debezium → Kafka → Spark SS | 4 | Not started |

---

## Program roadmap

Phase scope, dependencies, and status: [`ROADMAP.md`](ROADMAP.md) → Phase overview.

| Phase | Scope | Status |
| --- | --- | --- |
| 0 | Repository, toolchain, PostgreSQL 16, MinIO, CI | Verified |
| 1 | Multi-source ingestion | Verified (P1-09 withdrawn) |
| 2 | Lakehouse PoC, medallion, dbt | Verified |
| 3 | Airflow daily/weekly, backfills, quality | Verified |
| 4 | Streaming CDC | Not started |
| 5 | Observability, lineage, resource footprint | Not started |
| 6 | CI/CD expansion, resilience, security | Not started |
| 7 | Withdrawn: AWS path (R1-01, ADR-0029) | Withdrawn |
| 8 | JupyterLab exploration and Superset charts over Gold (read-only via Trino) | Not started (after Phase 2; parallel with 3–6) |
| 9 | Platform report site v1 on GitHub Pages from Phase 0–3 material | In progress: P9-01 to P9-04 verified (scaffold, all five pages); P9-05 delivered (Pages workflow; verified after the first deploy); runs before Phase 4 |
| 10 | Table contracts and quality observability | Not started (position not fixed; needs Phase 3) |
| 11 | BioGuardia Atlantic sources at scale: daily export, telemetry stream, PII, skew, SCD2, reconciliation | In progress (waiver): P11-01 to P11-03 verified; drops 2026-11-26 to 2026-12-31 in MinIO; stream running. Processing (P11-04 on) waits for Phase 3 |

---

## Risks and limitations

- `docker compose --profile orchestration stop` with no service names also stops PostgreSQL and MinIO. Stop the three Airflow services by name. The `spark` and Trino services are on the `lakehouse` profile and stay up.
- Spark `local[*]` runs inside the Airflow scheduler container and shares its CPU and memory. A full `transform_lakehouse` run took 10 min 12 s; the 30-minute timeout applies per task.
- The `lakehouse_gold` pool is held per task, not per run. A waiting `dbt_reconcile` can take the slot between two transform writes and read a half-rebuilt lakehouse (no table changes while it reads). On 2026-11-20 that stretched one transform run to 20 min 22 s.
- DAGs already in the `airflow` metadata database keep their paused state; `is_paused_upon_creation=True` applies only to new DAGs. A manual daily trigger gets the last complete UTC day, whatever `-l` says; `airflow dags trigger` without `-l` has no data interval and the extract fails.
- Late commits (a row with `updated_at` before midnight committed after the 00:00Z run) miss the daily window; the weekly re-extract (P3-05) picks them up. A row committed after the weekly run that covers its `updated_at` is still missed: P3-06 backfill and CDC (Phase 4). The first scheduled weekly run is 2026-11-23; only manual weekly runs have been exercised. Three synthetic `p3-05-late-*` sellers stay in the lake; the 2026-11-21 integration-test run removed them from local PostgreSQL (review T8).
- Since P3-13 a refresh reads only new raw objects, but it still lists every raw key, and a pending `incremental_window` or `append_only` Silver dataset re-reads its whole Bronze table. Since P3-08 the Bronze `MERGE` scans only its `dt` partitions, but each daily partition adds small files until compaction (P5-07), and a `quality_counts` write scans one more small file per run of its dataset (P5-07). Raw executions without a `_COMPLETE` marker never enter Bronze.
- dbt unit tests need Trino and the built parents, so CI does not run them; `make dbt-unit` runs them locally.
- A contract type change takes effect on `make live-rebuild`. Until it runs, an incremental refresh retypes only the Silver datasets it rebuilds, and the Gold `column_types` tests fail `dbt_build`. Olist and DummyJSON quarantine findings stored before P3-18 come from the earlier contract versions until those sources are ingested again.
- `stage-quarantine` still lists the whole `quarantine/` prefix. The 1_603 `order_reviews` findings in Gold come from the legacy sidecar of `execution_id=run-001` (contract v1 key, no `_COMPLETE` marker), which never entered Bronze; their `silver_action` says `flagged` although Silver holds no such row. Staging only the sidecars of promoted objects is a follow-up (P3-07).
- The P3-07 live evidence left synthetic executions in raw and Bronze: `demo_storefront` `p307-planted` (4 events, now valid) and `dummyjson_products` `p307-truncated` and `p307-restored`. Silver clickstream holds the 4 `p307-*` events.
- `--allow-volume-drop` is a `make live-refresh` flag; the Airflow `transform_lakehouse` DAG does not take it, so a real shrink is accepted from the host. The guard failure inside Airflow was not exercised; the task runs the same `medallion_build silver` step.
- Silver and Gold are visible to readers before their checks run, and Gold models are replaced one at a time (review R6, P6-09).
- Integration tests write to `ODP_TEST_RAW_BUCKET` (default `odp-test-raw`) and seed a scratch PostgreSQL database. Before that fix, the PostgreSQL integration test truncated the local source tables (review T8); a run on 2026-11-21 reset local PostgreSQL to the fixture.
- The windowed batch extract does not see hard deletes. A deleted row stays in Silver and Gold on the batch path; the CDC path (Phase 4) handles deletes.
- Spark JDBC and S3 secrets are a mode-600 properties file (`--properties-file`), not `--conf` arguments. The driver redacts `password`, `secret`, and `access.key` in the configuration log.
- Olist e-commerce + marketing funnel are **CC BY-NC-SA 4.0**: do not vendor; fetch locally (ADR-0018). MIT covers software/docs only.
- Exactly-once end-to-end will not be claimed without a written proof.
- Destructive volume ops need explicit approval.
- Keep daily batch, weekly batch, and streaming CDC distinguishable in design and demos.
- Community MinIO remains AGPLv3; pin documented in ADR-0002.
- Agent shells may lack Docker socket access; host terminal Compose path is source of truth for local ops.

## Blocked

None.

## Platform report site

Astro Starlight scaffold with the brand theme (violet and pink, Urbanist) and a header link to the portfolio, under `site/` (verified facts only). Site v1 (Phase 9) fills the pages from verified Phase 0–3 material and hosts them on GitHub Pages; P9-02 filled Home (purpose, a status table that matches the `ROADMAP.md` phase overview, the 3D map labeled target architecture) and Architecture (stack, medallion, contracts, Airflow orchestration, batch vs stream, with diagrams from `docs/assets/diagrams/`); P9-03 filled Pipeline and Operations; P9-04 filled Reference. From Phase 4 on, each phase has a site update issue before its close pair. Preview: `cd site && npm run dev` — see `site/README.md`. The site figure table in `ROADMAP.md` names each figure and the issue that publishes it (P8-08, P5-10).

| Page | Status |
| --- | --- |
| Home / Architecture | Verified Phase 0–3 content (P9-02) |
| Pipeline / Operations | Verified Phase 0–3 content (P9-03) |
| Reference | Links to the repository, setup, status, roadmap, decisions, and phase docs (P9-04) |

---

## Log

Chronological entries live in [`docs/status-log.md`](docs/status-log.md). Do not copy them here.
