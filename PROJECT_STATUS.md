# Project Status

Operational ledger for the **current phase**. Issue backlog and acceptance text live in [`ROADMAP.md`](ROADMAP.md). Full decision index: [`DECISIONS.md`](DECISIONS.md). Chronological archive: [`docs/status-log.md`](docs/status-log.md).

| Project | Current |
| --- | --- |
| Repository | [FilipeOliveira94/open-data-platform](https://github.com/FilipeOliveira94/open-data-platform) |
| Started | 2026-10-14 |
| Last updated | 2026-11-22 (P3-16 verified) |
| Current phase | Phase 3 — Orchestration and data quality |
| Phase status | In progress |

**Status values:** not started · in progress · delivered · verified · blocked · withdrawn

**Verified** means acceptance checks were run and evidence is recorded (here and/or in `docs/status-log.md`). Do not treat planned architecture as complete.

**What belongs here:** current objective, current-phase issue progress, phase acceptance checklist, latest validation, short foundation baseline, decisions that gate *this* work, active risks, blockers, next work.

**What does not:** cumulative issue inventories (use `ROADMAP.md`), full ADR tables (use `DECISIONS.md`), long history (use `docs/status-log.md`).

---

## Current objective

**P3-17** — Gold metric grain corrections. Not started.

P3-16 is verified. Each Bronze and Silver check is recorded per dataset in `lakehouse_state.layer_checks`. An open check runs again on the next refresh even with no new raw objects, and Silver and Gold do not build on it. The gap report lists windows missed after the last recorded one. The OLTP tables keep `updated_at` with a trigger, the daily extract waits a 15-minute settle lag, one window is read from one snapshot, and a retry keeps its `dt=` ([quality gate](docs/architecture/airflow-local.md#quality-gate-p3-16), [verification](docs/architecture/airflow-local.md#verification-2026-11-22-p3-16)).

A review of every verified issue on 2026-11-21 ([review](docs/reviews/2026-11-21-verified-issues-review.md)) found 19 pipeline and model findings and 8 test findings. The two highest: a failed Bronze or Silver check is bypassed by the next refresh that has no new raw objects, so Gold can be built from data that failed its checks (R1); and `mart_review_drivers` counts a review once per order item (R2). The Olist integration test had failed since 2026-10-26 without notice, because integration tests are not in CI; it and the other unsafe integration tests were fixed with the review. New issues P3-16 to P3-18 run before the close pair; P6-09 adds write-audit-publish. P3-16 closed R1 and the trailing-gap part of the missed-window acceptance.

Earlier Phase 3 results (P3-01 to P3-06, P3-11 to P3-13): the progress table below and [`docs/status-log.md`](docs/status-log.md).

## Next work

1. **P3-17** — Gold metric grain corrections. Acceptance: `ROADMAP.md` → P3-17.
2. **P3-18** — Exact decimal type for money fields.
3. **P3-07** — Quality checks and quarantine (now also cast-failure counts and a volume guard).

---

## Phase 3 progress

| ID | Title | Status | Evidence |
| --- | --- | --- | --- |
| P3-01 | Airflow in local Compose | Verified | [`docs/architecture/airflow-local.md`](docs/architecture/airflow-local.md) |
| P3-02 | Ingestion DAGs (thin wrappers) | Verified | [`docs/architecture/airflow-local.md`](docs/architecture/airflow-local.md) |
| P3-03 | Transform / dbt DAGs | Verified | [`docs/architecture/airflow-local.md`](docs/architecture/airflow-local.md) |
| P3-04 | Daily batch schedule (PostgreSQL) | Verified | [`docs/architecture/airflow-local.md`](docs/architecture/airflow-local.md) |
| P3-05 | Weekly batch schedule (PostgreSQL) | Verified | [`docs/architecture/airflow-local.md`](docs/architecture/airflow-local.md#weekly-postgresql-re-extract-p3-05) |
| P3-06 | Window coverage, gap report, and backfill | Verified | [`docs/architecture/airflow-local.md`](docs/architecture/airflow-local.md#window-coverage-gap-report-and-backfill-p3-06) |
| P3-07 | Quality checks and quarantine | Not started | — |
| P3-08 | Execution metadata and failure logging | Not started | — |
| P3-09 | Orchestration success/failure tests | Not started | — |
| P3-10 | Orchestration runtime hardening | Not started | — |
| P3-11 | Atomic table replace for readers | Verified | [`docs/architecture/phase-2-lakehouse.md`](docs/architecture/phase-2-lakehouse.md#atomic-table-replace-p3-11) |
| P3-12 | Update-pattern semantics and source refresh policy | Verified | [`docs/architecture/data-contracts.md`](docs/architecture/data-contracts.md#update-patterns-p3-12) |
| P3-13 | Incremental lakehouse refresh | Verified | [`docs/architecture/bronze-layer.md`](docs/architecture/bronze-layer.md#result-2026-11-20-incremental-refresh-p3-13) |
| P3-16 | Quality gate state and extract-window corrections | Verified | [`docs/architecture/airflow-local.md`](docs/architecture/airflow-local.md#quality-gate-p3-16) |
| P3-17 | Gold metric grain corrections | Not started | — |
| P3-18 | Exact decimal type for money fields | Not started | — |
| P3-14 | Live run, result, and consistency check | Not started | — |
| P3-15 | Phase 3 documentation and status | Not started | — |

Acceptance detail: `ROADMAP.md` → Phase 3.

### Phase 3 acceptance

- [ ] DAGs have clear dependencies and verifiable failure behavior.
- [ ] A **daily** scheduled batch path exists for PostgreSQL incremental extract (and downstream transforms as designed).
- [x] A **weekly** scheduled batch path exists (distinct schedule and documented purpose/grain vs daily) (P3-05).
- [ ] Backfills do not corrupt data or create undue duplication.
- [x] A missed window is caught up or reported as a gap; covered windows are queryable (P3-04, P3-06; trailing gaps since P3-16).
- [x] A failed Bronze or Silver check blocks the next layer until it passes, including on a later refresh with no new raw objects (P3-16).
- [ ] Gold metrics keep their documented grain: no fan-out, rate denominators count only eligible rows, and money is exact decimal (P3-17, P3-18).
- [x] Readers never see a missing, empty, or half-written table, including after a failed write (P3-11).
- [x] Each contract `update_pattern` has defined and enforced Bronze and Silver behavior, and every source has a documented refresh policy (P3-12).
- [x] A lakehouse refresh reads new raw objects and, for snapshot sources, the latest complete execution only. Its work does not grow with total raw history (P3-13). Raw object reads do not; the raw key listing, Silver rebuilds of `incremental_window`/`append_only` datasets (ADR-0024), and the Bronze `MERGE` over the whole unpartitioned table (review R5, P3-08) still do.
- [ ] Invalid records are identified and handled per documented rules.
- [ ] Success and failure scenarios have tests or reproducible evidence.
- [ ] A live run result is recorded and compared to the source windows (P3-14).
- [ ] Documentation and `PROJECT_STATUS.md` cite that result (P3-15).

---

## Latest validation

| Validation | Detail |
| --- | --- |
| Date | 2026-11-22 |
| Environment | Local Compose. PostgreSQL, MinIO, Trino, and the runtime image `odp-runtime:airflow3.4.0-spark3.5.8` with the branch source mounted |
| Checks | `scripts/p3_16_gate_check.sh`: extract on the old schema, `apply-schema`, a manual window, then refresh r1 with a planted Bronze count failure, r2 with no new raw objects, r3 after the fix, and r4 with nothing open. `uv run pytest tests/integration`, `make test`, `make lint` |
| Result | PASS. Old schema: extract exit 1 (missing trigger). r1 `bronze_checks` failed on `orders` only. r2 re-ran the open check (failed again) and `silver` failed with `silver blocked`, writing no table. r3: check passed, 4 Silver tables, 4 Silver checks passed, dbt PASS=287. r4 skipped. Integration: 9 passed. Unit: 459 passed |

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
| D-014 | CDC does not replace daily or weekly batch | `proposed` |
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

--- | --- | --- |
| 0 | Repository, toolchain, PostgreSQL 16, MinIO, CI | Verified |
| 1 | Multi-source ingestion | Verified (P1-09 withdrawn) |
| 2 | Lakehouse PoC, medallion, dbt | Verified |
| 3 | Airflow daily/weekly, backfills, quality | In progress (P3-01–P3-06, P3-11–P3-13 verified; P3-16–P3-18 added 2026-11-21) |
| 4 | Streaming CDC | Not started |
| 5 | Observability, lineage, FinOps model | Not started |
| 6 | CI/CD expansion, resilience, security | Not started |
| 7 | AWS path as Terraform IaC | Not started |
| 8 | JupyterLab exploration and Superset charts over Gold (read-only via Trino) | Not started (after Phase 2; parallel with 3–7) |
| 9 | Platform report site (hosting + polish) | Not started (early `site/` scaffold exists) |
| 10 | Table contracts and quality observability | Not started (position not fixed; needs Phase 3) |

---

## Risks and limitations

- `docker compose --profile orchestration stop` with no service names also stops PostgreSQL and MinIO. Stop the three Airflow services by name. The `spark` and Trino services are on the `lakehouse` profile and stay up.
- Spark `local[*]` runs inside the Airflow scheduler container and shares its CPU and memory. A full `transform_lakehouse` run took 10 min 12 s; the 30-minute timeout applies per task.
- The `lakehouse_gold` pool is held per task, not per run. A waiting `dbt_reconcile` can take the slot between two transform writes and read a half-rebuilt lakehouse (no table changes while it reads). On 2026-11-20 that stretched one transform run to 20 min 22 s.
- DAGs already in the `airflow` metadata database keep their paused state; `is_paused_upon_creation=True` applies only to new DAGs. A manual daily trigger gets the last complete UTC day, whatever `-l` says; `airflow dags trigger` without `-l` has no data interval and the extract fails.
- Late commits (a row with `updated_at` before midnight committed after the 00:00Z run) miss the daily window; the weekly re-extract (P3-05) picks them up. A row committed after the weekly run that covers its `updated_at` is still missed: P3-06 backfill and CDC (Phase 4). The first scheduled weekly run is 2026-11-23; only manual weekly runs have been exercised. Three synthetic `p3-05-late-*` sellers stay in the lake; the 2026-11-21 integration-test run removed them from local PostgreSQL (review T8).
- Since P3-13 a refresh reads only new raw objects, but it still lists every raw key, and a pending `incremental_window` or `append_only` Silver dataset re-reads its whole Bronze table. The Bronze `MERGE` joins against the whole unpartitioned table, so its scan grows with history (review R5, P3-08). Raw executions without a `_COMPLETE` marker never enter Bronze.
- Until P3-17, `mart_review_drivers` counts a review once per order item and `on_time_handoff_rate` counts cancelled and unshipped orders as late (review R2, R3). Until P3-18, money columns are `DOUBLE`.
- Silver and Gold are visible to readers before their checks run, and Gold models are replaced one at a time (review R6, P6-09).
- Integration tests write to `ODP_TEST_RAW_BUCKET` (default `odp-test-raw`) and seed a scratch PostgreSQL database. Before that fix, the PostgreSQL integration test truncated the local `oltp` tables (review T8); a run on 2026-11-21 reset local PostgreSQL to the fixture.
- The windowed batch extract does not see hard deletes. A deleted row stays in Silver and Gold on the batch path; the CDC path (Phase 4) handles deletes.
- Spark JDBC and S3 secrets are a mode-600 properties file (`--properties-file`), not `--conf` arguments. The driver redacts `password`, `secret`, and `access.key` in the configuration log.
- Olist e-commerce + marketing funnel are **CC BY-NC-SA 4.0**: do not vendor; fetch locally (ADR-0018). MIT covers software/docs only.
- Exactly-once end-to-end will not be claimed without a written proof.
- `terraform apply` / `destroy` and destructive volume ops need explicit approval.
- Keep daily batch, weekly batch, and streaming CDC distinguishable in design and demos.
- Community MinIO remains AGPLv3; pin documented in ADR-0002.
- Agent shells may lack Docker socket access; host terminal Compose path is source of truth for local ops.

## Blocked

None.

## Platform report site

Early Astro Starlight + Catppuccin scaffold under `site/` (verified facts only). Public hosting and the figure gallery are Phase 9. Preview: `cd site && npm run dev` — see `site/README.md`. JupyterLab and Superset are Phase 8 and are not started. The site figure table in `ROADMAP.md` names the charts Phase 9 will publish.

| Page | Status |
| --- | --- |
| Home / Architecture / Pipeline | Scaffold, including verified Phase 1 raw ingestion |
| Operations / Reference | Scaffold (Phase 0 facts) |

---

## Log

Chronological entries live in [`docs/status-log.md`](docs/status-log.md). Do not copy them here.
