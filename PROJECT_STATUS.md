# Project Status

Operational ledger for the **current phase**. Issue backlog and acceptance text live in [`ROADMAP.md`](ROADMAP.md). Full decision index: [`DECISIONS.md`](DECISIONS.md). Chronological archive: [`docs/status-log.md`](docs/status-log.md).

| | |
| --- | --- |
| Repository | [FilipeOliveira94/open-data-platform](https://github.com/FilipeOliveira94/open-data-platform) |
| Started | 2026-10-14 |
| Last updated | 2026-11-11 (Phase 2 verified; P3-01 is next) |
| Current phase | Phase 3 — Orchestration and data quality |
| Phase status | Not started |

**Status values:** not started · in progress · delivered · verified · blocked · withdrawn

**Verified** means acceptance checks were run and evidence is recorded (here and/or in `docs/status-log.md`). Do not treat planned architecture as complete.

**What belongs here:** current objective, current-phase issue progress, phase acceptance checklist, latest validation, short foundation baseline, decisions that gate *this* work, active risks, blockers, next work.

**What does not:** cumulative issue inventories (use `ROADMAP.md`), full ADR tables (use `DECISIONS.md`), long history (use `docs/status-log.md`).

---

## Current objective

**P3-01** — Airflow in local Compose. Not started.

Phase 2 is verified. Its index is [`docs/architecture/phase-2-lakehouse.md`](docs/architecture/phase-2-lakehouse.md). The live result it cites is [`docs/architecture/phase-2-live-run.md`](docs/architecture/phase-2-live-run.md).

## Next work

1. **P3-01** — Add Airflow to local Compose. Pin the image. Document UI and CLI access. Keep business logic out of DAG bodies. D-006 stays `proposed` until that evidence exists.

---

## Phase 3 progress

| ID | Title | Status | Evidence |
| --- | --- | --- | --- |
| P3-01 | Airflow in local Compose | Not started | — |
| P3-02 | Ingestion DAGs (thin wrappers) | Not started | — |
| P3-03 | Transform / dbt DAGs | Not started | — |
| P3-04 | Daily batch schedule (PostgreSQL) | Not started | — |
| P3-05 | Weekly batch schedule (PostgreSQL) | Not started | — |
| P3-06 | Idempotent backfill support | Not started | — |
| P3-07 | Quality checks and quarantine | Not started | — |
| P3-08 | Execution metadata and failure logging | Not started | — |
| P3-09 | Orchestration success/failure tests | Not started | — |
| P3-10 | Live run, result, and consistency check | Not started | — |
| P3-11 | Phase 3 documentation and status | Not started | — |

Acceptance detail: `ROADMAP.md` → Phase 3.

### Phase 3 acceptance

- [ ] DAGs have clear dependencies and verifiable failure behavior.
- [ ] A **daily** scheduled batch path exists for PostgreSQL incremental extract (and downstream transforms as designed).
- [ ] A **weekly** scheduled batch path exists (distinct schedule and documented purpose/grain vs daily).
- [ ] Backfills do not corrupt data or create undue duplication.
- [ ] Invalid records are identified and handled per documented rules.
- [ ] Success and failure scenarios have tests or reproducible evidence.
- [ ] A live run result is recorded and compared to the source windows (P3-10).
- [ ] Documentation and `PROJECT_STATUS.md` cite that result (P3-11).

---

## Latest validation

Phase 3 has no stack run. The check below closes P2-10. It does not start Phase 3.

| | |
| --- | --- |
| Date | 2026-11-11 |
| Environment | Documentation only. Compose was not started |
| Checks | `rg` for the stale phrases named in the P2-10 plan, across `README.md`, `docs/architecture`, `PROJECT_STATUS.md`, and `ROADMAP.md`. A second `rg` for `phase-2-live-run.md` in the README, this ledger, and `docs/architecture/phase-2-lakehouse.md` |
| Result | Hits are the verified issue id P2-10 in `ROADMAP.md` and in this ledger. No line in those paths still uses the outdated wording the plan named. `phase-2-live-run.md` is cited from the README, this ledger, and `docs/architecture/phase-2-lakehouse.md`. Detail: [`docs/status-log.md`](docs/status-log.md) (2026-11-11, P2-10) |

---

## Phase 2 baseline

Phase 2 is **verified**. Index: [`docs/architecture/phase-2-lakehouse.md`](docs/architecture/phase-2-lakehouse.md). Live result: [`docs/architecture/phase-2-live-run.md`](docs/architecture/phase-2-live-run.md) (2026-11-11).

Spark writes Bronze and Silver through the Iceberg JDBC catalog. Trino reads them. dbt-trino builds Gold. That run: catalog probe count 2, Bronze 18/18, Silver 20/20 `ok`, `dbt build` PASS=287, reconciliation PASS=56. Headline anchors match the 2026-11-05 / 2026-11-06 baseline. Bronze and Silver rebuild the whole table (`FULL_OVERWRITE`). Gold replaces each table. There is no Iceberg partition spec and no target file size. `INSERT OVERWRITE` leaves previous Parquet files (P5-07).

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
| D-006 | Airflow locally; image pin and DAG boundary are P3-01 | `proposed` |
| D-007 | Daily and weekly windows. The extract exists (P1-06). Schedules do not | `proposed` |
| D-014 | CDC does not replace daily or weekly batch | `proposed` |
| D-005 | DAGs call Spark Bronze/Silver and dbt-trino Gold. They do not reimplement them | Approved |
| D-013 | Invalid rows stay visible. Reprocessing quarantined source rows is P3-07 | Approved |

---

## Required update patterns

Three PostgreSQL refresh patterns remain in scope (`ROADMAP.md`). CDC does **not** replace daily/weekly batch.

| Use case | Pattern | Phases | Status |
| --- | --- | --- | --- |
| Daily batch | Windowed incremental extract + Airflow daily schedule | 1 (P1-06), 3 (P3-04) | Extract verified; schedule not started |
| Weekly batch | Windowed extract + Airflow weekly schedule | 1 (P1-06), 3 (P3-05) | Extract verified; schedule not started |
| Streaming | Logical replication → Debezium → Kafka → Spark SS | 4 | Not started |

---

## Program roadmap

| Phase | Scope | Status |
| --- | --- | --- |
| 0 | Repository, toolchain, PostgreSQL 16, MinIO, CI | Verified |
| 1 | Multi-source ingestion | Verified (P1-09 withdrawn) |
| 2 | Lakehouse PoC, medallion, dbt | Verified |
| 3 | Airflow daily/weekly, backfills, quality | Not started |
| 4 | Streaming CDC | Not started |
| 5 | Observability, lineage, FinOps model | Not started |
| 6 | CI/CD expansion, resilience, security | Not started |
| 7 | AWS path as Terraform IaC | Not started |
| 8 | JupyterLab exploration and Superset charts over Gold (read-only via Trino) | Not started (after Phase 2; parallel with 3–7) |
| 9 | Platform report site (hosting + polish) | Not started (early `site/` scaffold exists) |

---

## Risks and limitations

- D-006 and D-007 are still `proposed`. Adding Compose services in P3-01 does not approve them by itself.
- A Phase 3 schedule would call the current full rebuild of Bronze, Silver, and Gold. That rebuild is not a windowed lakehouse commit. The window that exists today is the PostgreSQL extract.
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

Recent entries only. Older history: [`docs/status-log.md`](docs/status-log.md).

### 2026-11-11 — README and partition candidate

The README opens with the path from a published number back to the source. Core features are one row each, with state, phase, and core stack. Iceberg partitions for Silver and Gold are a candidate in [`ROADMAP.md`](ROADMAP.md), not an issue. No stack command was run.

### 2026-11-11 — P2-10 verified

Phase 2 documentation is in [`docs/architecture/phase-2-lakehouse.md`](docs/architecture/phase-2-lakehouse.md). It cites the 2026-11-11 live run. No Compose command was run for this issue. Next issue is P3-01.
