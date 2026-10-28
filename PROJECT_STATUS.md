# Project Status

Operational ledger for the **current phase**. Issue backlog and acceptance text live in [`ROADMAP.md`](ROADMAP.md). Full decision index: [`DECISIONS.md`](DECISIONS.md). Chronological archive: [`docs/status-log.md`](docs/status-log.md).

| | |
| --- | --- |
| Repository | [FilipeOliveira94/open-data-platform](https://github.com/FilipeOliveira94/open-data-platform) |
| Started | 2026-10-14 |
| Last updated | 2026-10-28 (P1-11: Phase 1 verified; unit suite 216 passed) |
| Current phase | Phase 2 — Lakehouse and analytics |
| Phase status | Not started |

**Status values:** not started · in progress · delivered · verified · blocked · withdrawn

**Verified** means acceptance checks were run and evidence is recorded (here and/or in `docs/status-log.md`). Do not treat planned architecture as complete.

**What belongs here:** current objective, current-phase issue progress, phase acceptance checklist, latest validation, short foundation baseline, decisions that gate *this* work, active risks, blockers, next work.

**What does not:** cumulative issue inventories (use `ROADMAP.md`), full ADR tables (use `DECISIONS.md`), long history (use `docs/status-log.md`).

---

## Current objective

**P2-01** — Minimal Spark + Iceberg + catalog + query proof of concept.

Phase 2 out of scope until that proof succeeds: Airflow, CDC/streaming, AWS lakehouse modules. Do not scale transforms before P2-01 is verified.

## Next work

1. **P2-01** — Minimal Spark + Iceberg + catalog + query proof of concept.

---

## Phase 2 progress

| ID | Title | Status | Evidence |
| --- | --- | --- | --- |
| P2-01 | Minimal Spark + Iceberg + catalog + query PoC | Not started | — |
| P2-02 | Local catalog and query-engine choice | Not started | — |
| P2-03 | Bronze layer from raw ingestion | Not started | — |
| P2-04 | Silver normalization and deduplication | Not started | — |
| P2-05 | Analytical model design (grain, keys, SCD) | Not started | — |
| P2-06 | dbt project and initial Gold models | Not started | — |
| P2-07 | Data quality and reconciliation tests | Not started | — |
| P2-08 | Phase 2 docs (versions, partitions, limitations) | Not started | — |

Acceptance detail: `ROADMAP.md` → Phase 2.

### Phase 2 acceptance

- [ ] A table can be created, written and queried in the chosen local stack.
- [ ] Queries and transforms are reproducible via documented commands.
- [ ] Quality and reconciliation rules have tests.
- [ ] Documentation lists component versions and known limitations.

---

## Latest validation

| | |
| --- | --- |
| Date | 2026-10-28 |
| Environment | WSL2 / Ubuntu. No Compose services, no public API, no Kaggle |
| Checks | P1-11 documentation close. Ruff, unit tests, and the site production build. No Phase 2 code |
| Result | `uv run ruff check src tests` PASS. `uv run ruff format --check src tests` PASS. `uv run pytest tests/unit` → 216 passed in 10.56s. `cd site && npm ci && npm run build` completed: 6 pages. Node v22.14.0 is below the engine range some transitive packages declare; the build still finished. |

---

## Phase 1 baseline

Phase 1 is **verified** (P1-09 withdrawn). Index: [`docs/architecture/phase-1-ingestion.md`](docs/architecture/phase-1-ingestion.md).

Five sources land raw objects: Olist e-commerce and marketing funnel, DummyJSON products, PostgreSQL windowed extract, demo storefront clickstream, and IBGE `municipios`. The same execution identity overwrites an object. A new `execution_id` keeps the previous one. Keys and user metadata carry `source_id`, `execution_id`, and `_ingestion_ts`. Invalid rows stay in the raw object; findings go to `quarantine/…/{object_name}.jsonl`.

Daily and weekly batch share `extract_postgres_oltp` (one-day and seven-day UTC-midnight windows). Airflow schedules are Phase 3. Streaming CDC is Phase 4 and does not replace batch.

The staged Olist snapshot still quarantines 1_603 duplicate `review_id` rows in `order_reviews`. A shorter DummyJSON rerun on the same `execution_id` does not delete later page keys it does not rewrite.

Closed Phase 1 narrative: [`ROADMAP.md`](ROADMAP.md) (Phase 1), [`docs/status-log.md`](docs/status-log.md).

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
| D-003 | Table format for the P2-01 proof | Proposed |
| D-004 | Catalog and SQL engine; chosen after P2-01 | Proposed |
| D-005 | Spark and dbt | Proposed |
| D-012 | Bronze / Silver / Gold; Gold domains already named | Proposed |
| D-020 | Raw layout that Bronze will read (P2-03) | Approved |
| D-013 | Contracts; no silent drops | Approved |
| D-001 / D-002 | Compose + MinIO baseline | Approved |

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
| 2 | Lakehouse PoC, medallion, dbt | Not started |
| 3 | Airflow daily/weekly, backfills, quality | Not started |
| 4 | Streaming CDC | Not started |
| 5 | Observability, lineage, FinOps model | Not started |
| 6 | CI/CD expansion, resilience, security | Not started |
| 7 | AWS path as Terraform IaC | Not started |
| 8 | Platform report site (hosting + polish) | Not started (early `site/` scaffold exists) |

---

## Risks and limitations

- Spark / Iceberg / catalog / query-engine compatibility is unproven until P2-01.
- Olist e-commerce + marketing funnel are **CC BY-NC-SA 4.0**: do not vendor; fetch locally (ADR-0018). MIT covers software/docs only.
- Published Olist ZIP prefixes, closed-deal booleans, and catalog sizes match the contracts. The staged snapshot still quarantines 1_603 `order_reviews` rows with a repeated `review_id`.
- Exactly-once end-to-end will not be claimed without a written proof.
- `terraform apply` / `destroy` and destructive volume ops need explicit approval.
- Keep daily batch, weekly batch, and streaming CDC distinguishable in design and demos.
- Community MinIO remains AGPLv3; pin documented in ADR-0002.
- Agent shells may lack Docker socket access; host terminal Compose path is source of truth for local ops.

## Blocked

None.

## Platform report site

Early Astro Starlight + Catppuccin scaffold under `site/` (verified facts only). Public hosting is Phase 8. Preview: `cd site && npm run dev` — see `site/README.md`.

| Page | Status |
| --- | --- |
| Home / Architecture / Pipeline | Scaffold, including verified Phase 1 raw ingestion |
| Operations / Reference | Scaffold (Phase 0 facts) |

---

## Log

Recent entries only. Older history: [`docs/status-log.md`](docs/status-log.md).

### 2026-10-28 — P1-11 verified; Phase 1 closed

README and [`docs/architecture/phase-1-ingestion.md`](docs/architecture/phase-1-ingestion.md) index the five sources, raw keys, repeatable overwrite, and the three update patterns. Daily and weekly batch stay on the P1-06 window. Streaming stays in Phase 4. `uv run ruff check src tests` PASS. `uv run ruff format --check src tests` PASS. `uv run pytest tests/unit` → 216 passed in 10.56s. `cd site && npm ci && npm run build` completed: 6 pages in 1.85s. No live API, Kaggle, or MinIO. No Phase 2 code.
