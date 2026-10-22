# Project Status

Operational ledger for the **current phase**. Issue backlog and acceptance text live in [`ROADMAP.md`](ROADMAP.md). Full decision index: [`DECISIONS.md`](DECISIONS.md). Chronological archive: [`docs/status-log.md`](docs/status-log.md).

| | |
| --- | --- |
| Repository | [FilipeOliveira94/open-data-platform](https://github.com/FilipeOliveira94/open-data-platform) |
| Started | 2026-10-14 |
| Last updated | 2026-10-22 (P1-04 verified — Olist historical ingestion) |
| Current phase | Phase 1 — Multi-source ingestion |
| Phase status | In progress (P1-01, P1-02, P1-03, P1-04 verified) |

**Status values:** not started · in progress · delivered · verified · blocked

**Verified** means acceptance checks were run and evidence is recorded (here and/or in `docs/status-log.md`). Do not treat planned architecture as complete.

**What belongs here:** current objective, current-phase issue progress, phase acceptance checklist, latest validation, short foundation baseline, decisions that gate *this* work, active risks, blockers, next work.

**What does not:** cumulative issue inventories (use `ROADMAP.md`), full ADR tables (use `DECISIONS.md`), long history (use `docs/status-log.md`).

---

## Current objective

**P1-05** — paginated public API client (DummyJSON).

Phase 1 out of scope: streaming/CDC, Iceberg, Airflow schedules, lakehouse transforms, AWS Terraform apply, Phase 8 hosting polish.

## Next work

1. **P1-05** — paginated public API client (DummyJSON).
2. **P1-06**, **P1-07**, and **P1-08** are unblocked and can proceed in parallel with P1-05.

---

## Phase 1 progress

| ID | Title | Status | Evidence |
| --- | --- | --- | --- |
| P1-01 | Historical dataset license and redistribution check | Verified | `docs/sources/`; ADR-0018 / D-018; no Olist binaries in git (2026-10-17) |
| P1-02 | Raw storage layout and ingestion metadata conventions | Verified | ADR-0020; `docs/architecture/raw-storage-layout.md`; `src/open_data_platform/ingestion/`; unit tests 2026-10-18 |
| P1-03 | Data contracts for planned sources | Verified | `config/contracts/`; `docs/architecture/data-contracts.md`; `src/open_data_platform/contracts/`; unit tests 2026-10-18 |
| P1-04 | Historical e-commerce (+ marketing funnel) ingestion | Verified | `ingestion/olist/`, `ingestion/kaggle_download.py`; unit tests 76 passed; MinIO round-trip 1 passed (2026-10-22). Category translation CSV may start with a UTF-8 BOM |
| P1-05 | Paginated public API client (DummyJSON) | Not started | D-019 approved |
| P1-06 | PostgreSQL incremental batch extract | Not started | — |
| P1-07 | Synthetic event generator | Not started | — |
| P1-08 | IBGE Localidades ingest | Not started | Planned (D-019) |
| P1-09 | BrasilAPI CEP enrichment | Not started | Optional (D-019) |
| P1-10 | Failure and invalid-record behavior | Not started | — |
| P1-11 | Phase 1 documentation and status | Not started | — |

Acceptance detail: `ROADMAP.md` → Phase 1.

### Phase 1 acceptance

- [ ] Ingestions can be repeated without undue duplication or corruption.
- [ ] Unit tests do not require the public API to be online.
- [ ] Raw data is traceable by source and execution.
- [x] Contracts and expected fields are documented.
- [ ] Invalid records and errors have explicit, documented behavior.
- [ ] PostgreSQL extract accepts an explicit time/watermark window suitable for daily and weekly batch runs.

---

## Latest validation

| | |
| --- | --- |
| Date | 2026-10-22 |
| Environment | WSL2 / Ubuntu; local Compose MinIO healthy |
| Checks | `uv run ruff check src tests` PASS; `uv run ruff format --check src tests` PASS; `uv run pytest tests/unit` → 76 passed; `uv run pytest tests/integration/ingestion/test_olist_ingest.py` → 1 passed |
| Result | P1-04 verified on fake transport and synthetic MinIO writes: 11 raw CSV objects and the published `closed_deals.seller_id` join. A live Kaggle run downloaded the e-commerce zip and stopped on the category-file BOM; that check is covered by unit tests. The full live run after the fix was not re-executed. No Olist rows in git |

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

Full index: [`DECISIONS.md`](DECISIONS.md). List here only gates for current/near work or decisions approved in this phase.

| ID | Relevance | State |
| --- | --- | --- |
| D-020 | Raw object-store layout + ingestion metadata (P1-02) | Approved |
| D-018 | Olist datasets: no vendor; fetch-from-source (`docs/sources/`) | Approved (P1-01) |
| D-019 | DummyJSON (P1-05); IBGE (P1-08); CEP optional (P1-09) | Approved |
| D-012 | Gold domains `olist_analysis` / `product_analysis` | Proposed (domain split recorded) |
| D-013 | Data contracts / no silent drops — gates P1-03+ | Proposed (P1-03 evidence landed; not approved) |
| D-015 | Package owns logic; thin DAGs/IaC — gates ingestion layout | Proposed |
| D-001 / D-002 | Compose + MinIO baseline (unchanged) | Approved |

---

## Required update patterns

Three PostgreSQL refresh patterns remain in scope (`ROADMAP.md`). CDC does **not** replace daily/weekly batch.

| Use case | Pattern | Phases | Status |
| --- | --- | --- | --- |
| Daily batch | Windowed incremental extract + Airflow daily schedule | 1 (P1-06), 3 (P3-04) | Not started |
| Weekly batch | Windowed extract + Airflow weekly schedule | 1 (P1-06), 3 (P3-05) | Not started |
| Streaming | Logical replication → Debezium → Kafka → Spark SS | 4 | Not started |

---

## Program roadmap

| Phase | Scope | Status |
| --- | --- | --- |
| 0 | Repository, toolchain, PostgreSQL 16, MinIO, CI | Verified |
| 1 | Multi-source ingestion | In progress (P1-01, P1-02, P1-03, P1-04 verified) |
| 2 | Lakehouse PoC, medallion, dbt | Not started |
| 3 | Airflow daily/weekly, backfills, quality | Not started |
| 4 | Streaming CDC | Not started |
| 5 | Observability, lineage, FinOps model | Not started |
| 6 | CI/CD expansion, resilience, security | Not started |
| 7 | AWS path as Terraform IaC | Not started |
| 8 | Platform report site (hosting + polish) | Not started (early `site/` scaffold exists) |

---

## Risks and limitations

- Olist e-commerce + marketing funnel are **CC BY-NC-SA 4.0**: do not vendor; fetch locally (ADR-0018). MIT covers software/docs only.
- Spark / Iceberg / catalog / query-engine compatibility is unproven until P2-01.
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
| Home / Architecture / Operations / Reference | Scaffold (mostly Phase 0 facts) |
| Pipeline | Stub |

---

## Log

Recent entries only. Older history: [`docs/status-log.md`](docs/status-log.md).

### 2026-10-22 — P1-04 verified

Olist historical ingestion: Kaggle download client, safe unzip into gitignored staging, and raw writes for the 11 published CSVs. `closed_deals.seller_id` joins `sellers.seller_id` or `order_items.seller_id` (contract version 2). Each CSV is read and written on its own; a structural failure stops the remaining files. A leading UTF-8 BOM on `product_category_name_translation` is ignored for the header check and kept in the raw bytes. `uv run pytest tests/unit` → 76 passed; MinIO integration → 1 passed.

### 2026-10-18 — P1-03 verified

YAML contracts under `config/contracts/` for Olist e-commerce, marketing funnel, DummyJSON products, PostgreSQL v1 (`sellers`, `customers`, `orders`, `order_items`), and IBGE `municipios`. Structural loader only. `uv run pytest tests/unit` → 40 passed; ruff clean. D-013 stays `proposed`. Synthetic events and BrasilAPI CEP are out of this set.

### 2026-10-18 — P1-02 verified

Approved D-020: auditable raw in object store (`raw/{source_id}/{dataset}/dt=…/execution_id=…/`); `data/raw/` remains local staging only. Package helpers + minimal writer under `src/open_data_platform/ingestion/`. Unit tests: 21 passed; ruff clean.

### 2026-10-17 — D-019 approved; source docs slimmed

Approved DummyJSON for P1-05, IBGE (P1-08), BrasilAPI CEP optional (P1-09). Gold domains in ADR-0012. Source metadata standardized. Phase 1 closing issues are P1-10/P1-11; Phase 3 daily/weekly renumbered to P3-04/P3-05 (tests P3-09).

### 2026-10-17 — Ledger restructure

`PROJECT_STATUS.md` now tracks the current phase only. Cumulative Delivered/Verified/Open-decisions tables removed. Archive: `docs/status-log.md`.

### 2026-10-17 — P1-01 verified

Olist Brazilian e-commerce + Marketing Funnel: CC BY-NC-SA 4.0 recorded; D-018 / ADR-0018 approved (no vendor; fetch-from-source).
