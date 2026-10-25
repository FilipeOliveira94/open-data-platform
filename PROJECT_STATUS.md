# Project Status

Operational ledger for the **current phase**. Issue backlog and acceptance text live in [`ROADMAP.md`](ROADMAP.md). Full decision index: [`DECISIONS.md`](DECISIONS.md). Chronological archive: [`docs/status-log.md`](docs/status-log.md).

| | |
| --- | --- |
| Repository | [FilipeOliveira94/open-data-platform](https://github.com/FilipeOliveira94/open-data-platform) |
| Started | 2026-10-14 |
| Last updated | 2026-10-25 (P1-09 withdrawn — BrasilAPI CEP obsolete) |
| Current phase | Phase 1 — Multi-source ingestion |
| Phase status | In progress (P1-01, P1-02, P1-03, P1-04, P1-05, P1-06, P1-07, P1-08 verified; P1-09 withdrawn) |

**Status values:** not started · in progress · delivered · verified · blocked · withdrawn

**Verified** means acceptance checks were run and evidence is recorded (here and/or in `docs/status-log.md`). Do not treat planned architecture as complete.

**What belongs here:** current objective, current-phase issue progress, phase acceptance checklist, latest validation, short foundation baseline, decisions that gate *this* work, active risks, blockers, next work.

**What does not:** cumulative issue inventories (use `ROADMAP.md`), full ADR tables (use `DECISIONS.md`), long history (use `docs/status-log.md`).

---

## Current objective

**P1-10** — Failure and invalid-record behavior.

Phase 1 out of scope: streaming/CDC, Iceberg, Airflow schedules, lakehouse transforms, AWS Terraform apply, Phase 8 hosting polish.

## Next work

1. **P1-10** — Failure and invalid-record behavior.

---

## Phase 1 progress

| ID | Title | Status | Evidence |
| --- | --- | --- | --- |
| P1-01 | Historical dataset license and redistribution check | Verified | `docs/sources/`; ADR-0018 / D-018; no Olist binaries in git (2026-10-17) |
| P1-02 | Raw storage layout and ingestion metadata conventions | Verified | ADR-0020; `docs/architecture/raw-storage-layout.md`; `src/open_data_platform/ingestion/`; unit tests 2026-10-18 |
| P1-03 | Data contracts for planned sources | Verified | `config/contracts/`; `docs/architecture/data-contracts.md`; `src/open_data_platform/contracts/`; unit tests 2026-10-18 |
| P1-04 | Historical e-commerce (+ marketing funnel) ingestion | Verified | `ingestion/olist/`, `ingestion/kaggle_download.py`; unit tests 76 passed; MinIO round-trip 1 passed (2026-10-22). Category translation CSV may start with a UTF-8 BOM |
| P1-05 | Paginated public API client (DummyJSON) | Verified | `ingestion/dummyjson_products/`; unit tests 105 passed; MinIO fixture round-trip 1 passed; live run wrote 7 pages / 194 products (2026-10-24). Contract v2: `brand` nullable |
| P1-06 | PostgreSQL incremental batch extract | Verified | `ingestion/postgres_oltp/`; unit tests 135 passed; Postgres + MinIO round-trip 1 passed (2026-10-25). Half-open `updated_at` window on UTC midnights; synthetic seed replaces schema `oltp` |
| P1-07 | Demo storefront clickstream | Verified | `ingestion/demo_storefront/`; unit tests 152 passed; MinIO round-trip 1 passed (2026-10-25). One `events.jsonl` per execution, 500 actions per second of event time, no batch-size cap. Live rerun wrote 30_000 lines in one key |
| P1-08 | IBGE Localidades ingest | Verified | `ingestion/ibge_localidades/`; unit tests 194 passed; MinIO fixture round-trip 1 passed; live run `p108-live-20261025` wrote 5_571 municipalities (2026-10-25). Gzip is decoded before the contract check. Quarantine stays in P1-10 |
| P1-09 | BrasilAPI CEP enrichment | Withdrawn | ADR-0021 / D-021. An 8-digit CEP lookup does not join to the Olist zip prefix. IBGE and Olist geolocation cover that geography |
| P1-10 | Failure and invalid-record behavior | Not started | — |
| P1-11 | Phase 1 documentation and status | Not started | — |

Acceptance detail: `ROADMAP.md` → Phase 1.

### Phase 1 acceptance

- [ ] Ingestions can be repeated without undue duplication or corruption.
- [x] Unit tests do not require the public API to be online.
- [ ] Raw data is traceable by source and execution.
- [x] Contracts and expected fields are documented.
- [ ] Invalid records and errors have explicit, documented behavior.
- [x] PostgreSQL extract accepts an explicit time/watermark window suitable for daily and weekly batch runs.

---

## Latest validation

| | |
| --- | --- |
| Date | 2026-10-25 |
| Environment | WSL2 / Ubuntu |
| Checks | `uv run ruff check src tests` PASS; `uv run ruff format --check src tests` PASS; `uv run pytest tests/unit` → 194 passed |
| Result | `uv run pytest tests/unit` → 194 passed. BrasilAPI CEP is not a dataset (ADR-0021). |

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
| D-019 | DummyJSON (P1-05); IBGE (P1-08). CEP clause superseded | Approved |
| D-021 | BrasilAPI CEP withdrawn; no raw dataset | Approved |
| D-012 | Gold domains `olist_analysis` / `product_analysis` | Proposed (domain split recorded) |
| D-013 | Data contracts / no silent drops — gates P1-03+ | Proposed (P1-03 evidence landed; not approved) |
| D-015 | Package owns logic; thin DAGs/IaC — gates ingestion layout | Proposed |
| D-001 / D-002 | Compose + MinIO baseline (unchanged) | Approved |

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
| 1 | Multi-source ingestion | In progress (P1-01, P1-02, P1-03, P1-04, P1-05, P1-06, P1-07, P1-08 verified; P1-09 withdrawn) |
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

### 2026-10-25 — P1-09 withdrawn

BrasilAPI CEP is withdrawn (ADR-0021 / D-021). Olist publishes a 5-digit zip prefix, and geolocation already carries city, state, and coordinates at that grain. IBGE `municipios` covers every Olist state and the normalized municipality name on nearly all customer, seller, and geolocation rows. An 8-digit CEP lookup does not join to that prefix. DummyJSON and IBGE remain the HTTP sources.

### 2026-10-25 — shared GET checked against both live APIs

DummyJSON `/products` responds with no `Content-Encoding`. The body already starts with `{`. `--execution-id p105-live-20261025` wrote 7 plain JSON pages and 194 products, the same catalog size as the 2026-10-24 run. IBGE still sends gzip. `--execution-id p108-live-20261025b` wrote the decoded array, 5_571 municipalities, starting with `[`. Gzip decoding runs only when that header is present.

### 2026-10-25 — P1-08 live run

The first live call returned HTTP 200 with `Content-Encoding: gzip` (239_529 compressed bytes). `urllib` left the body compressed, so the contract check rejected it as non-JSON. `ingestion.http` now decodes gzip before the size check and the write. The rerun `--execution-id p108-live-20261025` wrote 2_372_050 decoded bytes: 5_571 municipalities, unique 7-digit `municipio-id`, 27 `UF-sigla` values, and one null micro/meso pair (`Boa Esperança do Norte`, MT). No contract change. Against the local Olist extract, `UF-sigla` covers every customer, seller, and geolocation state. Exact `municipio-nome` matches zero rows. Normalized names match 98_713 of 99_441 customer rows, 2_988 of 3_095 seller rows, and 993_930 of 1_000_163 geolocation rows. `uv run pytest tests/unit` → 194 passed.

### 2026-10-25 — P1-08 verified

IBGE Localidades: one JSON array of municipalities (`ibge_localidades` / `municipios`) from `GET /api/v1/localidades/municipios?orderBy=nome&view=nivelado`. The GET uses `ingestion.http` with this client's retry and no-redirect policy, and a 64 MiB response cap. The same execution overwrites `municipios.json`; a new `execution_id` keeps the previous object. Contract key mismatches and an empty array fail before the write. Null micro and meso region values, and a null or duplicate `municipio-id`, stay in the object. `uv run pytest tests/unit` → 191 passed; MinIO integration → 1 passed. The public API was not called. Gold geography for `olist_analysis` stays in P2-05. Quarantine stays in P1-10.

### 2026-10-25 — P1-07 verified

Demo storefront clickstream: one JSONL object of user actions from a fictional retail website (`demo_storefront` / `clickstream`). Event time is on the record, spaced at 500 actions per second. The same execution overwrites `events.jsonl`; a new `execution_id` keeps the previous object. Identifiers do not join Olist or DummyJSON. `uv run pytest tests/unit` → 152 passed; MinIO integration → 1 passed. Bronze, Silver, and Gold placement is noted for P2-03, P2-04, and P2-05 and was not built. CDC stays on PostgreSQL. Quarantine stays in P1-10.

### 2026-10-25 — P1-06 verified

PostgreSQL incremental extract for `sellers`, `customers`, `orders`, and `order_items`. The window is `updated_at >= start AND updated_at < end`, both bounds on UTC midnight. A one-day window and a seven-day window call the same function. Each dataset lands as one JSON array named `{dataset}_{start}_{end}.json`. The synthetic seed replaces schema `oltp`. `uv run pytest tests/unit` → 135 passed; Postgres + MinIO integration → 1 passed. Added `psycopg[binary]` 3.3.6. Quarantine, Airflow schedules, and missed-window detection stay in later issues.

### 2026-10-24 — P1-05 live run

The first live call failed with HTTP 403 (`error code: 1010`) because Cloudflare blocks the default urllib user agent. After a `User-Agent` header, the catalog also disagreed with contract v1: 92 of 194 products omit `brand`, and the short final page sets `limit` to the returned count (14, not 30). Contract v2 records that. A rerun with `--execution-id p105-live-20261024` wrote 7 raw pages. `uv run pytest tests/unit` → 105 passed.

### 2026-10-24 — P1-05 verified

DummyJSON `/products` client: `limit`/`skip` pagination, timeout and 429/5xx retries, and one raw JSON page per object (`products-skip-{skip:06d}.json`) under `dummyjson_products`. Structural mismatches fail the batch. `uv run pytest tests/unit` → 103 passed; MinIO integration → 1 passed. The public API was not called. A shorter re-run on the same `execution_id` does not delete later page keys.

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
