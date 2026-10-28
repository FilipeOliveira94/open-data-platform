# Status log (archive)

Chronological history of program status updates. The operational ledger is [`PROJECT_STATUS.md`](../PROJECT_STATUS.md) (current phase only). Append closed-phase or long narrative entries here when trimming the live ledger; keep `PROJECT_STATUS.md` → Log to recent / current-phase notes.

---

### 2026-10-28 — Phases 0 and 1 stay verified

The close pair (live run, then documentation and status) applies from Phase 2 on. Phases 0 and 1 stay verified on results already in this log, so P0-14, P0-15, P1-12, and P1-13 are removed. Phase 0 evidence is the 2026-10-16 Compose health check, PostgreSQL reachability, MinIO put/get, and green CI. Phase 1 evidence is the DummyJSON live run (7 pages / 194 products), the IBGE live run (5_571 municipalities), the P1-10 MinIO matrix (40 passed, including 1_603 duplicate `review_id` rows), and P1-11. The full Olist Kaggle rerun after the category-file BOM fix remains unrepeated; the staged-file matrix is the recorded Olist result. Next work is P2-01.

### 2026-10-28 — Phase close pattern

`ROADMAP.md` now ends every phase with a live run, result, and consistency check, then a phase documentation and status issue: P0-14/P0-15, P1-12/P1-13, P2-08/P2-09, P3-10/P3-11, P4-08/P4-09, P5-07/P5-08, P6-07/P6-08, P7-08/P7-09, P8-06/P8-07. P1-11 stays the earlier documentation pass. Phase 0 and Phase 1 feature issues keep their recorded status. Those phases are in progress until the new close is verified, so Phase 2 waits. `cd site && npm run build` completed: 6 pages in 1.75s. Unit tests were not re-run. The IDE browser could not open the local preview.

### 2026-10-28 — P1-11 Phase 1 documentation and status

README and `docs/architecture/phase-1-ingestion.md` index the five raw sources, object keys, repeatable overwrite, quarantine, and the three update patterns. Daily and weekly batch remain the P1-06 window (Airflow schedules stay P3-04 and P3-05). Streaming stays Phase 4. Phase 1 acceptance is checked in `ROADMAP.md`. The operational ledger moves to Phase 2, not started, at P2-01. Validation: `uv run ruff check src tests` PASS; `uv run ruff format --check src tests` PASS; `uv run pytest tests/unit` → 216 passed in 10.56s. `cd site && npm ci && npm run build` completed: 6 pages. Node v22.14.0 is below the engine range some transitive packages declare (`@astrojs/prism` >=22.22.0, `undici` >=22.19.0); the build still finished. No live API, Kaggle, or MinIO. No ingestion code change.

### 2026-10-26 — P1-10 live matrix

Ran each Phase 1 source against local MinIO. IBGE and DummyJSON used the public APIs. PostgreSQL used the synthetic fixture, then temporary rows for the error cases, then `DROP SCHEMA oltp` and a reseed. Olist used the gitignored staged CSVs. Injected payloads covered nulls, type mismatches, duplicate keys, structural failures, a clean rerun, and synthetic `fail_batch`. 40 cases passed. No product change. Before the contract fix, that Olist snapshot also quarantined leading-zero ZIP prefixes and closed-deal `True`/`False` and `100.0` cells. `819829d` matches those tokens. A later classification of the same files leaves only the 1_603 duplicate `review_id` rows.

### 2026-10-26 — D-013 approved; sidecar uses the object writer

D-013 is `approved`. Each ingestor calls `prepare_quarantine` and then `write_raw_object` for the raw payload. The quarantine sidecar is a second call to `write_raw_object` with prefix `quarantine`. A clean rerun still deletes that key. `uv run ruff check src tests` PASS; `uv run ruff format --check src tests` PASS; `uv run pytest tests/unit` → 216 passed.

### 2026-10-26 — P1-10 verified

Invalid rows stay in the raw object. A nullable null is not quarantined. A null in a required field, a wrong type, and every row in a repeated primary-key group are written to `quarantine/{source_id}/{dataset}/dt=…/execution_id=…/{object_name}.jsonl` in `ODP_RAW_BUCKET`. The same execution overwrites that sidecar. A clean rerun deletes it. Structural errors still fail before the write and leave earlier objects in place. `on_invalid: fail_batch` writes neither object. No shipped contract uses that policy. Geolocation has no primary key. `timestamp` and `date` are non-empty text, with no calendar check. CSV booleans are `true` and `false`. The PostgreSQL extract quarantines a null primary key instead of aborting. A naive timestamp or a non-finite number still fails before the write. `uv run ruff check src tests` PASS; `uv run ruff format --check src tests` PASS; `uv run pytest tests/unit` → 214 passed. No live API, Kaggle, or MinIO. D-013 stays `proposed`. Reprocess stays P3-07.

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

### 2026-10-22 — P1-04 Olist historical ingestion

Package fetch of `olistbr/brazilian-ecommerce` and `olistbr/marketing-funnel-olist` through `GET /api/v1/datasets/download/{owner}/{slug}`. One slug is extracted and its CSVs are written one at a time before the next slug starts. Published join: `closed_deals.seller_id` = `sellers.seller_id` or `order_items.seller_id` (those three contracts are version 2). Validation: `uv run ruff check src tests` PASS; `uv run ruff format --check src tests` PASS; `uv run pytest tests/unit` → 76 passed; `uv run pytest tests/integration/test_olist_ingest.py` → 1 passed on local MinIO. A live Kaggle run downloaded the e-commerce zip and stopped on the category-file BOM; the full live run after that fix was not re-executed. No Olist rows committed.

### 2026-10-18 — P1-02 raw storage layout and ingestion metadata

Approved D-020 / ADR-0020: auditable raw in object store under `raw/{source_id}/{dataset}/dt=…/execution_id=…/{object_name}`; required metadata `source_id`, `execution_id`, `_ingestion_ts`; local `data/raw/` remains fetch staging only. Package helpers and minimal writer in `src/open_data_platform/ingestion/`. Validation: `uv run ruff check src tests` PASS; `uv run pytest tests/unit` → 21 passed.

### 2026-10-17 — D-019 sources package reviewed

Owner approved DummyJSON (P1-05), IBGE (P1-08), BrasilAPI CEP optional (P1-09). Alternatives in ADR-0019; `docs/sources/` final choices with standardized metadata; Gold domains in ADR-0012; Phase 1 closing = P1-10/P1-11; Phase 3 daily/weekly = P3-04/P3-05. Validation: ruff PASS; pytest unit 6 passed; no vendored CSVs; field-schema check PASS.

### 2026-10-17 — PROJECT_STATUS ledger restructure

Restructured `PROJECT_STATUS.md` for Phase 1+: current-phase progress only; no cumulative Delivered / Verified / full Open decisions tables. Historical entries moved here. Updated `DECISIONS.md` mirroring rule and related pointers.

### 2026-10-17 — P1-01 historical dataset license / redistribution

Recorded CC BY-NC-SA 4.0 terms for Olist Brazilian e-commerce and Marketing Funnel (join on `seller_id`). Approved D-018 / ADR-0018: do not vendor; fetch-from-source under gitignored `data/raw/`. Docs under `docs/sources/`. No dataset files downloaded or committed.

### 2026-10-16 — Platform report site on Starlight + Catppuccin

Replaced the plain HTML scaffold with Astro Starlight and `@catppuccin/starlight` (preview: starlight-themes.netlify.app/catppuccin). Five pages keep Phase 0 verified content; Pipeline remains a stub. Preview: `cd site && npm run dev` (Node.js 22.12+). No public hosting (Phase 8).

### 2026-10-16 — Platform report site scaffold (Phase 0 content)

Added early `site/` scaffold with Phase 0 verified facts. Roadmap incremental rule updated to allow early scaffold; P8-01 marked in progress.

### 2026-10-16 — Phase 8 platform report site on roadmap

Added Phase 8 as the program close-out: multi-page static report (Home, Architecture, Pipeline, Operations, Reference). Documented incremental growth and P7-07 as in-repo case study feeding the site. Updated `ROADMAP.md`, status ledger, `README.md`, and `docs/project-context-for-cursor.md`.

### 2026-10-16 — Phase 0 verified; pin uv 0.13.1

GitHub Actions CI reported green for lint and unit tests after push. Marked Phase 0 verified. Pinned `uv` to `0.13.1` in `pyproject.toml` (`required-version`) and `.github/workflows/ci.yml` to avoid unpinned “latest” installs.

**Phase 0 acceptance (all verified):** clean clone setup; PostgreSQL healthy; MinIO write/read smoke; lint and tests via documented commands; no secrets/live `.env`/private datasets in VCS; CI runs the same lint and test checks.

**Delivered issues P0-01–P0-13:** see `ROADMAP.md` Phase 0. Evidence included MIT license, `AGENTS.md` / rules, status/roadmap/ADRs, Python 3.12 + `uv`, Ruff/pytest, Compose (PostgreSQL 16 + community MinIO), repo hygiene, GitHub Actions, D-001/D-002 approved.

### 2026-10-16 — Compose runtime verified + D-002 AGPL/AIStor note

Documented that MinIO withdrew official images for AIStor while community MinIO remains AGPLv3 (community images OK). Verified `docker compose up -d`: both services healthy; PostgreSQL reachable; MinIO put/get smoke passed.

### 2026-10-16 — P0-08–P0-13 foundation implementation

Added Python 3.12 package (`uv` + `uv.lock`), Ruff/pytest, Compose (PostgreSQL 16.15-bookworm + community MinIO pin), repo hygiene, README/Makefile, GitHub Actions lint/unit CI, and approved D-001/D-002 with image-distribution notes.

### 2026-10-15 — Decision log restructure (D-001–D-017)

Restructured ADRs: removed Phase-0-services, Kubernetes-standalone, and analytical-placeholder ADRs; split compute vs orchestration and observability vs lineage; added medallion, contracts, batch/streaming coexistence, monorepo, discovery, and dataviz. Index in `DECISIONS.md`; detail in `docs/decisions/` ADR-0001–0017. All remain `proposed` (D-001/D-002 later approved).

### 2026-10-15 — P0-07 decision log (summary + ADRs)

`DECISIONS.md` is a short index (status + one-line summary). Full rationale lives in `docs/decisions/`, with `_template.md` and README.

### 2026-10-15 — AWS path as Infrastructure as Code

Scope alignment: Phase 7 delivers the **AWS deployment path as Terraform IaC**—service mapping (MinIO→S3, catalog→Glue, Trino→Athena), modular resources, OIDC/IAM, cost model and case study—with CI `fmt`/`validate`. Local Compose remains the environment for pipeline development and verification. Cloud apply/destroy stay approval-gated.

### 2026-10-15 — Three update patterns (batch + stream)

Scope review: CDC (Phase 4) covers **streaming** only, not daily/weekly PostgreSQL loads. Updated roadmap and status to require three use cases—daily batch (P1-06 + P3-04), weekly batch (P1-06 + P3-05), streaming CDC (Phase 4)—and added P4-07 batch regression after CDC.

### 2026-10-15 — P0-06 roadmap

Added `ROADMAP.md` with issue-level scope for phases 0–8 (IDs `P0-xx` … `P8-xx`), dependencies, phase and issue acceptance criteria, and out-of-scope notes.

### 2026-10-14 — Phase 0 started

Initialized the public repository and the operating model for the program.

| Commit | Description |
| --- | --- |
| `ab3c9a6` | MIT license |
| `dc49fd8` | `AGENTS.md` and contributor workflow |
| `71e96c6` | Engineering rules by concern |

Also delivered: status ledger and program context under `docs/`.
