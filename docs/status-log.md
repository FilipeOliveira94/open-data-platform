# Status log (archive)

Chronological history of program status updates. The operational ledger is [`PROJECT_STATUS.md`](../PROJECT_STATUS.md) (current phase only; **rewritable**). Append closed-phase or long narrative entries here when trimming the live ledger. Architecture docs and ADRs should cite this log (or carry the facts inline), not `PROJECT_STATUS.md`, for durable evidence.

---

### 2026-11-11 — README and partition candidate

The README opens with the path from a published number back to the source: several sources, idempotent overwrite, a new raw execution kept beside the previous one, contracts, business-domain Gold, and reconciliation. Bronze and Silver stay full replacements. Status is the next section. Core features are one table row each, with state, phase, and core stack, and a labeled note under each row. Setup stays in collapsed sections. Iceberg partitions for Silver and Gold are a candidate in `ROADMAP.md`, not an issue. No stack command was run.

### 2026-11-11 — P2-10 phase documentation

Phase 2 is verified. [`docs/architecture/phase-2-lakehouse.md`](architecture/phase-2-lakehouse.md) lists the 2026-11-11 component versions, states that Bronze, Silver, and Gold have no Iceberg partition spec, states that no target file size is configured, and contrasts `WriteMode.FULL_OVERWRITE` (and dbt table replace) with the PostgreSQL `incremental_window` extract. Placeholders `INCREMENTAL_MERGE` and `APPEND_ONLY` still fail if selected. The page cites [`docs/architecture/phase-2-live-run.md`](architecture/phase-2-live-run.md) and does not copy the 55 Gold row counts. `README.md` now leads with what the platform does, what it does not do, and the reproduce path. Stale sentences updated: Bronze D-012 is `approved`; Silver quality points at Gold and the live run; the catalog note points at the Ivy cache. No Compose command was run. No application code changed.

Text check, from the repository root. Both commands exited 0.

```text
rg -n "P2-10|later issues|D-012 stays|downloads the pinned jars|Broader quality" README.md docs/architecture PROJECT_STATUS.md ROADMAP.md
rg -n "phase-2-live-run.md" README.md PROJECT_STATUS.md docs/architecture/phase-2-lakehouse.md
```

The first command printed eight lines, all for the issue id `P2-10`: `ROADMAP.md` lines 175, 190, 192, and 491, and `PROJECT_STATUS.md` lines 68, 74, 75, and 190. `README.md` and `docs/architecture` printed nothing. The phrases `later issues`, `D-012 stays`, `downloads the pinned jars`, and `Broader quality` did not appear. The second command printed nine lines: `docs/architecture/phase-2-lakehouse.md` lines 20 and 77, `PROJECT_STATUS.md` lines 27, 74, 75, and 81, and `README.md` lines 9, 60, and 77. Next issue is P3-01. Phase 3 was not started.

### 2026-11-11 — P2-09 live run

On the local stack, `make test-catalog-probe` printed Spark `2` and Trino `"2"` in 17.7 s. `make live-bronze` matched 18/18 raw counts in 359 s, including geolocation 1_000_163, customers 99_441, DummyJSON 194, and postgres_oltp 3/3/3/5. `make live-silver` returned 20/20 `ok` in 344 s. Silver anchors: geolocation 738_332, DummyJSON 194, postgres_oltp 2/2/2/3, clickstream null `product_id` 16. `make live-gold` printed quarantine `1603` and `dbt build` PASS=287 WARN=0 ERROR=0 SKIP=0 TOTAL=287 in 53.7 s (55 tables + 232 schema tests). `make dbt-test-reconciliation` PASS=56 WARN=0 ERROR=0 SKIP=0 TOTAL=56 in 558 s. On-time 89_936 / 96_470, late orders 6_534, `dim_customers` 96_096, `dim_zip_prefix` 19_015, `dim_date` 3_774, `dim_municipality` 5_571. Those anchors match the 2026-11-05 / 2026-11-06 notes. The Silver checker count is 20 statements after P2-07; the 2026-11-05 note said 22/22. Every Spark submit reported 6 jars already retrieved. `ensure_spark_ivy` is one Makefile line so the sourced `.env` stays in the shell that runs the command. Detail: [`docs/architecture/phase-2-live-run.md`](architecture/phase-2-live-run.md). Partitioning, file sizing, and the phase limitation write-up remain P2-10.

### 2026-11-11 — Site figures

The site scaffold still has no charts. Architecture and Pipeline no longer call the verified lakehouse "not started". `ROADMAP.md` now lists ten figures: nine Superset charts, one per question already in the Gold model, and one JupyterLab image of the same on-time query. P8-04 draws them. P8-06 exports static images and a caption manifest. Phase 9 publishes them on Home, Architecture, and Pipeline. No image file was added.

### 2026-11-11 — Phase 8 is exploration and BI; the site moves to Phase 9

`ROADMAP.md` inserts Phase 8 before the platform report. Phase 8 is JupyterLab on the host and Apache Superset in Compose, both read-only through Trino, and may start after Phase 2 in parallel with Phases 3–7. Issues are P8-01 through P8-05, then the close pair P8-06 (live run, result, and consistency check) and P8-07 (phase documentation and status). The report site is Phase 9 (P9-01–P9-07); its close pair is P9-06 then P9-07. D-017 records the selection and stays `proposed` until the P8-01 PoC against Trino 484. No notebook, image, or Compose service was added. Earlier log lines that call Phase 8 the site mean what is now Phase 9.

### 2026-11-11 — Quarantine and Spark credential exposure

Quarantine staging no longer inlines finding values in SQL. `make stage-quarantine` writes warehouse JSONL and loads it with Spark. Spark runners pass the JDBC password and S3 keys through a properties file. `tests/unit/infrastructure/test_spark_credentials.py` fails if another `docker/spark/*.sh` assigns those keys. The agent rule is `.cursor/rules/05-runtime-data-exposure.mdc`. `uv run ruff check src tests` passed. `uv run ruff format --check src tests` passed. `uv run pytest tests/unit` passed 294. `make stage-quarantine` was not re-run on the live stack. Detail: [`docs/architecture/gold-layer.md`](architecture/gold-layer.md). This entry was archived from the Phase 2 ledger when P2-10 trimmed it.

### 2026-11-11 — P2-07 quality and reconciliation tests

Removed the pinned `live_counts` module. Bronze `--all` writes `.local/bronze-source-counts.tsv` from the raw row counts of that run; `--print-live-checks` requires the file. Silver live checks return `ok` when the Silver count matches the Bronze dedup policy (distinct payload columns for geolocation) and when DummyJSON expansion and clickstream cart/purchase product ids hold. dbt singular tests in `dbt/tests/reconciliation/` cover all 55 Gold tables plus ephemeral `int_order_primary_payment`. Schema tests check type-family prefixes, grain uniqueness, closed value sets, and numeric bounds. Money sums use absolute tolerance `0.01`.

`uv run dbt test --project-dir dbt --profiles-dir dbt` → PASS=288 WARN=0 ERROR=0 SKIP=0 TOTAL=288 in 570.63s, against Gold already built from `odp-raw` / `execution_id=run-001`. Before that run, `brazil_geography.dim_municipality` returned `ICEBERG_MISSING_METADATA`; `DROP TABLE` then `dbt run --select dim_municipality` recreated 5_571 rows. `dbt run --select mart_storefront_user_behavior` recreated 5 rows after session and event aggregates were split. `uv run ruff check src tests` passed. `uv run ruff format --check src tests` passed. `uv run pytest tests/unit` → 285 passed at that point. `make live-bronze` and `make live-silver` were not re-executed. P2-09 remains the live consistency close. The PASS=288 run was before the tests were grouped by schema. After that move, `dbt ls --quiet` counted 56 tests tagged `reconciliation` and 232 tests outside that tag. `make dbt-build` and `make live-gold` pass `--exclude tag:reconciliation`. A `dbt build` without that flag still runs the reconciliation tests. A later `uv run pytest tests/unit` on this tree passed 286. `uv run ruff check src tests` and `uv run ruff format --check src tests` passed again.

### 2026-11-06 — P2-06 dbt Gold verified

Scaffolded `dbt/` with **dbt-trino** `1.11.0` (dbt Core `1.12.6`) against Trino `484` on the Iceberg JDBC catalog. Staged MinIO quarantine JSONL into `iceberg.gold_staging.quarantine_findings` (1_603 rows from the legacy `order_reviews` sidecar). `dbt build` created **55** Gold tables (plus one ephemeral primary-payment helper). Smoke: on-time rate **0.932** on `mart_delivery_performance`; `mart_late_orders` **6_534**; `dim_customers` 96_096; `dim_zip_prefix` 19_015. Compose publishes Trino `TRINO_PORT` (default 8080) for host dbt. Silver remains the Spark writer. D-005 and D-012 approved. `uv run ruff check src tests` passed. `uv run pytest tests/unit` → 272 passed. Detail: [`docs/architecture/gold-layer.md`](architecture/gold-layer.md).

### 2026-11-06 — Gold schemas split by audience

Revised [`docs/architecture/gold-model-design.md`](architecture/gold-model-design.md) and ADR-0012. Source families on contracts stay `olist_analysis` and `product_analysis`. Gold schemas are `marketplace_operations`, `customer_crm`, `seller_acquisition`, `brazil_geography`, `catalog_commerce`, `storefront_behavior`, and `oltp_operations`, plus shared `dim_date`. Delivered orders with both dates are 93.2% on time (89_936 / 96_470) and 6.8% late (6_534). `mart_delivery_performance` publishes the rate. `mart_late_orders` lists each late order. Each schema has `fct_*_quality_findings` for contract violations and business anomalies. Late delivery is not filed as a data error. D-012 stays `proposed`. No dbt model was built.

### 2026-11-06 — P2-05 Gold model design

Documented grain, keys, type 1 refresh, and marts for `olist_analysis`, `product_analysis`, and the PostgreSQL `oltp_` star. Profiled `data/raw` Olist CSVs and Trino `iceberg.silver` (`odp-raw`, `execution_id=run-001`) on this date. Published payments have no refund status: 484 of 625 canceled orders already had `order_approved_at`. Reviewed orders delivered after the estimate average score 2.272 (6_382) against 4.290 on time or early (89_448). IBGE city+UF match after lower, accent strip, and hyphen-to-space: 98_817 / 99_441 customers and 2_986 / 3_095 sellers, with no ambiguous pair. Category supplement for Gold: `pc_gamer` → `gaming_pc`, `portateis_cozinha_e_preparadores_de_alimentos` → `portable_kitchen_appliances_and_food_processors`. Clickstream has no URL and does not join DummyJSON or Olist. SCD Type 2 applications are in that design. Implementation is new roadmap issue P4-08 only. Phase 4 live run and documentation moved to P4-09 and P4-10. D-012 stays `proposed`. No dbt project, no Spark job, no unit-test run.

### 2026-11-06 — P2-08 Spark Ivy cache

Compose Spark mounts `.local/spark-ivy` at `/tmp/.ivy2`. `make spark-ivy-warm` seeds the pinned Iceberg/Hadoop/AWS/JDBC jars. Second warm and a Bronze clickstream `compose run --rm` both reported `6 already retrieved` with no Maven re-download. Host dirs are world-writable for Spark `uid=185`. Detail: `docs/architecture/phase-2-spark-ivy-cache.md`.

### 2026-11-05 — Full-data Bronze and Silver live (`odp-raw`)

Re-verified P2-03 / P2-04 against every object under `ODP_RAW_BUCKET=odp-raw` (not fixture buckets). Write mode is full load + overwrite only (`lakehouse.write_mode`). DummyJSON nested JSON expands via shared `silver.common` helpers; source module maps fields only. `make live-bronze` → Trino 18/18. `make live-silver` → Trino 22/22 (18 counts + DummyJSON shape + clickstream null product_id). Notable Silver deltas vs Bronze: geolocation 1_000_163→738_332; postgres_oltp 3/3/3/5→2/2/2/3. Unit fixtures remain pytest-only. Phase 2 close pair is P2-09 / P2-10; Spark Ivy cache is P2-08 (before the close). Hardcoded `live_counts` → P2-07.

### 2026-11-05 — P2-04 Silver layer

Silver is one Iceberg table per contract on the JDBC catalog, namespace `silver`, format version 2. Columns follow contract types. Modules under `lakehouse.silver` are split by `source_id`. `order_reviews` contract v2 primary key is `(review_id, order_id)`. `make test-silver` wrote the four Bronze live fixtures, promoted Bronze, then Silver. Spark printed `2`, `100`, `2`, `2`. Trino passed 17 checks, including category CSV dedup (3→2) and typed DummyJSON envelope fields. `uv run ruff check src tests` passed. `uv run ruff format --check src tests` passed. `uv run pytest tests/unit` → 261 passed. `docker compose config --services` listed `minio` and `postgres`. D-012 stays `proposed`. Gold is P2-05+.

### 2026-11-01 — snapshot retention moved to Phase 5

Iceberg `INSERT OVERWRITE` keeps old snapshots and their Parquet files. That maintenance is P5-07 in the FinOps phase: it uses the retention named by the P5-05 cost model and sits before the Phase 5 live run (P5-08) and documentation (P5-09). The Phase 5 close pair numbered P5-07/P5-08 on 2026-10-28 is now P5-08/P5-09. No cleanup job was added.

### 2026-11-01 — P2-03 rechecked on the shared Spark runtime

Each Bronze dataset is a separate Spark invocation. `make test-bronze` printed `100`, `2`, `2`, and `3`, then Trino matched 18 checks. The synthetic objects are written by `open_data_platform.lakehouse.fixtures`. `uv run pytest tests/unit` passed. The `site/` pages were left unchanged. D-012 stays `proposed`.

### 2026-10-31 — P2-03 Bronze layer

Bronze is one Iceberg table per contract on the JDBC catalog, namespace `bronze`, format version 2, every column `STRING`. Metadata columns are `source_id`, `execution_id`, `_ingestion_ts`, `raw_object_key`, `record_index`, and `dt`. The final `make test-bronze` used bucket `odp-bronze-live` and four synthetic objects. Spark printed `3`, `100`, `2`, `2`. Trino passed 18 checks, including duplicates, nulls, `São Teste`, and compact JSON text. `uv run ruff check src tests` passed. `uv run ruff format --check src tests` passed. `uv run pytest tests/unit` → 245 passed in 10.96s. `docker compose config --services` listed `minio` and `postgres`. The `site/` pages were left unchanged. D-012 stays `proposed`. Silver is P2-04.

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
