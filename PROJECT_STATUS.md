# Project Status

Operational ledger for the **current phase**. Issue backlog and acceptance text live in [`ROADMAP.md`](ROADMAP.md). Full decision index: [`DECISIONS.md`](DECISIONS.md). Chronological archive: [`docs/status-log.md`](docs/status-log.md).

| | |
| --- | --- |
| Repository | [FilipeOliveira94/open-data-platform](https://github.com/FilipeOliveira94/open-data-platform) |
| Started | 2026-10-14 |
| Last updated | 2026-11-01 (P2-03 Bronze verified) |
| Current phase | Phase 2 — Lakehouse and analytics |
| Phase status | In progress |

**Status values:** not started · in progress · delivered · verified · blocked · withdrawn

**Verified** means acceptance checks were run and evidence is recorded (here and/or in `docs/status-log.md`). Do not treat planned architecture as complete.

**What belongs here:** current objective, current-phase issue progress, phase acceptance checklist, latest validation, short foundation baseline, decisions that gate *this* work, active risks, blockers, next work.

**What does not:** cumulative issue inventories (use `ROADMAP.md`), full ADR tables (use `DECISIONS.md`), long history (use `docs/status-log.md`).

---

## Current objective

**P2-04** — Silver normalization and deduplication.

P2-03 promotes every contracted raw dataset into Iceberg `STRING` tables on the JDBC catalog from P2-02. The live check covered one dataset of each payload shape. Typing, deduplication, Airflow, CDC/streaming, and AWS lakehouse modules stay out of scope. The phase closes on P2-08, then P2-09.

## Next work

1. **P2-04** — Silver normalization and deduplication. Bronze stays string-typed and keeps duplicate keys.

---

## Phase 2 progress

| ID | Title | Status | Evidence |
| --- | --- | --- | --- |
| P2-01 | Minimal Spark + Iceberg + catalog + query PoC | Verified | Two runs returned count 2. [`docs/architecture/phase-2-spark-iceberg-poc.md`](docs/architecture/phase-2-spark-iceberg-poc.md) |
| P2-02 | Local catalog and query-engine choice | Verified | Spark and Trino both returned 2 on two runs. [`docs/architecture/phase-2-catalog-query.md`](docs/architecture/phase-2-catalog-query.md) |
| P2-03 | Bronze layer from raw ingestion | Verified | Four payload shapes. Spark counts 3, 100, 2, 2. Trino 18/18. [`docs/architecture/bronze-layer.md`](docs/architecture/bronze-layer.md) |
| P2-04 | Silver normalization and deduplication | Not started | — |
| P2-05 | Analytical model design (grain, keys, SCD) | Not started | — |
| P2-06 | dbt project and initial Gold models | Not started | — |
| P2-07 | Data quality and reconciliation tests | Not started | — |
| P2-08 | Live run, result, and consistency check | Not started | — |
| P2-09 | Phase 2 documentation and status | Not started | — |

Acceptance detail: `ROADMAP.md` → Phase 2.

### Phase 2 acceptance

- [ ] A table can be created, written and queried in the chosen local stack.
- [ ] Queries and transforms are reproducible via documented commands.
- [ ] Quality and reconciliation rules have tests.
- [ ] Documentation lists component versions and known limitations.
- [ ] A live run result is recorded and compared to the Phase 2 contracts (P2-08).
- [ ] Documentation and `PROJECT_STATUS.md` cite that result (P2-09).

---

## Latest validation

| | |
| --- | --- |
| Date | 2026-11-01 |
| Environment | WSL2 / Ubuntu. PostgreSQL and MinIO already healthy. GNU Make 4.4.1 |
| Checks | `make test-bronze` with one Spark process per dataset; `make test-iceberg-poc`; `make test-catalog-probe`; `docker compose config --services`; Ruff; unit tests |
| Result | Bronze Spark printed `100`, `2`, `2`, and `3` for clickstream, products, municipalities, and the category CSV. Trino matched all 18 checks, including a repeated category name, a null English label, 16 null `product_id` values, one null microregion, `São Teste`, a null `brand`, and tags text `["demo"]`. The Hadoop probe and the JDBC probe each printed `2` on the shared Spark runtime; Trino printed `"2"`. `docker compose config --services` lists `minio` and `postgres` only. `uv run ruff check src tests` passed. `uv run ruff format --check src tests` passed. `uv run pytest tests/unit` passed (exit 0) |

---

## Phase 1 baseline

Phase 1 is **verified** (P1-09 withdrawn). Index: [`docs/architecture/phase-1-ingestion.md`](docs/architecture/phase-1-ingestion.md). Live results already logged: DummyJSON 194 products, IBGE 5_571 municipalities, and the P1-10 MinIO matrix (40 passed).

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
| D-003 | Parquet + Iceberg 1.12.0, format version 2. Spark writes, Trino reads | Approved |
| D-004 | JDBC catalog on PostgreSQL plus Trino 484. Glue and Athena stay Phase 7 | Approved |
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
| 2 | Lakehouse PoC, medallion, dbt | In progress (P2-01, P2-02, and P2-03 verified) |
| 3 | Airflow daily/weekly, backfills, quality | Not started |
| 4 | Streaming CDC | Not started |
| 5 | Observability, lineage, FinOps model | Not started |
| 6 | CI/CD expansion, resilience, security | Not started |
| 7 | AWS path as Terraform IaC | Not started |
| 8 | Platform report site (hosting + polish) | Not started (early `site/` scaffold exists) |

---

## Risks and limitations

- P2-02 proved one table on the Iceberg JDBC catalog: Spark writes, Trino 484 reads. An overwrite leaves the previous Parquet files in the bucket. JDBC is not the Glue protocol. The Hadoop catalog remains the P2-01 probe only.
- P2-03 stores Bronze contract fields as `STRING` on that same catalog. The live check wrote fixtures to bucket `odp-bronze-live` and overwrote four Bronze tables. It did not read the Phase 1 raw bucket.
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

### 2026-11-01 — snapshot retention moved to Phase 5

Expiring Iceberg snapshots and deleting orphan Parquet is P5-07, after the cost model (P5-05) and before the Phase 5 live run (P5-08) and documentation (P5-09). It is not a Phase 2 issue. No cleanup command was added.

### 2026-11-01 — P2-03 rechecked on the shared Spark runtime

`make test-bronze` ran one Spark process per dataset. Spark printed `100`, `2`, `2`, and `3` (clickstream, products, municipalities, category CSV). Trino matched 18 checks. `make test-iceberg-poc` and `make test-catalog-probe` each printed `2` through the same runtime; Trino printed `"2"`. Fixture payloads live in `open_data_platform.lakehouse.fixtures`. `uv run pytest tests/unit` passed. The `site/` pages were left unchanged.

### 2026-10-31 — P2-03 verified

Bronze tables are Iceberg format version 2 in namespace `bronze` on the P2-02 JDBC catalog. Each table location is `s3a://odp-warehouse/bronze/{source_id}/{dataset}`. Columns are `STRING`. The final `make test-bronze` wrote four synthetic objects into `odp-bronze-live` and Spark printed `3`, `100`, `2`, `2`. Trino returned those counts and the null, duplicate, metadata, and nested-text checks: 18 of 18. Earlier Spark overwrites the same evening printed the same four counts. `docker compose config --services` lists `minio` and `postgres` only. `uv run pytest tests/unit` → 245 passed in 10.96s. The `site/` pages were left unchanged. D-012 stays `proposed`. Detail: [`docs/architecture/bronze-layer.md`](docs/architecture/bronze-layer.md).

### 2026-10-29 — P2-02 verified

Iceberg JDBC catalog on PostgreSQL database `iceberg`, schema V1, catalog name `local`. Spark `3.5.8-java17` with Iceberg `1.12.0` wrote `catalog_probe.probe` (`format-version` 2) to `s3a://odp-warehouse/catalog`. Trino `trinodb/trino:484` read it. Two runs of `make test-catalog-probe`: Spark printed `2`, Trino printed `"2"`. `iceberg_tables` has one row. After those two runs, prefix `catalog/` had 24 objects. `docker compose config --services` lists `minio` and `postgres` only. `uv run pytest tests/unit` → 224 passed in 10.47s. D-003 and D-004 are `approved`.

### 2026-10-28 — P2-01 verified

Spark `3.5.8-java17`, Iceberg `1.12.0`, Hadoop catalog, warehouse `s3a://odp-warehouse/poc`, table `poc.probe`. Two runs of `docker compose --profile lakehouse run --rm -T spark` each printed count `2`. The image has no S3A class; `hadoop-aws` 3.3.4 and `aws-java-sdk-bundle` 1.12.262 were added after `ClassNotFoundException`. Java 11 plus Iceberg 1.12 was rejected from the Iceberg 1.11 release note and was not run. `uv run pytest tests/unit` → 219 passed in 11.07s. D-003 and D-004 were still `proposed` at this date.

### 2026-10-28 — Phases 0 and 1 stay verified

Logged results already cover those closes. Phase 0: Compose healthy, PostgreSQL reachable, MinIO put/get, CI green (2026-10-16). Phase 1: DummyJSON 194 products, IBGE 5_571 municipalities, P1-10 matrix 40 passed, P1-11 documentation. P0-14, P0-15, P1-12, and P1-13 are removed. The live-run and documentation pair starts at Phase 2 (P2-08, P2-09). Next work is P2-01. `cd site && npm run build` completed: 6 pages in 1.76s. Unit tests were not re-run.

### 2026-10-28 — P1-11 verified; documentation pass

README and [`docs/architecture/phase-1-ingestion.md`](docs/architecture/phase-1-ingestion.md) index the five sources, raw keys, repeatable overwrite, and the three update patterns. Daily and weekly batch stay on the P1-06 window. Streaming stays in Phase 4. `uv run ruff check src tests` PASS. `uv run ruff format --check src tests` PASS. `uv run pytest tests/unit` → 216 passed in 10.56s. `cd site && npm ci && npm run build` completed: 6 pages in 1.85s. No live API, Kaggle, or MinIO. No Phase 2 code.
