# Project Status

Operational ledger for the **current phase**. Issue backlog and acceptance text live in [`ROADMAP.md`](ROADMAP.md). Full decision index: [`DECISIONS.md`](DECISIONS.md). Chronological archive: [`docs/status-log.md`](docs/status-log.md).

| | |
| --- | --- |
| Repository | [FilipeOliveira94/open-data-platform](https://github.com/FilipeOliveira94/open-data-platform) |
| Started | 2026-10-14 |
| Last updated | 2026-11-11 (Phase 8 roadmap insert; P2-07 still the latest verified issue) |
| Current phase | Phase 2 — Lakehouse and analytics |
| Phase status | In progress |

**Status values:** not started · in progress · delivered · verified · blocked · withdrawn

**Verified** means acceptance checks were run and evidence is recorded (here and/or in `docs/status-log.md`). Do not treat planned architecture as complete.

**What belongs here:** current objective, current-phase issue progress, phase acceptance checklist, latest validation, short foundation baseline, decisions that gate *this* work, active risks, blockers, next work.

**What does not:** cumulative issue inventories (use `ROADMAP.md`), full ADR tables (use `DECISIONS.md`), long history (use `docs/status-log.md`).

---

## Current objective

**P2-09** — Live run, result, and consistency check.

P2-07 verified dbt and Python quality and reconciliation tests ([`docs/architecture/gold-layer.md`](docs/architecture/gold-layer.md)). Silver stays the Spark writer. The phase closes on P2-09, then P2-10.

## Next work

1. **P2-09** — On the local stack, record component versions, row counts, the quality-test result, and Gold metrics against the source or Silver counts for that run. Unit tests do not close this issue.

---

## Phase 2 progress

| ID | Title | Status | Evidence |
| --- | --- | --- | --- |
| P2-01 | Minimal Spark + Iceberg + catalog + query PoC | Verified | Two runs returned count 2. [`docs/architecture/phase-2-spark-iceberg-poc.md`](docs/architecture/phase-2-spark-iceberg-poc.md) |
| P2-02 | Local catalog and query-engine choice | Verified | Spark and Trino both returned 2 on two runs. [`docs/architecture/phase-2-catalog-query.md`](docs/architecture/phase-2-catalog-query.md) |
| P2-03 | Bronze layer from raw ingestion | Verified | Full `odp-raw` live. Trino 18/18 against that run's raw counts. [`docs/architecture/bronze-layer.md`](docs/architecture/bronze-layer.md) |
| P2-04 | Silver normalization and deduplication | Verified | Full Bronze→Silver live. Trino 22/22 (18 counts + 4 shape). Geolocation 1_000_163→738_332. [`docs/architecture/silver-layer.md`](docs/architecture/silver-layer.md) |
| P2-05 | Analytical model design (grain, keys, SCD) | Verified | [`docs/architecture/gold-model-design.md`](docs/architecture/gold-model-design.md) |
| P2-06 | dbt project and initial Gold models | Verified | `dbt build` PASS=55. On-time rate 0.932; late orders 6_534. [`docs/architecture/gold-layer.md`](docs/architecture/gold-layer.md) |
| P2-07 | Data quality and reconciliation tests | Verified | `dbt test` PASS=288. Unit tests 286 passed. `make dbt-build` excludes `tag:reconciliation`. [`docs/status-log.md`](docs/status-log.md) |
| P2-08 | Spark lakehouse jar / Ivy cache | Verified | Host `.local/spark-ivy` → `/tmp/.ivy2`. Second warm: 6 already retrieved. [`docs/architecture/phase-2-spark-ivy-cache.md`](docs/architecture/phase-2-spark-ivy-cache.md) |
| P2-09 | Live run, result, and consistency check | Not started | — |
| P2-10 | Phase 2 documentation and status | Not started | — |

Acceptance detail: `ROADMAP.md` → Phase 2.

### Phase 2 acceptance

- [x] A table can be created, written and queried in the chosen local stack.
- [ ] Queries and transforms are reproducible via documented commands.
- [x] Quality and reconciliation rules have tests.
- [ ] Documentation lists component versions and known limitations.
- [ ] A live run result is recorded and compared to the Phase 2 contracts (P2-09).
- [ ] Documentation and `PROJECT_STATUS.md` cite that result (P2-10).

---

## Latest validation

| | |
| --- | --- |
| Date | 2026-11-11 |
| Environment | WSL2 / Ubuntu. PostgreSQL, MinIO, Trino healthy; Gold from `odp-raw` / `run-001` present |
| Checks | `uv run dbt test --project-dir dbt --profiles-dir dbt` (before the schema split); review re-run: `uv run ruff check src tests`; `uv run ruff format --check src tests`; `uv run pytest tests/unit`; `dbt ls` |
| Result | `dbt test` PASS=288 WARN=0 ERROR=0 TOTAL=288 (570.63s). Review re-run: Ruff passed; unit tests 286 passed; `dbt ls` 56 reconciliation and 232 other tests. Detail: [`docs/status-log.md`](docs/status-log.md) |

---

## Phase 1 baseline

Phase 1 is **verified** (P1-09 withdrawn). Index: [`docs/architecture/phase-1-ingestion.md`](docs/architecture/phase-1-ingestion.md). Live results already logged: DummyJSON 194 products, IBGE 5_571 municipalities, and the P1-10 MinIO matrix (40 passed).

Five sources land raw objects: Olist e-commerce and marketing funnel, DummyJSON products, PostgreSQL windowed extract, demo storefront clickstream, and IBGE `municipios`. The same execution identity overwrites an object. A new `execution_id` keeps the previous one. Keys and user metadata carry `source_id`, `execution_id`, and `_ingestion_ts`. Invalid rows stay in the raw object; findings go to `quarantine/…/{object_name}.jsonl`.

Daily and weekly batch share `extract_postgres_oltp` (one-day and seven-day UTC-midnight windows). Airflow schedules are Phase 3. Streaming CDC is Phase 4 and does not replace batch.

`order_reviews` contract v2 uses primary key `(review_id, order_id)`. The P1-10 matrix recorded 1_603 duplicate findings under contract v1 (`review_id` alone). A shorter DummyJSON rerun on the same `execution_id` does not delete later page keys it does not rewrite.

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
| D-005 | Spark for Bronze/Silver; dbt-trino for Gold | Approved |
| D-012 | Bronze / Silver / Gold; seven Gold schemas plus `gold_shared.dim_date` | Approved |
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
| 2 | Lakehouse PoC, medallion, dbt | In progress (P2-01–P2-08 verified) |
| 3 | Airflow daily/weekly, backfills, quality | Not started |
| 4 | Streaming CDC | Not started |
| 5 | Observability, lineage, FinOps model | Not started |
| 6 | CI/CD expansion, resilience, security | Not started |
| 7 | AWS path as Terraform IaC | Not started |
| 8 | JupyterLab exploration and Superset charts over Gold (read-only via Trino) | Not started (after Phase 2; parallel with 3–7) |
| 9 | Platform report site (hosting + polish) | Not started (early `site/` scaffold exists) |

---

## Risks and limitations

- P2-02 proved one table on the Iceberg JDBC catalog: Spark writes, Trino 484 reads. An overwrite leaves the previous Parquet files in the bucket. JDBC is not the Glue protocol. The Hadoop catalog remains the P2-01 probe only.
- P2-03 stores Bronze contract fields as `STRING` on that same catalog. P2-04 Silver reads those tables, casts by contract type, and deduplicates per source module.
- P2-06 Gold writes through Trino CTAS (`dbt-trino`). Quarantine findings are staged from MinIO JSONL into `gold_staging` before quality facts. The Spark load script names the staging object path. Finding values stay in that JSONL file.
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

### 2026-11-11 — Site figure table

The scaffold still has no charts. The roadmap growth scheme now lists ten figures for Phase 9: nine Superset charts and one JupyterLab image of the same on-time rate. P8-06 exports them. Detail: [`docs/status-log.md`](docs/status-log.md).

### 2026-11-11 — Phase 8 exploration and BI inserted

Phase 8 is JupyterLab (host) plus Apache Superset (Compose), both read-only through Trino, and may start after Phase 2. The platform report site is Phase 9 (P9-01–P9-07). The Phase 8 close pair is P8-06 then P8-07. D-017 stays `proposed` until the P8-01 PoC. No notebook, Superset service, or test was added.

### 2026-11-11 — Log exposure fix

Quarantine staging no longer inlines finding values in SQL. `make stage-quarantine` writes warehouse JSONL and loads it with Spark. Spark runners pass the JDBC password and S3 keys through a properties file. `tests/unit/infrastructure/test_spark_credentials.py` fails if another `docker/spark/*.sh` assigns those keys. The agent rule is `.cursor/rules/05-runtime-data-exposure.mdc`. `uv run ruff check src tests` passed. `uv run ruff format --check src tests` passed. `uv run pytest tests/unit` passed 294. `make stage-quarantine` was not re-run on the live stack. Detail: [`docs/architecture/gold-layer.md`](docs/architecture/gold-layer.md).

### 2026-11-11 — P2-07 verified

`dbt test` PASS=288 against the existing Gold tables. Bronze checks read `.local/bronze-source-counts.tsv` from the run. Silver checks return `ok` from the contract dedup policy. Money sums use a 0.01 tolerance. `dim_municipality` was dropped and rebuilt (5_571 rows) after missing Iceberg metadata. `mart_storefront_user_behavior` was rebuilt (5 rows). Unit tests on this tree: 286 passed. `make live-bronze` and `make live-silver` were not re-run. Reconciliation stays out of `make live-gold` (`dbt build --exclude tag:reconciliation`) and is run with `make dbt-test-reconciliation`. Detail: [`docs/status-log.md`](docs/status-log.md).

### 2026-11-06 — P2-06 verified

dbt-trino `1.11.0` builds 55 Gold tables from Silver. Quarantine staging wrote 1_603 sidecar rows. On-time rate 0.932; `mart_late_orders` 6_534. D-005 and D-012 approved. Detail: [`docs/architecture/gold-layer.md`](docs/architecture/gold-layer.md).

### 2026-11-06 — Gold schemas revised

The model design now opens with seven business schemas: marketplace operations, customer CRM, seller acquisition, Brazil geography, catalog, storefront, and OLTP operations. On-time rate and `mart_late_orders` carry the delay analysis. Each schema has a quality-findings fact. Contract `governance.domain` stays the source family. Detail: [`docs/architecture/gold-model-design.md`](docs/architecture/gold-model-design.md).

### 2026-11-06 — P2-05 verified

Gold grain, keys, refresh rules, and marts are in [`docs/architecture/gold-model-design.md`](docs/architecture/gold-model-design.md). Payment rows have no refund status; cancellation after approval is the money-at-risk mart. Silver stays Spark. SCD Type 2 is a single Phase 4 issue, P4-08, and the phase still closes on P4-09 then P4-10. No dbt model was built in that issue.
