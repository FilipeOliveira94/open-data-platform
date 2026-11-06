# Project Status

Operational ledger for the **current phase**. Issue backlog and acceptance text live in [`ROADMAP.md`](ROADMAP.md). Full decision index: [`DECISIONS.md`](DECISIONS.md). Chronological archive: [`docs/status-log.md`](docs/status-log.md).

| | |
| --- | --- |
| Repository | [FilipeOliveira94/open-data-platform](https://github.com/FilipeOliveira94/open-data-platform) |
| Started | 2026-10-14 |
| Last updated | 2026-11-06 (P2-06 dbt Gold) |
| Current phase | Phase 2 — Lakehouse and analytics |
| Phase status | In progress |

**Status values:** not started · in progress · delivered · verified · blocked · withdrawn

**Verified** means acceptance checks were run and evidence is recorded (here and/or in `docs/status-log.md`). Do not treat planned architecture as complete.

**What belongs here:** current objective, current-phase issue progress, phase acceptance checklist, latest validation, short foundation baseline, decisions that gate *this* work, active risks, blockers, next work.

**What does not:** cumulative issue inventories (use `ROADMAP.md`), full ADR tables (use `DECISIONS.md`), long history (use `docs/status-log.md`).

---

## Current objective

**P2-07** — Data quality and reconciliation tests.

P2-06 verified Gold with dbt-trino ([`docs/architecture/gold-layer.md`](docs/architecture/gold-layer.md)). Silver stays the Spark writer. Formal Bronze→Silver→Gold consistency is P2-09. The phase closes on P2-09, then P2-10.

## Next work

1. **P2-07** — dbt/Python quality and reconciliation tests against source/Silver counts for the run (replace hardcoded `live_counts` snapshot pins where the roadmap requires).

---

## Phase 2 progress

| ID | Title | Status | Evidence |
| --- | --- | --- | --- |
| P2-01 | Minimal Spark + Iceberg + catalog + query PoC | Verified | Two runs returned count 2. [`docs/architecture/phase-2-spark-iceberg-poc.md`](docs/architecture/phase-2-spark-iceberg-poc.md) |
| P2-02 | Local catalog and query-engine choice | Verified | Spark and Trino both returned 2 on two runs. [`docs/architecture/phase-2-catalog-query.md`](docs/architecture/phase-2-catalog-query.md) |
| P2-03 | Bronze layer from raw ingestion | Verified | Full `odp-raw` live. Trino 18/18 (`live_counts.BRONZE_COUNTS`). [`docs/architecture/bronze-layer.md`](docs/architecture/bronze-layer.md) |
| P2-04 | Silver normalization and deduplication | Verified | Full Bronze→Silver live. Trino 22/22 (18 counts + 4 shape). Geolocation 1_000_163→738_332. [`docs/architecture/silver-layer.md`](docs/architecture/silver-layer.md) |
| P2-05 | Analytical model design (grain, keys, SCD) | Verified | [`docs/architecture/gold-model-design.md`](docs/architecture/gold-model-design.md) |
| P2-06 | dbt project and initial Gold models | Verified | `dbt build` PASS=55. On-time rate 0.932; late orders 6_534. [`docs/architecture/gold-layer.md`](docs/architecture/gold-layer.md) |
| P2-07 | Data quality and reconciliation tests | Not started | — |
| P2-08 | Spark lakehouse jar / Ivy cache | Verified | Host `.local/spark-ivy` → `/tmp/.ivy2`. Second warm: 6 already retrieved. [`docs/architecture/phase-2-spark-ivy-cache.md`](docs/architecture/phase-2-spark-ivy-cache.md) |
| P2-09 | Live run, result, and consistency check | Not started | — |
| P2-10 | Phase 2 documentation and status | Not started | — |

Acceptance detail: `ROADMAP.md` → Phase 2.

### Phase 2 acceptance

- [x] A table can be created, written and queried in the chosen local stack.
- [ ] Queries and transforms are reproducible via documented commands.
- [ ] Quality and reconciliation rules have tests.
- [ ] Documentation lists component versions and known limitations.
- [ ] A live run result is recorded and compared to the Phase 2 contracts (P2-09).
- [ ] Documentation and `PROJECT_STATUS.md` cite that result (P2-10).

---

## Latest validation

| | |
| --- | --- |
| Date | 2026-11-06 |
| Environment | WSL2 / Ubuntu. PostgreSQL, MinIO, Trino healthy; Silver from `odp-raw` / `run-001` present |
| Checks | `make stage-quarantine`; `uv run dbt build --project-dir dbt --profiles-dir dbt`; smoke Trino on-time rate; `uv run ruff check src tests`; `uv run pytest tests/unit` |
| Result | Quarantine staged 1_603 rows. `dbt build` PASS=55. On-time rate 0.932 (eligible 96_470); `mart_late_orders` 6_534. Unit tests passed. Detail: [`docs/architecture/gold-layer.md`](docs/architecture/gold-layer.md) |

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
| 2 | Lakehouse PoC, medallion, dbt | In progress (P2-01–P2-06 and P2-08 verified) |
| 3 | Airflow daily/weekly, backfills, quality | Not started |
| 4 | Streaming CDC | Not started |
| 5 | Observability, lineage, FinOps model | Not started |
| 6 | CI/CD expansion, resilience, security | Not started |
| 7 | AWS path as Terraform IaC | Not started |
| 8 | Platform report site (hosting + polish) | Not started (early `site/` scaffold exists) |

---

## Risks and limitations

- P2-02 proved one table on the Iceberg JDBC catalog: Spark writes, Trino 484 reads. An overwrite leaves the previous Parquet files in the bucket. JDBC is not the Glue protocol. The Hadoop catalog remains the P2-01 probe only.
- P2-03 stores Bronze contract fields as `STRING` on that same catalog. P2-04 Silver reads those tables, casts by contract type, and deduplicates per source module.
- P2-06 Gold writes through Trino CTAS (`dbt-trino`). Quarantine findings are staged from MinIO JSONL into `gold_staging` before quality facts.
- Olist e-commerce + marketing funnel are **CC BY-NC-SA 4.0**: do not vendor; fetch locally (ADR-0018). MIT covers software/docs only.
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

### 2026-11-06 — P2-06 verified

dbt-trino `1.11.0` builds 55 Gold tables from Silver. Quarantine staging wrote 1_603 sidecar rows. On-time rate 0.932; `mart_late_orders` 6_534. D-005 and D-012 approved. Detail: [`docs/architecture/gold-layer.md`](docs/architecture/gold-layer.md).

### 2026-11-06 — Gold schemas revised

The model design now opens with seven business schemas: marketplace operations, customer CRM, seller acquisition, Brazil geography, catalog, storefront, and OLTP operations. On-time rate and `mart_late_orders` carry the delay analysis. Each schema has a quality-findings fact. Contract `governance.domain` stays the source family. Detail: [`docs/architecture/gold-model-design.md`](docs/architecture/gold-model-design.md).

### 2026-11-06 — P2-05 verified

Gold grain, keys, refresh rules, and marts are in [`docs/architecture/gold-model-design.md`](docs/architecture/gold-model-design.md). Payment rows have no refund status; cancellation after approval is the money-at-risk mart. Silver stays Spark. SCD Type 2 is a single Phase 4 issue, P4-08, and the phase still closes on P4-09 then P4-10. No dbt model was built in that issue.
