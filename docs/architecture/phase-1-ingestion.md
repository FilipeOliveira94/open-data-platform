# Phase 1 ingestion

Index of the verified multi-source raw landing. Field lists live in [data contracts](data-contracts.md). Object keys and metadata live in [raw storage layout](raw-storage-layout.md). Per-source commands live under [`docs/sources/`](../sources/).

Phase 1 lands auditable bytes in the object store. It does not build Bronze, Silver, or Gold tables, Airflow schedules, or streaming CDC.

## Sources

| Source | `source_id` | Issue | Note |
| --- | --- | --- | --- |
| Olist Brazilian e-commerce | `olist_brazilian_ecommerce` | P1-04 | [olist-brazilian-ecommerce.md](../sources/olist-brazilian-ecommerce.md) |
| Olist marketing funnel | `olist_marketing_funnel` | P1-04 | [olist-marketing-funnel.md](../sources/olist-marketing-funnel.md) |
| DummyJSON products | `dummyjson_products` | P1-05 | [dummyjson-products.md](../sources/dummyjson-products.md) |
| PostgreSQL OLTP | `postgres_oltp` | P1-06 | [postgres-oltp.md](../sources/postgres-oltp.md) |
| Demo storefront clickstream | `demo_storefront` | P1-07 | [demo-storefront.md](../sources/demo-storefront.md) |
| IBGE Localidades | `ibge_localidades` | P1-08 | [ibge-localidades.md](../sources/ibge-localidades.md) |
| BrasilAPI CEP | — | P1-09 | Withdrawn ([ADR-0021](../decisions/0021-withdraw-brasilapi-cep.md)) |

Olist files are CC BY-NC-SA 4.0 and are not vendored (ADR-0018). Fetch them into gitignored `data/raw/`. DummyJSON and IBGE are the HTTP sources (ADR-0019).

Each writer is a module under `open_data_platform.ingestion`. Run it from the repository root with `ODP_RAW_BUCKET` and the MinIO settings in the environment. The source note has the command and flags.

## Traceability

Every raw object uses this key:

```text
raw/{source_id}/{dataset}/dt={YYYY-MM-DD}/execution_id={execution_id}/{object_name}
```

`dt=` is the UTC calendar date of `_ingestion_ts`. The object user-metadata carries `source_id`, `execution_id`, and `_ingestion_ts`. Event time stays on the record when the source has one. The PostgreSQL window is encoded in the object name (`{dataset}_{start}_{end}.json`), not in the path.

## Repeatable runs

The same `(source_id, dataset, dt, execution_id, object_name)` overwrites that object. A new `execution_id` keeps the previous object.

Invalid rows stay in the raw object. Findings go to `quarantine/{source_id}/{dataset}/dt=…/execution_id=…/{object_name}.jsonl` in the same bucket. The same execution overwrites that sidecar. A clean rerun deletes it. Structural errors fail before that object is written and leave earlier objects from the execution in place. Detail: [data contracts](data-contracts.md).

A shorter DummyJSON catalog on the same `execution_id` does not delete later page keys that the run does not rewrite. A new `execution_id` is the audit path for a new snapshot.

## Update patterns

CDC does not replace scheduled batch. Daily and weekly refreshes share the PostgreSQL extract. Streaming is a later phase.

| Use case | Phase 1 | Later |
| --- | --- | --- |
| Daily batch | `extract_postgres_oltp` with a one-day window. Both bounds are UTC midnight. The predicate is `updated_at >= start AND updated_at < end`. | Airflow daily schedule (P3-04). ADR-0007 stays `proposed`. |
| Weekly batch | The same function with a seven-day window. | Airflow weekly schedule (P3-05). |
| Streaming | Not in this phase. | Logical replication, Debezium, Kafka, and Spark Structured Streaming (Phase 4). ADR-0008 and ADR-0014 stay `proposed`. |

The clickstream, Olist snapshots, DummyJSON, and IBGE are batch landings. They are not the daily, weekly, or CDC paths.

## Evidence

Recorded on the Phase 1 issues. This page does not add a new live run.

| Check | Result |
| --- | --- |
| Unit tests do not call the public API | Fixtures and fakes in `tests/unit` |
| Same execution overwrites; a new execution keeps the previous object | P1-02 writer; P1-04–P1-08 integration round-trips on local MinIO |
| Raw key and metadata | P1-02 (`source_id`, `execution_id`, `_ingestion_ts`) |
| Contracts | `config/contracts/`; P1-03 |
| Invalid records | P1-10. Live matrix: 40 passed (2026-10-26). Unit suite at that review: 216 passed |
| PostgreSQL window | P1-06. One-day and seven-day windows call the same function |
| Phase close | 2026-10-28: `uv run ruff check src tests` PASS; `uv run ruff format --check src tests` PASS; `uv run pytest tests/unit` → 216 passed. No live API, Kaggle, or MinIO |

## Limitations

- The staged Olist snapshot still has 1_603 `order_reviews` rows with a repeated `review_id`. Those rows stay in the CSV and are quarantined.
- `product_category_name_translation.csv` may start with a UTF-8 BOM. The header check ignores it. The raw bytes keep it.
- Reprocessing a quarantined row is P3-07. Choosing which duplicate survives is P2-04.
- Exactly-once end-to-end is not claimed.

## Out of scope

Bronze, Silver, and Gold (Phase 2). Airflow schedules and backfills (Phase 3). Streaming CDC (Phase 4). AWS Terraform apply (Phase 7).
