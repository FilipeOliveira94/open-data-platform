---
title: Pipeline
description: Verified raw ingestion and the lakehouse, orchestration, and CDC work that is not started.
---

Phase 1 raw ingestion is **verified**. Bronze, Silver, Gold, Airflow schedules, and streaming CDC are **not started**.

## Verified raw landing

Five sources write objects to MinIO. The key is `raw/{source_id}/{dataset}/dt={YYYY-MM-DD}/execution_id={execution_id}/{object_name}`. User metadata carries `source_id`, `execution_id`, and `_ingestion_ts`. The same execution identity overwrites that object. A new `execution_id` keeps the previous object.

| Source | Landing |
| --- | --- |
| Olist e-commerce and marketing funnel | One raw CSV per published file |
| DummyJSON `/products` | One JSON page per object |
| PostgreSQL OLTP | One JSON array per table for a half-open `updated_at` window |
| Demo storefront clickstream | One `events.jsonl` per execution |
| IBGE Localidades | One `municipios.json` array |

BrasilAPI CEP is withdrawn. Invalid rows stay in the raw object. Findings go to a `quarantine/…/{object_name}.jsonl` sidecar in the same bucket.

Index: [Phase 1 ingestion](https://github.com/FilipeOliveira94/open-data-platform/blob/main/docs/architecture/phase-1-ingestion.md).

## Update patterns

| Use case | Where it stands |
| --- | --- |
| Daily batch | PostgreSQL extract with a one-day UTC-midnight window. The Airflow schedule is Phase 3 |
| Weekly batch | The same extract with a seven-day window. The Airflow schedule is Phase 3 |
| Streaming | Phase 4. CDC does not replace the batch extract |

## Not started

| Status | Item |
| --- | --- |
| **Not started** | Bronze / Silver / Gold and dbt (Phase 2) |
| **Not started** | Airflow daily and weekly schedules, backfills (Phase 3) |
| **Not started** | Streaming CDC (Phase 4) |
