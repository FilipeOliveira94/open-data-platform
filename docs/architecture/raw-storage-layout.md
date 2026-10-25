# Raw storage layout and ingestion metadata

Conventions for Phase 1 auditable landing in object storage. Decision record: [ADR-0020](../decisions/0020-raw-storage-layout.md).

## Raw vs local staging vs Bronze

| Location | Role | When |
| --- | --- | --- |
| Object store `raw/…` | Auditable lakehouse landing (source of truth for ingested bytes) | Every batch writer (P1-04+) |
| Local `data/raw/` | Gitignored fetch/staging (e.g. Kaggle unzip) | Before upload; never committed (ADR-0018) |
| Bronze tables | Near-source **tabular** layer + metadata columns | Phase 2 (P2-03); not defined here |

## Bucket and configuration

| Setting | Purpose | Example |
| --- | --- | --- |
| `ODP_RAW_BUCKET` | Bucket for auditable raw objects | `odp-raw` |
| MinIO endpoint / creds | Same as Phase 0 (`MINIO_*`) | See `.env.example` |

Local MinIO and AWS S3 share the same key layout; only endpoint and credentials change.

## Object key layout

```text
raw/{source_id}/{dataset}/dt={YYYY-MM-DD}/execution_id={execution_id}/{object_name}
```

| Segment | Meaning |
| --- | --- |
| `raw` | Fixed layer prefix (not Bronze/Silver/Gold) |
| `source_id` | Stable source identifier (snake_case) |
| `dataset` | Dataset or entity within the source (snake_case) |
| `dt=` | UTC calendar date of the run (`ingestion_ts` date unless overridden) |
| `execution_id` | Single pipeline/run identifier (UUID or orchestrator run id) |
| `object_name` | File name within the run (e.g. `orders.csv`, `products.json`) |

### Examples

| Source | Example key |
| --- | --- |
| Olist e-commerce | `raw/olist_brazilian_ecommerce/orders/dt=2026-10-18/execution_id=run-001/olist_orders_dataset.csv` |
| Olist marketing funnel | `raw/olist_marketing_funnel/qualified_leads/dt=2026-10-18/execution_id=run-001/olist_marketing_qualified_leads_dataset.csv` |
| DummyJSON products | `raw/dummyjson_products/products/dt=2026-10-18/execution_id=run-001/products-skip-000000.json` |
| PostgreSQL extract | `raw/postgres_oltp/orders/dt=2026-10-18/execution_id=run-001/orders_2026-10-17_2026-10-18.json` |
| IBGE Localidades | `raw/ibge_localidades/municipios/dt=2026-10-18/execution_id=run-001/municipios.json` |
| Demo storefront clickstream | `raw/demo_storefront/clickstream/dt=2026-10-18/execution_id=run-001/events.jsonl` |

`source_id` / `dataset` values above are the writer conventions. Field lists, null rules, and classification live in [`config/contracts/`](../../config/contracts/) ([data contracts](data-contracts.md)).

## Ingestion metadata

Required on every raw write (S3/MinIO user-metadata and package helpers):

| Field | Type | Meaning |
| --- | --- | --- |
| `source_id` | string | Same as path segment |
| `execution_id` | string | Same as path segment |
| `_ingestion_ts` | string (UTC ISO-8601) | Processing/write time when the object was landed |

Time vocabulary (for later transforms; raw records write time as `_ingestion_ts`):

| Concept | In raw layer |
| --- | --- |
| Ingestion / processing time | `_ingestion_ts` (required) |
| Event time | Source-specific; not required on the object envelope |
| Business / watermark window | Encoded in `object_name` or future sidecar; not a mandatory path segment in P1-02 |

## Idempotency

- Same `(source_id, dataset, dt, execution_id, object_name)` → same object key.
- A re-run with the same execution identity **overwrites** the object (last write wins).
- Changing `execution_id` creates a new key (new run), which is intentional for audit of distinct executions.
- Record-level deduplication and quarantine are **not** raw-writer responsibilities (P1-10 / Silver).

## Package API

| Module | Responsibility |
| --- | --- |
| `open_data_platform.ingestion.paths` | Build and validate raw object keys |
| `open_data_platform.ingestion.metadata` | Typed metadata → object user-metadata map |
| `open_data_platform.ingestion.writer` | Put bytes to `ODP_RAW_BUCKET` with key + metadata |
| `open_data_platform.ingestion.http` | GET with the caller's retry, redirect, and response-size policy |
| `open_data_platform.ingestion.kaggle_download` | Download one Kaggle dataset zip and extract an expected file set |
| `open_data_platform.ingestion.olist` | Fetch the Olist snapshots and write each CSV through the raw writer |
| `open_data_platform.ingestion.dummyjson_products` | Fetch DummyJSON `/products` pages and write each page through the raw writer |
| `open_data_platform.ingestion.postgres_oltp` | Extract a half-open `updated_at` window from schema `oltp` and write one JSON array per dataset |
| `open_data_platform.ingestion.demo_storefront` | Generate a fictional storefront clickstream and write one JSONL object |

Olist historical ingestion, the DummyJSON products client, the PostgreSQL windowed extract, and the demo storefront generator call these helpers. Later source writers do the same. DummyJSON products calls `ingestion.http`. The Kaggle download client keeps its own redirect and credential handling.

## Out of scope (this document)

- Data contract field lists — [`data-contracts.md`](data-contracts.md) (this document keeps path and object metadata only)
- Other Phase 1 source clients (IBGE)
- Bronze/Iceberg tables — P2-03
- Quarantine layout — P1-10 / P3-07
