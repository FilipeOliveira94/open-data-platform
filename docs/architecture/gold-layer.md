# Gold layer

Business marts built with **dbt-trino** from [Silver](silver-layer.md). Grain, keys, and mart definitions stay in [Gold model design](gold-model-design.md). Decision context: [ADR-0005](../decisions/0005-compute-and-transform.md), [ADR-0012](../decisions/0012-medallion-layers.md).

## Pins (P2-06)

| Component | Version |
| --- | --- |
| dbt Core | `1.12.6` (via `dbt-trino`) |
| dbt-trino | `1.11.0` (`>=1.11.0,<1.12` in `pyproject.toml`) |
| Trino | `trinodb/trino:484` (Compose `lakehouse` profile) |
| Catalog | Iceberg JDBC `local` on PostgreSQL (`ICEBERG_CATALOG_DB`) |
| Warehouse | `s3a://{ODP_WAREHOUSE_BUCKET}/gold/{schema}/{table}` |

Silver remains the **Spark** writer. Gold only reads `iceberg.silver.*`.

## Layout

| Iceberg schema | Contents |
| --- | --- |
| `gold_shared` | `dim_date` |
| `brazil_geography` | Municipality and ZIP-prefix dimensions + quality findings |
| `marketplace_operations` | Olist ops facts, dims, marts |
| `customer_crm` | Customers, reviews, CRM marts |
| `seller_acquisition` | Marketing funnel facts and marts |
| `catalog_commerce` | DummyJSON catalog |
| `storefront_behavior` | Demo clickstream |
| `oltp_operations` | PostgreSQL OLTP star |
| `gold_staging` | `quarantine_findings` (MinIO JSONL staged for contract violations) |

Every model is a **full refresh** (`table` + `on_table_exists: replace`). Dimensions are type 1. SCD Type 2 is P4-08 only.

One ephemeral helper (`int_order_primary_payment`) supports marketplace marts and is not a Gold table.

## Quarantine staging

Contract-violation rows live under `quarantine/{source_id}/{dataset}/…/*.jsonl` in `ODP_RAW_BUCKET`. Before `dbt build`:

```bash
make stage-quarantine
```

That replaces `iceberg.gold_staging.quarantine_findings`. Quality facts union those rows with Silver-derived `business_anomaly` reasons from the model design. Reprocessing quarantined source rows remains P3-07.

## Commands

```bash
docker compose --profile lakehouse up -d --wait trino
make stage-quarantine
make dbt-parse
make dbt-build
# or
make live-gold
```

Profile: `dbt/profiles.yml` (localhost Trino on `TRINO_PORT`, default `8080`). Compose publishes that port for host-side dbt.

## Verification (2026-11-06)

Against Silver from `odp-raw` / `execution_id=run-001` (same snapshot as the model-design profile):

| Check | Result |
| --- | --- |
| `dbt build` | PASS=55 table models |
| `mart_late_orders` | 6_534 |
| On-time rate (`sum(on_time)/sum(eligible)` on `mart_delivery_performance`) | 0.932 (89_936 / 96_470 eligible) |
| `dim_date` | 3_774 days (2016-09-01 … 2026-12-31) |
| `dim_zip_prefix` | 19_015 |
| `dim_customers` | 96_096 |
| Quarantine staged | 1_603 rows (legacy `order_reviews` sidecar under contract v1 keying) |

Formal Bronze→Silver→Gold consistency close remains [P2-09](../../ROADMAP.md). Reconciliation tests are [P2-07](../../ROADMAP.md).

## Limitations

- Host dbt needs Trino’s published port (`TRINO_PORT`); the service stays on the `lakehouse` profile.
- Quarantine staging builds one CTAS from VALUES; large sidecars are acceptable for this local dataset size.
- Accidental schemas from early misconfigured runs should be dropped if present; `generate_schema_name` now uses the model schema as the Iceberg namespace with no `target.schema` prefix.
- dbt SSL warning from dbt-trino on HTTP local Trino is expected.
