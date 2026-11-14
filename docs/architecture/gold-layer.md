# Gold layer

Business marts built with **dbt-trino** from [Silver](silver-layer.md). Grain, keys, and mart definitions stay in [Gold model design](gold-model-design.md). Decision context: [ADR-0005](../decisions/0005-compute-and-transform.md), [ADR-0012](../decisions/0012-medallion-layers.md).

## Pins (P2-06)

| Component | Version |
| --- | --- |
| dbt Core | `1.12.6` (via `dbt-trino`) |
| dbt-trino | `1.11.0` (group `dbt` in `pyproject.toml`; installed only in the runtime image venv `/opt/dbt-venv` since P3-03) |
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

Python reads those sidecars and writes one JSONL object at `gold/_staging/gold_staging/quarantine_findings/rows.jsonl` in `ODP_WAREHOUSE_BUCKET`. Spark loads that file into `iceberg.gold_staging.quarantine_findings` (`gold_staging` namespace on the JDBC catalog). The Spark SQL names the object path and the columns. Finding values stay in the JSONL object. The CLI prints the row count. An empty sidecar set still creates the table with zero rows and deletes the staging object. The staged columns are the finding reason, field, primary-key JSON (`business_key`), and object key. The sidecar's full `record` is not copied.

Quality facts union those rows with Silver-derived `business_anomaly` reasons from the model design. Reprocessing quarantined source rows remains P3-07.

## Quality and reconciliation

Schema tests run with the Gold build. Reconciliation does not.

- `column_types` compares `information_schema.columns` with a type-family prefix (`varchar`, `bigint`, `integer`, `double`, `decimal`, `real`, `timestamp`, `date`, `boolean`).
- Grain columns use `not_null` and `unique` or `unique_combination`. Closed sets from the contracts and the model design use `accepted_values`. Numeric bounds use `accepted_range`. Integer scores use `accepted_range` because `accepted_values` quotes every entry as varchar and Trino will not compare that list with an integer column.
- Orphan foreign keys stay in the tables (D-013). There is no relationship test that fails on them.
- Singular tests under `dbt/tests/reconciliation/<domain>/` fail by returning rows. One directory matches each Gold schema, including `gold_shared`. Population checks compare Gold keys with Silver (or with the parent fact). Measure checks compare counts and sums. Money sums use an absolute tolerance of `0.01`, the same epsilon as the payment-mismatch finding, because Trino `double` sums of the same values can differ in the last digits. The tag is `reconciliation`. `make dbt-build` and `make live-gold` exclude that tag. `make dbt-test-reconciliation` runs the full set. One domain is `dbt test --select tag:reconciliation,tag:<domain>`. The intended cadence is daily. The scheduler itself is Phase 3.

Bronze and Silver live checks are generated from the contracts and from `.local/bronze-source-counts.tsv`. See [Bronze](bronze-layer.md) and [Silver](silver-layer.md).

## Commands

```bash
docker compose --profile lakehouse up -d --wait trino
make stage-quarantine
make dbt-parse
make dbt-build
# or
make live-gold

# Separate from the medallion run. Intended cadence is daily.
make dbt-test-reconciliation
```

Since P3-03 dbt runs only in the shared runtime image (`docker/runtime`). The Make targets call `python -m open_data_platform.lakehouse.medallion_build dbt-parse|dbt-build|dbt-reconcile` in the `spark` service. Airflow calls the same module. The profile `dbt/profiles.yml` reaches Trino at `ODP_TRINO_HOST:ODP_TRINO_PORT` (default `trino:8080`) on the Compose network. dbt `target/` and logs go to the `odp_lakehouse_work` volume under the run label. The host does not install dbt. Before P3-03, dbt ran on the host through `uv run dbt` against `localhost:8080`; the 2026-11-06 result below used that path.

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

## Limitations

- dbt reaches Trino inside the Compose network. Trino stays on the `lakehouse` profile and still publishes `TRINO_PORT` for host clients.
- Quarantine staging replaces `gold_staging.quarantine_findings` through Spark (drop, create, insert), so `make stage-quarantine` needs the JDBC catalog database and the runtime image Spark, same as Bronze. Trino reads the table afterwards. The staging JSONL is the finding payload at rest in the warehouse bucket. A malformed staging line can still surface in a Spark `FAILFAST` exception.
- Accidental schemas from early misconfigured runs should be dropped if present; `generate_schema_name` now uses the model schema as the Iceberg namespace with no `target.schema` prefix.
- dbt SSL warning from dbt-trino on HTTP local Trino is expected.
