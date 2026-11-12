# P2-09 — Live run, result, and consistency check

One local run of the documented Phase 2 path on 2026-11-11 (UTC-3). dbt's own log clock was UTC. The run overwrote Bronze, Silver, and Gold. It did not delete raw objects or Docker volumes. Earlier layer snapshots stay in [Bronze](bronze-layer.md), [Silver](silver-layer.md), and [Gold](gold-layer.md). Partitioning, file sizing, the incremental strategy, and the phase limitations are in [Phase 2 lakehouse](phase-2-lakehouse.md).

## Versions

| Piece | Version observed |
| --- | --- |
| PostgreSQL image | `postgres:16.15-bookworm` |
| MinIO image | `coollabsio/minio:RELEASE.2025-10-15T17-29-55Z` |
| Spark image | `spark:3.5.8-java17` |
| Iceberg runtime | `org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.12.0` |
| Hadoop AWS | `org.apache.hadoop:hadoop-aws:3.3.4` |
| AWS SDK bundle | `com.amazonaws:aws-java-sdk-bundle:1.12.262` |
| PostgreSQL JDBC | `org.postgresql:postgresql:42.7.14` |
| Trino image | `trinodb/trino:484` |
| dbt Core | `1.12.6` (`uv run dbt --version`) |
| dbt-trino | `1.11.0` |

`docker compose ps` showed `minio`, `postgres`, `spark`, and `trino` up. Every Spark submit in this run reported `0 artifacts copied, 6 already retrieved` from `.local/spark-ivy`. `chmod -R` on that cache printed `Permission denied` for jars owned by the Spark uid. The recipe continues, and no jar was downloaded.

## Commands

`ensure_spark_ivy` is one Makefile line. A newline there was starting a second shell, so the `.env` sourced above the macro never reached Python or `psql`. The documented targets below ran after that fix. `tests/unit/infrastructure/test_make_lakehouse_env.py` locks the single-shell recipe.

```bash
make test-catalog-probe
make live-bronze
make live-silver
make live-gold
make dbt-test-reconciliation
```

| Command | Result | Wall time |
| --- | --- | --- |
| `make test-catalog-probe` | Spark printed `2`. Trino printed `"2"`. Exit 0 | 17.7 s |
| `make live-bronze` | 18/18 Trino counts matched this run's raw counts. Exit 0 | 359 s |
| `make live-silver` | 20/20 checks returned `ok`. Exit 0 | 344 s |
| `make live-gold` | Quarantine CLI printed `1603`. `dbt build` PASS=287 WARN=0 ERROR=0 SKIP=0 TOTAL=287. Exit 0 | 53.7 s |
| `make dbt-test-reconciliation` | PASS=56 WARN=0 ERROR=0 SKIP=0 TOTAL=56. Exit 0 | 558 s |

`make live-gold` is `stage-quarantine` then `dbt build --exclude tag:reconciliation`. The PASS=287 line is 55 table models and 232 schema tests. Reconciliation is the separate command. dbt printed its existing HTTP certificate warning on local Trino. The build still passed.

## Bronze counts

Each check is `COUNT(*)` on `iceberg.bronze.*` against the raw row count written to `.local/bronze-source-counts.tsv` by this run.

| Table | Rows |
| --- | --- |
| `demo_storefront__clickstream` | 100 |
| `dummyjson_products__products` | 194 |
| `ibge_localidades__municipios` | 5_571 |
| `olist_brazilian_ecommerce__customers` | 99_441 |
| `olist_brazilian_ecommerce__geolocation` | 1_000_163 |
| `olist_brazilian_ecommerce__order_items` | 112_650 |
| `olist_brazilian_ecommerce__order_payments` | 103_886 |
| `olist_brazilian_ecommerce__order_reviews` | 99_224 |
| `olist_brazilian_ecommerce__orders` | 99_441 |
| `olist_brazilian_ecommerce__product_category_name_translation` | 71 |
| `olist_brazilian_ecommerce__products` | 32_951 |
| `olist_brazilian_ecommerce__sellers` | 3_095 |
| `olist_marketing_funnel__closed_deals` | 842 |
| `olist_marketing_funnel__qualified_leads` | 8_000 |
| `postgres_oltp__customers` | 3 |
| `postgres_oltp__order_items` | 3 |
| `postgres_oltp__orders` | 3 |
| `postgres_oltp__sellers` | 5 |

## Silver

The current checker emits 18 relational statements plus two shape statements. All 20 returned `ok`. Relational checks compare the Silver count with the Bronze dedup groups. The shape statements require expanded DummyJSON columns and a product id on cart and purchase events.

Trino aggregates for the anchors named in the 2026-11-05 Silver note:

| Anchor | This run |
| --- | --- |
| `olist_brazilian_ecommerce__geolocation` | 738_332 |
| `dummyjson_products__products` | 194 |
| `postgres_oltp` customers / orders / order items / sellers | 2 / 2 / 2 / 3 |
| `demo_storefront__clickstream` rows with null `product_id` | 16 |

Geolocation 1_000_163 → 738_332 and postgres_oltp 3/3/3/5 → 2/2/2/3 are the documented dedup, same as that note.

## Gold row counts

`dbt build` created these 55 tables. `gold_staging.quarantine_findings` is the Spark staging table, not a dbt model. Its CLI count was 1_603, and Trino `COUNT(*)` after the build was 1_603.

| Table | Rows |
| --- | --- |
| `brazil_geography.dim_municipality` | 5_571 |
| `brazil_geography.dim_zip_prefix` | 19_015 |
| `brazil_geography.fct_geography_quality_findings` | 8 |
| `catalog_commerce.dim_catalog_products` | 194 |
| `catalog_commerce.fct_catalog_quality_findings` | 1 |
| `catalog_commerce.fct_catalog_reviews` | 582 |
| `catalog_commerce.mart_catalog_prices` | 24 |
| `catalog_commerce.mart_catalog_review_patterns` | 108 |
| `customer_crm.dim_customers` | 96_096 |
| `customer_crm.fct_crm_quality_findings` | 2_497 |
| `customer_crm.fct_reviews` | 99_224 |
| `customer_crm.mart_customer_geography` | 4_020 |
| `customer_crm.mart_customer_retention` | 93_358 |
| `customer_crm.mart_review_drivers` | 265 |
| `gold_shared.dim_date` | 3_774 |
| `marketplace_operations.dim_products` | 32_951 |
| `marketplace_operations.dim_sellers` | 3_095 |
| `marketplace_operations.fct_operations_quality_findings` | 1_769 |
| `marketplace_operations.fct_order_items` | 112_650 |
| `marketplace_operations.fct_orders` | 99_441 |
| `marketplace_operations.fct_payments` | 103_886 |
| `marketplace_operations.mart_delivery_performance` | 565 |
| `marketplace_operations.mart_freight` | 107 |
| `marketplace_operations.mart_late_orders` | 6_534 |
| `marketplace_operations.mart_order_cancellations` | 122 |
| `marketplace_operations.mart_order_milestones` | 91 |
| `marketplace_operations.mart_payment_mix` | 28 |
| `marketplace_operations.mart_platform_sales` | 634 |
| `marketplace_operations.mart_seller_buyer_geography` | 60_921 |
| `marketplace_operations.mart_seller_performance` | 3_095 |
| `oltp_operations.dim_oltp_customers` | 2 |
| `oltp_operations.dim_oltp_sellers` | 3 |
| `oltp_operations.fct_oltp_order_items` | 2 |
| `oltp_operations.fct_oltp_orders` | 2 |
| `oltp_operations.fct_oltp_quality_findings` | 3 |
| `oltp_operations.mart_oltp_buyer_geography` | 2 |
| `oltp_operations.mart_oltp_cancellations` | 0 |
| `oltp_operations.mart_oltp_delivery_performance` | 1 |
| `oltp_operations.mart_oltp_freight` | 2 |
| `oltp_operations.mart_oltp_late_orders` | 0 |
| `oltp_operations.mart_oltp_order_milestones` | 1 |
| `oltp_operations.mart_oltp_platform_sales` | 2 |
| `oltp_operations.mart_oltp_seller_performance` | 2 |
| `seller_acquisition.fct_acquisition_quality_findings` | 523 |
| `seller_acquisition.fct_closed_deals` | 842 |
| `seller_acquisition.fct_leads` | 8_000 |
| `seller_acquisition.mart_lead_outcome` | 8_000 |
| `seller_acquisition.mart_marketing_acquisition` | 11 |
| `storefront_behavior.dim_storefront_products` | 50 |
| `storefront_behavior.dim_storefront_users` | 5 |
| `storefront_behavior.fct_storefront_events` | 100 |
| `storefront_behavior.fct_storefront_quality_findings` | 0 |
| `storefront_behavior.mart_storefront_product_behavior` | 50 |
| `storefront_behavior.mart_storefront_session` | 20 |
| `storefront_behavior.mart_storefront_user_behavior` | 5 |

On-time delivery from `mart_delivery_performance`: `sum(on_time_count)` 89_936, `sum(late_count)` 6_534, `sum(delivered_with_both_dates)` 96_470. 89_936 + 6_534 = 96_470. The rate 89_936 / 96_470 rounds to 0.932.

The 56 reconciliation tests compare Gold keys, counts, and sums with Silver or with the parent fact. Money sums use the 0.01 tolerance already in those tests. All 56 passed. No reconciliation test returned a mismatch row.

## Comparison

Baseline: Bronze and Silver notes of 2026-11-05, Gold note of 2026-11-06, same `odp-raw` snapshot. Headline anchors match.

| Anchor | 2026-11-05 / 2026-11-06 | This run |
| --- | --- | --- |
| Bronze geolocation / customers / DummyJSON | 1_000_163 / 99_441 / 194 | same |
| Bronze postgres_oltp customers / orders / items / sellers | 3 / 3 / 3 / 5 | same |
| Silver geolocation | 738_332 | 738_332 |
| Silver DummyJSON | 194 | 194 |
| Silver postgres_oltp | 2 / 2 / 2 / 3 | 2 / 2 / 2 / 3 |
| Clickstream null `product_id` | 16 | 16 |
| `mart_late_orders` | 6_534 | 6_534 |
| On-time / eligible | 89_936 / 96_470 | 89_936 / 96_470 |
| `dim_customers` | 96_096 | 96_096 |
| `dim_zip_prefix` | 19_015 | 19_015 |
| `dim_date` | 3_774 | 3_774 |
| `dim_municipality` | 5_571 | 5_571 |
| Quarantine findings staged | 1_603 | 1_603 |

Checker inventory differs from the 2026-11-05 sentence that recorded Silver as 22/22. This run's generator, from P2-07, emits 20 statements: 18 relational checks and 2 shape checks. All 20 returned `ok`. The row anchors above are unchanged.

Quality inventory matches the P2-07 split. That run's `dbt test` was PASS=288 on Gold already built. This run executed the build and the tests on the fresh tables: 232 schema tests inside PASS=287, plus 56 reconciliation tests. 232 + 56 = 288. WARN=0 and ERROR=0 on both commands.

No row-count mismatch against the baseline. This run does not claim exactly-once. `INSERT OVERWRITE` still leaves previous Parquet files in the bucket; cleanup is P5-07.
