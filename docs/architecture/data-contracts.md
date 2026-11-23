# Data contracts

Explicit contracts for the Phase 1 sources that writers will land in raw storage. Decision record: [ADR-0013](../decisions/0013-data-contracts.md) (`approved`). Files: [`config/contracts/`](../../config/contracts/).

## What a contract is

One YAML file per `source_id` / `dataset` pair:

`config/contracts/{source_id}/{dataset}.yaml`

The directory and file stem must match `source_id` and `dataset`. Those identifiers use the same segment rules as raw object keys ([raw storage layout](raw-storage-layout.md)).

| Block | Fields | Meaning |
| --- | --- | --- |
| Identity | `contract_version`, `source_id`, `dataset`, `description`, `grain`, `primary_key` | What one record is, and which fields identify it |
| Payload | `payload_format` (`csv`, `json`, or `jsonl`), optional `record_locator` + `envelope_fields` | How the raw object is shaped |
| Schema | `fields[]` with `name`, `type`, `nullable`, `classification`, `description`; `precision` and `scale` on a `decimal` field | Expected columns or object keys |
| Invalid rows | `on_invalid` (`quarantine_exclude`, `quarantine_flag`, or `fail_batch`) | Declared row-level policy: [Silver handling](#silver-handling-of-quarantined-records-p3-07) |
| Volume guard | `volume_guard.min_row_ratio` (in `(0, 1]`) | Required on `snapshot` and `full_refresh`, rejected on the others: [volume guard](#volume-guard-p3-07) |
| Structural errors | `on_structural_error` | Always `fail_batch` (missing column, unreadable payload) |
| Change pattern | `update_pattern` | `snapshot`, `full_refresh`, `incremental_window`, or `append_only`. Downstream behavior: [Update patterns](#update-patterns-p3-12) |
| Governance | `governance.source_family`, `governance.owner` | Ingest source family and owning role |
| Provenance | `provenance.system` | Producer of the bytes |
| Evolution | `changelog[]` | Note for each `contract_version` |

Logical types are `string`, `integer`, `number`, `decimal`, `boolean`, `timestamp`, `date`, and `json`. `number` is a binary double. `decimal` is exact and needs `precision` (1 to 38 digits) and `scale` (0 to `precision` digits after the point); the loader rejects them on any other type. Bronze stores each field as Iceberg `STRING` ([bronze layer](bronze-layer.md)). Casting to the logical type is [Silver](silver-layer.md) (P2-04).

Field classification is `public`, `internal`, or `pii`. The dataset classification is the strictest of its fields (envelope included). It is computed by the loader and is not stored, so the rollup cannot drift from the fields.

`governance.owner` is a role. Version 1 uses `platform`. `governance.source_family` is the ingest label: `olist_analysis` (Olist files, marketing funnel, IBGE, PostgreSQL) or `product_analysis` (DummyJSON, clickstream). It is not a Gold schema. Gold schemas are listed in [Gold model design](gold-model-design.md) and [ADR-0012](../decisions/0012-medallion-layers.md). One source family feeds several Gold schemas.

`provenance.system` is the static producer name: `kaggle`, `dummyjson`, `postgres`, `ibge`, or `generator`. `generator` is the in-repo demo storefront clickstream generator. Execution lineage stays on the raw object (`source_id`, `execution_id`, `_ingestion_ts` from P1-02). Job and column lineage is emitted later from real runs (OpenLineage and dbt, D-010). Contracts do not list downstream models.

## Invalid records

Every shipped contract uses `on_invalid: quarantine_exclude` or `quarantine_flag`, and `on_structural_error: fail_batch`. The loader checks that those values are in the allowed set. Both `quarantine_*` policies write the same sidecar at ingestion; they differ only in what Silver does. Runtime classification is `open_data_platform.contracts.records`. Each ingestor calls `prepare_quarantine` and then `write_raw_object` for the raw payload. The sidecar put also goes through `write_raw_object` with the `quarantine` prefix. Reprocessing a quarantined row is described [below](#reprocessing-a-quarantined-record-p3-07). Choosing which duplicate survives in Silver is documented in [silver layer](silver-layer.md).

A structural error fails the batch before that object is written. That covers an unreadable payload, a CSV header or data row whose width does not match the contract, a JSON key set that does not match, and an empty IBGE array. Objects already written in the same execution stay, including any sidecar they already have. The failing object gets no raw put and no quarantine put.

After the shape is readable, each record is classified. The raw bytes are still stored whole.

| Finding | When |
| --- | --- |
| none | Null in a nullable field, including a nullable key that is omitted |
| `null_violation` | Null in a non-nullable field, or a null primary-key value. An empty CSV cell is null |
| `type_mismatch` | Value has the wrong type. CSV integers are base-10 without a leading zero. CSV numbers are plain decimals. A `decimal` is a JSON number or a plain CSV decimal with at most `scale` digits after the point, as written, and at most `precision - scale` before it; a value that would need rounding, or one already read as a binary float, is a mismatch. CSV booleans are `true`, `false`, `True`, or `False`. `timestamp` and `date` are non-empty text, with no calendar check. `json` is an object or an array |
| `duplicate_primary_key` | The primary key repeats inside that object. Every row in the group is reported. A null primary key is not grouped. An empty `primary_key` skips this check |

Olist geolocation is the empty-key case: the ZIP prefix repeats and is not a duplicate finding.

`on_invalid: quarantine_exclude` and `quarantine_flag` write the original payload under `raw/` and the findings under:

```text
quarantine/{source_id}/{dataset}/dt={YYYY-MM-DD}/execution_id={execution_id}/{object_name}.jsonl
```

The sidecar is in the same bucket (`ODP_RAW_BUCKET`). Each line has `record_index`, `reason`, `field`, `primary_key`, and `record`. The same execution identity overwrites that key. A rerun with no invalid records deletes it. Another `execution_id` keeps the previous sidecar.

`on_invalid: fail_batch` writes neither object. No shipped contract uses that policy. A synthetic contract covers it in the unit tests.

### Silver handling of quarantined records (P3-07)

Bronze reads the sidecar of each raw object it promotes and stores the reasons of each record, sorted and comma-joined, in `_quarantine_reasons` (NULL for a clean record). The record values in the sidecar are not copied. Silver then applies the contract:

| `on_invalid` | Silver | Gold `silver_action` |
| --- | --- | --- |
| `quarantine_exclude` | The record is dropped before deduplication and counted as `quarantine_excluded` | `excluded` |
| `quarantine_flag` | The record is kept with `_quarantined = true` and its `_quarantine_reasons`, and counted as `quarantine_flagged` | `flagged` |

Every Silver table has both columns; in an exclude dataset `_quarantined` is always false.

| Source | `on_invalid` | Why |
| --- | --- | --- |
| `postgres_oltp` (4 datasets) | `quarantine_exclude` | Operational facts: an invalid order or item would change GMV and counts |
| `demo_storefront.clickstream` | `quarantine_exclude` | An invalid event should not become a session |
| `olist_brazilian_ecommerce` (9) | `quarantine_flag` | A fixed historical release: excluding breaks joins and the published counts. In `order_reviews` the sidecar quarantines every row of a duplicate key group, so excluding would remove real reviews |
| `olist_marketing_funnel` (2) | `quarantine_flag` | Same reason |
| `dummyjson_products.products` | `quarantine_flag` | Catalog dimension referenced by facts |
| `ibge_localidades.municipios` | `quarantine_flag` | Reference dimension |

### Quality counts (P3-07)

The Silver script of each dataset records, after its replace, one row per metric in `iceberg.silver_audit.quality_counts` with the key `(run_label, source_id, dataset, column_name, metric)`. A rerun of the same run replaces its rows; a metric dropped between two attempts of one run would stay, and the Silver check, which counts the metrics of the latest run, then fails (P3-08). Zero counts are recorded. Counts are over the Bronze rows Silver reads (the selection of the update pattern), before exclusion and deduplication.

| Metric | `column_name` | Counts |
| --- | --- | --- |
| `cast_null` | The Silver column | Rows where the source text is not NULL and the typed value is NULL: a failed `TRY_CAST`, timestamp or date parse, decimal that does not fit, boolean that is not `true` or `false`, or a DummyJSON JSON path or array that does not parse. Plain strings are not counted |
| `null_key_dropped` | The key columns, comma-joined | Rows dropped because a key column is NULL (for geolocation, any payload column) |
| `quarantine_excluded` or `quarantine_flagged` | `_quarantine_reasons` | Rows with a sidecar finding |

The Silver check reads that table: the latest run that wrote counts for a dataset recorded every metric, and an exclude table holds no `_quarantined` row. The statements name tables, columns, paths, and the run label; no value reaches the SQL.

### Volume guard (P3-07)

A `snapshot` or `full_refresh` Silver table is the latest complete execution, so a short fetch would remove records. Before the replace, the Silver script compares the Bronze row counts of the latest and the previous complete execution (same order as the Silver selection). When `latest < previous * min_row_ratio`, `assert_true` fails the script with both counts: Silver keeps its previous snapshot, the dataset stays pending for Silver, and the Airflow `silver` task fails. The first execution has no previous one and passes.

| Source | `min_row_ratio` |
| --- | --- |
| `olist_brazilian_ecommerce`, `olist_marketing_funnel` | 1.0 (a fixed release never shrinks) |
| `dummyjson_products.products` | 0.9 |
| `ibge_localidades.municipios` | 0.99 |

To accept a real shrink, run the refresh with `--allow-volume-drop SOURCE.DATASET` (repeatable), for example `make live-refresh REFRESH_FLAGS="--allow-volume-drop dummyjson_products.products"`. The flag skips that dataset's guard for that run only; a name that is not a guarded dataset fails the step.

### Reprocessing a quarantined record (P3-07)

| Situation | Path |
| --- | --- |
| The source sends a corrected record | Ingest the window again. A clean rerun of the same execution deletes the sidecar; the next refresh re-promotes the object by its new ETag, the Bronze row loses `_quarantine_reasons`, Silver rebuilds, and the finding leaves Gold |
| The contract was wrong (a type too narrow, a field that is nullable) | Change the contract, bump `contract_version`, and re-ingest the affected executions so the sidecar is classified again; then `make live-rebuild` |
| The record stays invalid | Nothing: it stays in raw, in the sidecar, in Bronze, and in the Gold findings with its `silver_action` |

## Update patterns (P3-12)

`update_pattern` defines what Bronze keeps, which Bronze rows Silver selects survivors from, what a re-ingest does, and what happens to a record that disappears from the source. The Silver writer is `render_silver_sql` in `lakehouse/silver/common.py`; the Silver reconciliation check in `lakehouse/reconciliation.py` reads Bronze the same way.

### Completion marker

An execution is the raw prefix `raw/{source_id}/{dataset}/dt={YYYY-MM-DD}/execution_id={execution_id}/`. Every ingestion entry point deletes that prefix's `_COMPLETE` marker before its first object and writes the marker after its last object (`ingestion.writer.clear_completion_marker` and `write_completion_marker`). The marker is JSON with `object_names`, `object_count`, and `completed_at`; it holds names, not row values. A run that fails halfway, such as a DummyJSON page set that stops after page 0, has no marker. A retry under the same identity clears and rewrites it.

Bronze reads each marker and fills `_execution_completed_ts` on every row: the marker's `completed_at` when its `object_names` equal the data objects found under that prefix, otherwise null. A missing marker, an unreadable one, or a different object set leaves the execution incomplete. Bronze keeps every execution, complete or not, for audit.

### Behavior by pattern

| `update_pattern` | Silver reads | A new run of the same data | A record removed at the source | Sources |
| --- | --- | --- | --- | --- |
| `snapshot` | Only the latest complete execution: greatest `_execution_completed_ts`, then `dt`, then `execution_id`. Survivors are deduplicated by key inside it | Same rows and counts. Lineage columns point to the newer execution | Leaves Silver (a static release has none) | Olist, marketing funnel |
| `full_refresh` | Same as `snapshot` | Replaces the previous execution in Silver | Leaves Silver, and Gold on its next build | DummyJSON, IBGE |
| `incremental_window` | Every window, complete or not; latest `updated_at` per key (unchanged from P2-04) | Same key: no duplicate | Invisible on the batch path: a hard delete is never extracted, so the row stays in Silver and Gold. CDC (Phase 4) is the path that handles deletes | PostgreSQL |
| `append_only` | Every complete execution; a repeated key keeps the first ingested row | A repeated event key is kept once | Not applicable: events are immutable | Storefront clickstream |

`incremental_window` windows may overlap. Since P3-05 the weekly PostgreSQL re-extract (`ingest_postgres_oltp_weekly`, window `[W-8, W-1)`) lands each day a second time to pick up late commits; Silver keeps one row per key, and a hard delete stays invisible on this path ([Local Airflow](airflow-local.md#weekly-postgresql-re-extract-p3-05)).

For `snapshot`, `full_refresh`, and `append_only`, a Silver build with no complete execution in Bronze fails before the replace (`assert_true` ahead of `CREATE OR REPLACE TABLE`), so the table keeps its previous snapshot instead of becoming empty. `incremental_window` window completeness is left to the coverage manifest of P3-06.

`append_only` keeps `event_time` as the event clock. A late event (ingested after events with a later `event_time`) keeps its own `event_time`, and Gold groups by it (`fct_storefront_events`), not by ingestion time.

Raw objects written before P3-12 have no marker. Their executions are incomplete, so snapshot sources need one new ingestion run before Silver builds again. The 2026-11-20 lake rebuild re-ingested every snapshot source ([status log](../status-log.md)).

Evidence: unit tests in `tests/unit/lakehouse/test_silver.py`, `test_bronze.py`, `test_reconciliation.py`, and the ingestion tests; the live check `scripts/p3_12_refresh_check.sh` (result in the [status log](../status-log.md)).

## Money (P3-18)

A money field is `decimal`, never `number`. JSON readers (`contracts.decimals.json_loads`) parse a number with a fraction as a `Decimal`, and writers (`json_dumps`) write a `Decimal` as its exact text, so `10.50` stays `10.50` from the source to Silver. The PostgreSQL extract writes `numeric(12,2)` as that text, with no float conversion.

| Source | Dataset | Field | Type | Currency |
| --- | --- | --- | --- | --- |
| `olist_brazilian_ecommerce` | `order_items` (v4) | `price`, `freight_value` | `decimal(12,2)` | BRL |
| `olist_brazilian_ecommerce` | `order_payments` (v3) | `payment_value` | `decimal(12,2)` | BRL |
| `olist_marketing_funnel` | `closed_deals` (v5) | `declared_monthly_revenue` | `decimal(12,2)` | BRL (declared by the seller) |
| `postgres_oltp` | `order_items` (v3) | `price`, `freight_value` | `decimal(12,2)` (PostgreSQL `numeric(12,2)`) | BRL (the synthetic OLTP mirrors Olist) |
| `dummyjson_products` | `products` (v4) | `price` | `decimal(12,2)` | Not stated by the API |

The local Olist files fit `decimal(12,2)`: at most two decimal places, largest values 6_735.00 (`price`), 409.68 (`freight_value`), 13_664.08 (`payment_value`), and 50_000_000.0 (`declared_monthly_revenue`), measured on 2026-11-22.

Currency rules:

- Every field holds one currency, named in its `description`. No contract has a currency column, and no source mixes currencies in a field.
- The scale is the currency's minor unit, per field: `decimal(p,0)` for JPY or CLP, `decimal(p,3)` for KWD or BHD. `(12,2)` belongs to the BRL fields above, not to the type.
- A value with more digits than the field allows is never rounded. It is a `type_mismatch` finding at ingestion, Bronze keeps its text, and Silver casts it to NULL ([silver layer](silver-layer.md)). A source that starts sending a three-decimal currency shows up as findings, not as a wrong total.
- A field that mixes currencies is out of scope until a source needs it. That issue adds a required ISO 4217 `currency_code` next to the amount, a scale that covers the largest accepted minor unit with a per-row check against the row's currency, Gold sums grouped by currency and never across currencies, and conversion to BRL only through a dated rate with an explicit rounding rule, keeping the original amount, currency, rate, and converted amount. Phase 10 brings the BCB US dollar selling rate (SGS series 1), one such rate.

### Pipeline design checklist (P3-18)

| Question | P3-18 answer |
| --- | --- |
| Change type | No `update_pattern` changes. The column type changes: re-reading the same execution gives the same rows, now `DECIMAL` |
| Window and watermark | Unchanged: same windows, same `updated_at` watermark. Missed windows follow P3-06 and P3-16 |
| Re-run and backfill | `make live-rebuild` re-reads every complete raw object under the new contracts (`CREATE OR REPLACE`). An old PostgreSQL raw object carries `10.5`, a new one `10.50`: both are the same `DECIMAL`, so re-extracting a covered window changes no count and no sum |
| Deletes | Unchanged. The batch path does not see a hard delete (CDC, Phase 4); `snapshot` and `full_refresh` follow the latest complete execution |
| What a run reads | A scheduled refresh reads what it read before. The one-off rebuild reads all raw history |
| Reader view | Silver swaps type and rows in one commit, so a reader sees the `DOUBLE` table or the `DECIMAL` one, never a missing table. Gold replaces one model at a time (review R6, P6-09): during the rebuild some Gold models can still be `double` |
| Ownership and concurrency | Same writers (Spark Bronze and Silver, dbt Gold). The rebuild ran with the Airflow services stopped, so no `transform_lakehouse` run shared the tables or the Ivy cache |
| Trigger | No new trigger. A refresh started by an Asset before the rebuild retypes only the Silver datasets it rebuilds, and the `column_types` tests fail `dbt_build` until `make live-rebuild` runs |
| Failure | A failed rebuild leaves each table at its previous snapshot; the retry is another `make live-rebuild`. A value outside the contract is written exactly to raw, becomes a quarantine finding, and is NULL in Silver |
| Evidence | Unit tests (`tests/unit/contracts`, the extract, Bronze, Silver, and DummyJSON tests), `make dbt-unit`, `pytest tests/integration`, and the 2026-11-22 rebuild with exact reconciliation ([status log](../status-log.md)) |

## Evolution

Raise `contract_version` and append a `changelog` note in the same change as any field, nullability, key, or classification edit. Do not rewrite history in place. There is no migration engine in this issue. A type change takes effect on the next `make live-rebuild`, which rebuilds every table from all complete raw objects ([ADR-0024](../decisions/0024-incremental-lakehouse-refresh.md)); an incremental refresh retypes only the Silver datasets it rebuilds, and the Gold `column_types` tests fail until the rebuild.

## Sources in version 1

| `source_id` | Datasets | `update_pattern` | `provenance.system` |
| --- | --- | --- | --- |
| `olist_brazilian_ecommerce` | 9 published CSV datasets | `snapshot` | `kaggle` |
| `olist_marketing_funnel` | `qualified_leads`, `closed_deals` | `snapshot` | `kaggle` |
| `dummyjson_products` | `products` (page envelope + item) | `full_refresh` | `dummyjson` |
| `postgres_oltp` | `sellers`, `customers`, `orders`, `order_items` | `incremental_window` | `postgres` |
| `ibge_localidades` | `municipios` (`view=nivelado`) | `full_refresh` | `ibge` |
| `demo_storefront` | `clickstream` (one user action per JSONL line) | `append_only` (contract version 3; `snapshot` before P3-12) | `generator` |

Olist column `product_name_lenght` and `product_description_lenght` keep the published spelling.

DummyJSON `id` is not Olist `product_id`. Nested product objects (`dimensions`, `reviews`, `meta`, `tags`, `images`) stay type `json`. Reviewer names and emails inside `reviews` are synthetic demo values and are classified `public` with the rest of that API. `brand` is nullable: a live check on 2026-10-24 found products that omit the key. The response `limit` on a short final page is the number of products returned.

PostgreSQL v1 adds `updated_at` (`timestamp`, not null) on each table. It is the watermark for windowed extract. Other columns follow the Olist dataset of the same grain. The raw payload is a JSON array of rows.

IBGE `municipios` is a JSON array. UF and macro-region attributes are columns on that row (`UF-*`, `regiao-*`). Micro and meso region id and name are nullable. Separate region and UF endpoint contracts are not part of version 1.

Demo storefront `clickstream` is JSONL: one user action per line, with no envelope. `event_time` is UTC with millisecond precision and a `Z` suffix. Actions are spaced at 500 per second of event time. `product_id` is null on some `page_view` rows. Identifiers are generated tokens and do not join Olist or DummyJSON. Daily batch, weekly batch, and CDC stay on PostgreSQL.

BrasilAPI CEP is not a contracted source ([ADR-0021](../decisions/0021-withdraw-brasilapi-cep.md)). IBGE and Olist geolocation cover the joinable geography.

## Package

`open_data_platform.contracts` loads the YAML with PyYAML (`safe_load`) and rejects unknown keys, missing nullability, keys that are not fields, and `source_id` / `dataset` values that cannot appear in a raw object key. `contracts.records` classifies records after a writer has already accepted the payload shape. It does not open files or call a storage client.

`config/contracts` is a repository directory. The loader finds it by walking parents of the module. It is not installed as package data.

## Out of scope

- OpenLineage events, discovery catalogs, retention periods, SLAs, and cost tags
- Masking or tokenization of `pii` fields

Silver typing and survivor rules: [silver layer](silver-layer.md).
