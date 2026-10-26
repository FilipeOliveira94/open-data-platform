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
| Schema | `fields[]` with `name`, `type`, `nullable`, `classification`, `description` | Expected columns or object keys |
| Invalid rows | `on_invalid` (`quarantine` or `fail_batch`) | Declared row-level policy |
| Structural errors | `on_structural_error` | Always `fail_batch` (missing column, unreadable payload) |
| Change pattern | `update_pattern` | `snapshot`, `full_refresh`, or `incremental_window` |
| Governance | `governance.domain`, `governance.owner` | Gold domain and owning role |
| Provenance | `provenance.system` | Producer of the bytes |
| Evolution | `changelog[]` | Note for each `contract_version` |

Logical types are `string`, `integer`, `number`, `boolean`, `timestamp`, `date`, and `json`. They are not Iceberg or Spark types. That mapping waits for the Phase 2 compatibility proof.

Field classification is `public`, `internal`, or `pii`. The dataset classification is the strictest of its fields (envelope included). It is computed by the loader and is not stored, so the rollup cannot drift from the fields.

`governance.owner` is a role. Version 1 uses `platform`. `governance.domain` is `olist_analysis` or `product_analysis`.

`provenance.system` is the static producer name: `kaggle`, `dummyjson`, `postgres`, `ibge`, or `generator`. `generator` is the in-repo demo storefront clickstream generator. Execution lineage stays on the raw object (`source_id`, `execution_id`, `_ingestion_ts` from P1-02). Job and column lineage is emitted later from real runs (OpenLineage and dbt, D-010). Contracts do not list downstream models.

## Invalid records

Every shipped contract uses `on_invalid: quarantine` and `on_structural_error: fail_batch`. The loader checks that those values are in the allowed set. Runtime classification is `open_data_platform.contracts.records`. Each ingestor calls `prepare_quarantine` and then `write_raw_object` for the raw payload. The sidecar put also goes through `write_raw_object` with the `quarantine` prefix. Reprocessing a quarantined row is P3-07. Choosing which duplicate survives is P2-04.

A structural error fails the batch before that object is written. That covers an unreadable payload, a CSV header or data row whose width does not match the contract, a JSON key set that does not match, and an empty IBGE array. Objects already written in the same execution stay, including any sidecar they already have. The failing object gets no raw put and no quarantine put.

After the shape is readable, each record is classified. The raw bytes are still stored whole.

| Finding | When |
| --- | --- |
| none | Null in a nullable field, including a nullable key that is omitted |
| `null_violation` | Null in a non-nullable field, or a null primary-key value. An empty CSV cell is null |
| `type_mismatch` | Value has the wrong type. CSV integers are base-10 without a leading zero. CSV numbers are plain decimals. CSV booleans are `true`, `false`, `True`, or `False`. `timestamp` and `date` are non-empty text, with no calendar check. `json` is an object or an array |
| `duplicate_primary_key` | The primary key repeats inside that object. Every row in the group is reported. A null primary key is not grouped. An empty `primary_key` skips this check |

Olist geolocation is the empty-key case: the ZIP prefix repeats and is not a duplicate finding.

`on_invalid: quarantine` writes the original payload under `raw/` and the findings under:

```text
quarantine/{source_id}/{dataset}/dt={YYYY-MM-DD}/execution_id={execution_id}/{object_name}.jsonl
```

The sidecar is in the same bucket (`ODP_RAW_BUCKET`). Each line has `record_index`, `reason`, `field`, `primary_key`, and `record`. The same execution identity overwrites that key. A rerun with no invalid records deletes it. Another `execution_id` keeps the previous sidecar.

`on_invalid: fail_batch` writes neither object. No shipped contract uses that policy. A synthetic contract covers it in the unit tests.

## Evolution

Raise `contract_version` and append a `changelog` note in the same change as any field, nullability, key, or classification edit. Do not rewrite history in place. There is no migration engine in this issue.

## Sources in version 1

| `source_id` | Datasets | `update_pattern` | `provenance.system` |
| --- | --- | --- | --- |
| `olist_brazilian_ecommerce` | 9 published CSV datasets | `snapshot` | `kaggle` |
| `olist_marketing_funnel` | `qualified_leads`, `closed_deals` | `snapshot` | `kaggle` |
| `dummyjson_products` | `products` (page envelope + item) | `full_refresh` | `dummyjson` |
| `postgres_oltp` | `sellers`, `customers`, `orders`, `order_items` | `incremental_window` | `postgres` |
| `ibge_localidades` | `municipios` (`view=nivelado`) | `full_refresh` | `ibge` |
| `demo_storefront` | `clickstream` (one user action per JSONL line) | `snapshot` | `generator` |

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

- Reprocessing a quarantined record (P3-07)
- Choosing a surviving duplicate (P2-04)
- OpenLineage events, discovery catalogs, retention periods, SLAs, and cost tags
- Masking or tokenization of `pii` fields
- Iceberg schemas
