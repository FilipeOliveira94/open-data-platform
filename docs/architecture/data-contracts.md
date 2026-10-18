# Data contracts

Explicit contracts for the Phase 1 sources that writers will land in raw storage. Decision record: [ADR-0013](../decisions/0013-data-contracts.md) (still `proposed`). Files: [`config/contracts/`](../../config/contracts/).

## What a contract is

One YAML file per `source_id` / `dataset` pair:

`config/contracts/{source_id}/{dataset}.yaml`

The directory and file stem must match `source_id` and `dataset`. Those identifiers use the same segment rules as raw object keys ([raw storage layout](raw-storage-layout.md)).

| Block | Fields | Meaning |
| --- | --- | --- |
| Identity | `contract_version`, `source_id`, `dataset`, `description`, `grain`, `primary_key` | What one record is, and which fields identify it |
| Payload | `payload_format` (`csv` or `json`), optional `record_locator` + `envelope_fields` | How the raw object is shaped |
| Schema | `fields[]` with `name`, `type`, `nullable`, `classification`, `description` | Expected columns or object keys |
| Invalid rows | `on_invalid` (`quarantine` or `fail_batch`) | Declared row-level policy |
| Structural errors | `on_structural_error` | Always `fail_batch` (missing column, unreadable payload) |
| Change pattern | `update_pattern` | `snapshot`, `full_refresh`, or `incremental_window` |
| Governance | `governance.domain`, `governance.owner` | Gold domain and owning role |
| Provenance | `provenance.system` | External system that produces the bytes |
| Evolution | `changelog[]` | Note for each `contract_version` |

Logical types are `string`, `integer`, `number`, `boolean`, `timestamp`, `date`, and `json`. They are not Iceberg or Spark types. That mapping waits for the Phase 2 compatibility proof.

Field classification is `public`, `internal`, or `pii`. The dataset classification is the strictest of its fields (envelope included). It is computed by the loader and is not stored, so the rollup cannot drift from the fields.

`governance.owner` is a role. Version 1 uses `platform`. `governance.domain` is `olist_analysis` or `product_analysis`.

`provenance.system` is the static upstream name: `kaggle`, `dummyjson`, `postgres`, or `ibge`. Execution lineage stays on the raw object (`source_id`, `execution_id`, `_ingestion_ts` from P1-02). Job and column lineage is emitted later from real runs (OpenLineage and dbt, D-010). Contracts do not list downstream models.

## Invalid records

Every shipped contract uses `on_invalid: quarantine` and `on_structural_error: fail_batch`. The loader checks that those values are in the allowed set. It does not read source files, call APIs, or write a quarantine path. Runtime handling is P1-10 and P3-07.

An empty `primary_key` is valid when the published dataset has no unique key. Olist geolocation is that case: the ZIP prefix repeats.

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

Olist column `product_name_lenght` and `product_description_lenght` keep the published spelling.

DummyJSON `id` is not Olist `product_id`. Nested product objects (`dimensions`, `reviews`, `meta`, `tags`, `images`) stay type `json`. Reviewer names and emails inside `reviews` are synthetic demo values and are classified `public` with the rest of that API.

PostgreSQL v1 adds `updated_at` (`timestamp`, not null) on each table. It is the watermark for windowed extract. Other columns follow the Olist dataset of the same grain. The raw payload is a JSON array of rows.

IBGE `municipios` is a JSON array. UF and macro-region attributes are columns on that row (`UF-*`, `regiao-*`). Micro and meso region id and name are nullable. Separate region and UF endpoint contracts are not part of version 1.

Out of this set: synthetic events (P1-07 owns that schema) and BrasilAPI CEP (optional P1-09).

## Package

`open_data_platform.contracts` loads the YAML with PyYAML (`safe_load`) and rejects unknown keys, missing nullability, keys that are not fields, and `source_id` / `dataset` values that cannot appear in a raw object key.

`config/contracts` is a repository directory. The loader finds it by walking parents of the module. It is not installed as package data.

## Out of scope

- Reading or validating source payloads
- Quarantine storage and reprocessing
- OpenLineage events, discovery catalogs, retention periods, SLAs, and cost tags
- Masking or tokenization of `pii` fields
- Iceberg schemas
