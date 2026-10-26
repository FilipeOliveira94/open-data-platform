# PostgreSQL OLTP

| Field | Value |
| --- | --- |
| Name | PostgreSQL OLTP |
| Type | Database (seed + CDC) |
| Role | Windowed batch extract and CDC change stream in the Olist domain |
| URL | Local Compose service (`docker-compose.yml`; not a public dataset URL) |
| Gold Domain | `olist_analysis` |
| License | Seed from Olist under CC BY-NC-SA 4.0 constraints, or synthetic Olist-like rows in CI |
| Access | App connection via env placeholders; seed from local `data/raw/…` or synthetics |
| Decision | ADR-0007 / ADR-0008 (patterns); seed notes with D-019 sources |
| Related Issue | P1-06 (extract); Phase 4 (CDC) |

Minimal v1 is locked in [`config/contracts/postgres_oltp/`](../../config/contracts/postgres_oltp/): `sellers`, `customers`, `orders`, `order_items`. Each table has an explicit primary key and a non-null `updated_at` watermark for windowed extract. Other columns follow the Olist dataset of the same grain. Vocabulary: [`docs/architecture/data-contracts.md`](../architecture/data-contracts.md).

## Schema and window

The tables live in schema `oltp` inside the Compose database (`POSTGRES_DB`, default `odp`). DDL: `src/open_data_platform/ingestion/postgres_oltp/schema.sql`. `updated_at` is `timestamptz`.

The extract predicate is half-open:

```text
updated_at >= start AND updated_at < end
```

`start` and `end` are timezone-aware instants normalized to UTC, and both must fall on UTC midnight. A daily call passes a one-day window. A weekly call passes a seven-day window. Both calls use `extract_postgres_oltp`. Any other pair of UTC midnights uses that same function. The caller supplies both bounds. This module does not store a high-water mark.

Each dataset becomes one JSON array. The object name is `{dataset}_{start_date}_{end_date}.json`, using the UTC dates of the bounds. Example: `orders_2026-10-17_2026-10-18.json`. The `dt=` partition stays the ingestion date ([raw storage layout](../architecture/raw-storage-layout.md)). A window with no rows is stored as `[]`. The same `execution_id`, ingestion timestamp, and window overwrite that object. A structural mismatch fails the batch and leaves objects already written for that execution in place. A null or a value of the wrong JSON type stays in the array and is copied to the quarantine sidecar. A naive timestamp or a non-finite number fails before the write. The synthetic seed still rejects a duplicated fixture key before it loads.

Grain and keys stay on the v1 contracts.

## Commands

`seed` creates the schema when it is missing and replaces every row in the four `oltp` tables with `tests/fixtures/postgres_oltp/synthetic.json`. The fixture is synthetic. Olist files stay outside git. Re-running `seed` loads that fixture again.

```bash
uv run python -m open_data_platform.ingestion.postgres_oltp seed
```

```bash
uv run python -m open_data_platform.ingestion.postgres_oltp extract \
  --window-start 2026-10-17T00:00:00Z \
  --window-end 2026-10-18T00:00:00Z \
  --execution-id run-001 \
  --ingestion-ts 2026-10-25T12:00:00Z
```

Connection settings are `POSTGRES_HOST`, `POSTGRES_PORT`, `POSTGRES_USER`, `POSTGRES_PASSWORD`, and `POSTGRES_DB`. The raw bucket is `ODP_RAW_BUCKET`. A weekly window is the same command with `--window-end` seven days after `--window-start`.

Airflow schedules and catchup are Phase 3. CDC is Phase 4.
