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
