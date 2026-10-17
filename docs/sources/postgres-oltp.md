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

Mutable tables need explicit PKs and `updated_at` (or equivalent). Minimal v1 (lock in P1-03): `sellers`, `customers`, `orders`, `order_items`.
