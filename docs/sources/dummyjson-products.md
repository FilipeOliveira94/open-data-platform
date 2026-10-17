# DummyJSON products

| Field | Value |
| --- | --- |
| Name | DummyJSON products |
| Type | HTTP (paginated) |
| Role | Paginated product catalog for HTTP ingest demos; separate from Olist products |
| URL | https://dummyjson.com/docs/products |
| Gold Domain | `product_analysis` |
| License | Public demo API (no auth); respect publisher terms |
| Access | `GET /products?limit={n}&skip={n}` — response includes `products`, `total`, `skip`, `limit` |
| Decision | ADR-0019 / D-019 |
| Related Issue | P1-05 |

Page with `skip += limit` until `skip >= total`. Do not map DummyJSON `id` to Olist `product_id`. Unit tests use fixtures only.
