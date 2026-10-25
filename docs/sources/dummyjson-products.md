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

Page with `skip += limit` until `skip >= total`. The default page size is 30, and `limit` must be an integer greater than or equal to 1. Do not map DummyJSON `id` to Olist `product_id`. Unit tests use fixtures only.

Each response is one raw object:

`raw/dummyjson_products/products/dt={YYYY-MM-DD}/execution_id={execution_id}/products-skip-{skip:06d}.json`

The object body is the response bytes. `source_id` is `dummyjson_products`. The contract domain is `product_analysis`. Gold marts for that domain stay in P2-05.

Requests send `User-Agent: open-data-platform/<version> (dummyjson-products)`. Cloudflare rejects the default Python urllib agent with HTTP 403. `brand` may be absent. On a short final page the response `limit` is the number of products returned, not the requested page size.

The client retries timeouts, HTTP 429, and HTTP 5xx, with at most 3 attempts. The pause is 1s, then 2s, then 4s when another attempt is configured. A numeric `Retry-After` on HTTP 429 sets that pause and is capped at 30 seconds. An HTTP-date `Retry-After` leaves the 1s/2s/4s backoff in place. Other 4xx responses and redirects fail on the first response. Error text omits the response body. A response larger than 5 MiB fails. A reported `total` that needs more than 1000 pages fails before the first write.

A page whose envelope or product keys differ from `config/contracts/dummyjson_products/products.yaml` fails the batch before the next write. Pages already written for that execution stay in place. Nulls and duplicate ids stay in the stored page. Quarantine of those rows is P1-10.

The same `--execution-id` and `--ingestion-ts` overwrite the page keys from that run. A shorter catalog on that same execution leaves any later page keys that this run does not rewrite. A new `execution_id` is the audit path for a new snapshot.

From the repository root, with `ODP_RAW_BUCKET` and MinIO settings in the environment:

```bash
uv run python -m open_data_platform.ingestion.dummyjson_products --execution-id run-001
```

Optional flags: `--ingestion-ts` (timezone-aware ISO-8601), `--limit`, and `--base-url` (API origin, default `https://dummyjson.com`).
