# IBGE Localidades

| Field | Value |
| --- | --- |
| Name | IBGE Localidades |
| Type | HTTP (collection) |
| Role | Brazilian locality reference (regions, UFs, municipalities) for `olist_analysis` |
| URL | https://servicodados.ibge.gov.br/api/docs/localidades |
| Gold Domain | `olist_analysis` |
| License | Public government API; no auth |
| Access | `GET /api/v1/localidades/municipios?orderBy=nome&view=nivelado` |
| Decision | ADR-0019 / D-019 |
| Related Issue | P1-08 |

One GET returns the full municipality list. `view=nivelado` flattens UF and region attributes onto each municipality. `orderBy=nome` keeps the stored row order stable. Separate region and UF endpoints are not datasets in contract version 1. This client does not paginate and does not replace DummyJSON.

Each run writes one object:

`raw/ibge_localidades/municipios/dt={YYYY-MM-DD}/execution_id={execution_id}/municipios.json`

The object body is the JSON entity. IBGE sends ``Content-Encoding: gzip``, which is decoded before the write. The 64 MiB cap applies to the decoded bytes. A response without that header is stored as received. `source_id` is `ibge_localidades`. The contract domain is `olist_analysis`. `municipio-id` and `UF-sigla` are the reference attributes for later geography in that domain. No Gold model is built here.

Requests send `User-Agent: open-data-platform/<version> (ibge-localidades)`.

The client retries timeouts, HTTP 429, and HTTP 5xx, with at most 3 attempts. The pause is 1s, then 2s, then 4s when another attempt is configured. A numeric `Retry-After` on HTTP 429 sets that pause and is capped at 30 seconds. An HTTP-date `Retry-After` leaves the 1s/2s/4s backoff in place. Other 4xx responses and redirects fail on the first response. Error text omits the response body. A response larger than 64 MiB fails. Redirects are not followed.

An array whose items are not objects, or whose keys differ from `config/contracts/ibge_localidades/municipios.yaml`, fails before the write. An empty array fails before the write. Null micro and meso region values stay in the object and are not quarantined. A null `municipio-id` stays in the object and is copied to the quarantine sidecar as `null_violation`. A repeated `municipio-id` stays in the object, and every row in that group is `duplicate_primary_key`. The sidecar key ends in `municipios.json.jsonl` under `quarantine/ibge_localidades/municipios/`.

The same `--execution-id` and `--ingestion-ts` overwrite `municipios.json`. A new `execution_id` keeps the previous object.

From the repository root, with `ODP_RAW_BUCKET` and MinIO settings in the environment:

```bash
uv run python -m open_data_platform.ingestion.ibge_localidades --execution-id run-001
```

Optional flags: `--ingestion-ts` (timezone-aware ISO-8601) and `--base-url` (API origin, default `https://servicodados.ibge.gov.br`).
