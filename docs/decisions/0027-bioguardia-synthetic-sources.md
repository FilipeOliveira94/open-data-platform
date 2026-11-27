# ADR-0027: BioGuardia Atlantic synthetic sources

| Field | Value |
| --- | --- |
| Status | `approved` |
| Date opened | 2026-11-26 |
| Date decided | 2026-11-27 |
| Owner | Platform architect |
| Index ID | D-027 |
| Related issues | P11-01, P11-02, P11-03; processing P11-04 to P11-18 |
| Supersedes | — |
| Superseded by | — |

## Context / problem

The platform's sources are small or static: Olist is a fixed release, DummyJSON and IBGE are a few thousand rows, the OLTP sample grows slowly, and the demo clickstream is a 100-event batch. None of them can test a pipeline at volume, keep a scheduled job busy every day, or keep a streaming job running around the clock. None carries the cases Filipe asked to demonstrate: PII masking, data skew, SCD Type 2 from changing master data, and reconciliation between systems.

Filipe asked (2026-11-26) for one big daily batch source with new data for every future daily run (at least 20M rows a day, at most 2 GB a day), and one streaming source at 1M events per hour that keeps feeding a broker while Airflow is down. The domain is a fictitious environmental agency in the Atlantic Forest: BioGuardia Atlantic.

## Decision

Build three synthetic sources in this repository, outside the platform's ingestion code (`src/open_data_platform/simulators/bioguardia/`), run as Compose services that do not depend on Airflow:

- `bioguardia_ops`: a daily file drop of 21 operational tables (22 on the first day of a month) in MinIO bucket `odp-landing`, released at 02:00 BRT for the previous local day, with a manifest of control totals and a `_SUCCESS` marker written last. About 24M rows and 357 MB of gzip (3.2 GB uncompressed) per day at scale 1, measured; a drop above 2 GB of gzip fails.
- `bioguardia_finance`: a daily drop of `fine_payments`, released at 06:00 BRT.
- `bioguardia_telemetry`: device telemetry on Kafka at about 1M events per hour (broker: [ADR-0028](0028-telemetry-message-broker.md)).

One deterministic world model drives all three. Synthetic personal data is built so that it cannot be a real person's: CPF and CNPJ with wrong check digits, emails under the reserved `.example` domain, phone area codes ANATEL has not assigned. Defects are planted on purpose and listed in a separate bucket, `odp-evaluation`, that the ingestion never reads. The model is described in [`docs/architecture/bioguardia-domain.md`](../architecture/bioguardia-domain.md).

P11-01 to P11-03 start before Phase 3 closes (waiver recorded in `DECISIONS.md`, 2026-11-26): they change no platform table, DAG, or contract. P11-04 onward wait for Phase 3 to be verified.

## Alternatives considered

| Option | Pros | Cons | Why not (if rejected) |
| --- | --- | --- | --- |
| Generated sources from one world model (chosen) | Volume set by a scale factor; PII, skew, SCD2 and reconciliation cases built in; planted defects make checks exact; no public API in tests | The data is synthetic; the model has to be maintained | — |
| Real open data (INPE fire hotspots, ANA river gauges, GBIF occurrences) | Real distributions | Licenses differ per source; volume cannot be set; no personal data to mask; defects cannot be planted; tests must not depend on public APIs (AGENTS.md section 5); none streams at 1M events per hour | Rejected for the core; a later issue may add one as enrichment |
| Faker (`pt_BR`) | Quick to write | Its CPF and CNPJ pass the checksum and can match real people and companies; slow at 20M rows a day; its output can change between releases | Rejected |
| Extend the PostgreSQL OLTP and capture it with CDC | Reuses the Phase 4 path | Telemetry at 1M events per hour is not change data capture; it would mix two domains in one database and load PostgreSQL with writes | Rejected; Phase 4 keeps CDC on the OLTP |
| Raise the demo clickstream to 1M events per hour | Already exists | Batch generator with no device dimension, so no SCD2 as-of case; no relation to a batch source | Rejected |

## Consequences and trade-offs

- New ingestion patterns for the platform: a file drop with a manifest (P11-05) and a 24/7 stream (P11-09).
- Storage grows every day; the budget is in the domain document and P11-15 sets retention.
- Two new Compose profiles (`sources`, `streaming`), one new image (`odp-sources`), and two new buckets (`odp-landing`, `odp-evaluation`).
- The generator's output is reproducible: the same seed, scale and date give the same content, so a test or a reconciliation can recompute what a drop holds.

## Evidence / validation

P11-02 (2026-11-26): 37 ops drops (2026-11-26 to 2026-12-31 plus a version 2 of 2026-12-15) and 36 finance drops generated at scale 1: 880,848,289 rows and 13.19 GB of gzip in all, about 23.8M rows and 356 MB of gzip per day, every drop under the 2 GB cap. Regenerating a date gives byte-identical files (same SHA-256), and a killed run leaves no partial drop. P11-03 (2026-11-26 and 27): the stream delivered 1,022,299 messages in 60 minutes across 00:00 UTC (100.01% of the model), and `telemetry verify` found every modeled event after a planned 10-minute stop and an unplanned 2 h 50 min stop. Detail: the source notes ([ops](../sources/bioguardia-ops-export.md#measured-volume), [telemetry](../sources/bioguardia-telemetry.md#measured-volume)) and [`docs/status-log.md`](../status-log.md).

## Revisit when

- A drop at scale 1 exceeds 2 GB or takes too long to generate on the development host.
- A real open dataset with a compatible license is wanted for enrichment.
- The domain needs binary media (audio, images, rasters), which is a roadmap candidate today.

## Notes

Phone area codes: ANATEL open data [`PGCN.csv`](https://www.anatel.gov.br/dadosabertos/PDA/Codigo_Nacional/PGCN.csv) lists 67 assigned codes (checked 2026-11-26); the generator uses only codes outside that list. IBGE municipality codes come from the IBGE Localidades API (checked 2026-11-26), the same source as P1-08.
