# ADR-0019: HTTP API sources

| Field | Value |
| --- | --- |
| Status | approved |
| Date opened | 2026-10-17 |
| Date decided | 2026-10-17 |
| Owner | Program |
| Index ID | D-019 |
| Related issues | P1-05 (DummyJSON); P1-08 (IBGE); P1-09 (optional CEP) |
| Supersedes | — |
| Superseded by | — |

## Context / problem

Phase 1 needs HTTP ingestion distinct from Olist **files**. Olist has no public HTTP API. Choices must cover pagination (P1-05), Brazilian reference data, and optional CEP enrichment without conflating those patterns.

## Decision

1. **P1-05 (required):** **DummyJSON** `GET https://dummyjson.com/products` with `limit` / `skip` (response includes `total`). Separate product domain in Gold (`product_analysis`). No foreign keys into Olist `product_id`.
2. **IBGE Localidades (P1-08):** `https://servicodados.ibge.gov.br/api/v1/localidades/…` (e.g. municípios) as Brazilian geography reference. Client may chunk large JSON responses; it does **not** replace DummyJSON for P1-05 pagination acceptance.
3. **BrasilAPI CEP (P1-09, optional):** point lookups for zip enrichment; small cached samples only; respect anti-crawl terms. Not a P1-05 substitute.

Final source notes: [`docs/sources/`](../sources/). Gold domain split: [ADR-0012](0012-medallion-layers.md).

## Alternatives considered

| Option | Pros | Cons | Outcome |
| --- | --- | --- | --- |
| DummyJSON `/products` | Clear `limit`/`skip`/`total`; no auth | Not Brazilian | **Chosen for P1-05** |
| Platzi Fake Store | Similar pagination | Duplicate of DummyJSON | Rejected |
| IBGE as sole P1-05 API | Brazilian official data | No classic page metadata; large payloads | **Chosen as separate planned source**, not P1-05 |
| BrasilAPI/ViaCEP as P1-05 | Fits Olist zip fields | Point lookup; bulk use discouraged | **Optional CEP issue only** |
| Local API over Olist CSVs | Same domain | Not a third-party HTTP source | Rejected for P1-05 |
| Skip HTTP sources | Less work | Misses multi-source HTTP pattern | Rejected |

## Consequences and trade-offs

- Contracts and dbt projects keep **olist_analysis** and **product_analysis** separate (ADR-0012).
- IBGE and CEP need their own contracts/issues when scheduled; they do not block P1-02.
- Unit tests for HTTP clients use fixtures; no live dependency in unit tests.

## Evidence / validation

Owner approved DummyJSON for P1-05, IBGE as preferred BR locality API, and optional BrasilAPI CEP (2026-10-17). Implementation evidence lands with P1-05+.

## Revisit when

DummyJSON or IBGE endpoints change materially; CEP ToS blocks even small cached enrichment.
