# ADR-0021: Withdraw BrasilAPI CEP

| Field | Value |
| --- | --- |
| Status | approved |
| Date opened | 2026-10-25 |
| Date decided | 2026-10-25 |
| Owner | Program |
| Index ID | D-021 |
| Related issues | P1-09 (withdrawn); P1-08 (IBGE remains) |
| Supersedes | CEP clause of [ADR-0019](0019-public-api-ingestion-source.md) |
| Superseded by | — |

## Context / problem

ADR-0019 reserved BrasilAPI CEP as an optional enrichment of Olist zip prefixes (P1-09). That enrichment does not fit the published data.

Olist stores a 5-digit zip prefix as an integer on customers, sellers, and geolocation. Geolocation already carries city, state, latitude, and longitude at that grain. BrasilAPI CEP returns one 8-digit postal code. The publisher forbids scanning the CEP range, so the endpoint cannot be applied to the Olist prefix set. A street-level response is also a different grain from the prefix.

IBGE Localidades (P1-08) is the geography reference that joins to those tables. The comparison recorded with that ingest found `UF-sigla` on every customer, seller, and geolocation state. Normalized municipality names matched 98_713 of 99_441 customer rows, 2_988 of 3_095 seller rows, and 993_930 of 1_000_163 geolocation rows.

## Decision

BrasilAPI CEP is not a platform source. It is not ingested, not contracted, and has no raw dataset. P1-09 is withdrawn. Geography for `olist_analysis` is Olist geolocation plus IBGE `municipios`. DummyJSON and IBGE in ADR-0019 are unchanged.

## Alternatives considered

| Option | Pros | Cons | Why not (if rejected) |
| --- | --- | --- | --- |
| Point lookups of selected CEPs | Respects the publisher terms | Does not cover the Olist prefixes; adds no joinable attribute | Rejected |
| Look up every Olist prefix | Would touch the historical rows | Forbidden scan; a prefix is not an 8-digit CEP | Rejected |
| Pad each prefix with `000` | Uses the published prefix | Many of those CEPs do not exist; one street does not describe the prefix | Rejected |
| Olist geolocation plus IBGE | Covers state, municipality, and the published coordinates | No street-level address | **Chosen** |

## Consequences and trade-offs

- `olist_analysis` does not carry street, neighborhood, or CEP-level coordinates from BrasilAPI.
- A postal-code file that joins to the Olist 5-digit prefix, and that a Gold model needs beyond IBGE and Olist geolocation, is a new decision.
- ADR-0019 remains approved for DummyJSON and IBGE. Only its CEP clause is superseded.

## Evidence / validation

Decided 2026-10-25. IBGE coverage figures are the comparison recorded with P1-08 in `PROJECT_STATUS.md`.

## Revisit when

A licensed postal-code dataset joins to the Olist 5-digit prefix without a CEP scan, and a Gold model needs an attribute that IBGE and Olist geolocation do not already carry.
