# BrasilAPI CEP

| Field | Value |
| --- | --- |
| Name | BrasilAPI CEP |
| Type | HTTP (point lookup) |
| Role | Not a platform source |
| URL | https://brasilapi.com.br/docs |
| Gold Domain | — |
| License | Public API; no CEP crawl or full scan (publisher terms) |
| Access | Not ingested |
| Decision | ADR-0021 / D-021 (supersedes the CEP clause of ADR-0019) |
| Related Issue | P1-09 (withdrawn) |

Olist geography is the published zip prefix, with city, state, latitude, and longitude, plus IBGE municipalities (P1-08). BrasilAPI returns one 8-digit CEP. That grain does not join to the Olist prefix, and the publisher forbids scanning CEPs to build the missing coverage. There is no contract and no raw dataset for this source.
