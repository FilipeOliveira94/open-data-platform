# BrasilAPI CEP

| Field | Value |
| --- | --- |
| Name | BrasilAPI CEP |
| Type | HTTP (point lookup) |
| Role | Optional zip/CEP enrichment for Olist address prefixes |
| URL | https://brasilapi.com.br/docs |
| Gold Domain | `olist_analysis` |
| License | Public API; no CEP crawl or full scan (publisher terms) |
| Access | `GET /api/cep/v1/{cep}` (or v2); small cached samples only |
| Decision | ADR-0019 / D-019 |
| Related Issue | P1-09 (optional) |

Not a substitute for P1-05 pagination.
