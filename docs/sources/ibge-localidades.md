# IBGE Localidades

| Field | Value |
| --- | --- |
| Name | IBGE Localidades |
| Type | HTTP (collection) |
| Role | Brazilian locality reference (regions, UFs, municipalities) for `olist_analysis` |
| URL | https://servicodados.ibge.gov.br/api/docs/localidades |
| Gold Domain | `olist_analysis` |
| License | Public government API; no auth |
| Access | e.g. `GET https://servicodados.ibge.gov.br/api/v1/localidades/municipios?orderBy=nome&view=nivelado` |
| Decision | ADR-0019 / D-019 |
| Related Issue | P1-08 |

Large JSON responses; client should chunk/stream as needed. Does not replace DummyJSON for P1-05 pagination acceptance.
