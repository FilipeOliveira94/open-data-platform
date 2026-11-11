# ADR-0017: Analytics visualization

| Field | Value |
| --- | --- |
| Status | `proposed` |
| Date opened | 2026-10-15 |
| Date decided | — |
| Owner | Platform architect |
| Index ID | D-017 |
| Related issues | P8-01–P8-07; Phase 9 may cite the verified result |
| Supersedes | — |
| Superseded by | — |

## Context / problem

Business users need charts and dashboards over Gold marts. That is separate from **ops** dashboards in Grafana ([ADR-0009](0009-observability.md)) and from raw SQL in Trino/Athena ([ADR-0004](0004-table-catalog-and-sql-engines.md)).

## Decision

On 2026-11-11 the platform selected **JupyterLab** for iterative exploration and **Apache Superset** for saved business charts. Both read verified Gold marts through Trino and do not write Iceberg. Spark stays the Bronze/Silver writer. dbt-trino stays the Gold writer. Grafana stays the operations UI ([ADR-0009](0009-observability.md)). Amazon QuickSight stays the AWS counterpart and is not built in Phase 8.

This row stays `proposed` until P8-01 records one read-only Trino 484 query from JupyterLab and one Superset chart on a verified Gold mart. If that PoC fails, record the failure here and pick another candidate before any later Phase 8 issue starts.

JupyterLab runs as a host `uv` process against the published Trino port, the same pattern as dbt. Superset runs in Compose because it is a long-running app. Its metadata database is on the existing PostgreSQL instance and is neither `odp` nor `iceberg`. P8-01 names any extra service the pinned image actually requires.

## Alternatives considered

| Option | Pros | Cons | Why not (if rejected) |
| --- | --- | --- | --- |
| JupyterLab + Superset (selected, pending P8-01) | Widely used notebook UI; Apache-2.0 BI; Trino through SQLAlchemy | `.ipynb` diffs; Superset is a heavier service; SQL Lab overlaps notebooks | — |
| Marimo + Lightdash | Notebooks are plain Python; charts can sit on dbt models; Apache-2.0 and MIT | Smaller ecosystem than Jupyter | Not selected on 2026-11-11 |
| Metabase | Simple question UX | AGPL-3.0; its semantic layer repeats Gold grain | Not selected on 2026-11-11 |
| JupyterLab + Metabase | Familiar notebook plus a simple BI UI | Same AGPL and semantic-layer cost | Not selected on 2026-11-11 |
| Marimo + Superset | Git-friendly notebooks plus Apache-2.0 BI | Still carries the Superset operational cost | Not selected on 2026-11-11 |
| Amazon QuickSight | Native AWS BI | Cost; weaker local parity | AWS counterpart only; not built in Phase 8 |
| Tableau / Power BI | Familiar enterprise BI | Licensing; weak Compose reproducibility | External optional; not core |
| SQL only (Trino/Athena) with no BI tool | Minimal | Poor repeated-chart path | Interim until Phase 8 |
| Grafana as primary business BI | Already planned for Phase 5 | Ops-oriented; weak semantic BI modeling | Rejected as primary BI |

## Consequences and trade-offs

- Four surfaces stay distinct: JupyterLab explores, Superset saves charts, Grafana (Phase 5) is operations, and the Phase 9 site publishes the static images from those tools.
- Superset SQL Lab is not a second notebook product. P8-05 writes that boundary.
- The site figure table in `ROADMAP.md` is the chart set. P8-06 exports the images. Phase 9 places them on the pages. `.ipynb` files stay free of outputs.
- Charts use metrics already defined for Gold. They do not invent a figure.
- Notebook outputs and row extracts stay out of git. Olist data is CC BY-NC-SA 4.0: local aggregates only.
- Operational cost is one Compose app plus one PostgreSQL database, and a host Jupyter process. Redis or a Celery worker is in scope only when P8-01 shows the pinned image requires it.
- Jupyter stays off Compose unless that same PoC shows the host process cannot reach Trino.

## Evidence / validation

None yet — P8-01 has not run.

## Revisit when

P8-01 cannot query Trino 484 from JupyterLab or Superset. The pinned Superset image requires a service set that duplicates Grafana or the warehouse. A license or image pin changes.
