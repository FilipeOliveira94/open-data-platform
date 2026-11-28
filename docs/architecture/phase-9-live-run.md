# P9-06 — Live site run, result, and consistency check

One check of the published report site on 2026-11-28, final run 04:26Z (the runs at 04:14Z and 04:22Z gave the same pages, links, and 108 claims), at https://filipeoliveira94.github.io/open-data-platform/. Two methods were used. `scripts/p9_06_live_site_check.py` fetched the five pages, requested every link and asset on them, and extracted every claim that holds a digit. A browser walk then went through the site navigation in both themes and at phone width. Each claim was compared to the repository source that owns it and to [`PROJECT_STATUS.md`](../../PROJECT_STATUS.md). The site is public and holds no row values or secrets; the script output names URLs, statuses, and counts.

Result: no broken link, no missing page, and no claim on the site that disagrees with its source. One finding, F1, is in `PROJECT_STATUS.md` itself. P9-07 owns it.

## Deployed commit

Findings apply to this commit.

| Check | Result |
| --- | --- |
| Last successful `pages.yml` run on `main` | Push of `2ec9eb0` (merge of #37), 2026-11-28 04:05Z |
| `origin/main` at the check | `2ec9eb0`, the same commit, so the live site is `main` |
| `site/` between the deployed commit and `main` | No difference (same commit) |

## Pages

| Page | URL | HTTP | `<title>` and `<h1>` | Reached through the site navigation |
| --- | --- | --- | --- | --- |
| Home | `/open-data-platform/` | 200 | Open Data Platform | Yes, wide and phone width |
| Architecture | `/open-data-platform/architecture/` | 200 | Architecture | Yes |
| Pipeline | `/open-data-platform/pipeline/` | 200 | Pipeline | Yes |
| Operations | `/open-data-platform/operations/` | 200 | Operations | Yes, wide and phone width (menu button) |
| Reference | `/open-data-platform/reference/` | 200 | Reference | Yes |

Other paths: an unknown path returns 404 with the site's 404 page. `architecture` without the slash redirects to `architecture/`. The repository's About → Website is the Pages URL, and the README Platform report block links it.

## Links

Every `href`, `src`, and `srcset` on the five pages and every `url(...)` in the site CSS was requested once per URL, with HEAD falling back to GET and one retry. Statuses: **ok** is 2xx after redirects, **broken** is 4xx or 5xx after the retry or a missing `#fragment`, **unverified** is 429, a timeout, or a network error after the retry. Only `http` and `https` links are requested; an `href` that cannot be parsed counts as a broken internal link. The script exits 1 when a page or an internal link is broken, 2 when only external links are broken, and 3 on a usage error. External broken links are still findings. Unverified links do not fail the run.

| Referenced from | Internal ok | External ok | Broken | Unverified |
| --- | --- | --- | --- | --- |
| Home | 15 | 6 | 0 | 0 |
| Architecture | 19 | 28 | 0 | 0 |
| Pipeline | 19 | 18 | 0 | 0 |
| Operations | 15 | 14 | 0 | 0 |
| Reference | 15 | 23 | 0 | 0 |
| Site CSS (fonts) | 2 | 0 | 0 | 0 |
| Distinct URLs | 27 | 52 | 0 | 0 |

Every URL returned 200. The 34 in-site `#fragment` links (counted per page, including same-page table of contents links) all point to an id on their target page. The external hosts are `github.com` (docs, ADRs, workflows, the repository) and the portfolio. GitHub renders heading ids in the browser, so the 16 `#fragment`s on links to this repository's Markdown were checked against the heading slugs of the same files in the checkout. All 16 exist.

## Browser walk

| Check | Result |
| --- | --- |
| Navigation | Home → Architecture → Pipeline → Operations → Reference, each by clicking the sidebar link; on the last four the sidebar marks the current page (`aria-current`) |
| Light theme | The lakehouse and orchestration diagrams (Architecture) and the quality gate diagram (Pipeline) load their light images |
| Dark theme, set with the theme selector | The same three diagrams load their `-dark` images |
| Phone width (375 × 667) | None of the five pages is wider than the viewport. On Home the sidebar is hidden; the Menu button opened it and its Operations link navigated |
| Home target map | Renders and labels itself "Target architecture". The status table below it says what runs |
| Header | Portfolio and GitHub links on all five pages (script); both returned 200 |
| Console | No console entries in the two page snapshots read (Pipeline wide, Home at phone width) |

Screenshots were taken during the walk as support only. They are not the evidence; the tables here are.

## Claims

The script extracted 108 claims: each table row and each sentence inside `<main>` that holds a digit, code blocks excluded. Claims without a digit, such as the "planned" labels, are covered by the [site consistency tests](../../tests/unit/site/test_site_content.py), which run in CI. Every extracted claim has a row below: 96 match their source, and 12 are skipped as not a claim (a heading, a citation, or a link label whose link was checked). The skip reasons are fixed: heading or summary, heading anchor label, citation, link label. The "Source checked" column names the durable source. The `PROJECT_STATUS.md` column says what the ledger states, or "—" when it does not state it.

| Result | Claims |
| --- | --- |
| Match | 95 |
| Match, target map labeled as target | 1 |
| Skipped: heading or summary | 2 |
| Skipped: heading anchor label | 1 |
| Skipped: citation, link checked | 4 |
| Skipped: link label, link checked | 5 |
| Mismatch | 0 |
| Total | 108 |

## Findings

| Finding | Detail | Owner |
| --- | --- | --- |
| F1 | `PROJECT_STATUS.md` header says "Current phase: Phase 4 — Streaming CDC, Not started", while its current objective, the `ROADMAP.md` phase overview, and the Home status table say Phase 9 is in progress. The site matches the roadmap; the ledger header disagrees with its own body | P9-07 |

No broken link, no missing page, and no site claim that disagrees with its source.

Observations, none of which is a mismatch:

1. **Home row 9 says "In progress".** That is true today. When P9-07 closes the phase, the Home table, the roadmap overview, and the README need to say Verified in the same change, and the site needs a redeploy.
2. **The Home target map shows planned parts.** Debezium, Kafka, Prometheus, Grafana, OpenLineage, JupyterLab, Superset, the BCB SGS source, ODCS contracts, and security checks all appear on it. The map says "Target architecture" and that the status table says what runs, and every one of those parts is labeled planned on its page.
3. **The live check is a script, not a CI job.** Later site updates (P4-11, P5-10, and on) can rerun it with `python3 scripts/p9_06_live_site_check.py <site url> <out.json> .`.

## How to rerun

```bash
python3 scripts/p9_06_live_site_check.py \
  https://filipeoliveira94.github.io/open-data-platform/ /tmp/p9-06-live.json .
```

The third argument is the repository root, used for the heading anchor check. The JSON holds the pages, every link with its status and the pages that reference it, and the claims. The comparison in the claim table was done by hand against that file.

## Claim table

| # | Page | Kind | Claim on the live site | Source checked | `PROJECT_STATUS.md` | Result |
| --- | --- | --- | --- | --- | --- | --- |
| 1 | Home | table row | 0 \| Repository, Python toolchain, PostgreSQL 16, MinIO, CI \| Verified | ROADMAP phase overview | Phase 0–3 baselines: verified | match |
| 2 | Home | table row | 1 \| Multi-source ingestion: Olist files, DummyJSON and IBGE APIs, PostgreSQL windowed extract, demo storefront clickstream \| Verified (P1-09 withdrawn) | ROADMAP phase overview | Phase 0–3 baselines: verified | match |
| 3 | Home | table row | 2 \| Lakehouse: Bronze and Silver on Iceberg with Spark, Gold with dbt-trino, Trino reads \| Verified | ROADMAP phase overview | Phase 0–3 baselines: verified | match |
| 4 | Home | table row | 3 \| Airflow daily and weekly schedules, backfills, quality gate and quarantine \| Verified | ROADMAP phase overview | Phase 0–3 baselines: verified | match |
| 5 | Home | table row | 4 \| Streaming CDC: logical replication, Debezium, Kafka, Spark Structured Streaming \| Not started | ROADMAP phase overview | Phase 4 progress: not started | match |
| 6 | Home | table row | 5 \| Observability, lineage, resource footprint and cost to run \| Not started | ROADMAP phase overview | — | match |
| 7 | Home | table row | 6 \| CI/CD expansion, resilience tests, security checks \| Not started | ROADMAP phase overview | — | match |
| 8 | Home | table row | 7 \| AWS deployment path, removed in favour of an open-source-only core \| Withdrawn | ROADMAP phase overview; ADR-0029 | — | match |
| 9 | Home | table row | 8 \| JupyterLab exploration and Superset charts over Gold \| Not started | ROADMAP phase overview | — | match |
| 10 | Home | table row | 9 \| This report site, hosted on GitHub Pages \| In progress | ROADMAP phase overview | Current objective: Phase 9 in progress; header: current phase Phase 4 (F1) | match |
| 11 | Home | table row | 10 \| Table contracts and quality observability, BCB SGS source \| Not started | ROADMAP phase overview | — | match |
| 12 | Home | table row | 11 \| BioGuardia Atlantic operational sources at scale \| In progress (P11-01 to P11-03) | ROADMAP phase overview | Current objective: P11-01 to P11-03 verified | match |
| 13 | Home | sentence | olist-synth-cdc: Synthetic Olist-shaped operational database on PostgreSQL 16: windowed batch extracts and logical replication for CDC. | Target map labeled target; ADR-0008 (proposed), Phase 1 ingestion | — | match (target map) |
| 14 | Home | sentence | MinIO raw zone: Auditable raw objects by source, dataset, date and execution id, over the S3 API. | ADR-0020 raw storage layout | — | match |
| 15 | Home | sentence | Everything runs locally on Windows 11, WSL2 and Docker Compose. | ADR-0029 | Current objective: ADR-0029 | match |
| 16 | Home | sentence | There is no cloud deployment path (ADR-0029). | ADR-0029 | Current objective: ADR-0029 | match |
| 17 | Architecture | table row | PostgreSQL \| postgres:16.15-bookworm \| olist-synth-cdc source, Iceberg catalog, Airflow metadata, run ledgers \| D-001 | DECISIONS D-001; docker-compose.yml | — | match |
| 18 | Architecture | table row | MinIO \| coollabsio/minio:RELEASE.2025-10-15T17-29-55Z \| Raw objects, quarantine sidecars, Iceberg warehouse, Airflow logs (S3 API) \| D-002 | DECISIONS D-002; docker-compose.yml | — | match |
| 19 | Architecture | table row | Apache Iceberg \| org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.12.0 \| Table format, version 2, over Parquet files \| D-003 | DECISIONS D-003; docker/spark/run_*.sh | — | match |
| 20 | Architecture | table row | Iceberg JDBC catalog \| Schema V1, driver org.postgresql:postgresql:42.7.14 \| Catalog in the PostgreSQL database iceberg, shared by Spark and Trino \| D-004 | DECISIONS D-004; docker/spark/run_*.sh | — | match |
| 21 | Architecture | table row | Apache Spark \| 3.5.8, copied with its JRE from spark:3.5.8-java17 into the runtime image \| Writes Bronze and Silver; runs only in the runtime image \| D-005 | DECISIONS D-005; docker/runtime/Dockerfile | — | match |
| 22 | Architecture | table row | Trino \| trinodb/trino:484 \| Reads every layer; dbt runs on it \| D-004 | DECISIONS D-004; docker-compose.yml | — | match |
| 23 | Architecture | table row | dbt \| 1.12.6 with dbt-trino 1.11.0 \| Builds and tests the Gold models \| D-005 | DECISIONS D-005; pyproject.toml, uv.lock | — | match |
| 24 | Architecture | table row | Apache Airflow \| apache/airflow:3.4.0-python3.12 \| LocalExecutor scheduler, profile orchestration \| D-006 | DECISIONS D-005, D-006; docker/runtime/Dockerfile, docker-compose.yml | Phase 3 baseline: Airflow 3.4.0 | match |
| 25 | Architecture | table row | Runtime image \| odp-runtime:airflow3.4.0-spark3.5.8 \| One image runs Airflow, Spark and dbt \| D-005, D-006 | DECISIONS D-005, D-006; docker/runtime/Dockerfile, docker-compose.yml | Phase 3 baseline: Airflow 3.4.0 | match |
| 26 | Architecture | table row | ingest_olist_synth_cdc_daily \| Daily, 00:00 UTC \| Extracts the window [D-1, D) on updated_at in one consistent snapshot | airflow/dags/*.py; phase-3-orchestration.md | Phase 3 baseline: daily 00:00 UTC, weekly Monday [W-8, W-1) | match |
| 27 | Architecture | table row | ingest_olist_synth_cdc_weekly \| Monday, 00:00 UTC \| Re-extracts [W-8, W-1) to pick up commits the daily run missed | airflow/dags/*.py; phase-3-orchestration.md | Phase 3 baseline: daily 00:00 UTC, weekly Monday [W-8, W-1) | match |
| 28 | Architecture | table row | ingest_dummyjson_products \| Monday, 03:00 UTC \| Full fetch of the product catalog | airflow/dags/*.py; phase-3-orchestration.md | Phase 3 baseline: daily 00:00 UTC, weekly Monday [W-8, W-1) | match |
| 29 | Architecture | table row | ingest_ibge_localidades \| Day 1 of the month, 03:00 UTC \| Full fetch of municipalities | airflow/dags/*.py; phase-3-orchestration.md | Phase 3 baseline: daily 00:00 UTC, weekly Monday [W-8, W-1) | match |
| 30 | Architecture | table row | Daily batch \| Windowed PostgreSQL extract of the previous UTC day \| Verified (Phase 3) | ROADMAP phase overview; phase-3-orchestration.md | Phase 3 baseline: verified | match |
| 31 | Architecture | table row | Weekly batch \| Re-extract of the previous week for late commits; Silver keeps one row per key \| Verified (Phase 3) | ROADMAP phase overview; phase-3-orchestration.md | Phase 3 baseline: verified | match |
| 32 | Architecture | table row | Streaming CDC \| PostgreSQL logical replication, Debezium, Kafka, Spark Structured Streaming \| Planned (Phase 4) | ROADMAP phase overview; ADR-0008 | Phase 4 progress: not started | match |
| 33 | Architecture | sentence | What the repository has verified through Phase 3, and what is still planned. | — | — | skipped: not a claim (heading or summary) |
| 34 | Architecture | sentence | There is no cloud deployment path (ADR-0029). | ADR-0029 | Current objective: ADR-0029 | match |
| 35 | Architecture | sentence | Versions observed on the verified runs and pinned in the repository (Phase 2 lakehouse, DECISIONS.md). | DECISIONS D-001 to D-006 | — | match |
| 36 | Architecture | sentence | Medallion lakehouse, verified in Phase 2 and scheduled in Phase 3. | ROADMAP phase overview; phase-2-lakehouse.md, phase-3-orchestration.md | Phase 2 and 3 baselines: verified | match |
| 37 | Architecture | sentence | Source: docs/architecture/phase-2-lakehouse.md. | — | — | skipped: not a claim (citation; link checked ok) |
| 38 | Architecture | sentence | JupyterLab notebooks and Superset charts over Gold are planned for Phase 8; the site figures arrive with them. | ROADMAP Phase 8, P8-08 | — | match |
| 39 | Architecture | sentence | Table contracts in the ODCS format, checked by the Data Contract CLI, are planned for Phase 10. | ROADMAP Phase 10 | — | match |
| 40 | Architecture | sentence | Airflow orchestration, verified in Phase 3. | ROADMAP phase overview; phase-2-lakehouse.md, phase-3-orchestration.md | Phase 2 and 3 baselines: verified | match |
| 41 | Architecture | sentence | Source: docs/architecture/phase-3-orchestration.md. | — | — | skipped: not a claim (citation; link checked ok) |
| 42 | Architecture | sentence | Airflow 3.4 runs locally with the LocalExecutor. | DECISIONS D-006 | Phase 3 baseline: Airflow 3.4.0 (LocalExecutor) | match |
| 43 | Architecture | sentence | The DAGs call package entry points (Phase 3 orchestration, Local Airflow). | phase-3-orchestration.md (Local Airflow); ADR-0015 | — | match |
| 44 | Architecture | sentence | Silver and Gold tables are visible to readers before their checks run; write-audit-publish is planned (P6-09). | ROADMAP P6-09 | — | match |
| 45 | Architecture | sentence | The proposed design keeps three update paths apart, so streaming does not replace batch (ADR-0014). | ADR-0014 (proposed) | — | match |
| 46 | Architecture | sentence | D-001 — Docker Compose runtime approved | DECISIONS.md status column | — | match |
| 47 | Architecture | sentence | D-002 — MinIO object storage approved | DECISIONS.md status column | — | match |
| 48 | Architecture | sentence | D-003 — Parquet and Iceberg approved | DECISIONS.md status column | — | match |
| 49 | Architecture | sentence | D-004 — Iceberg JDBC catalog and Trino approved | DECISIONS.md status column | — | match |
| 50 | Architecture | sentence | D-005 — Spark, and dbt for Gold approved | DECISIONS.md status column | — | match |
| 51 | Architecture | sentence | D-006 — Airflow locally approved | DECISIONS.md status column | — | match |
| 52 | Architecture | sentence | D-007 — Daily and weekly batch approved | DECISIONS.md status column | — | match |
| 53 | Architecture | sentence | D-008 — Streaming CDC proposed | DECISIONS.md status column | — | match |
| 54 | Architecture | sentence | D-012 — Medallion layers approved | DECISIONS.md status column | — | match |
| 55 | Architecture | sentence | D-013 — Data contracts approved | DECISIONS.md status column | — | match |
| 56 | Architecture | sentence | D-014 — Batch and streaming coexist proposed | DECISIONS.md status column | — | match |
| 57 | Architecture | sentence | D-020 — Raw storage layout approved | DECISIONS.md status column | — | match |
| 58 | Architecture | sentence | D-024 — Incremental lakehouse refresh approved | DECISIONS.md status column | — | match |
| 59 | Architecture | sentence | D-025 — Extract window coverage ledger approved | DECISIONS.md status column | — | match |
| 60 | Architecture | sentence | D-029 — Open-source-only platform approved | DECISIONS.md status column | — | match |
| 61 | Pipeline | table row | ingest_olist_synth_cdc_daily \| Daily 00:00 UTC, explicit CronDataIntervalTimetable \| [D-1, D) on updated_at, read after a 15 min settle lag in one REPEATABLE READ snapshot \| True from 2026-11-19, one run at a time | airflow/dags/*.py; phase-3-orchestration.md | Phase 3 baseline: daily 00:00 UTC, weekly Monday [W-8, W-1) | match |
| 62 | Pipeline | table row | ingest_olist_synth_cdc_weekly \| Monday 00:00 UTC \| Re-extracts [W-8, W-1) (Sunday to Saturday) to pick up late commits the daily run missed \| True from 2026-11-16, one run at a time | airflow/dags/*.py; phase-3-orchestration.md | Phase 3 baseline: daily 00:00 UTC, weekly Monday [W-8, W-1) | match |
| 63 | Pipeline | table row | ingest_dummyjson_products \| Monday 03:00 UTC \| Full fetch, no window \| False: the next fetch covers a missed week | airflow/dags/*.py; phase-3-orchestration.md | Phase 3 baseline: daily 00:00 UTC, weekly Monday [W-8, W-1) | match |
| 64 | Pipeline | table row | ingest_ibge_localidades \| Day 1 of the month, 03:00 UTC \| Full fetch, no window \| False | airflow/dags/*.py; phase-3-orchestration.md | Phase 3 baseline: daily 00:00 UTC, weekly Monday [W-8, W-1) | match |
| 65 | Pipeline | table row | transform_lakehouse \| Any raw Asset event (AssetAny), max_active_runs=1, events coalesced \| Bronze → checks → Silver → checks → quarantine staging → dbt_build, only for new raw objects \| — | airflow/dags/*.py; phase-3-orchestration.md | Phase 3 baseline: daily 00:00 UTC, weekly Monday [W-8, W-1) | match |
| 66 | Pipeline | sentence | Streaming CDC is planned (Phase 4). | ROADMAP phase overview; ADR-0008 | Phase 4 progress: not started | match |
| 67 | Pipeline | sentence | Domain charts arrive with P8-08. | ROADMAP Phase 8, P8-08 | — | match |
| 68 | Pipeline | sentence | Each source writes objects to MinIO under one key layout (Phase 1 ingestion, raw storage layout): | ADR-0020 raw storage layout | — | match |
| 69 | Pipeline | sentence | BrasilAPI CEP is withdrawn (ADR-0021). | ADR-0021; DECISIONS D-021 | — | match |
| 70 | Pipeline | sentence | Every DAG runs on local Airflow with the LocalExecutor and calls package entry points; the business logic stays out of the DAG files (Phase 3 orchestration, Local Airflow). | phase-3-orchestration.md (Local Airflow); ADR-0015 | — | match |
| 71 | Pipeline | sentence | Detail: catchup, gaps, and backfill, ADR-0025. | — | — | skipped: not a claim (citation; link checked ok) |
| 72 | Pipeline | sentence | It exits 1 on a gap or an overlap. | ADR-0025; Makefile coverage-report | Phase 3 baseline: gap report lists missed windows | match |
| 73 | Pipeline | sentence | Backfill. airflow backfill create --dag-id <dag> --from-date A --to-date B --max-active-runs 1. | phase-3-orchestration.md (Backfill) | — | match |
| 74 | Pipeline | sentence | Phase 3 live result | — | — | skipped: not a claim (heading or summary) |
| 75 | Pipeline | sentence | Section titled “Phase 3 live result” | — | — | skipped: not a claim (heading anchor label) |
| 76 | Pipeline | sentence | The P3-14 live run, 2026-11-26 to 2026-11-27, ran the shipped DAGs (summary): | phase-3-live-run.md; status-log 2026-11-27 | Phase 3 baseline: P3-14, 2026-11-26 to 2026-11-27 | match |
| 77 | Pipeline | sentence | A refresh with no new raw objects wrote no table; 152 Silver reads during refreshes all succeeded, none empty. | phase-3-live-run.md (Reader view: 76 per refresh, two refreshes); status-log | Phase 3 baseline: 152 reads all succeeded | match |
| 78 | Pipeline | sentence | Each refresh triggered by a new extract read 4 new raw objects out of the 245 to 273 promoted so far. | phase-3-live-run.md (What each refresh read: 4 / 245 to 4 / 273) | — | match |
| 79 | Pipeline | sentence | The failure paths (a missed day, retries, a failed reconcile_gold, a killed Silver write that leaves the previous table readable, coalescing, and a failed check that stays blocked) are the P3-09 scenarios, run on 2026-11-24. | phase-3-live-run.md; airflow-local.md (P3-09 scenarios) | — | match |
| 80 | Pipeline | sentence | Streaming CDC is planned (Phase 4): PostgreSQL logical replication, Debezium, Kafka, and Spark Structured Streaming (D-008 proposed). | ADR-0008 (proposed); ROADMAP Phase 4 | Phase 4 progress: not started | match |
| 81 | Pipeline | sentence | It runs beside the daily and weekly batch and does not replace it (D-014 proposed). | ADR-0014 (proposed) | — | match |
| 82 | Pipeline | sentence | From Phase 3 orchestration: | — | — | skipped: not a claim (citation; link checked ok) |
| 83 | Pipeline | sentence | Daily MERGEs add small files; nothing compacts them yet (P5-07). | phase-3-orchestration.md (Limitations); ROADMAP P5-07 (compaction in acceptance) | — | match |
| 84 | Operations | table row | Structured logging, Prometheus and Grafana, pipeline dashboards, alerts and runbooks \| Planned (Phase 5: P5-01 to P5-03, P5-06) | ROADMAP P5-01 to P5-07, P5-10 | — | match |
| 85 | Operations | table row | Lineage through OpenLineage \| Planned (Phase 5: P5-04) | ROADMAP P5-01 to P5-07, P5-10 | — | match |
| 86 | Operations | table row | Resource footprint and cost to run, measured on a stated machine \| Planned (Phase 5: P5-05) | ROADMAP P5-01 to P5-07, P5-10 | — | match |
| 87 | Operations | table row | Iceberg snapshot expiry and orphan file cleanup \| Planned (Phase 5: P5-07) | ROADMAP P5-01 to P5-07, P5-10 | — | match |
| 88 | Operations | table row | The Grafana dashboard image on this page \| Planned (Phase 5: P5-10) | ROADMAP P5-01 to P5-07, P5-10 | — | match |
| 89 | Operations | table row | CI hardening, dbt checks in CI, an isolated integration job, resilience evidence, security checks \| Planned (Phase 6: P6-01, P6-02, P6-04 to P6-06) | ROADMAP P6-01, P6-02, P6-04 to P6-06 | — | match |
| 90 | Operations | table row | Write-audit-publish for Silver and Gold \| Planned (Phase 6: P6-09) | ROADMAP P6-09 | — | match |
| 91 | Operations | table row | Table contracts in ODCS, a check results table, and contract alerts \| Planned (Phase 10: P10-06 to P10-09) | ROADMAP P10-06 to P10-09 | — | match |
| 92 | Operations | sentence | CI and the Phase 3 quality gate are verified. | ROADMAP phase overview (Phases 0 and 3) | Phase 3 baseline: verified | match |
| 93 | Operations | sentence | Observability, lineage, and the resource footprint are planned (Phase 5). | ROADMAP phase overview (Phase 5) | — | match |
| 94 | Operations | sentence | GitHub Actions runs one job, lint-and-test, on every push to main or master and on every pull request, with uv 0.13.1 and Python 3.12 (ci.yml). | .github/workflows/ci.yml | — | match |
| 95 | Operations | sentence | Spark secrets off the command line. credentials.sh writes the S3 keys and the JDBC password to a mode-600 properties file passed with --properties-file, not --conf. spark.redaction.regex covers password, secret, and access.key. | docker/spark/credentials.sh, run_bronze.sh, run_silver.sh; AGENTS.md §8 | — | match |
| 96 | Operations | sentence | The Phase 3 gate keeps a failed check from reaching the next layer (Phase 3 orchestration, quality gate). | phase-3-orchestration.md (quality gate) | Phase 3 baseline: failed checks block the next layer | match |
| 97 | Operations | sentence | The check tasks have retries=0. | airflow/dags/transform_lakehouse.py | — | match |
| 98 | Operations | sentence | Silver and Gold tables are visible to readers before their checks run; write-audit-publish is planned (P6-09). | ROADMAP P6-09 | — | match |
| 99 | Operations | sentence | A snapshot or full_refresh dataset fails Silver when its latest execution holds fewer Bronze rows than min_row_ratio of the previous one: 1.0 for the Olist releases, 0.9 for DummyJSON, 0.99 for IBGE. make live-refresh REFRESH_FLAGS="--allow-volume-drop SOURCE.DATASET" accepts a real shrink for one run. | config/contracts/*.yaml volume_guard; Makefile REFRESH_FLAGS; medallion_build.py | — | match |
| 100 | Operations | sentence | Every successful PostgreSQL extract records its window in a coverage ledger. make coverage-report FREQUENCY=daily (or weekly) lists gaps and overlaps and exits 1 on either. | ADR-0025; Makefile coverage-report | Phase 3 baseline: gap report lists missed windows | match |
| 101 | Operations | sentence | A missed day, retries, a failed reconcile_gold, a killed Silver write, coalesced triggers, and a failed check that stays blocked were run on 2026-11-24 (P3-09 scenarios). | phase-3-live-run.md; airflow-local.md (P3-09 scenarios) | — | match |
| 102 | Reference | table row | ADR-0029 \| Open-source-only platform on the local stack, with no cloud deployment path | ADR-0029 title | — | match |
| 103 | Reference | table row | Phase 1 ingestion \| Sources, traceability, repeatable runs, and update patterns | — | — | skipped: not a claim (link label; link checked ok) |
| 104 | Reference | table row | Phase 2 lakehouse \| Bronze, Silver, and Gold on Iceberg with Spark, Trino, and dbt | — | — | skipped: not a claim (link label; link checked ok) |
| 105 | Reference | table row | Phase 2 live run \| The recorded end-to-end lakehouse run | — | — | skipped: not a claim (link label; link checked ok) |
| 106 | Reference | table row | Phase 3 orchestration \| Airflow schedules, backfills, the quality gate, and quarantine | — | — | skipped: not a claim (link label; link checked ok) |
| 107 | Reference | table row | Phase 3 live run \| The recorded orchestration run | — | — | skipped: not a claim (link label; link checked ok) |
| 108 | Reference | sentence | The figure manifest (question, value, grain, Gold table, tool, and run id for each site figure) is planned and arrives with P8-08. | ROADMAP Phase 8, P8-08 | — | match |
