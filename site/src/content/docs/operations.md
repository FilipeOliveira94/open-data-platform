---
title: Operations
description: CI since Phase 0, secrets handling, and the verified Phase 3 quality gate; observability, lineage, and the resource footprint are planned for Phase 5.
---

CI and the Phase 3 quality gate are **verified**. Observability, lineage, and the resource footprint are **planned (Phase 5)**. The pipeline steps these checks guard are on the [Pipeline](../pipeline/) page.

## Continuous integration

GitHub Actions runs one job, `lint-and-test`, on every push to `main` or `master` and on every pull request, with uv `0.13.1` and Python 3.12 ([`ci.yml`](https://github.com/FilipeOliveira94/open-data-platform/blob/main/.github/workflows/ci.yml)). It installs the locked dependencies and runs these steps:

| Step | Command |
| --- | --- |
| Sync dependencies | `uv sync --frozen --group airflow` |
| Ruff | `uv run ruff check src tests airflow docker/airflow` |
| Markdown tables | `python scripts/check_markdown_tables.py` |
| Pytest (unit) | `uv run --frozen --group airflow pytest tests/unit` |

The unit tests use fixtures and fakes, not public APIs. They include the DAG import checks and the [site consistency tests](https://github.com/FilipeOliveira94/open-data-platform/blob/main/tests/unit/site/test_site_content.py), which compare these pages with the repository docs. CI starts no Compose service, needs no cloud credentials, and deploys nothing. A separate workflow, [`pages.yml`](https://github.com/FilipeOliveira94/open-data-platform/blob/main/.github/workflows/pages.yml), builds this site on every pull request and push to `main`, checks its links under the GitHub Pages base, and deploys it to GitHub Pages only from `main`.

## Secrets and runtime data

The rules are in [`AGENTS.md`](https://github.com/FilipeOliveira94/open-data-platform/blob/main/AGENTS.md#8-security-and-cost).

- **No secrets in the repository.** `.gitignore` excludes every `.env` file except [`.env.example`](https://github.com/FilipeOliveira94/open-data-platform/blob/main/.env.example), which holds variable names and placeholders only.
- **Spark secrets off the command line.** [`credentials.sh`](https://github.com/FilipeOliveira94/open-data-platform/blob/main/docker/spark/credentials.sh) writes the S3 keys and the JDBC password to a mode-600 properties file passed with `--properties-file`, not `--conf`. `spark.redaction.regex` covers `password`, `secret`, and `access.key`. A [unit test](https://github.com/FilipeOliveira94/open-data-platform/blob/main/tests/unit/infrastructure/test_spark_credentials.py) fails if another Spark script assigns those keys.
- **No row values in query text.** Spark and Trino statements name tables, columns, and object paths; the rows travel in a data file. Error messages name the dataset or field, not the value.

## Quality gate

The Phase 3 gate keeps a failed check from reaching the next layer ([Phase 3 orchestration](https://github.com/FilipeOliveira94/open-data-platform/blob/main/docs/architecture/phase-3-orchestration.md#quality-gate-and-quarantine), [quality gate](https://github.com/FilipeOliveira94/open-data-platform/blob/main/docs/architecture/airflow-local.md#quality-gate-p3-16)).

- **Check state.** `lakehouse_state.layer_checks` holds one row per layer and dataset: `pending`, `failed`, or `passed`. A write opens the check; the next check step runs every open check of its layer and fails if any fails. The check tasks have `retries=0`.
- **Blocking.** Silver will not build a dataset with an open Bronze check. Quarantine staging and the dbt build will not run while any check is open. Each names what is open and fails before writing.
- **Visibility.** Silver and Gold tables are visible to readers before their checks run; write-audit-publish is planned (P6-09).

## Quarantine and quality counts

Detail: [data contracts](https://github.com/FilipeOliveira94/open-data-platform/blob/main/docs/architecture/data-contracts.md#quality-counts-p3-07).

- **Counts.** Each Silver build records, per dataset and column, the values a typed cast turned into NULL (`cast_null`), the rows dropped for a null key (`null_key_dropped`), and the rows with a finding (`quarantine_excluded` or `quarantine_flagged`) in `iceberg.silver_audit.quality_counts`. Zero counts are recorded. The Silver check fails when the latest run is missing a metric.
- **Volume guard.** A `snapshot` or `full_refresh` dataset fails Silver when its latest execution holds fewer Bronze rows than `min_row_ratio` of the previous one: 1.0 for the Olist releases, 0.9 for DummyJSON, 0.99 for IBGE. `make live-refresh REFRESH_FLAGS="--allow-volume-drop SOURCE.DATASET"` accepts a real shrink for one run.
- **Findings in Gold.** Each finding reaches the Gold quality facts with its `silver_action`, `excluded` or `flagged`, so a dropped record is still counted.

## Gold tests and reconciliation

Detail: [Gold layer](https://github.com/FilipeOliveira94/open-data-platform/blob/main/docs/architecture/gold-layer.md#quality-and-reconciliation).

- **Schema tests** run with every Gold build: grain columns `not_null` and unique, closed sets with `accepted_values`, numeric bounds with `accepted_range`, and column types.
- **Reconciliation tests** compare Gold keys, counts, and sums with Silver. Money sums are exact decimals and compare with equality. They run in the weekly `reconcile_gold` DAG and with `make dbt-test-reconciliation`.
- **dbt unit tests** build a model from a small fixture where the metric logic needs one.

## Coverage and concurrency

Detail: [catchup, gaps, and backfill](https://github.com/FilipeOliveira94/open-data-platform/blob/main/docs/architecture/phase-3-orchestration.md#catchup-gaps-and-backfill), [one writer at a time](https://github.com/FilipeOliveira94/open-data-platform/blob/main/docs/architecture/phase-3-orchestration.md#one-writer-at-a-time).

- **Coverage.** Every successful PostgreSQL extract records its window in a coverage ledger. `make coverage-report FREQUENCY=daily` (or `weekly`) lists gaps and overlaps and exits 1 on either.
- **One writer.** `transform_lakehouse` runs one at a time, and the `lakehouse_gold` pool serializes its Gold writes with `reconcile_gold`. Nothing serializes a `make live-*` target with a DAG run: stop the Airflow services or pause the DAGs first.
- **Failure scenarios.** A missed day, retries, a failed `reconcile_gold`, a killed Silver write, coalesced triggers, and a failed check that stays blocked were run on 2026-11-24 ([P3-09 scenarios](https://github.com/FilipeOliveira94/open-data-platform/blob/main/docs/architecture/airflow-local.md#orchestration-success-and-failure-scenarios-p3-09)).

## Planned

| Item | Status |
| --- | --- |
| Structured logging, Prometheus and Grafana, pipeline dashboards, alerts and runbooks | Planned (Phase 5: P5-01 to P5-03, P5-06) |
| Lineage through OpenLineage | Planned (Phase 5: P5-04) |
| Resource footprint and cost to run, measured on a stated machine | Planned (Phase 5: P5-05) |
| Iceberg snapshot expiry and orphan file cleanup | Planned (Phase 5: P5-07) |
| The Grafana dashboard image on this page | Planned (Phase 5: P5-10) |
| CI hardening, dbt checks in CI, an isolated integration job, resilience evidence, security checks | Planned (Phase 6: P6-01, P6-02, P6-04 to P6-06) |
| Write-audit-publish for Silver and Gold | Planned (Phase 6: P6-09) |
| Table contracts in ODCS, a check results table, and contract alerts | Planned (Phase 10: P10-06 to P10-09) |
