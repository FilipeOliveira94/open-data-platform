# ADR-0025: Extract window coverage ledger

| ADR-0025 | Detail |
| --- | --- |
| Status | approved |
| Date opened | 2026-11-21 |
| Date decided | 2026-11-21 |
| Owner | Filipe Oliveira |
| Index ID | D-025 |
| Related issues | P3-04, P3-05, P3-06, P3-13 |
| Supersedes | — |
| Superseded by | — |

## Context / problem

The daily and weekly PostgreSQL DAGs (P3-04, P3-05) take their window from the Airflow data interval and keep no watermark. Two questions had no answer outside the Airflow metadata database: which windows have been extracted successfully, and which are missing or overlap. Raw object keys carry the window in their name, but listing raw storage does not say whether all datasets of an execution finished, which Airflow run wrote them, or how many rows they held. P3-06 needs a queryable record that a gap report can compare with the expected windows of each cadence.

## Decision

Each successful PostgreSQL extract writes one row per dataset to `ingestion_state.extract_coverage` in the catalog database (`ICEBERG_CATALOG_DB`). That is the same PostgreSQL server and database as the ADR-0024 promotion registry, under a separate schema. The key is `(source_id, dataset, execution_id)`. A row also holds `frequency` (`daily`, `weekly`, `manual`), `window_start`, `window_end`, `dag_id`, `airflow_run_id`, `row_count`, and `recorded_at`. A `CHECK` constraint ties `frequency` to the window length: a daily window is 1 day and a weekly window is 7 days. The extract writes all rows in one transaction, and only after every dataset has its `_COMPLETE` marker. `open_data_platform.ingestion.coverage report --frequency daily|weekly` (`make coverage-report`) rebuilds the expected windows of the cadence from its first window up to the last recorded one. It lists the gaps (expected windows that lack a dataset) and the overlaps (two different recorded windows of the same cadence that intersect).

## Alternatives considered

| Option | Pros | Cons | Why not (if rejected) |
| --- | --- | --- | --- |
| PostgreSQL table in the catalog database (chosen) | Queryable by SQL; transactional upsert; same server and credentials as the promotion registry; no new service | The extract now also needs the catalog database | — |
| JSON manifest per execution in MinIO | No new runtime dependency for the extract | The report lists and parses objects; no transaction across datasets; harder to query | Harder to query, and the issue asks for a queryable record |
| Airflow metadata database (`dag_run`, `task_instance`) | Already there | Not a platform contract; no row counts; the issue excludes it | Excluded by the P3-06 acceptance |
| Derive `frequency` from `dag_id` | No extra column | The report would map DAG names to cadences; a manual run and a rename are ambiguous | The column with its `CHECK` keeps cadence and window consistent (decided with Filipe 2026-11-21) |

## Consequences and trade-offs

- A ledger outage after the raw write fails the task. The retry rewrites the same raw keys (same ETag when the content is identical, so Bronze does not promote again) and then records coverage.
- A failed extract records nothing, so its window shows as a gap until a retry or a backfill covers it.
- The ledger grows by four rows per daily window and four per weekly window. The report reads one source and one frequency.
- Windows extracted before this change have no row. The gap report shows them as gaps until they are backfilled.
- The cadence start dates live in `coverage.CADENCES` and in the DAG files; a unit test fails when they drift.
- The PostgreSQL extract DAGs default the execution id to `<dag_id>__<run_id>`. Airflow run ids repeat across DAGs (`scheduled__<W>T00:00:00+00:00` at Monday 00:00Z, `backfill__<date>`), and with the run id alone the daily and weekly extracts shared one raw prefix and one ledger key. The P3-06 live run found it: the weekly backfill overwrote the daily `_COMPLETE` marker, and Bronze skipped the prefix as incomplete.
- Other sources do not write the ledger yet. They are full refresh or snapshot sources (P3-12) with no window to cover.

## Evidence / validation

Unit tests: `tests/unit/ingestion/test_coverage.py` (planted gap, planted overlap, re-run, backfill before start, statement builder) and `tests/unit/ingestion/test_postgres_oltp_extract.py` (coverage recorded only after every marker, nothing recorded on failure). Live run on 2026-11-21: [`docs/architecture/airflow-local.md`](../architecture/airflow-local.md#window-coverage-gap-report-and-backfill-p3-06) and [`docs/status-log.md`](../status-log.md).

## Revisit when

- CDC (Phase 4) replaces the window extract for PostgreSQL.
- Another source gains an incremental window.
- The AWS path (Phase 7) moves the catalog off PostgreSQL. The ledger would then need its own store.
