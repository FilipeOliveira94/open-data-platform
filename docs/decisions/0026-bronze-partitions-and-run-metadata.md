# ADR-0026: Bronze `dt` partitions and lakehouse run metadata

| ADR-0026 | Detail |
| --- | --- |
| Status | approved |
| Date opened | 2026-11-23 |
| Date decided | 2026-11-23 |
| Owner | Filipe Oliveira |
| Index ID | D-026 |
| Related issues | P3-08, P3-13, P3-14, P5-03 |
| Supersedes | — |
| Superseded by | — |

## Context / problem

Since P3-13 (ADR-0024) a Bronze refresh reads only new raw objects, but its `MERGE` matched on lineage columns only and Bronze tables had no partition spec. Every incremental `MERGE` joined against the whole table, so its read I/O grew with total history (2026-11-21 review R5). The P3-13 run measured the file it wrote, not what it scanned.

The refresh also left no queryable record of its own work: step durations, failures, rows read and written per dataset, and what each Spark write scanned were only in task logs. P3-08 asks for that record, kept across runs, so Phase 5 (P5-03) can chart growth over time.

## Decision

**Partition spec.** Every Bronze table is `PARTITIONED BY (dt)`, an identity partition on the `dt` column that Bronze already carries: the ingestion date of the raw key (`raw/{source}/{dataset}/dt=…/…`). The incremental `MERGE` adds `target.dt IN (<dt values of the staged objects>)` to its `ON` clause. A raw key contains its `dt`, so the rows a `MERGE` can match, and the old rows of a rewritten key, are always in those partitions. Iceberg prunes the rest. The values are dates parsed from layout keys and checked as ISO dates; no row value enters the statement.

Tables created before P3-08 keep their unpartitioned spec. Before an incremental `MERGE` the Bronze step reads the table's current metadata through the JDBC catalog (`iceberg_tables.metadata_location`) and fails with a message naming `make live-rebuild` when the default spec has no `dt` identity field. The migration is that one documented rebuild, not a silent `ALTER TABLE … ADD PARTITION FIELD`: an added field leaves the old files unpartitioned and still scanned.

**Run metadata.** `medallion_build` writes three tables in `lakehouse_state`, next to the promotion registry (`lakehouse/run_log.py`):

| Table | Grain | Content |
| --- | --- | --- |
| `step_runs` | One attempt of one step (`run_label`, `step`, `started_at`) | Start, end, seconds, `ran` / `skipped` / `failed`, `full_rebuild`, exception type, one-line message |
| `dataset_runs` | Run, layer, dataset | Status, write mode, raw objects read, rows read, rows written, table total, seconds. Gold rows are dbt models from `run_results.json` |
| `table_io` | Run, layer, dataset, Iceberg table | Scans, scanned and skipped data files, scanned bytes, rows scanned; commits, added and removed files and rows, table total |

`run_label` is the Airflow run id (or `make-<UTC time>`). A retry of the same run adds a `step_runs` row and replaces its `dataset_runs` and `table_io` rows. `bronze_refreshes` (ADR-0024) stays: it records the promotion decision (objects listed, incomplete, promoted).

**Measurement.** The Spark counts are measured, not derived from metadata tables. Each medallion Spark script runs with a mode-700 capture directory under the run work dir:

- the Spark event log (`spark.eventLog.*`). Iceberg reports each `BatchScan`'s result data files, skipped data files, and data file bytes as driver metrics, and the scan's output rows as task metrics.
- the Iceberg `CommitReport` lines of `LoggingMetricsReporter`, routed by `docker/spark/log4j2-metrics.properties` to a mode-600 file: added and removed files and records, and the table total.

Iceberg 1.12 does not send Spark scans to `LoggingMetricsReporter` (a scan with its own reporter replaces the logging one), so scans come from the event log and commits from the reporter. `lakehouse/spark_metrics.py` keeps only table names and counts, then deletes the directory.

The run log is best effort: a write failure prints one line to stderr and does not fail or mask the step.

**Silver audit table.** The same review applied to `silver_audit.quality_counts`: its `MERGE` used `WHEN NOT MATCHED BY SOURCE`, so every Silver write scanned and rewrote the whole table. The `ON` clause now fixes the run and dataset and the clause is dropped; a write adds one file.

## Alternatives considered

| Option | Pros | Cons | Why not (if rejected) |
| --- | --- | --- | --- |
| Identity partition on `dt` (chosen) | The column exists; every `MERGE` match key implies it; one partition per ingestion day | Small files per day until compaction (P5-07) | — |
| Partition on `days(_ingestion_ts)` | Same grain | The `MERGE` would have to filter on a timestamp range derived from the key; no gain over `dt` | Same pruning with more SQL |
| Partition on `execution_id` | Matches the unit of promotion | One partition per execution; snapshot sources land many | Too many partitions |
| `ALTER TABLE … ADD PARTITION FIELD` in place | No rebuild | Old files stay unpartitioned and are still scanned | Does not fix R5 for existing history |
| Scan counts from the `files` metadata table | No event log | An estimate of what a pruned scan would read, not a measurement | The issue asks for what the `MERGE` scans |
| Iceberg `LoggingMetricsReporter` for scans | One mechanism | Spark scans never reach it in Iceberg 1.12 (probe 2026-11-23) | Not available |
| Airflow task durations | Already recorded | Per task, not per dataset; no rows or scan counts; not a platform contract | Not enough |

## Consequences and trade-offs

- An existing local lake needs one `make live-rebuild` before the next incremental Bronze write. Until then that write fails with the rebuild message, and its objects stay unregistered.
- A daily partition adds at least one file per Bronze table per ingestion day. Compaction stays with P5-07 (review R15).
- The capture files hold statement text and plan descriptions. They are private to the run, parsed for counts only, and deleted. Spark's `spark.redaction.regex` still covers the event log environment.
- `quality_counts` files are pruned by dataset, not by run: Iceberg truncates the `run_label` bounds to 16 characters (`scheduled__2026-`). A dataset's write scans one more small file per run until compaction (P5-07).
- `rows_written` of a Gold model is the dbt-trino `rows_affected` when the adapter reports one, else `NULL`.
- The run log grows by about one `step_runs` row per step, one `dataset_runs` row per written dataset or dbt model, and one `table_io` row per touched table, per run.

## Evidence / validation

Unit tests: `tests/unit/lakehouse/test_run_metadata.py`, `tests/unit/lakehouse/test_medallion_build.py` (Trino error fields and total timeout). Live evidence (2026-11-23, [`docs/status-log.md`](../status-log.md)): a pre-P3-08 table failed the guard; after `make live-rebuild`, a `MERGE` into an existing `dt` partition scanned 1 of the table's 3 files; a refresh with nothing new recorded six skipped steps; an Airflow run recorded its steps under its run id.

## Revisit when

- A Silver or Gold table gains incremental writes; it would then need its own spec.
- The AWS path (Phase 7) moves the catalog off PostgreSQL or Spark off the local event log.
- Iceberg sends Spark scan reports to the configured reporter.
