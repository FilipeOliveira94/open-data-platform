# ADR-0024: Incremental lakehouse refresh

| Field | Value |
| --- | --- |
| Status | `approved` |
| Date opened | 2026-11-20 |
| Date decided | 2026-11-20 |
| Owner | Platform architect |
| Index ID | D-024 |
| Related issues | P3-13; builds on P3-11 (atomic replace) and P3-12 (update patterns); P3-14 records the live run |
| Supersedes | The `FULL_OVERWRITE` default of `lakehouse/write_mode.py` (P2-03 / P2-04) |
| Superseded by | — |

## Context / problem

Until P3-13 every lakehouse refresh rebuilt every Bronze table from every raw object, in one Python process that held each dataset's rows in memory, then rebuilt every Silver table and every Gold model. The work of one refresh grew with total raw history, not with what was new. Three more defects came with it:

- Bronze staging used a fixed key (`bronze/_staging/{source_id}/{dataset}/rows.jsonl`), so a `make` run and an Airflow run overwrote each other's staging.
- Only `ingest_postgres_oltp_daily` started the transform, through a `TriggerDagRunOperator`. A catch-up of N missed days queued N full rebuilds. The other ingestion DAGs started nothing.
- A refresh with nothing new still rewrote every table.

## Decision

1. **Promotion registry.** PostgreSQL schema `lakehouse_state` in the Iceberg catalog database (`ICEBERG_CATALOG_DB`). Table `bronze_promotions` has one row per raw object promoted into Bronze, keyed by `raw_object_key`, with its ETag, row count, run label, and time. A raw object is new when its key is absent or its ETag differs.
2. **Bronze appends new objects (`WriteMode.INCREMENTAL_APPEND`).** Only data objects of a **complete** execution (a valid `_COMPLETE` marker, P3-12) are promoted. An incomplete execution waits for its marker, and Bronze never sees an execution that will never get one; its raw objects stay auditable. Markers are read only for executions that hold a new object, so a refresh with nothing new reads no object body.
   - One `MERGE INTO bronze.{table}` per dataset inserts the staged rows whose lineage (`raw_object_key`, `_raw_etag`, `record_index`) is not already in the table. It is one Iceberg commit. `_raw_etag` is a new Bronze-only column; Silver does not carry it.
   - When a registered key has a new ETag (same execution identity re-ingested), the same `MERGE` deletes that key's old rows (`WHEN NOT MATCHED BY SOURCE AND raw_object_key IN (…) THEN DELETE`). Object keys are layout paths; no row value enters the SQL.
   - The registry is written **after** Spark commits. A crash in between leaves the objects unregistered; the retry stages them again and the `MERGE` inserts nothing for rows already present.
3. **Streaming.** Objects are read one at a time. Each object's rows are appended to a local JSONL file in the run's work directory, which is uploaded with a managed (multipart) upload. At most one raw object is in memory.
4. **Per-run staging.** Staging keys and work directories carry the run label: `bronze/_staging/{run_label}/{source_id}/{dataset}/rows.jsonl` and `gold/_staging/{run_label}/gold_staging/quarantine_findings/rows.jsonl`. Airflow passes its run id. Make passes `make-<UTC timestamp>`, one per invocation. Bronze staging is deleted after the dataset is registered.
5. **Pending layers.** `lakehouse_state.pending_layers` holds a Silver flag per dataset and one Gold flag. Registering new objects marks the dataset pending for Silver in the same transaction. A successful Silver write clears its flag, if the flag was not re-marked during the write, and marks Gold pending in the same transaction. A successful `dbt build` clears the Gold flag. A failed or skipped layer therefore runs on the next refresh even when no new raw object arrives.
6. **Silver** rebuilds only the datasets pending for it. Each rebuild is the P3-11 `CREATE OR REPLACE TABLE … AS SELECT` over the rows P3-12 selects (`WriteMode.FULL_OVERWRITE` per dataset).
7. **Gold** (quarantine staging and `dbt build`) runs only while Gold is pending. It stays a dbt full table build; see Evidence for the measured duration.
8. **No new data.** A refresh with no new raw object and nothing pending writes no table. `lakehouse_state.bronze_refreshes` records, per dataset and run, the objects listed, waiting for a marker, and promoted, the rows appended, and the table total. Every later step logs that it has nothing to do and exits with `--skip-exit-code`; Airflow maps that code to a skipped task.
9. **`--full-rebuild`** (`make live-rebuild`) rebuilds every Bronze table from all complete objects (`CREATE OR REPLACE`), replaces the registry rows of each dataset, rebuilds every Silver table, and runs Gold. It is for a contract version change, a Bronze schema change such as the new `_raw_etag` column, or recovery. It fails for a dataset with no complete execution instead of emptying the table. It is never the scheduled path. A dataset that the registry has never seen is bootstrapped the same way on its first refresh.
10. **Trigger.** Every ingestion DAG emits an Airflow Asset per raw source it lands (`odp-raw://{source_id}`) on success. `transform_lakehouse` is scheduled on any of them through `AssetTriggeredTimetable(…, batch_asset_events=True)` with `max_active_runs=1`: while a run is queued or running the scheduler defers the DAG, and one follow-up run consumes every queued event. This replaces the P3-04 `TriggerDagRunOperator`.

## Alternatives considered

| Option | Pros | Cons | Why not (if rejected) |
| --- | --- | --- | --- |
| Registry as an Iceberg table | No extra database | Every read from Python needs a Spark JVM or Trino. The registry write cannot share a transaction with the pending flags | Rejected for now. The registry write is guarded by the idempotent `MERGE`, so the store can move later |
| Registry as JSON manifests in the warehouse bucket | No database | Reading it lists every manifest, so it grows with history. No transaction across manifests | Rejected |
| Plain `INSERT INTO` for Bronze | Cheapest write | A crash between commit and registry write duplicates rows on retry | Rejected. `MERGE` makes the retry a no-op |
| Promote incomplete executions with a null completion time (P3-12 behavior) | Bronze holds every raw object | A partial page set enters Bronze. The incremental path would have to re-stamp rows when the marker lands | Rejected. Incomplete executions wait for the marker |
| `MERGE` into Silver for `incremental_window` / `append_only` | Silver work grows with new rows only | New SQL per pattern, plus late-row and delete semantics to prove | Deferred. Silver rebuilds the pending dataset; the cost is stated below |
| Keep `TriggerDagRunOperator`, add one to each ingestion DAG | Explicit run ids | N upstream runs give N refreshes; the transform must know its producers | Rejected for Assets |

## Consequences and trade-offs

- **What a scheduled run reads.** Raw: one listing of each dataset prefix (metadata only, 1 000 keys per page; it grows with the number of keys), plus the bodies and markers of new objects. Registry: one indexed query per dataset. Bronze: the `MERGE` reads the match columns of the target table; see Evidence for what was measured. Silver: a pending `snapshot` or `full_refresh` dataset reads its latest complete execution; a pending `incremental_window` or `append_only` dataset re-reads the whole Bronze table of that dataset. That cost grows with that dataset's Bronze history and is accepted until Silver `MERGE` lands. Gold: a full dbt build whenever any Silver table changed.
- **Upgrade.** Existing Bronze tables have no `_raw_etag` column. The first refresh after this change bootstraps each dataset with a full rebuild, because the registry is empty. A `MERGE` against a table that the registry knows but whose schema lacks the column fails loudly; `make live-rebuild` is the fix.
- **Raw executions without a marker** (landed before P3-12) no longer enter Bronze. For a `snapshot` source, Silver already ignored them. For `postgres_oltp` (`incremental_window`) the local windows were re-extracted with new execution ids so that they carry markers (Evidence).
- **Concurrency.** Staging no longer collides. Two writers of the same Iceberg table still must not run at the same time; `make` targets and the DAG share the tables, the registry, and the Ivy cache ([one writer at a time](../architecture/phase-3-orchestration.md#one-writer-at-a-time)). The `lakehouse_gold` pool serializes the DAG's writes.
- **Checks.** `bronze-checks` compares each written Bronze table's `COUNT(*)` with the registry row total of that dataset. `silver-checks` covers only the Silver tables rebuilt in the run.

## Evidence / validation

Measured on the local stack on 2026-11-20. Full table: [Bronze layer, P3-13 result](../architecture/bronze-layer.md#result-2026-11-20-incremental-refresh-p3-13). Chronology: [status log](../status-log.md).

| Refresh | Raw objects / rows | Wall |
| --- | --- | --- |
| `--full-rebuild` | 28 / 1_565_638; Bronze 18, Silver 20, dbt 287 checks passed | 565 s |
| No new raw object | 0 / 0; no table written, every step skipped | 13 s (Airflow: 8.2 s, six tasks skipped) |
| One new daily window | 4 / 5; Bronze `MERGE` committed as `append` (1 file, 0 rewritten) | 151 s |
| Re-ingested object | 1 / 60 then 100; old rows deleted in the same `MERGE` | Bronze 16 s |

Asset coalescing: with Airflow 3.4's default `batch_asset_events=False`, four events gave four runs; with `batch_asset_events=True` and `max_active_runs=1`, three events that landed during a refresh gave one follow-up run. `dbt build` took 25–27 s per refresh, so Gold stays a full build. A `MERGE` into a large Bronze table (no new Olist release) was not measured.

Unit tests: `tests/unit/lakehouse/test_refresh.py` (bootstrap, no-op, new execution, incomplete execution, rewritten object, Spark failure and retry, full rebuild, Silver pending, Gold gate, skip exit code, planted value absent from SQL), `test_bronze.py`, `test_write_mode.py`, `test_reconciliation.py`, `tests/unit/infrastructure/test_dagbag.py` (Assets, batching, trigger rules).

## Revisit when

- A Silver `incremental_window` or `append_only` rebuild dominates refresh time (move to `MERGE`).
- The raw listing of one prefix becomes the bottleneck (add a per-dataset listing watermark or S3 inventory).
- The `dbt_build` duration dominates a refresh (name incremental models).

## Notes

Code: `lakehouse/promotion_registry.py`, `lakehouse/bronze.py`, `lakehouse/refresh.py`, `lakehouse/medallion_build.py`, `airflow/dags/ingest_common.py`, `airflow/dags/transform_lakehouse.py`.
