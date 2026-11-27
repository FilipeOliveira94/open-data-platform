# P3-14 — Live run, result, and consistency check

One scripted run of the shipped Phase 3 DAGs on local Airflow, 2026-11-26 05:52Z to 2026-11-27 00:26Z. The script is `scripts/p3_14_live_run.sh`. It drives the daily and weekly PostgreSQL DAGs through the Airflow CLI. It compares each landing to the PostgreSQL rows of the same window and to the previous landing of that window. No fault was injected: the failure paths are in [P3-09](airflow-local.md#orchestration-success-and-failure-scenarios-p3-09). Logs went to `.local/p3-14/` and hold windows, states, counts, and durations, never row values.

## Versions

| Piece | Version observed |
| --- | --- |
| Airflow | 3.4.0, LocalExecutor, image `odp-runtime:airflow3.4.0-spark3.5.8` (Spark 3.5.8, dbt in the same image) |
| PostgreSQL image | `postgres:16.15-bookworm` (OLTP source, Airflow metadata, Iceberg JDBC catalog) |
| MinIO image | `coollabsio/minio:RELEASE.2025-10-15T17-29-55Z` |
| Trino image | `trinodb/trino:484` |

## How the run compares

- **Watermark:** the `updated_at` window `[start, end)` that each extract reads, taken from the Airflow data interval and recorded per execution in `ingestion_state.extract_coverage`. The table below gives it for every run.
- **Task states (Airflow `task_instance`):** every extract succeeded on its first try, and so did `settle` on the daily runs. The cleared 2026-10-24 run is the exception: the clear made its second try, which also succeeded. Every refresh had `bronze`, `bronze_checks`, `silver`, `silver_checks`, `stage_quarantine`, and `dbt_build` succeed on try 1, or skipped when nothing was pending, and `prune_work` skipped. The per-task durations are in `.local/p3-14/<step>.log`.
- **Source window:** `scripts/p3_05_late_commit.py count` counts the PostgreSQL rows of each `oltp` table with `updated_at` in `[start, end)`. That is the predicate the extract uses. The count runs right after the extract task succeeds, and nothing else writes `oltp` during the run.
- **Landing:** `ingestion_state.extract_coverage.row_count` of the execution, per dataset.
- **Previous landing:** for a rerun, the first landing of the same window. For the weekly run, the previous weekly execution of the same window and the sum of the latest daily landings inside it.
- **Silver:** Trino counts of the four `silver.postgres_oltp__*` tables. It also counts the rows with `updated_at` in the window, against the source window, and checks that every primary key is unique.
- **Quarantine:** `iceberg.silver_audit.quality_counts` for the refresh's run label.

The OLTP source is small, at most five rows on any `updated_at` day. The `daily` and `late` steps therefore commit one synthetic seller each, as P3-05 does, and those rows stay in PostgreSQL, raw, Silver, and Gold.

## Results

| Step | What ran | Result |
| --- | --- | --- |
| Daily-shaped run | Daily backfill of D = 2026-10-22, with one on-time seller committed in D | Extract 10 s. Window `[10-22, 10-23)`: sellers landed 1, source 1; customers, orders, order items 0 and 0. The Asset-scheduled refresh succeeded in 120 s: Bronze 20 s, Silver 59 s, `stage_quarantine` 13 s, `dbt_build` 28 s. Silver rows in the window equal the source. Silver sellers went from 10 to 11 |
| Rerun, same run | Daily run of 2026-10-24 cleared through the REST API (same run id) | Succeeded. Its landing equals the first (sellers 2, others 0) and the source. The refresh read 0 raw objects: the rewritten objects kept their ETags, so nothing was promoted and no table was written (1.3 s) |
| Rerun, new execution | Manual extract of `[10-24, 10-25)` under execution `p314-rerun-manual-…` | Landing equals the first landing and the source. The refresh promoted the 4 new objects (2 rows) and rebuilt Silver (111 s). Silver counts unchanged (customers 2, order items 2, orders 2, sellers 9), no duplicate key, daily gap report clean |
| Late row | One seller committed with `updated_at` 2026-11-18 12:00Z, after the daily run had landed that window with 0 sellers | The source then held 1 seller in that window, against 0 landed |
| Weekly-shaped run | Weekly DAG triggered with logical date now: data interval `[11-16, 11-23)`, extract window `[11-15, 11-22)` | Extract 10 s. Sellers landed 1, source 1; others 0 and 0. The previous weekly landing of the window had sellers 0, and the 7 daily landings inside it summed to 0: the difference is the late seller. The refresh succeeded (131 s). Silver sellers 9 → 10, the other tables unchanged, the late day's Silver rows equal the source, no duplicate key |
| Refresh with no new raw objects | Manual `transform_lakehouse` with nothing pending | Succeeded in 2.6 s. `bronze`, `silver`, `stage_quarantine`, `dbt_build` skipped. None of the Iceberg tables got a new `metadata_location` |
| Reader view | Trino count loop on the four Silver `postgres_oltp` tables during the daily and weekly refreshes | 76 queries during each refresh. None failed, none saw an empty table |
| Missed day | Daily DAG paused 2026-11-26 06:20:45Z, after the 11-26 scheduled run. Unpaused 2026-11-27 00:22:59Z | One run, `scheduled__2026-11-27T00:00:00+00:00`, for `[11-26, 11-27)`. `settle` passed (past the 15 min lag) and the run succeeded in 10 s. Landing 0 rows in all four datasets, source 0. No other daily run. One refresh followed and succeeded (126 s). `coverage report --frequency daily`: 8 expected windows, 0 gaps, 0 overlaps, `clean` |

Quarantine and cast counts: every refresh that built Silver recorded `cast_null`, `null_key_dropped`, and `quarantine_excluded` for the four `postgres_oltp` datasets, all 0.

The first `daily` attempt, on D = 2026-10-24, logged one FAIL from a script check that expected Silver sellers to grow by exactly the planted seller. D held one more source seller that no earlier window had extracted. The landing (2) and the source (2) agreed, and Silver grew by 2. The check now compares the window's Silver rows to the source, and the rerun on 2026-10-22 above passed it. The weekly step ran with the earlier sellers-only check (Silver rows of the late day against the source, which passed). The script now also compares all four datasets for the weekly window; that path has not run live.

## What each refresh read

From `lakehouse_state.dataset_runs` for each refresh, against `lakehouse_state.bronze_promotions` up to the refresh's last step. The Silver column compares the Bronze rows that Silver's scripts scanned to the rows in the Bronze tables of the datasets it rebuilt.

| Refresh | Raw objects read / promoted so far | Raw rows read / promoted so far | Silver rows scanned / Bronze rows of those datasets | Step seconds |
| --- | --- | --- | --- | --- |
| Daily, D = 10-24 (first attempt) | 4 / 245 | 2 / 1_571_999 | 166 / 83 | 119.4 |
| After the rerun of the same run | 0 / 245 | 0 / 1_571_999 | 0 / - | 1.3 |
| After the new-execution rerun | 4 / 249 | 2 / 1_572_001 | 170 / 85 | 110.7 |
| Weekly | 4 / 253 | 1 / 1_572_002 | 172 / 86 | 130.5 |
| No new raw objects | 0 / 253 | 0 / 1_572_002 | 0 / - | 2.6 |
| Daily, D = 10-22 | 4 / 257 | 1 / 1_572_003 | 174 / 87 | 122.2 |
| Catch-up of 11-26 | 4 / 273 | 0 / 1_572_006 | 180 / 90 | 126.0 |

Bronze reads only the new raw objects: four per extract, one per dataset, against more than 250 promoted. Silver rebuilds each `incremental_window` dataset that has new Bronze rows from the whole Bronze table (ADR-0024). Its scan is twice the Bronze table, because the volume and quality reads scan it again. Between the weekly run and the 10-22 daily run, P3-09's `happy` scenario ran on the moved helpers and promoted 16 more objects.

## Mismatches and observations

No landing, Silver count, or gap report disagreed with the source.

Observations, none of which changes a published number:

1. **An empty window still rebuilds Silver and Gold.** The catch-up window landed 0 rows, but its four empty raw objects were new. Bronze promoted them, and Silver (61 s) and `dbt_build` (43 s) ran. A refresh after an empty window could skip Silver when Bronze added no row. This is a cost, not a correctness issue. It is a candidate for Phase 5 cost work, not a P3-14 change.
2. **Silver scans the Bronze table twice for `postgres_oltp`.** The scan grows with the Bronze table (90 rows today), not with the window. ADR-0024 and the P3-13 acceptance note already list it.
3. **The gap report lists daily windows before the cadence start as `before_start`.** These are the backfills and P3-05/P3-09 runs before 2026-11-19. They are reported, not counted as gaps or overlaps.
4. **Hard deletes are not exercised.** They are invisible on the `incremental_window` path (P3-12).
5. **Close-together upstream runs are not exercised here.** With one missed day the catch-up produced one run. The coalescing evidence is P3-09 `coalesce`.

## Reproduce

```bash
make airflow-up                         # Airflow, Trino, Spark runtime
scripts/p3_14_live_run.sh all-day       # daily rerun late weekly no-new-raw report
scripts/p3_14_live_run.sh pause         # any time before 00:00Z
scripts/p3_14_live_run.sh catchup report  # after 00:15Z the next day
```

Do not run `make live-*` targets while the script runs. They share the Iceberg tables and the Ivy cache with the DAGs (P3-15 documents this).
