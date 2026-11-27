# Phase 3 orchestration and data quality

Index of the verified Phase 3 path: Airflow schedules, catchup, backfill, the quality gate, quarantine, the review findings closed in this phase, and the citation of the live result. Settings, commands, and per-issue evidence stay in the linked notes; this page does not repeat them.

Phase 3 does not capture CDC, stream, compact Iceberg files, or apply Terraform.

## Schedules

All DAGs run on local Airflow 3.4.0 (LocalExecutor, D-006) in the runtime image `odp-runtime:airflow3.4.0-spark3.5.8`, which also runs Spark and dbt. The DAGs call package entrypoints; business logic stays out of the DAG bodies ([Local Airflow](airflow-local.md)).

| DAG | Schedule | Window or grain | Catchup | Detail |
| --- | --- | --- | --- | --- |
| `ingest_postgres_oltp_daily` | Daily 00:00 UTC, explicit `CronDataIntervalTimetable` | `[D-1, D)` on `updated_at`, read after a 15 min settle lag in one `REPEATABLE READ` snapshot | `True` from 2026-11-19, one run at a time | [Daily schedule (P3-04)](airflow-local.md#daily-postgresql-schedule-p3-04), [watermark and snapshot (P3-16)](airflow-local.md#watermark-settle-lag-and-snapshot-p3-16) |
| `ingest_postgres_oltp_weekly` | Monday 00:00 UTC | Re-extracts `[W-8, W-1)` (Sunday to Saturday) to pick up late commits the daily run missed | `True` from 2026-11-16, one run at a time | [Weekly re-extract (P3-05)](airflow-local.md#weekly-postgresql-re-extract-p3-05) |
| `ingest_dummyjson_products` | Monday 03:00 UTC | Full fetch, no window | `False`: the next fetch covers a missed week | [Source refresh matrix](../../ROADMAP.md#phase-3--orchestration-and-data-quality) |
| `ingest_ibge_localidades` | Day 1 of the month, 03:00 UTC | Full fetch, no window | `False` | Same |
| `ingest_olist`, `ingest_demo_storefront`, `ingest_postgres_oltp` | Manual | Fixed release, demo generator, ad hoc window | — | Same |
| `transform_lakehouse` | Any raw Asset event (`AssetAny`), `max_active_runs=1`, events coalesced | Bronze → checks → Silver → checks → quarantine staging → `dbt_build`, only for new raw objects | — | [Refresh trigger (P3-13)](airflow-local.md#lakehouse-refresh-trigger-p3-13), [ADR-0024](../decisions/0024-incremental-lakehouse-refresh.md) |
| `reconcile_gold` | Weekly | Gold reconciliation checks | — | [Transform DAGs (P3-03)](airflow-local.md#transform-dags-p3-03) |

Only a successful extract emits the raw Asset, so a failed extract never starts a refresh. The update pattern of each source, and what a new run of the same data means, are in [data contracts](data-contracts.md#update-patterns-p3-12).

## Catchup, gaps, and backfill

- **Catchup.** The daily and weekly DAGs create one run per missed interval, oldest first. P3-14 paused the daily DAG over one midnight; unpausing it made exactly one catch-up run and the gap report stayed clean ([live run](phase-3-live-run.md#results)).
- **Coverage ledger.** Each successful PostgreSQL extract records its window, execution id, Airflow run id, and row count per dataset in `ingestion_state.extract_coverage` ([ADR-0025](../decisions/0025-extract-window-coverage-ledger.md)).
- **Gap report.** `make coverage-report FREQUENCY=daily|weekly` lists gaps and overlaps up to the last window due at report time, so a stopped DAG shows its trailing gaps (P3-16). It exits 1 on a gap or an overlap ([Gap report](airflow-local.md#gap-report)).
- **Backfill.** `airflow backfill create --dag-id <dag> --from-date A --to-date B --max-active-runs 1`. `--reprocess-behavior completed` clears a covered run, which keeps its run id and overwrites its own raw keys and ledger rows ([Backfill parameters](airflow-local.md#backfill-parameters)).
- **No duplication.** A cleared run keeps its execution id and rewrites the same raw keys; unchanged content keeps its ETag, so Bronze does not promote it again. A new run of the same window adds an execution that Silver deduplicates by key. P3-14 reran one window both ways: landings equal to the first landing and the source, Silver counts unchanged, no duplicate key.

## Quality gate and quarantine

- **Gate.** The promotion registry records the result of `bronze_checks` and `silver_checks` per dataset. A failed check blocks the next layer until it passes, also on a later refresh with no new raw objects ([Quality gate (P3-16)](airflow-local.md#quality-gate-p3-16)).
- **Quarantine.** Each contract declares `on_invalid`. `quarantine_exclude` drops an invalid record before deduplication; `quarantine_flag` keeps it with `_quarantined = true` and its reasons. Findings stay in `quarantine/{source_id}/{dataset}/…/{object_name}.jsonl` next to the raw object, and `stage_quarantine` loads them for Gold. Reprocessing means fixing the source and ingesting a new execution ([Silver handling of quarantined records (P3-07)](data-contracts.md#silver-handling-of-quarantined-records-p3-07)).
- **Counts.** Silver records, per dataset and column, the values a typed cast turned into NULL, the rows dropped for a null key, and the rows excluded by quarantine, in `iceberg.silver_audit.quality_counts` ([Silver layer](silver-layer.md)). A `snapshot` or `full_refresh` execution that shrinks below the contract's `volume_guard.min_row_ratio` fails the Silver step; `make live-refresh REFRESH_FLAGS="--allow-volume-drop SOURCE.DATASET"` accepts a real shrink for one run ([volume guard](data-contracts.md#volume-guard-p3-07)).
- **Atomic replace.** No writer that readers use runs `DROP TABLE` then `CREATE`; each replace is one Iceberg commit ([Atomic table replace (P3-11)](phase-2-lakehouse.md#atomic-table-replace-p3-11)). Silver and Gold are still visible before their checks run (review R6); write-audit-publish is P6-09.

## One writer at a time

`make live-*` targets and the Airflow DAGs must not run at the same time against the same stack. They write the same Iceberg tables, the promotion registry, the coverage ledger, and the layer check results, and their Spark jobs share the Ivy cache at `.local/spark-ivy` ([Ivy cache](phase-2-spark-ivy-cache.md)).

- Inside Airflow, `transform_lakehouse` has `max_active_runs=1` and the `lakehouse_gold` pool serializes its writes with `reconcile_gold` ([pool](airflow-local.md#lakehouse_gold-pool-p3-04)).
- Nothing serializes a `make` target with a DAG run. Before `make live-rebuild`, `make live-refresh`, or another `live-*` target, stop the three Airflow services by name, or pause `transform_lakehouse` and the ingestion DAGs and wait for running tasks to end.
- `docker compose --profile orchestration stop` without service names also stops PostgreSQL and MinIO.

## Review findings closed in Phase 3

The [2026-11-21 review](../reviews/2026-11-21-verified-issues-review.md) of the verified issues produced 19 pipeline findings and 8 test findings. Phase 3 closed these:

| Finding | Fixed by | What changed |
| --- | --- | --- |
| R1 Quality gate bypassed on the next refresh, T3 tests that encoded it | P3-16 | Check results stored per dataset; the next layer stays blocked until the check passes |
| R2 Review fan-out in `mart_review_drivers` | P3-17 | Each `(review_id, order_id)` counted once |
| R3 Wrong denominator for `on_time_handoff_rate` | P3-17 | Only eligible orders in the denominator |
| R4 Money as `DOUBLE` | P3-18 | Contract type `decimal` with precision and scale for every money field |
| R5 Bronze `MERGE` scans the whole table | P3-08 | The `MERGE` scans only the `dt` partitions it writes |
| R7 Gap report blind to trailing gaps | P3-16 | The report runs to the last window due at report time |
| R8 Daily watermark with no lag and no DB-maintained `updated_at` | P3-16 | 15 min settle lag and a `set_updated_at` trigger |
| R9 Silent nulls and no exclusion in Silver | P3-07 | Cast and null-key counts; `quarantine_exclude` drops and counts |
| R10 No volume guard for `snapshot` and `full_refresh` | P3-07 | `volume_guard.min_row_ratio` per contract; a shrinking execution fails Silver unless accepted for that run |
| R13 Extract reads four snapshots | P3-16 | One `REPEATABLE READ, READ ONLY` transaction |
| R14 Retry after midnight duplicates an execution | P3-16 | `dt=` from the run's `run_after` |
| R16 Implicit Spark session time zone | P3-10 | `spark.sql.session.timeZone=UTC` in the Bronze and Silver Spark scripts |
| R19 `purchase_city` label on OLTP orders | P3-17 | Documented as the current customer address (Type 1); history is P4-08 |
| T7 No test of Gold metric logic | P3-17 | dbt unit tests from small fixtures |
| T1, T2, T4, T6, T8 | The review itself | Test fixes listed in the review |

Still open after Phase 3: R6 (P6-09), R11 and T5 (P4-04), R12 (P4-01), R15 (P5-07), R17 (P6-02, P6-04), R18 (P6-06), and R19 history (P4-08).

## Live result

[P3-14 live run](phase-3-live-run.md), 2026-11-26 to 2026-11-27, with `scripts/p3_14_live_run.sh` on the shipped DAGs:

- Every daily and weekly landing matched the PostgreSQL rows of its window and the previous landing of that window. The weekly run landed the one late seller the daily runs had missed. The weekly Silver check compared only the sellers of the late day; the script's newer all-four-dataset weekly comparison has not run live.
- A rerun of the same run and of the same window left Silver unchanged, with no duplicate key.
- A refresh with no new raw objects wrote no table; 152 Silver reads during refreshes all succeeded, none empty.
- One missed midnight gave one catch-up run and a clean gap report.
- Each refresh read 4 new raw objects out of 245 to 273.

Failure paths (missed day, retries, failed `reconcile_gold`, a killed Silver write that leaves the previous table readable, coalescing, and a failed check that stays blocked) are the [P3-09 scenarios](airflow-local.md#orchestration-success-and-failure-scenarios-p3-09), run on 2026-11-24. Chronology: [status log](../status-log.md).

## Limitations

- Hard deletes are invisible on the batch path; the row stays in Silver and Gold. CDC (Phase 4) handles deletes.
- A row committed after the weekly run that covers its `updated_at` is missed until a backfill of that window.
- A refresh still lists every raw key, and a pending `incremental_window` or `append_only` Silver dataset re-reads its whole Bronze table (ADR-0024, P4-04).
- An empty window still promotes empty raw objects and rebuilds Silver and Gold (P3-14 observation; Phase 5 cost work).
- Daily `MERGE`s add small files; nothing compacts them yet (P5-07).
- Spark runs `local[*]` inside the Airflow scheduler container, capped by environment variables ([Spark resource caps (P3-10)](airflow-local.md#spark-resource-caps-p3-10)).
