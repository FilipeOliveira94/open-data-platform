# Review of verified issues (2026-11-21)

Scope: every issue marked **Verified** in [`ROADMAP.md`](../../ROADMAP.md) on 2026-11-21 (Phases 0–3: P0-09 to P3-13). The question was whether each implemented solution is sound from a data engineering standpoint. Decisions and ADRs were not taken as proof: a finding stands even when an ADR accepted the behavior.

Method: code reading of `src/`, `airflow/dags/`, `docker/spark/`, `dbt/`, and `tests/`. Commands run before any test change: `uv run pytest tests/unit` (444 passed) and `uv run pytest tests/integration` against the local Compose stack (5 passed, 1 failed, see T1). No pipeline, DAG, or refresh was run for this review. Findings marked *confirmed by reading* trace the code path; none of them was reproduced on the stack. The issues that fix them carry the live evidence.

## Pipeline and model findings

| Finding | Affected verified issue | What is wrong | Severity | Effort | Fix issue |
| --- | --- | --- | --- | --- | --- |
| R1 Quality gate bypassed on the next refresh | P3-03, P3-13 | Run 1: `bronze` writes and marks Silver pending, `bronze_checks` fails. Run 2: no new raw object, so `bronze` writes an empty count file and returns "ran"; `bronze_checks` sees no count and skips; `silver` (`trigger_rule=none_failed`) builds from the Bronze that failed. Same path from a failed `silver_checks` to `stage_quarantine` and `dbt_build`: Gold is published from Silver that failed its checks. Code: `_run_step` in `lakehouse/medallion_build.py`, `trigger_rule` in `airflow/dags/transform_lakehouse.py`. Checks also retry twice on a deterministic data failure | High | Low–medium | P3-16 |
| R2 Review fan-out in `mart_review_drivers` | P2-06, P2-09 | The mart joins `fct_order_items` at item grain, so a review of an order with N items counts N times. `review_count` is inflated and `mean_score` is weighted by item count. Site figure 4 reads this mart | High | Low | P3-17 |
| R3 Wrong denominator for `on_time_handoff_rate` | P2-06 | `avg(on_time_handoff_flag)` runs over every order, including cancelled and never-shipped orders, which count as late. In `mart_delivery_performance`, `mart_seller_performance`, `mart_oltp_delivery_performance`, `mart_oltp_seller_performance` | Medium | Low | P3-17 |
| R4 Money as `DOUBLE` | P1-03, P1-06, P2-04 | Contract type `number` maps to Iceberg `DOUBLE`; the PostgreSQL extract turns `numeric(12,2)` into a JSON float. GMV, freight, and payment sums carry binary rounding error. Ten `scalars_near` reconciliation checks use a tolerance that hides it | Medium | Medium | P3-18 |
| R5 Bronze `MERGE` scans the whole table | P3-13 | Bronze tables have no partition spec and the `MERGE ON` clause matches only lineage columns, so every incremental `MERGE` joins against all Bronze rows. Read I/O per refresh grows with total history, against the P3-13 claim. The P3-13 run measured one file written, not the bytes scanned | Medium | Low–medium | P3-08 |
| R6 No write-audit-publish | P2-07, P3-11 | Silver and Gold are visible to readers before their checks run; a failing dbt test leaves the new table published. Gold models are replaced one at a time and Silver datasets one at a time, so a reader can join a new fact to old marts. P3-11 made each table atomic, not the set | Medium | High | P6-09 |
| R7 Gap report blind to trailing gaps | P3-06 | `coverage_report` builds expected windows only up to the last recorded window end. A DAG that stops reports no gap; the report has no freshness check against the current time | Medium | Low | P3-16 |
| R8 Daily watermark with no lag and no DB-maintained `updated_at` | P1-06, P3-04, P3-05 | The daily window `[D-1, D)` is read at exactly `D 00:00Z`. `updated_at` is whatever the writer sets (no trigger in `schema.sql`); with `now()` it is the transaction start, so any transaction open across midnight misses the daily window by design and waits up to 8 days for the weekly run. An `UPDATE` that does not set `updated_at` is never extracted | Medium | Low | P3-16 |
| R9 Silent nulls and no exclusion in Silver | P1-10, P2-04 | `TRY_CAST` and `TRY_TO_TIMESTAMP` turn an invalid value into NULL with no count. Rows with a null key are dropped by `WHERE pk IS NOT NULL` with no count. Rows copied to the quarantine sidecar still enter Silver and the Gold metrics: `on_invalid: quarantine` neither excludes nor flags them in Silver | Medium | Medium | P3-07 |
| R10 No volume guard for `snapshot` and `full_refresh` | P3-12 | Silver takes the latest complete execution. A complete but truncated fetch (an API that returns fewer records) removes those records from Silver with no warning | Medium | Low | P3-07 |
| R11 `incremental_window` Silver rebuilds from all Bronze | P3-13 | Each pending PostgreSQL dataset is rebuilt from its whole Bronze table. ADR-0024 accepted it, but it is the "re-read all history" pattern the daily use case rules out | Medium (scale) | Medium | P4-04 |
| R12 One PostgreSQL server and one role for everything | P0-10, P1-06, P3-01 | The OLTP source, Airflow metadata, Iceberg catalog, promotion registry, and coverage ledger share one server and the same `POSTGRES_USER`. The extract reads the source with the role that administers the platform, and Phase 4 logical replication would run on that shared server | Medium | Medium | P4-01 |
| R13 Extract reads four snapshots | P1-06 | The four tables are read in one `READ COMMITTED` transaction, so each `SELECT` has its own snapshot and `orders` and `order_items` of one window can disagree | Low | Low | P3-16 |
| R14 Retry after midnight duplicates an execution | P1-02, P3-04 | `ingestion_ts` defaults to now, and the raw key carries `dt=` from it. A retry of a run whose first try completed, on a later UTC day, writes a second complete prefix with the same execution id. Silver deduplicates; Bronze and the audit trail hold two copies | Low | Low | P3-16 |
| R15 No compaction | P2-10, P3-13 | Daily `MERGE` appends into unpartitioned tables add small files. P5-07 expires snapshots and orphans; nothing rewrites data files or manifests | Low now | Low | P5-07 |
| R16 Implicit Spark session time zone | P2-04 | The ISO fallbacks strip `Z` and parse in the JVM default time zone. `spark.sql.session.timeZone` is not set | Low | Low | P3-10 |
| R17 CI runs lint and unit tests only | P0-12 | A broken dbt model or integration test reaches `main` | Low–medium | Low | P6-02, P6-04 (already scoped) |
| R18 Olist download without a checksum | P1-04 | Nothing in `ingestion/kaggle_download.py` checks a hash, so "a fixed historical release" is not enforced | Low | Low | P6-06 |
| R19 `purchase_city` on OLTP orders is the current address | P2-06 | `fct_oltp_orders` joins the current customer row (Type 1) and names it `purchase_*` | Low | Low | P3-17 (label), P4-08 (history) |

## Test findings

T1, T2, T4, T6, and T8 were fixed in the same PR as this review, in a separate commit. R1 and R7 each got a `pytest.mark.xfail(strict=True)` unit test that reproduces the defect: `test_a_failed_bronze_check_is_checked_again_before_silver_builds` and `test_a_stopped_dag_reports_its_trailing_gaps`. P3-16 makes them pass, and strict mode then fails the suite until the marker is removed. After the fix: `pytest tests/unit` 446 passed and 2 xfailed; `pytest tests/integration` on local Compose 6 passed, with no test object left in `odp-raw` or `odp-test-raw` and no scratch database left.

| Finding | Affected verified issue | What is wrong | Effort | Fix issue |
| --- | --- | --- | --- | --- |
| T1 Olist integration test broken since 2026-10-26 | P1-04, P1-10 | `test_olist_ingest_round_trip_overwrites_the_same_object` appends `replacement\n`, a one-column row, to a header-only CSV. Since P1-10 (`1636d49`) that is a structural error, and the test fails with `CSV row 0 does not match contract for orders`. Nobody saw it: integration tests are not in CI and skip without Compose settings | Low | Fixed: the test appends a row with the contract's column count. P6-04 makes a skipped integration test fail CI |
| T2 Integration tests write to the real raw bucket and leave markers | P1-04–P1-08 | The tests use `ODP_RAW_BUCKET` (`odp-raw`) and delete only the data objects, leaving each `_COMPLETE` marker. Three runs during this review left 48 markers; they were deleted. A marker without data is ignored by Bronze, but a test whose cleanup fails leaves a complete synthetic execution that the next refresh promotes | Low | Fixed: `tests/integration/conftest.py` gives a test bucket (`ODP_TEST_RAW_BUCKET`, default `odp-test-raw`, rejected if equal to the raw bucket) and deletes every raw, marker, and quarantine object of the test's executions at teardown |
| T3 Unit tests encode the gate bypass | P3-13 | `test_bronze_checks_skip_when_no_table_was_written` and `test_silver_checks_skip_when_silver_wrote_nothing` (`tests/unit/lakehouse/test_refresh.py`) assert the skip that causes R1. No test ran two refreshes with a failed check between them | Low | Strict xfail test for R1 added; P3-16 changes the two tests with the fix |
| T4 Atomic-replace test covers only the rebuild path | P3-11, P3-13 | `test_bronze_replaces_each_table_in_one_statement` renders `render_bronze_sql`, the full-rebuild SQL. The scheduled `MERGE` script had no "no `DROP TABLE`" assertion | Low | Fixed: `test_scheduled_bronze_merge_never_drops_or_replaces_the_table` |
| T5 Dead write-mode placeholders | P3-13 | `WriteMode.INCREMENTAL_MERGE` and `WriteMode.APPEND_ONLY` are placeholders, still tested by `test_incremental_placeholders_fail_loudly`. `APPEND_ONLY` shares its name with the `append_only` update pattern, which Silver implements differently. Remove them or implement them in P4-04 | Low | P4-04 |
| T6 Coverage ledger has no integration test | P3-06 | `tests/integration/ingestion/test_postgres_oltp_ingest.py` did not pass a ledger. The ledger was covered by unit tests with fakes and by the P3-06 live run | Low | Fixed: the test records three daily extracts in the ledger and checks rows, counts, and a clean report with one re-run window |
| T7 No test of Gold metric logic | P2-06, P2-07 | Reconciliation compares Gold with Gold or with Silver totals (20 of 56 files read Silver). No test builds a mart from a small fixture, so a grain error such as R2 passes every check | Medium | P3-17 |
| T8 PostgreSQL integration test truncates the local OLTP tables | P1-06 | The test ran `seed_synthetic`, which truncates `oltp.*`, on `POSTGRES_DB` before and after the test. Running it reset the local source to the fixture. The review run did this on 2026-11-21: the three `p3-05-late-*` sellers and any other manual rows are gone from local PostgreSQL and remain only in the lake | Low | Fixed: the test creates a scratch database `odp_it_<hex>` for the OLTP tables and the ledger, and drops it at teardown |

## Where the fixes go

| Fix issue | Findings | Position |
| --- | --- | --- |
| P3-16 Quality gate state and extract-window corrections (new) | R1, R7, R8, R13, R14, T3 | Phase 3, first after P3-06 |
| P3-17 Gold metric grain corrections (new) | R2, R3, R19, T7 | Phase 3, before P3-14 |
| P3-18 Exact decimal type for money fields (new) | R4 | Phase 3, before P3-14 |
| P3-07, P3-08, P3-10 (amended) | R9, R10, R5, R16 | Phase 3 |
| P4-01, P4-04 (amended) | R12, R11, T5 | Phase 4 |
| P5-07 (amended) | R15 | Phase 5 |
| P6-02, P6-04 (already scoped), P6-06 (amended), P6-09 write-audit-publish (new) | R17, T1 (CI on skip), R18, R6 | Phase 6 |

## Documentation drift found

| Location | Drift |
| --- | --- |
| `docs/architecture/phase-2-lakehouse.md` (Partitioning) | Says the Bronze write replaces the whole table; since P3-13 it is a `MERGE` |
| `PROJECT_STATUS.md` (Risks) | Still lists Silver `DROP` → `CREATE` → `INSERT OVERWRITE`, `full_refresh` records that never leave Silver, and clickstream labelled `snapshot`, all fixed by P3-11 and P3-12 |
| `PROJECT_STATUS.md` (Phase 3 acceptance, program roadmap) | Acceptance boxes differ from `ROADMAP.md`; the Phase 3 row omits P3-05 and P3-06 |
| `ROADMAP.md` (source refresh matrix) | Says only the PostgreSQL daily DAG starts `transform_lakehouse`; since P3-13 every ingestion DAG emits an Asset |

All four are corrected with this review.
