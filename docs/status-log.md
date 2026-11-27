# Status log (archive)

Chronological history of program status updates. The operational ledger is [`PROJECT_STATUS.md`](../PROJECT_STATUS.md) (current phase only; **rewritable**). Append closed-phase or long narrative entries here when trimming the live ledger. Architecture docs and ADRs should cite this log (or carry the facts inline), not `PROJECT_STATUS.md`, for durable evidence.

---

### 2026-11-27 — R1-01 Remove the AWS deployment path

Owner decision: the platform core is open source only ([ADR-0029](decisions/0029-open-source-only-platform.md), D-029 approved). Phase 7 (P7-01 to P7-09) and P6-03 are withdrawn; phase numbers are kept. P5-05 becomes the local resource footprint and cost to run (measured CPU, memory, disk and storage per service, run time per layer, a machine cost from stated assumptions, and a performance analysis). ADR-0011 is superseded, ADR-0001 keeps its Compose half, and the other ADRs, `DECISIONS.md`, `AGENTS.md`, the README, architecture docs, cover images, and site no longer name AWS counterparts. A Refactor R1 table in `ROADMAP.md` lists R1-01 to R1-03.

| Check | Result |
| --- | --- |
| `make lint` | Clean; markdown tables: 71 files, 0 with an empty header row |
| `uv run pytest tests/unit -q` | Passed |
| `cd site && npm run build` | 6 pages built |
| Leftover `git grep` for AWS, Terraform, Athena, Glue, OIDC, QuickSight, MWAA, EMR, MSK, CloudWatch, DynamoDB, FinOps, cost model and Phase 7, outside the status log, genesis doc, reviews, ADR-0001, ADR-0011, ADR-0029 and lockfiles, with S3 client names filtered | Remaining hits: withdrawn rows, the R1 table, D-001's superseded note, changelog lines, verified Phase 0–1 issue text, rejected-alternative rows citing ADR-0029, the Hadoop AWS jar pin, and links to the ADR-0001 file name |

The S3 client libraries MinIO needs (`hadoop-aws`, `aws-java-sdk-bundle`, `fs.s3a.*`, Trino `s3.aws-*`, boto3 `aws_*`, the Airflow `aws` connection type) stay.

---

### 2026-11-27 — P3-15 Phase 3 documentation and status; Phase 3 verified

New phase index [`docs/architecture/phase-3-orchestration.md`](architecture/phase-3-orchestration.md): the DAG schedules, catchup, gap report and backfill, the quality gate and quarantine, the rule that `make live-*` targets and the DAGs never run at the same time (they share the Iceberg tables, the promotion registry, the coverage ledger, and the Ivy cache), the 2026-11-21 review findings closed in Phase 3 and the ones left to later phases, and the P3-14 live result. ADR-0024 and the P3-14 live-run doc now link to that rule.

The open Phase 3 acceptance items are checked on recorded evidence: DAG dependencies and failure behavior (P3-03, P3-09), the daily path (P3-04, P3-13, P3-14), backfills without duplication (P3-06, and the P3-14 reruns with no duplicate key), readers never see a missing or half-written table (P3-11, the P3-09 killed Silver write, the 152 P3-14 reads), success and failure evidence (P3-09), and this documentation. Tables are still visible before their checks run (review R6, P6-09).

| Check | Result |
| --- | --- |
| `make lint` | Clean |

`PROJECT_STATUS.md` moves to Phase 4. `README.md` marks orchestration as implemented.

---

### 2026-11-27 — P3-14 live run, result, and consistency check

`scripts/p3_14_live_run.sh` drove the shipped daily and weekly PostgreSQL DAGs on local Airflow 3.4.0 from 2026-11-26 05:52Z to 2026-11-27 00:26Z, with no fault injected. The Airflow, catalog, and Trino helpers of the P3-09 script moved to `scripts/lib/live_check.sh`, which both scripts source. P3-09 `no-new-raw` and `happy` passed on the moved helpers. Each landing (`extract_coverage.row_count`) was compared to the PostgreSQL rows with `updated_at` in its window, counted right after the extract.

| Check | Result |
| --- | --- |
| Daily backfill of 2026-10-22, one planted on-time seller | Landed sellers 1, source 1, other datasets 0 and 0. The refresh took 120 s. Silver rows in the window equal the source |
| Rerun of the 2026-10-24 daily run (cleared, same run id) | Landing equal to the first and to the source. The refresh read 0 raw objects because the ETags were unchanged (1.3 s) |
| New execution of `[10-24, 10-25)` | Landing equal to the first. Silver counts unchanged, no duplicate key, daily gap report clean |
| Late seller in 2026-11-18, then the weekly run, window `[11-15, 11-22)` | Weekly landed sellers 1 against 0 in the previous weekly landing and in the daily sum. Silver sellers +1, other tables unchanged, no duplicate key |
| Refresh with no new raw objects | 2.6 s. Every write step skipped and no Iceberg table got a new version |
| Silver reads during the daily and weekly refreshes | 152 Trino counts, 0 failed, 0 empty |
| Daily DAG paused 11-26 06:20Z, unpaused 11-27 00:22Z | One scheduled run for `[11-26, 11-27)`, 0 rows landed and 0 in the source. One refresh (126 s). Gap report: 8 windows, clean |
| Raw objects read per refresh | 4 new objects out of 245 to 273 promoted; 0 to 2 rows out of about 1.57 M |
| Quarantine and cast counts | All 0 for the four `postgres_oltp` datasets |
| `make lint`, `make test` | Clean; 617 passed |

The first daily attempt on 2026-10-24 logged one FAIL: the script expected Silver sellers to grow by exactly the planted seller. That day held one more source seller that no earlier window had extracted, and landing, source, and Silver all agreed on 2. The check now compares the window's Silver rows to the source. Observations: an empty window still rebuilds Silver and Gold, and Silver scans the `postgres_oltp` Bronze tables twice. Detail: [`docs/architecture/phase-3-live-run.md`](architecture/phase-3-live-run.md).

---

### 2026-11-26 — P11-03 Kafka broker and BioGuardia telemetry producer

Kafka `apache/kafka:4.3.2` (KRaft, one node, [ADR-0028](decisions/0028-telemetry-message-broker.md)) runs on the Compose profile `streaming` with `kafka-init` (`docker/kafka/create-topics.sh`) creating the four topics: readings 6 partitions, health 3, alerts 3, gateway 1; `retention.ms` 259200000 (72 h), `retention.bytes` caps, automatic creation off. The `bioguardia-telemetry` service (`telemetry run`, image `odp-sources`, `confluent-kafka` 2.16.1 in the uv group `sources`) publishes the world model's events with `acks=all`, idempotence and zstd, keyed by `device_id`, checkpoints every 5 s on the volume `odp_telemetry_state`, and has no Airflow dependency. New commands: `telemetry model` and `telemetry verify`, `make stream-up`, `stream-stop`, `stream-check`. Source note: [`bioguardia-telemetry.md`](sources/bioguardia-telemetry.md).

| Check | Result |
| --- | --- |
| Unit tests (`tests/unit/simulators/bioguardia/test_telemetry.py`) | Passed: modeled rate at least 1.0M per hour at scale 1, delivered events per device and UTC day equal `generated - dropped` of `device_daily_counters`, unique `event_id`, contiguous sequences, restart skip counts equal to the gateway records, an outage ending at 00:00 UTC is uploaded, overlapping outages merge, payload fields listed in the source note, a restarted producer replays and logs counts only |
| Topics | `kafka-topics.sh --describe`: partitions, replication factor 1, retention and caps as documented |
| Rate, 60 minutes | 2026-11-26 11:13:18Z to 12:13:30Z with Airflow stopped: 1,019,332 messages (1,015,945 per hour), 99.97% of the model's 1,019,639 for the window; producer 2.4% CPU and 162 MiB, broker 23.9% CPU and 1.03 GiB |
| Rate, 60 minutes across 00:00 UTC | 2026-11-26 23:21:37Z to 2026-11-27 00:21:40Z, with Airflow running (scheduler, API server, DAG processor): 1,022,299 messages (1,021,447 per hour), 100.01% of the model's 1,022,162 for the window; producer 2.2% CPU and 173 MiB, broker 14.3% CPU and 1,002 MiB; segment growth 56.7 MB |
| Verify across 00:00 UTC | `telemetry verify` for 2026-11-26 23:30Z to 2026-11-27 00:15Z: 763,408 expected, all found, gateway-skipped 0, missing 0, 793 duplicate copies |
| Storage | About 51 bytes per message in the segment files: 52 MB per hour, 1.24 GB per day, 3.7 GB for 72 hours |
| Unplanned stop | 2 h 50 min down (host sleep and reboot); the restart replayed 2,888,706 events and skipped 103 older than the buffer, with a gateway record; `telemetry verify` 08:00Z to 08:40Z: 676,950 expected, all found, 47,185 duplicate copies |
| Planned 10-minute stop | Stopped 12:14:49Z, started 12:24:53Z: the restart was 619 s behind, replayed 175,697 events, skipped 0, and wrote a gateway record. `telemetry verify` for 12:10Z to 12:30Z: 338,006 expected, all found, 0 missing, 1,169 duplicate copies (0.1% retransmissions and at most 5 seconds re-sent after the checkpoint) |
| `code-reviewer` | Round 1 BLOCK (shared with P11-02: outage at 00:00 UTC, overlapping outages), fixed; round 2 PASS; warnings fixed (heartbeat during a long restart walk, stops over 7 days recorded as `gap_too_long`, buffer capped at 24 h) |


---

### 2026-11-26 — P11-02 BioGuardia daily export generator

`open_data_platform.simulators.bioguardia` generates the BioGuardia Operations Hub and Finance daily exports from one deterministic world model ([ADR-0027](decisions/0027-bioguardia-synthetic-sources.md), [domain model](architecture/bioguardia-domain.md), source notes [`bioguardia-ops-export.md`](sources/bioguardia-ops-export.md) and [`bioguardia-finance-export.md`](sources/bioguardia-finance-export.md)). Each drop is a set of gzip CSV and JSON Lines files, a `_manifest.json` (rows, bytes, uncompressed bytes, SHA-256, columns, exact decimal control totals, time range), and `_SUCCESS` written last, in MinIO `odp-landing`; the planted-defect lists go only to `odp-evaluation`. New: image `odp-sources` (`docker/sources/Dockerfile`, `python:3.12.15-slim-bookworm` by digest, dependencies exported from `uv.lock` with hashes), Compose service `bioguardia-ops-export` on profile `sources` (the release loop: ops at 02:00 BRT, finance at 06:00 BRT, version 2 three days after version 1, catch-up in date order), Make targets `sources-build`, `sources-up`, `sources-requirements`, `bioguardia-export`, and the `.env.example` names. The drops from 2026-11-26 to 2026-12-31 were published in advance at Filipe's request (`--allow-future`), so they are visible before their release time; P11-05 must ignore a drop whose `released_at` is in the future.

| Check | Result |
| --- | --- |
| `uv run ruff check` | Passed |
| `uv run pytest tests/unit/simulators` | 44 passed: determinism, a date alone equal to the same date inside a range, cost not growing with history, manifests against files, SCD2 signals (late-entered calibrations, future-dated assignments), deletes, synthetic PII (CPF and CNPJ checksums fail, `.example` emails, unassigned area codes), skew shares, referential integrity, file defects, version 2 corrections, the byte cap, release times, an upload interrupted mid-drop, logs without row values, source notes listing every column |
| `make test`, `make lint` | 661 passed; lint passed |
| `pytest tests/integration/simulators -m integration` | 1 passed against the local MinIO (`odp-test-landing`) |
| Backfill 2026-11-26 to 2026-12-31 | 37 ops drops (36 dates and the version 2 of 2026-12-15) and 36 finance drops; 880,848,289 rows and 13,187,980,547 bytes of gzip (118.5 GB uncompressed); 23.8M rows and 356 MB per ops drop; 22 min 9 s with 12 workers in the `odp-sources` container; every manifest's time range parses; the 2026-12-01 drop holds 22 tables (`canopy_surveys`), the others 21. Per-drop figures are in the source note |
| Planted | File defects on 2026-12-20 (`deleted_records` truncated, `deforestation_alerts` missing) and 2026-12-22 (`planting_lots` truncated); 19,065 corrections in the version 2 of 2026-12-15; 18,611 record defects and 7,605 free-text PII values in December; `devices` schema version 2 from 2026-12-15 |
| Regeneration | 2026-12-07 regenerated in the same image into a test bucket: the 21 SHA-256 values equal the published ones |
| Kill drill | A run for 2026-12-08 killed after 90 s left no object and no `_SUCCESS`; the rerun completed it with the published SHA-256 values |
| One drop alone | 218 s and 432 MB of peak resident memory at scale 1 |
| `code-reviewer` | Round 1 BLOCK: an outage ending at 00:00 UTC was never uploaded by the stream and overlapping outages were counted per outage; both fixed with tests (outages are merged and the upload reads the previous day's plan). Round 2 PASS; its warnings fixed |

---

### 2026-11-26 — P11-01 BioGuardia Atlantic domain model and Phase 11

Filipe approved the Phase 11 plan on 2026-11-26 with two changes: the daily batch export grows to at least 20M rows a day with a 2 GB cap per drop, and the telemetry stream is 1M events an hour instead of 5M. Added [`docs/architecture/bioguardia-domain.md`](architecture/bioguardia-domain.md) (organization, use cases, sources, 23-dataset catalog, telemetry topics, world model, the PII, skew, SCD2 and reconciliation cases, synthetic-data safety, planted defects, storage budget), [ADR-0027](decisions/0027-bioguardia-synthetic-sources.md) and [ADR-0028](decisions/0028-telemetry-message-broker.md) (`proposed`, D-027 and D-028), an addendum to ADR-0008, and Phase 11 in `ROADMAP.md` (P11-01 to P11-18). The waiver that lets P11-01 to P11-03 run before Phase 3 closes is in the `DECISIONS.md` changelog. P4-02 now runs Debezium on the P11-03 broker, P4-04 depends on the P11-04 proof, P4-08 is the only CDC-based SCD issue, and P5-07's maintenance command is shared with P11-08 and P11-15.

| Check | Result |
| --- | --- |
| Phone area codes | ANATEL open data `PGCN.csv` (downloaded 2026-11-26): 67 assigned codes; unassigned 20, 23, 25, 26, 29, 30, 36, 39, 40, 50, 52, 56 to 60, 70, 72, 76, 78, 80, 90 |
| IBGE codes | 54 curated municipalities in BA, ES, MG, RJ, SP, PR, SC, RS matched by name and UF in the IBGE Localidades API (5,571 municipalities, 2026-11-26) |
| Versions | `apache/kafka:4.3.2` (Docker Hub, 2026-10-20), `python:3.12.15-slim-bookworm` (2026-10-21), `confluent-kafka` 2.16.1 (PyPI, 2026-10-28) |

---

### 2026-11-26 — P3-10 orchestration runtime hardening

Spark local mode in the runtime image is capped. `run_bronze.sh` and `run_silver.sh` take `local[${ODP_SPARK_CORES}]` and `--driver-memory ${ODP_SPARK_DRIVER_MEMORY}` (Compose defaults 2 and `2g`; before: `local[*]` and Spark's 1g default) and exit 1 on a malformed value. Every script under `docker/spark` that starts Spark sets `spark.sql.session.timeZone=UTC` (review R16). The runtime container's JVM default was already UTC (`date +%Z`, `TZ` unset). The Airflow pins moved from the uv `dev` group to a non-default `airflow` group; `test_dagbag.py` and `test_run_python_args.py` skip without it, and `make test` and CI sync it. `medallion_build prune-work` deletes run folders under `/opt/odp/work` whose newest file is older than `ODP_WORK_RETENTION_DAYS` (default 7). It never deletes the current run's folder and touches no Airflow run, task log, table, or raw object. `transform_lakehouse` runs it as `prune_work`, a task with no edges; `make live-prune-work` runs it from Make. `.env.example` replaces `TRINO_HOST`, `TRINO_PORT`, and `TRINO_USER` with `ODP_TRINO_HOST_PORT` (the published host port) and `ODP_TRINO_USER`; `dbt/profiles.yml` stays versioned and reads the user from `ODP_TRINO_USER`; `dbt/profiles.yml.example` was removed.

| Check | Result |
| --- | --- |
| `make lint` | All checks passed; markdown tables 63 files, 0 empty headers |
| `make test` | 614 passed |
| Unit tests in a venv synced without the `airflow` group | All passed; `test_dagbag.py` and `test_run_python_args.py` skipped |
| `test_spark_scripts.py` with the time zone conf removed from `warm_ivy.sh` | Failed as expected; passes with it |
| `make live-rebuild` with the defaults | Exit 0 in 603.7 s. Bronze 259.3 s, Bronze checks 2.5 s, Silver 279.1 s, Silver checks 9.9 s, stage-quarantine 13.6 s, dbt-build 29.0 s (`PASS=294`). The 2026-11-23 rebuild with `local[*]` took 632 s |
| Spark process during the rebuild | Driver JVM `-Xmx2g`, `--driver-memory`, `spark.sql.session.timeZone=UTC`; Spark logged `Spark master: local[2]` |
| Row counts | 18 Silver tables, `gold_shared.dim_date`, and `gold_staging.quarantine_findings`: identical before and after. 56 Gold and staging base tables: identical to their previous Iceberg snapshot. `silver_audit.quality_counts` 866 to 870, its per-run audit rows |
| `transform_lakehouse` run `manual__2026-11-26T05:27:14.456317+00:00` | Success. Refresh tasks skipped (no new raw objects); `prune_work` success. Work volume 113 folders (474_478_934 bytes), 6 older than seven days, to 108 folders (444_862_796 bytes) including the new run's folder |
| `make live-prune-work` right after | `removed 0 run folders older than 7 days (0 bytes), kept 108 and this run's folder`; step skipped |

Not run: a full refresh through the DAG with new raw objects under the caps (the Make rebuild exercised the same scripts). The `prune_work` summary line of the DAG run is in the remote task log bucket and was not read; the folder counts above are the evidence.

---

### 2026-11-25 — Test suite pruning and P2 probe retirement

Removed unit tests that only matched source text or compared a name with itself: the three P2 Spark probe contract files, `tests/unit/test_package.py`, the re-export and `MAX_RESPONSE_BYTES` asserts, the grep-based DAG checks that `test_dagbag.py` already reads from the loaded DAG objects, and the image-tag, `.env.example` and Makefile-phrase checks in `test_airflow_compose_contract.py`.

The invariants those files held that nothing else covered became behavior checks:
- `test_dagbag.py`: the daily `start_date` equals `CADENCES["daily"].first_start`, the weekly one is `CADENCES["weekly"].first_start` plus one day, and `platform_env` runs its four service checks after `imports`.
- `test_reconciliation.py`: every directory under `dbt/tests/reconciliation` has a `dbt_project.yml` entry tagged `reconciliation`, which `dbt build` excludes.
- `test_airflow_compose_contract.py`: every Airflow service is on the `orchestration` profile with `LocalExecutor`, there is no Celery, Redis or Flower, and the code mounts are read-only with no repo-root mount.
- `test_dagbag.py`: the manual extract renders `--frequency manual`, its window params and run id.
- `test_package_exports.py`: every name in `open_data_platform.ingestion.__all__` resolves.
- `test_ingestion_dags.py`: the DAG directory listing, no `shell=True`, and no SQL `SELECT` in DAG sources.

`make test-iceberg-poc`, `make test-catalog-probe` and their four `docker/spark` files were retired. The Bronze step creates the catalog database (`ensure_catalog_database`) and the warehouse bucket, so the README run list starts at `make live-bronze`. `make live-bronze` and `make live-refresh` now run the `ensure-catalog` step before Trino starts, which the probe used to do. `uv run pytest tests/unit`: 570 passed. `ruff check`, `ruff format --check` and `make lint` passed. `make live-bronze` on a fresh volume: a separate Compose project (`-p odp-scratch`, own container names, ports and volumes) started with no `iceberg` database and no warehouse bucket. The 541 `raw/` objects were copied from the local raw bucket with their S3 metadata. `ensure-catalog` created the database in 0.7 s, Trino came up healthy before any Iceberg table existed, the `bronze` step created the warehouse bucket and 18 tables, and `bronze-checks` passed 18/18. Exit 0 in 273 s. A first attempt copied the objects without their metadata, and Bronze refused them with `object metadata does not match the key`. `make live-refresh` on a fresh volume was not run.

---

### 2026-11-24 — P3-09 orchestration success and failure scenarios

`scripts/p3_09_orchestration_check.sh` drives the shipped DAGs on the local Airflow 3.4.0 (LocalExecutor). It reads task states and try numbers from the Airflow metadata database, step results from `lakehouse_state.step_runs`, and table versions from `iceberg_tables.metadata_location`. Faults come from outside the code: `ALTER DATABASE odp ALLOW_CONNECTIONS false`, `docker compose stop trino`, a SIGTERM to the Silver step's process group, and a `BEFORE UPDATE` trigger that adds 1 to the Bronze `orders` expected count. An `EXIT` trap undoes them. No DAG or package code changed.

| Scenario | Result |
| --- | --- |
| `catchup` | Daily paused at 2026-11-23 23:19Z and unpaused at 00:08Z with the OLTP database refusing connections. One `scheduled__2026-11-24T00:00:00+00:00` run; `settle` released it at 00:15:23Z. Three extract tries failed and the run failed, with no coverage row and no transform run. The gap report exited 1 with `gap [2026-11-23,2026-11-24)`. After the clear, try 4 succeeded, the report was `clean`, and one refresh ran in 100 s |
| `happy` | Manual extract, daily backfill of 2026-10-11, weekly run with `-l now` (interval `[11-16, 11-23)`), and `reconcile_gold` all succeeded. The refreshes took 120, 90, and 100 s; `dbt_reconcile` 580 s |
| `retry` | Extract try 1 failed with the source closed and try 2 succeeded; one `dt=` prefix for the execution. `dbt_build` try 1 failed with Trino stopped and try 2 succeeded; the refresh took 150 s |
| `reconcile` | Try 1 failed (27 s) and try 2 succeeded (562 s). With Trino down, 3 tries failed and the run failed. After the clear, try 4 succeeded (541 s) |
| `write-failure` | Silver try 1 was killed after 9 s. Four Silver `postgres_oltp` tables kept their `metadata_location` and count, and 152 reader queries all succeeded with non-zero counts. Try 2 took 65 s |
| `no-new-raw` | Four write steps skipped and none of the 94 Iceberg tables changed; the run took 14 s |
| `coalesce` | Two extracts finished during a running refresh. One follow-up run consumed both Asset events and succeeded |
| `gate` | r1: `bronze_checks` failed and the four later tasks were `upstream_failed`. r2 (no new raw): the check failed again and Silver `orders` kept its version. r3, after the plant was removed: the check passed and Silver was built |

Airflow 3.4 behavior the script had to handle:
- A data-interval DAG triggered without `-l` has no interval.
- A one-day backfill of the day after an earlier one-day backfill was refused with `already exists`.
- `task_instance` keeps one row per try.
- A manual run, which has no logical date, is cleared through REST `clearTaskInstances`.
- The runtime image has no `kill` binary.

The first gate attempt planted the mismatch with a `BEFORE INSERT OR UPDATE` trigger. The registry's `INSERT ... ON CONFLICT DO UPDATE` fired both triggers and added 2, so the trigger is now `BEFORE UPDATE` only.

`make lint` passed. `uv run pytest tests/unit`: 582 passed. `test_main_reports_a_failed_step` fails when `POSTGRES_USER` is exported in the shell (it expects the variable to be missing). Not covered: a scheduler crash during a task, and Spark resource limits (P3-10).

---

### 2026-11-23 — P3-08 execution metadata, Bronze `dt` partitions, Trino client

ADR-0026 approved. Bronze tables are `PARTITIONED BY (dt)` and the incremental `MERGE` adds `target.dt IN (<staged dt values>)` to its `ON` clause (`lakehouse/bronze.py`). Before an incremental `MERGE` the Bronze step reads the table's Iceberg metadata through the JDBC catalog and fails, naming `make live-rebuild`, when the default spec has no `dt` identity field (`lakehouse/iceberg_catalog.py`). `medallion_build` records `lakehouse_state.step_runs` (every attempt: status, seconds, exception type, one-line message), `dataset_runs` (per run, layer, dataset: objects, rows read and written, table total, seconds; Gold rows from dbt `run_results.json`), and `table_io` (per Iceberg table a dataset's Spark script touched: scan files, skipped files, bytes, rows; commit files and rows) (`lakehouse/run_log.py`). Scan counts come from the Spark event log and commit counts from Iceberg `LoggingMetricsReporter` lines, both captured per script in a mode-700 directory, parsed for counts, and deleted (`lakehouse/spark_metrics.py`, `docker/spark/log4j2-metrics.properties`). A probe showed that Iceberg 1.12 does not send Spark scans to `LoggingMetricsReporter`, so the plan's scan-report source moved to the event log. Trino errors name `errorName`, `errorType`, `errorCode`; the `nextUri` loop stops at `ODP_TRINO_QUERY_TIMEOUT` (default 600 s) and cancels the query. `bronze_refreshes` is kept.

Review of the Silver audit table (asked by Filipe): every Silver write `MERGE`d into `silver_audit.quality_counts` with `WHEN NOT MATCHED BY SOURCE`, so it scanned and rewrote the whole table. The `ON` clause now fixes the run and dataset and that clause is gone.

| Run (local stack) | Result |
| --- | --- |
| Probe, partitioned table with 3 `dt` files | `MERGE` with `target.dt IN (…)` scanned 1 file and skipped 2; the same `MERGE` without it scanned 4 of 4 |
| `make live-bronze` on pre-P3-08 tables, window `[2026-10-18, 2026-10-19)` | `bronze` failed in 1.0 s: `bronze.postgres_oltp__sellers has no dt partition spec; run make live-rebuild once`. `step_runs` `failed` with `RefreshError`; the objects stayed unregistered |
| `make live-rebuild` | 10 min 35 s. Bronze 276.4 s (18 tables, 142 objects, 1_571_774 rows; 18 checks passed), Silver 282.0 s (18 tables, 9_429_824 Bronze rows scanned, 1_303_814 written; 43 checks passed), stage-quarantine 14.9 s, dbt-build 36.5 s (55 models, 768_196 rows affected) |
| `quality_counts` during that rebuild (old `MERGE`) | Each write scanned the whole table: 153 rows at the first dataset, 256 at the last; one file removed and one added per commit |
| `make live-refresh`, window `[2026-10-19, 2026-10-20)` | 2 min 22 s. `orders` and `order_items` 1 row each; their `MERGE` scanned 0 files (the `dt=2026-11-23` partition was empty). `quality_counts` writes added 1 file each, removed 0, scanned 2 files (8_918 bytes) each while skipped files grew 0, 2, 4, 6 |
| Re-extract of `[2026-10-19, 2026-10-20)` into the same `dt`, `make live-bronze` | `orders` `MERGE` scanned 1 file (6_877 bytes) of the table's 3, `order_items` 1 (6_437 bytes) of 3: only the partition it writes |
| `make live-refresh`, nothing new | Six steps `skipped`, each 0.2 to 0.6 s |
| Airflow `transform_lakehouse` `manual__p3-08-check` | Six `step_runs` rows under the run id; the DummyJSON snapshot `MERGE` scanned 1 file and skipped 1 (58_580 bytes), 194 rows. The catch-up runs after Airflow started were recorded too (`dbt-reconcile` 632.9 s) |

`uv run pytest tests/unit`: 582 passed. `make lint` and `ruff format --check` passed. Not exercised live: the Trino total timeout (unit test with a fake clock) and a Spark script failure after the event log started (unit test; the live failure path was the partition guard). `quality_counts` files are pruned by dataset only: Iceberg truncates `run_label` bounds to 16 characters, so a dataset's write still scans one more small file per run until compaction (P5-07).

---

### 2026-11-23 — P3-07 quality checks, quarantine, cast-failure counts, and volume guard

`on_invalid` becomes `quarantine_exclude`, `quarantine_flag`, or `fail_batch`; the plain `quarantine` value is rejected. Filipe approved the per-source choice: exclude for `postgres_oltp` (4) and `demo_storefront.clickstream`, flag for the 9 Olist e-commerce datasets, the 2 funnel datasets, DummyJSON products, and IBGE municipalities. `snapshot` and `full_refresh` contracts declare `volume_guard.min_row_ratio` (Olist and funnel 1.0, DummyJSON 0.9, IBGE 0.99); the loader requires it on those patterns and rejects it elsewhere. All 18 contracts got a new version.

Bronze reads the sidecar of each promoted raw object and stores `_quarantine_reasons` per row (only `record_index` and `reason` are read; a bad line fails the object without echoing its value). This replaces the separate `bronze.quarantine_records` table in the approved plan: the reasons travel with the row in the same Bronze commit, so there is no second table to keep in step. Silver adds `_quarantined` and `_quarantine_reasons` to every table, filters `NOT _quarantined` before deduplication for exclude datasets, runs a volume guard before the replace for `snapshot` and `full_refresh`, and after the replace `MERGE`s per-run counts into `iceberg.silver_audit.quality_counts` (`cast_null` per typed column, `null_key_dropped`, `quarantine_excluded` or `quarantine_flagged`). `medallion_build --allow-volume-drop SOURCE.DATASET` skips one guard for one run. The Silver checks add one quality-count statement per dataset and one exclusion statement per exclude dataset. `gold_staging.quarantine_findings` and the 7 `fct_*_quality_findings` models carry `silver_action`; the 7 models gained dbt unit tests and moved from `pending` to `covered`.

Live, local Compose with Airflow stopped:

| Step | Result |
| --- | --- |
| `make live-rebuild` | 632 s. Bronze checks 18, Silver checks 43, `stage-quarantine` 1_603, dbt PASS=294. 101 quality-count rows, every count 0. `order_reviews` 99_224 rows, 0 flagged: the 1_603 staged findings belong to `execution_id=run-001` (contract v1, no marker), not to the promoted execution |
| Planted `demo_storefront` execution `p307-planted` (4 events: valid, unparsable `event_time`, null `event_id`, integer `session_id`; sidecar 2 findings) + `make live-refresh` | Bronze +4 rows, 2 with reasons. Silver 100 → 102. Counts: `event_time` `cast_null` 1, `null_key_dropped` 1, `quarantine_excluded` 2. Gold storefront findings: `type_mismatch` and `null_violation`, both `excluded` |
| DummyJSON execution `p307-truncated` (1 of 7 pages, 30 products) + `make live-refresh` | `silver` failed: "volume guard: dummyjson_products.products latest complete execution has 30 rows, previous has 194; min_row_ratio 0.9". Silver stayed 194; `pending_layers` kept `silver dummyjson_products products` |
| `make live-refresh REFRESH_FLAGS="--allow-volume-drop dummyjson_products.products"` | Guard skipped for that dataset; Silver 30; dbt PASS=294 |
| DummyJSON `p307-restored` (7 pages, 194) and a clean rerun of `p307-planted` with corrected events (sidecar deleted) + `make live-refresh` | Clickstream object re-promoted (`replaced=1`), 0 rows with reasons; Silver 104 with the 4 `p307-*` events; `quarantine_excluded` 0, `cast_null` 1 (the timestamp stays unparsable); storefront contract findings 0. DummyJSON Silver 194 |
| `make live-refresh` with nothing new | Every step skipped |

`make dbt-unit` PASS=34. `uv run pytest tests/integration` 9 passed. `uv run pytest tests/unit` 568 passed. `make lint` and `make dbt-parse` pass. Not exercised: the guard failure inside the Airflow `transform_lakehouse` DAG (same `medallion_build silver` step). `stage-quarantine` still lists the whole prefix, which is why the legacy v1 sidecar stays in Gold.

### 2026-11-22 — P3-18 exact decimal type for money fields

The contract vocabulary gains `decimal` with `precision` and `scale` (R4). Seven money fields move to `decimal(12,2)` in new contract versions: Olist `order_items` v4 (`price`, `freight_value`), `order_payments` v3 (`payment_value`), `postgres_oltp/order_items` v3 (`price`, `freight_value`), `dummyjson_products/products` v4 (`price`), and `olist_marketing_funnel/closed_deals` v5 (`declared_monthly_revenue`, added to the acceptance by Filipe). Each field names its currency in its description (BRL; DummyJSON: not stated by the API), and `data-contracts.md` states the currency rules. `contracts.decimals` reads JSON numbers as `Decimal` and writes them as exact text. The PostgreSQL extract writes `numeric(12,2)` as its text, with no float. The classifier accepts an `int` or a `Decimal` that fits as written and reports a float or a value that needs rounding as `type_mismatch`. Bronze keeps the exact digits of a `decimal` field; other numbers keep their old text. Silver casts to `DECIMAL(12,2)` only text that fits (`decimal_sql`), so a value that would round is NULL. Gold amounts and sums stay decimal; ratios, means, and the catalog median are `double` from the exact sums. The ten `scalars_near` checks became exact `scalars_differ`, and the macro was removed.

Two facts in the plan were wrong and were corrected on the local Trino 484: a decimal quotient is `decimal(38,6)`, not scale 2, and a decimal mean keeps the input scale (the mean of 1.00 and 2.01 is 1.51); the ratio columns are `double` for both reasons. `approx_percentile` turns a decimal into a `real`, so the `mart_catalog_prices` reconciliation test now casts to `double` as the model does.

Local stack, 2026-11-22, Airflow services stopped:
- Spark 3.5.8 in the runtime image (`spark-sql`, literals only): `CAST` and `TRY_CAST` of `'9.999'` to `DECIMAL(12,2)` both give 10.00. The guarded cast gives NULL for `'9.999'` and `'1E+2'`, and 10.50 for `'10.50'` and the old token `'10.5'`.
- `make live-rebuild`: exit 0 in 570 s. Bronze promoted 125 objects and 1_571_543 rows in 268.7 s, 18 Bronze checks passed (2.5 s), Silver 237.5 s with 20 checks passed (5.7 s), stage-quarantine 13.7 s, `dbt build` PASS=287 in 32.4 s.
- Bronze against Silver, joined on `raw_object_key` and `record_index`, for every money column: no value differs, Bronze and Silver have the same non-null count (Olist items 112_650, payments 103_886, closed deals 842, DummyJSON 194, OLTP items 2), and Silver is `decimal(12,2)`.
- Gold before and after: [P3-18 exact money](architecture/gold-model-design.md#p3-18-exact-money-2026-11-22). Item GMV is 15843553.24 in the fact and in every mart (three different doubles before). `payment_amount_mismatch` went from 381 to 303 orders: 273 orders differ by exactly 0.01, and the double comparison counted 78 of them above the threshold.
- `make dbt-test-reconciliation` PASS=56 with exact equality. `make dbt-unit` PASS=27, including the new planted-cents test (0.10 + 0.20 = 0.30). The same test with `double` inputs, in a temporary file that was then removed, failed with 0.3 → 0.30000000000000004.
- Re-extract of the covered window [2026-10-19, 2026-10-20) under a new execution id: the landed `order_items` text equals PostgreSQL `numeric::text` (1 of 1 row). The incremental refresh (175 s) promoted 4 objects and 2 rows, wrote 4 Silver tables, and `dbt build` PASS=287. Silver `postgres_oltp__order_items` kept 2 rows with sums 30.50 and 5.75, and OLTP item GMV stayed 36.25. A refresh with no new raw objects took 13 s and wrote nothing.

Commands: `uv run pytest tests/unit` 530 passed; `uv run pytest tests/integration` 9 passed (the extract round trip compares the landed text with `numeric::text`); `make lint` passed; `make dbt-parse` passed. Not run: the Olist and DummyJSON ingestion entry points, so their stored quarantine findings come from the earlier contract versions (the Silver check above shows every money value cast); the Airflow DAG path on the new contracts (P3-14); the dbt unit tests in CI (they need Trino).

### 2026-11-22 — P3-17 Gold metric grain corrections

`mart_review_drivers` resolves the English category per order before the review join: one distinct category gives that category, more than one gives `mixed`, no items or no category gives null (R2). The reconciliation check now compares `sum(review_count)` with the `(review_id, order_id)` pairs on `fct_reviews`. `on_time_handoff_rate` in `mart_delivery_performance`, `mart_seller_performance`, `mart_oltp_delivery_performance`, and `mart_oltp_seller_performance` divides by orders (or seller–order pairs) with a carrier date and a shipping limit, exposed as `handoff_eligible_count`; four reconciliation checks compare that count with the facts (R3). `fct_oltp_orders` and its `purchase_*` columns are documented as the current customer address (Type 1) until P4-08 (R19); `gold-model-design.md` had said the opposite. Five dbt unit tests (`dbt/models/<domain>/unit_tests.yml`) build the corrected marts from an order with two items and one review and a cancelled order with no carrier date (T7). `medallion_build dbt-unit` and `make dbt-unit` run them; `dbt build` excludes `test_type:unit`.

Filipe widened the acceptance in the same issue: `gold-layer.md` gains a unit-test rule (fan-out join, tie-break, rate on a subset, bucket or threshold, null-key row), and `dbt/unit_test_policy.yml` lists all 56 Gold models as covered (26), exempt with a reason (23), or pending (the 7 `fct_*_quality_findings` models, proposed for P3-07). `tests/unit/lakehouse/test_dbt_unit_test_policy.py` enforces the file. 21 more unit tests cover the other models the rule applies to: primary payment, latest customer address, latest category translation, and modal ZIP tie-breaks; late-order, cancellation, and platform-sales fan-out; freight bands; milestone and retention counts; the null-municipality row; repeated-email share; win rate and segment tie; storefront session and user counts.

Local stack, 2026-11-22: `make dbt-unit` PASS=26. With the old `mart_review_drivers.sql` restored, the review unit test failed (FAIL 1). Seven deliberate rule breaks (primary payment tie, customer address tie, storefront last-event tie, retention status filter, modal state tie, translation order, segment tie) each failed their test (FAIL 1) and were reverted. `make dbt-build REFRESH_FLAGS=--full-rebuild` PASS=287 WARN=0 ERROR=0 in 29.5 s. `make dbt-test-reconciliation` PASS=56 ERROR=0 in 600 s. Before and after on the same Silver: review mart rows 265 → 269, `sum(review_count)` 113_131 → 99_224, review-weighted mean score 4.0170 → 4.0864, 732 reviews in `mixed`; marketplace handoff 88_808 / 99_441 = 0.8931 → 88_808 / 97_657 = 0.9094; seller handoff 90_087 / 100_010 = 0.9008 → 90_087 / 99_000 = 0.9100; OLTP handoff rate 0.0 → null (0 of 2 orders eligible). Detail: [`gold-model-design.md`](architecture/gold-model-design.md#p3-17-metric-corrections-2026-11-22).

Commands: `uv run pytest tests/unit` 462 passed; `make lint` passed; `make dbt-parse` passed. Not run: the unit tests in CI (they need Trino), and the Airflow `transform_lakehouse` Gold task with the new argv (P3-14 live run).

### 2026-11-22 — Roadmap changelog and status ledger compacted

The `ROADMAP.md` changelog moves out of the Document control table into a `Changelog` section with one line per change; the detail stays in this log. Three changelog facts had no entry here and are recorded now: 2026-11-20, the development flow was simplified (`AGENTS.md` §3 and §13) and Filipe, not a PO, decides the Phase 10 position; 2026-11-06, the Phase 4 close pair is P4-09/P4-10 and P4-08 (SCD Type 2) sits before it; 2026-11-05, the Phase 2 close pair is P2-09/P2-10 and the Spark Ivy cache was inserted as P2-08. `PROJECT_STATUS.md` drops the earlier Phase 3 narrative, the stale program roadmap table, the Log copies, and two risks that no longer hold (the D-006/D-007 note and the R1 check bypass closed by P3-16). The P1-03 and P1-04 entries below are back in date order. No issue status changed.

### 2026-11-22 — P3-16 quality gate state and extract-window corrections

The promotion registry gains `lakehouse_state.layer_checks` (layer, dataset, status `pending`/`failed`/`passed`, expected Bronze rows, marking and checking run). A Bronze registration opens the Bronze check in the transaction that marks Silver pending; a Silver write opens the Silver check in the transaction that clears its flag. `bronze-checks` and `silver-checks` run every open check of their layer and record each result. `silver` refuses a dataset with an open Bronze check; `stage-quarantine` and `dbt-build` refuse while any check is open. `bronze` and `silver` end as "ran" while a check is open, so the check task is not skipped. Check tasks have `retries=0` (review R1, T3).

`coverage_report` takes `as_of`; expected windows run to the last window due (end plus the cadence run delay: 15 minutes daily, one day weekly), so a stopped DAG reports trailing gaps (R7). `schema.sql` adds the `set_updated_at` `BEFORE UPDATE` trigger (`clock_timestamp()` when an UPDATE leaves `updated_at` unchanged); the extract fails without it, and the new `apply-schema` command adds it without reseeding. The extract reads every dataset of a window in one `REPEATABLE READ, READ ONLY` transaction before writing, and refuses a window before its end plus 15 minutes; the daily DAG has a `settle` sensor (reschedule mode). The PostgreSQL DAGs pass `--ingestion-ts {{ dag_run.run_after.isoformat() }}` (R8, R13, R14).

`scripts/p3_16_gate_check.sh` on the local stack, 2026-11-22 03:17Z:
- The extract on the old schema failed on the missing trigger, and `apply-schema` created 4 triggers.
- r1: Bronze wrote 2 tables (30 s). With `orders.expected_rows` raised from 9 to 10, `bronze-checks` failed on `orders` only.
- r2: no new raw object. `bronze` ran, `bronze-checks` failed again, `silver` failed with `silver blocked` and wrote no table, and `dbt-build` was not pending.
- r3: with the value restored, the check passed, Silver wrote 4 tables (54 s), the Silver checks passed 4, and dbt PASS=287 (41 s).
- r4 skipped.

Commands: `uv run pytest tests/unit` 459 passed; `uv run pytest tests/integration` on local Compose 9 passed; `make lint` passed. Not run: the `settle` sensor and a task retry on a live Airflow scheduler (P3-09). Other ingestion DAGs still take `ingestion_ts` from the clock.

### 2026-11-21 — Review of verified issues

Every issue marked verified (Phases 0–3) was reviewed for data engineering soundness, without taking an ADR as proof. Findings, with code paths, severity, and the fix issue: [`docs/reviews/2026-11-21-verified-issues-review.md`](reviews/2026-11-21-verified-issues-review.md).

19 pipeline and model findings (R1–R19) and 8 test findings (T1–T8). Highest: a failed `bronze_checks` or `silver_checks` is bypassed by the next refresh with no new raw objects, which skips the check and builds the next layer (R1); `mart_review_drivers` counts a review once per order item (R2); `on_time_handoff_rate` counts cancelled and unshipped orders as late (R3); money is `DOUBLE` (R4); the Bronze `MERGE` scans the whole unpartitioned table (R5); the gap report misses trailing windows (R7). These come from reading the code. None was reproduced on the stack for this review.

Commands: `uv run pytest tests/unit` passed 444. `uv run pytest tests/integration` against local Compose passed 5 and failed 1: `test_olist_ingest_round_trip_overwrites_the_same_object` has failed since the P1-10 structural check (`1636d49`, 2026-10-26), because it appends a one-column row (T1). Those runs left 48 `_COMPLETE` markers of the test executions in `odp-raw` (T2); they were deleted. The PostgreSQL integration test truncates the `oltp` tables of `POSTGRES_DB` (T8), so the runs reset local PostgreSQL to the synthetic fixture: the `p3-05-late-*` sellers remain only in the lake.

Test fixes in the same PR: `tests/integration/conftest.py` routes integration tests to `ODP_TEST_RAW_BUCKET` (default `odp-test-raw`) and deletes every raw, marker, and quarantine object of their executions at teardown; the PostgreSQL test seeds a scratch database `odp_it_<hex>` and also checks the coverage ledger (T6); the Olist test appends a contract-width row (T1); a unit test covers the scheduled Bronze `MERGE` (T4); strict xfail tests reproduce R1 and R7 for P3-16. After the fixes: `pytest tests/unit` 446 passed, 2 xfailed; `pytest tests/integration` 6 passed, with no test object in `odp-raw` or `odp-test-raw` and no scratch database left.

Roadmap: new P3-16, P3-17, P3-18 (before P3-14) and P6-09; P3-07, P3-08, P3-09, P3-10, P3-15, P4-01, P4-04, P5-07, P6-04, P6-06, and P6-07 amended. The Phase 3 missed-window acceptance is reopened. No issue status changed. `PROJECT_STATUS.md` risks no longer list the Silver `DROP` and `full_refresh` limits that P3-11 and P3-12 closed.

### 2026-11-21 — P3-06 window coverage, gap report, and backfill

`src/open_data_platform/ingestion/coverage.py` adds the coverage ledger `ingestion_state.extract_coverage` in the catalog database ([ADR-0025](decisions/0025-extract-window-coverage-ledger.md)) and the gap report (`make coverage-report FREQUENCY=daily|weekly`). The PostgreSQL extract writes one row per dataset (source, dataset, execution id, frequency, window, DAG id, Airflow run id, row count) in one transaction after every `_COMPLETE` marker. A `CHECK` ties `frequency` to the window length; Filipe asked for the column during planning. The DAGs pass `--frequency`, `--dag-id`, and `--airflow-run-id`. Backfill parameters for the daily and weekly DAGs are in [Local Airflow](architecture/airflow-local.md#backfill-parameters).

Live run on the local stack: a daily window extracted before the ledger existed showed as `gap [2026-11-19,2026-11-20)` (exit 1). A backfill of six daily windows (`2026-11-14` to `2026-11-19`, `--reprocess-behavior completed`) closed it with one `transform_lakehouse` run (107 s), and the report was clean. The six backfill runs started together: `airflow backfill create` does not inherit `max_active_runs=1`, so the docs say to pass `--max-active-runs 1`. Re-running the covered windows 2026-11-19 and 2026-11-20 left Silver `postgres_oltp` at sellers 6, customers 2, orders 2, order items 2, and the report clean with two re-run windows. A weekly backfill (`--from-date 2026-11-09`) covered `[2026-11-08, 2026-11-15)`.

That weekly backfill found a P3-05 defect. Its run id `backfill__2026-11-16T00:00:00+00:00` equalled a daily backfill's, so both extracts shared one raw prefix: the weekly marker replaced the daily one, Bronze skipped the prefix as incomplete, and the weekly ledger rows overwrote the daily ones. The scheduled daily and weekly runs collide the same way every Monday 00:00Z. The PostgreSQL execution id is now `<dag_id>__<run_id>`; after re-running both windows, Bronze sellers went 20 → 23 and Silver stayed 6. The stray 2026-11-21 prefix stays in raw storage, ignored as incomplete.

`uv run pytest tests/unit`: 444 passed. `make lint` passed.

### 2026-11-21 — P3-05 weekly PostgreSQL re-extract

`airflow/dags/ingest_postgres_oltp_weekly.py` runs on `CronDataIntervalTimetable("0 0 * * 1", "UTC")` with `catchup=True`, `max_active_runs=1`, paused at creation, and `start_date` 2026-11-16 (first scheduled run 2026-11-23). It calls `postgres_extract_task` with the data interval shifted back one day, `[W-8, W-1)`, instead of the `[W-7, W)` in the acceptance text: Filipe approved the shift so the last day of the window does not share the 00:00Z edge with its daily run. The execution id is the weekly run id. A successful extract emits `raw_postgres_oltp`, which schedules `transform_lakehouse`.

`scripts/p3_05_weekly_check.sh` (helper `scripts/p3_05_late_commit.py`, bound parameters, counts only) passed on the local stack. One seller with `updated_at` 2026-11-13 06:00Z was committed and landed by the daily backfill `backfill__2026-11-14T00:00:00+00:00`. A second seller in the same day, committed after that run, stayed out of Silver. The manual weekly run `manual__2026-11-21T15:51:08.631343+00:00` (`-l` 2026-11-21T15:51:03Z) extracted `[2026-11-08, 2026-11-15)` in 1.9 s; its transform took 105 s. Silver sellers went 5 → 6 with 6 distinct keys, the Silver day count equalled PostgreSQL (2), and Bronze held 3 rows for the day (the on-time seller in both executions). `airflow tasks clear` of the weekly run re-extracted the same key; its transform took 6.7 s and no count changed. A first pass on 2026-11-12 with no on-time row also passed. Three synthetic `p3-05-late-*` sellers stay in PostgreSQL and the lake. A hard delete stays invisible (P3-12). Not run: a scheduled weekly run.

`uv run pytest tests/unit`: 417 passed. `make lint` passed.

---

### 2026-11-20 — P3-13 incremental lakehouse refresh

ADR-0024 approved. Bronze promotes only data objects of complete executions whose key is absent from `lakehouse_state.bronze_promotions` or whose ETag changed, reading and staging one object at a time (`lakehouse/bronze.py`, `lakehouse/refresh.py`). One `MERGE INTO` per dataset appends the rows; a rewritten object's old rows are deleted in the same commit. Staging keys and work directories carry the run label. `lakehouse_state.pending_layers` keeps Silver and Gold pending until their write succeeds, and `bronze_refreshes` records each run. `--full-rebuild` (`make live-rebuild`) rebuilds every table. Every ingestion DAG emits an `odp-raw://{source_id}` Asset; `transform_lakehouse` is scheduled on them with `batch_asset_events=True`, and the P3-04 `TriggerDagRunOperator` is gone. A step with nothing to do exits 99 and its task is skipped.

The local raw executions from before P3-12 have no marker and no longer enter Bronze. The two `postgres_oltp` windows that existed only that way, `[2026-10-17, 2026-10-24)` and `[2026-11-19, 2026-11-20)`, were re-extracted under `p3-13-reland-*`.

| Run | Result |
| --- | --- |
| `make live-rebuild` | 565 s. 28 objects, 1_565_638 rows. Bronze 251.7 s (18 checks passed), Silver 255.4 s (20 passed), stage-quarantine 15.1 s, dbt-build 26.5 s (PASS=287) |
| `make live-refresh`, nothing new | 13 s. 102 keys listed, no body read, no table written, every step skipped |
| `make live-refresh` after `postgres_oltp` `[2026-10-17, 2026-10-18)` (`p3-13-daily-2026-10-17`) | 151 s. 4 objects, 5 rows; Bronze 50.7 s, Silver 50.3 s (4 tables), dbt-build 26.2 s. `MERGE` committed as Iceberg `append`, one file, none rewritten; geolocation snapshots unchanged |
| Clickstream execution re-ingested with 60, then 100 events | `MERGE` with delete; Bronze 60, then 100, equal to the registry |
| `make live-refresh`, Silver and Gold pending only | 67 s. Bronze skipped promotion, Silver 1 table, Gold rebuilt |
| Airflow, default batching | Four Asset events gave four transform runs; the last two wrote nothing |
| Airflow, `batch_asset_events=True` | Three `ingest_postgres_oltp` runs during refresh A gave one follow-up run consuming 3 events (12 objects) |
| Airflow, manual refresh with nothing new | 8.2 s, six tasks skipped |

`uv run pytest tests/unit`: 411 passed. `uv run ruff check src tests airflow docker/airflow` and `ruff format --check` passed. Not measured: a `MERGE` into a large Bronze table (no new Olist release landed).

---

### 2026-11-20 — P3-12 update-pattern semantics and source refresh policy

Every ingestion entry point (Olist, DummyJSON, IBGE, storefront, PostgreSQL) deletes the execution's `_COMPLETE` marker before its first object and writes it after its last one (`ingestion/writer.py`). The marker lists object names and `completed_at`, no row values. Bronze (`lakehouse/bronze.py`) adds `_execution_completed_ts`: the marker time when its names equal the objects found, else null. Silver (`lakehouse/silver/common.py`) selects Bronze rows by `update_pattern`: the latest complete execution for `snapshot` and `full_refresh`; every complete execution for `append_only`, first ingested row per key; every window for `incremental_window`, unchanged. Without a complete execution, an `assert_true` fails the Silver script before `CREATE OR REPLACE`. `append_only` is a new contract value; `demo_storefront.clickstream` moves to it in contract version 3. The Silver reconciliation check scopes Bronze the same way. `ingest_dummyjson_products` runs Monday 03:00 UTC and `ingest_ibge_localidades` day 1 03:00 UTC, both `catchup=False` and created paused; Olist and the storefront stay manual. The source refresh matrix in `ROADMAP.md` is filled.

`scripts/p3_12_refresh_check.sh` ran against the live stack with the shipped ingestion code and a scripted HTTP transport, in a separate raw bucket. It passed 19 of 19 checks (`.local/p3-12/results.tsv`):

| Check | Result |
| --- | --- |
| DummyJSON run A (3 products), then B (2 products) | Silver 2 rows, all from B; the removed product is absent; Bronze keeps A's 3 rows |
| Run C stopped after page 0 (no marker) | Bronze holds its 2 rows with null `_execution_completed_ts`; Silver stays on B |
| Run D re-ingests B's snapshot | Silver 2 rows from D; payload checksum equal to B's |
| Raw holds only the incomplete run | Silver Spark script exits non-zero with the guard message; Silver keeps 2 rows |
| Clickstream batch A (50), the same 50 again, a late batch (20, earlier `event_time`) | Bronze 100 after the repeat, Silver 50 from the first batch; after the late batch Silver 70, 20 of them before 2026-11-10 |

Raw objects from before P3-12 have no marker. The local lake was rebuilt: Olist, DummyJSON, IBGE, and the storefront were re-ingested under `p3-12-rebuild-20261120`, then `make live-bronze` (310 s, 18 checks passed), `make live-silver` (248 s, 20 checks passed), and `make live-gold` (42 s, dbt 287 passed). Silver counts match the previous build: geolocation 738_332, orders 99_441, DummyJSON 194, IBGE 5_571. Silver clickstream is 100: only the new batch is complete. Bronze still holds every execution (geolocation 3_000_489 rows across three Olist executions); P3-13 removes that re-read.

Not exercised: a scheduled run of the two new DAGs on Airflow (DagBag tests cover schedule, catchup, and paused state).

---

### 2026-11-20 — P3-11 atomic table replace

Bronze (`lakehouse/bronze.py`), Silver (`lakehouse/silver/common.py`), and quarantine staging (`lakehouse/quarantine_stage.py`) now replace each table with one `CREATE OR REPLACE TABLE … USING iceberg LOCATION … AS SELECT`. Silver and quarantine no longer run `DROP TABLE` → `CREATE TABLE` → `INSERT OVERWRITE`. Bronze no longer runs `CREATE TABLE IF NOT EXISTS` + `INSERT OVERWRITE`, which kept the first schema. Spark rejects a column list in a CTAS, so each column is cast to its contract type in the select. Zero-row writes select typed `NULL` columns `WHERE 1 = 0`. A new contract schema lands in the same commit. dbt Gold was already atomic: `on_table_exists: replace` renders 55 `create or replace table` statements. Replace is atomic because Spark uses `SparkCatalog` over the JDBC catalog. A probe first confirmed that replacing with the same `LOCATION` and an added column works and keeps snapshots.

`tests/unit/lakehouse/test_atomic_replace.py` fails on `DROP TABLE`, `CREATE TABLE`, or `INSERT OVERWRITE` in any rendered writer SQL and pins `on_table_exists: replace`. `scripts/p3_11_replace_check.sh` ran live. A Silver geolocation write forced to fail with `raise_error` kept 738_332 rows and 1 snapshot. A Trino count loop over Bronze, Silver, quarantine, and `fct_orders` during a full rebuild (live-bronze 309 s, live-silver 260 s, stage-quarantine 19 s, dbt-build 36 s) ran 410 queries with 0 errors and 0 zero counts. Each table gained one snapshot. Unit tests: 371 passed. The loop checks reads between statements, not a scan already running when a commit lands.

---

### 2026-11-20 — Pipeline review: roadmap gaps filed

A general review of the orchestrated pipelines, outside the issue text, found defects that the roadmap had not asked about. They are filed in `ROADMAP.md`; no pipeline code changed in this entry.

- `promote_bronze` lists every object under `raw/{source}/{dataset}/`, from every execution and date, loads them into Python memory, and rewrites one staging file per dataset on every run. Each new Olist execution doubles Bronze (geolocation 2_000_326 rows against 1_000_163). The work of each refresh grows with total history. Filed as P3-13, incremental lakehouse refresh, with ADR-0024.
- Silver writes `DROP TABLE` → `CREATE TABLE` → `INSERT OVERWRITE` (`lakehouse/silver/common.py`). Readers see no table during that window, a failed insert leaves it empty, and Iceberg history is lost on every run. Bronze (`INSERT OVERWRITE`) and dbt (`on_table_exists: replace`) are already single commits. Filed as P3-11.
- `update_pattern` (`snapshot`, `full_refresh`, `incremental_window`) has no downstream behavior. Silver unions every execution and keeps the latest row per key, so a product or municipality removed at the source never leaves Silver, and `demo_storefront.clickstream` is labelled `snapshot` although it is an event stream. Filed as P3-12, with a source refresh matrix.
- Only the PostgreSQL daily DAG triggers the refresh; other sources enter whenever a refresh happens to run, and N upstream runs queue N full rebuilds. Folded into P3-13 (ingestion Assets, coalesced refresh).
- The daily `catchup=False` gap was fixed in P3-04 (entry below). P3-06 gains window coverage and a gap report.

The roadmap gains a pipeline design checklist that every pipeline issue answers in its plan and acceptance. `AGENTS.md` §6 and the plan prompt in `docs/agent-workflow.md` point to it. P3-05 becomes a trailing seven-day re-extract for late commits. P3-08 records rows read and durations per refresh, and P5-03 charts them. P3-09 adds gap, failed-write, no-op, and coalescing scenarios. P4-04 separates stream and batch table ownership. Phase 10 follows P3-12 and P3-13 instead of `FULL_OVERWRITE`. The Phase 3 close pair moves from P3-11/P3-12 to P3-14/P3-15; entries before this one that say P3-11/P3-12 mean the close pair. ADR-0006 related issues are `P3-01`–`P3-15`.

---

### 2026-11-20 — P3-04 catchup corrected

A review after P3-04 found that `catchup=False` loses windows. The daily window comes only from the schedule and no watermark is stored, so a day the scheduler did not run (DAG paused, scheduler down) would never be extracted, and nothing would fail. `ingest_postgres_oltp_daily` now sets `catchup=True` with `start_date` 2026-11-19, the first window it ran. After a gap the scheduler runs each missed day, oldest first, one at a time. A DagBag test walks three missed days after the 2026-11-19 run and checks that the first run on a fresh database is the 2026-11-19 window. After `airflow dags reserialize`, the live DAG reported its next data interval as `2026-11-20T00:00Z` → `2026-11-21T00:00Z`, with no import errors. A gap filled on a live scheduler is not yet recorded; that is the Phase 3 live run. Each caught-up day still triggers a full `transform_lakehouse` rebuild. The P3-04 live run below used `catchup=False`.

---

### 2026-11-20 — P3-04 daily PostgreSQL schedule

`ingest_postgres_oltp_daily` uses `CronDataIntervalTimetable("0 0 * * *", "UTC")`, `catchup=False`, and `max_active_runs=1`. The extract renders `--window-start`/`--window-end` from `data_interval_start`/`data_interval_end`, and `execution_id` stays `params.execution_id or run_id`. After a successful extract, `TriggerDagRunOperator` starts `transform_lakehouse` with run id `ingest_postgres_oltp_daily__<run_id>`, `reset_dag_run=True`, `wait_for_completion=False`, and `fail_when_dag_is_paused=True`. The manual `ingest_postgres_oltp` DAG does not trigger it. The extract vector moved to `postgres_extract_task` in `ingest_common.py`. Late commits after the 00:00Z run miss the daily window; P3-05, P3-06, and CDC (Phase 4) cover that. The P3-04 acceptance now includes the transform trigger.

`lakehouse_gold` is a 1-slot pool that `airflow-init` creates. It holds `transform_lakehouse` `bronze`, `silver`, `stage_quarantine`, `dbt_build`, and `reconcile_gold` `dbt_reconcile`. That is wider than the `dbt_build`-only acceptance text, by Filipe's decision. The pool was chosen over an Asset because an Asset only orders runs. Every DAG sets `is_paused_upon_creation=True`. DAGs already in the metadata database kept their state. `run_python_args` uses `Popen(start_new_session=True)` and sends `SIGTERM`, then `SIGKILL` after 10 s, to the process group when the wait ends.

Airflow 3.4 checks: a cron string gives a zero-length `CronTriggerTimetable` interval; a task in a missing pool is never scheduled; `AirflowTaskTimeout` and `AirflowTaskTerminated` derive from `BaseException`. Divergence: `airflow dags trigger -l` sets only the logical date. The data interval comes from the trigger time, so a past window used `airflow backfill create`.

Live run, 2026-11-20 UTC: the scheduled run `scheduled__2026-11-20T00:00:00+00:00`, the manual run `manual__2026-11-20T03:42:53.785961+00:00` (both window 2026-11-19, empty), and the backfill run `backfill__2026-10-18T00:00:00+00:00` (window 2026-10-17, 2/1/1/1 rows, equal to `COUNT(*)` on `oltp`) finished `success`. The three transform runs they triggered finished `success`, every task on attempt 1: 20 min 22 s, 9 min 22 s, and 9 min 51 s, each with PASS=287. `reconcile_gold` `manual__2026-11-20T03:42:11.378124+00:00` waited for the slot and ran 03:46:51–03:57:12, between `bronze` (ended 03:46:50) and `silver` (started 03:57:14); PASS=56. Clearing the backfill run overwrote the same four raw keys and reset the same transform run (10 min 06 s, PASS=287). Bronze sellers stayed 11, Silver `postgres_oltp` 2/2/2/3, and quarantine 1_603. No secret appeared in the 33 task logs. `uv run pytest tests/unit` → 364 passed; ruff check, ruff format --check, and `make lint` passed. Evidence: [`docs/architecture/airflow-local.md`](architecture/airflow-local.md).

---

### 2026-11-17 — Markdown tables name their columns

8 key and value tables had an empty header row (`| | |`), which GFM renders as a blank row on top. Each now has a header drawn from its context (`Project | Current`, `Validation | Detail`, `Decision log | Reference`, `Document | Reference`, `ADRs | Location`, `Component | Setting`, `Cache | Setting`). `scripts/check_markdown_tables.py` fails on an empty header row in any tracked Markdown file, outside fenced code; `make lint` and CI run it. AGENTS.md §9 states the rule.

---

### 2026-11-15 — Phase 10 switched to Data Contract CLI

The contract-watcher project was cancelled and its repository deleted, so the link in the entry below no longer resolves. Phase 10 keeps ODCS v3.2.0 table contracts and now lints and runs them with Data Contract CLI (`datacontract-cli`, MIT; `1.2.10` on PyPI on 2026-11-15, ODCS v3.2.0 since `1.2.0`). `soda-core` was set aside because v4 is not Apache-2.0 (Elastic License 2.0 in its repository) and does not read ODCS; `great_expectations` because it does not read ODCS. The library has no business-day calendar, result history, or alert deduplication: P10-06 adds `data_quality.business_days` and `type: sql` rules, P10-07 stores the library's JSON results, and P10-09 computes alert state. CW-16 and CW-17 are no longer dependencies; P10-07 now follows P10-06. P10-01 gains a proof of concept and still delivers ADR-0022, which does not exist yet. Documentation only. `uv run pytest tests/unit` → 348 passed.

### 2026-11-15 — Phase 10 added to the roadmap

`ROADMAP.md` gains Phase 10, Table contracts and quality observability, with P10-01–P10-12, all `not started`. Table contracts are ODCS v3.2.0 and run through [contract-watcher](https://github.com/FilipeOliveira94/contract-watcher), a kit in its own repository. The phase depends on Phase 3 verified. From P10-06 onward it also depends on the kit's `0.1.0` release (CW-16, which depends on CW-17): P10-06 pins the uv `contracts` group and runs `contract-watcher lint` in CI, and P10-07 takes the `check_results` schema from the kit's sink. The ADRs and the source part (P10-01 to P10-05) do not wait on the kit; the P10-03 adapter reads the ODCS YAML with this repository's own code. Its position in the roadmap is not fixed; the PO decides it during execution, toward the end. The source for the SLAs is BCB SGS series 1 and 11, with freshness and volume on a business-day calendar. ADR-0022 and ADR-0023 are the deliverables of P10-01 and P10-02 and do not exist yet. `PROJECT_STATUS.md` gains one row in the program table; Phase 3 and Next work are unchanged. The Phase 9 goal now says it publishes the report, not that it finishes the program, and P0-06 says "all phases". Documentation only. `uv run ruff check src tests airflow docker/airflow` passed. `uv run pytest tests/unit` → 348 passed.

### 2026-11-14 — Review fixes for the AGENTS.md consolidation

The git policy (local Conventional Commits on `main`, never push) moved from `CLAUDE.md` into `AGENTS.md` §13 Git, so tools that read only `AGENTS.md` get it; `CLAUDE.md` now holds only `@AGENTS.md`. The entry below that says `CLAUDE.md` adds the git policy describes the state before this change. `ROADMAP.md` (verified status) and `DECISIONS.md` (Implemented column) now say evidence is recorded in the architecture doc or ADR and in `docs/status-log.md`, and `PROJECT_STATUS.md` summarizes it, matching `AGENTS.md` §3. `docs/agent-workflow.md`: the plan prompt also asks which §8 Runtime data exposure rules the issue touches, the implement prompt says not to commit, and the prompts cite §12 "Issue Close Checklist" by its heading. Documentation only. `uv run ruff check src tests airflow docker/airflow` passed. `uv run pytest tests/unit` → 348 passed.

### 2026-11-14 — Agent rules consolidated into AGENTS.md

The way the project is implemented changed: it moved from Cursor with `.cursor/rules/*.mdc` rules to Claude Code via T3, with a root `CLAUDE.md` that imports `AGENTS.md` and adds the git policy (local Conventional Commits on `main`, no push). `.cursor/rules/` (`00`–`05`) was removed after a line-by-line check against `AGENTS.md`. Rules that were only in the `.mdc` files moved into `AGENTS.md`: the local→AWS mapping (MinIO→S3, catalog→Glue, Trino→Athena), the Runtime data exposure subsection (no values in SQL, mode-600 secrets, `spark.redaction.regex`, exception and `print` content, dbt `store_failures`, the planted-value test and `test_spark_credentials.py`), "do not silently change architecture, dependencies or acceptance criteria", "do not drop invalid records silently", "do not leave versions floating", "check the roadmap item", "plan before editing files for non-trivial work", "do not rebuild cumulative tables in `PROJECT_STATUS.md`", and "validate compatibility before adopting a combination". `AGENTS.md` gained an Issue close checklist.

`docs/agent-workflow.md` now holds three tool-neutral prompts (plan, implement, close) and a fallback note for other tools. `docs/project-context-for-cursor.md` was renamed to `docs/project-genesis.md` with a historical note at the top; its content is unchanged. P0-03 and P0-05 evidence and the Related rows in `ROADMAP.md` and `DECISIONS.md` point at the new locations and name the old ones. Entries below this one that cite `.cursor/rules/` or `docs/project-context-for-cursor.md` mean those removed or renamed files. `PROJECT_STATUS.md` Next work for P3-04 now names the 2026-11-14 criteria. Documentation and agent config only. `uv run ruff check src tests airflow docker/airflow` passed. `uv run pytest tests/unit` → 348 passed.

### 2026-11-14 — Phase 3 renumbering after the P3-03 review

The review of P3-03 (commits `7bb3653`..`bb03f6b`) produced eight findings. They are filed in `ROADMAP.md` Phase 3; no code changed. Findings 1 and 2 go to P3-04: `reconcile_gold` must not run while a `transform_lakehouse` dbt build runs, every DAG is created paused, and `run_python_args` kills the child process group on timeout or exception. P3-04 now depends on P3-03. Findings 3, 4, 6, and 8 go to the new P3-10, Orchestration runtime hardening: Spark driver memory and cores capped inside the Airflow runtime, Airflow in its own uv group, retention for `odp_lakehouse_work`, and `ODP_TRINO_*` and dbt profile examples. Finding 5 goes to P3-12: `make` targets and Airflow DAGs must not run concurrently against the same Iceberg tables and Ivy cache. Finding 7 goes to P3-08: Trino error fields and a total timeout on `nextUri` polling.

The Phase 3 close pair moves from P3-10/P3-11 to P3-11/P3-12. The live run depends on P3-08, P3-09, and P3-10. Entries before this one that say P3-10/P3-11 mean the close pair. ADR-0006 Related issues is `P3-01`–`P3-12`. Documentation only; no tests were run.

### 2026-11-13 — P3-03 transform and reconciliation DAGs

One runtime image, `odp-runtime:airflow3.4.0-spark3.5.8` (`docker/runtime`), runs Airflow `3.4.0`, Spark `3.5.8` with Temurin JRE `17.0.20` copied from the digest-pinned `spark:3.5.8-java17`, and dbt Core `1.12.6` with dbt-trino `1.11.0` in `/opt/dbt-venv`. The Airflow services and the lakehouse `spark` service use it. No service runs `spark:3.5.8-java17`, and the host no longer installs dbt (`uv.lock` group `dbt`, exported with hashes to `docker/runtime/dbt-requirements.txt`). ADR-0005 addendum approved after the DAG runs. D-006 names the new image.

`python -m open_data_platform.lakehouse.medallion_build <step>` renders SQL, runs `run_bronze.sh` or `run_silver.sh` once per file, checks Trino, or runs dbt. `make live-bronze`, `make live-silver`, `make live-gold`, the dbt targets, and both DAGs call it. `transform_lakehouse` (no schedule): `bronze` → `bronze_checks` → `silver` → `silver_checks` → `stage_quarantine` → `dbt_build`. `reconcile_gold` (`@weekly`): `dbt_reconcile`. Tasks reuse the P3-02 retries 2, 2-minute delay, and 30-minute timeout.

`transform_lakehouse` run `manual__2026-11-14T03:25:52.218916+00:00` (UTC) finished `success` in 10 min 12 s, every task on attempt 1: Bronze 18 Spark files and 18/18 checks, Silver 18 files and 20/20 checks, quarantine 1_603, `dbt build` PASS=287. `reconcile_gold` run `manual__2026-11-14T03:36:32.717852+00:00` PASS=56 in 590 s. Silver geolocation 738_332, Silver postgres_oltp 2/2/2/3, `mart_late_orders` 6_534, on-time 89_936 / 96_470, `dim_customers` 96_096, `dim_zip_prefix` 19_015, `dim_date` 3_774, `dim_municipality` 5_571, and 55 Gold tables match P2-09. Bronze geolocation is 2_000_326 because the raw prefix holds two Olist executions; Bronze checks compare with the raw read of the same run. The Make targets on the same image exited 0 with the same check results. Task logs did not contain the MinIO password, the Fernet key, or the JWT secret.

`DagBag` unit tests load all ten DAGs with zero import errors. `uv run ruff check src tests airflow docker/airflow` passed. `uv run ruff format --check src tests airflow docker/airflow` passed. `uv run pytest tests/unit` → 348 passed. Detail: [`docs/architecture/airflow-local.md`](architecture/airflow-local.md).

### 2026-11-12 — P3-02 ingestion DAG wrappers

Airflow DAGs call the Phase 1 ingestion CLIs with `python -m` and an argument vector. The stock image `apache/airflow:3.4.0-python3.12` already imports `boto3` 1.43.117, `psycopg` 3.3.6, and `yaml` 6.0.3, so no custom image was built. Compose mounts `src/`, `config/`, `pyproject.toml`, `tests/fixtures`, and `data/raw` at `/opt/odp/repo` and sets `PYTHONPATH=/opt/odp/repo/src`. Inside those containers `POSTGRES_HOST=postgres`, `POSTGRES_PORT=5432`, `POSTGRES_DB=odp`, and `MINIO_ENDPOINT_URL=http://minio:9000`. Metadata stays on database `airflow`. `airflow-db-create` remains a client of the same PostgreSQL server.

DAGs: `platform_env`, `ingest_olist`, `ingest_dummyjson_products`, `ingest_ibge_localidades`, `ingest_demo_storefront`, `ingest_postgres_oltp`, and `seed_postgres_oltp`, plus the existing `platform_health`. Each wrapper uses retries 2, a 2-minute delay, and a 30-minute execution timeout. `schedule=None`. `execution_id` defaults to the Airflow `run_id`. `seed_postgres_oltp` is not upstream of the extract. `ingest_postgres_oltp` passes window bounds through and does not calculate a daily or weekly window. P3-04, P3-05, and Phase 4 set the schedules.

`seed_postgres_oltp` run `manual__2026-11-12T17:12:21.152531+00:00` finished `success` and left sellers 5, customers 2, orders 2, and order items 2, with the fixture `updated_at` span. `platform_env` run `manual__2026-11-12T17:06:20.317037+00:00` finished `success` on all five tasks. Ingestion runs, all `success`, with the run id as `execution_id`: storefront `events.jsonl` 100 lines; PostgreSQL window `2026-10-17` to `2026-10-18` customers 1, order items 1, orders 1, sellers 2, matching `COUNT(*)` on `oltp`; DummyJSON 7 pages and 194 products; IBGE `municipios.json` 5_571 rows; Olist 11 CSV objects from the staged files. No quarantine object was written for these runs. The five ingestion task logs did not contain the MinIO password, the Kaggle token, the Fernet key, or the JWT secret.

`uv run ruff check src tests airflow docker/airflow` passed. `uv run ruff format --check src tests airflow docker/airflow` passed. `uv run pytest tests/unit` → 324 passed in 13.00s. Detail: [`docs/architecture/airflow-local.md`](architecture/airflow-local.md).

### 2026-11-12 — P3-01 Airflow in local Compose

Airflow `apache/airflow:3.4.0-python3.12` runs on the Compose profile `orchestration` with LocalExecutor. Processes: `airflow-db-create`, `airflow-init`, `airflow-apiserver`, `airflow-scheduler`, `airflow-dag-processor`. Metadata database `airflow` on PostgreSQL 16. Host UI port 8081. `docker compose up` does not start the profile. `docker compose config --services` listed `minio` and `postgres`.

`make airflow-up` reached healthy api-server, scheduler, and dag processor. `airflow version` printed `3.4.0`. Health: `metadatabase`, `scheduler`, and `dag_processor` healthy; `triggerer` down because this profile has no triggerer. `airflow dags list` showed `platform_health`.

`EmptyOperator` does not produce a task log on Airflow 3.4. The smoke task is a `BashOperator` that prints `platform_health ready`. Manual run `manual__2026-11-12T15:04:09.043021+00:00`, task `ready`, state `success`, pid 303, duration 3.704131 seconds. MinIO object `task-logs/dag_id=platform_health/run_id=manual__2026-11-12T15:04:09.043021+00:00/task_id=ready/attempt=1.log`, 5502 bytes. The body contained the fixed line and did not contain the MinIO password, the Fernet key, or the JWT secret.

Stopping the three Airflow services left PostgreSQL and MinIO up and the object still listed. A second `airflow-init` printed `Database migration done!` and `airflow already exists in the db`. The UI login page loaded at `http://localhost:8081/auth/login/`.

`docker compose --profile orchestration stop` with no service names also stopped PostgreSQL and MinIO. Spark and Trino stayed up. PostgreSQL and MinIO were started again; the log object was still in the bucket. The documented stop command names the three Airflow services.

D-006 is `approved`. CeleryExecutor, Redis, and Flower are not a planned follow-up. D-007 stays `proposed`. `uv run ruff check src tests airflow docker/airflow` passed. `uv run ruff format --check src tests airflow docker/airflow` passed. `uv run pytest tests/unit` → 304 passed in 14.03s. Detail: [`docs/architecture/airflow-local.md`](architecture/airflow-local.md).

### 2026-11-11 — README and partition candidate

The README opens with the path from a published number back to the source: several sources, idempotent overwrite, a new raw execution kept beside the previous one, contracts, business-domain Gold, and reconciliation. Bronze and Silver stay full replacements. Status is the next section. Core features are one table row each, with state, phase, and core stack, and a labeled note under each row. Setup stays in collapsed sections. Iceberg partitions for Silver and Gold are a candidate in `ROADMAP.md`, not an issue. No stack command was run.

### 2026-11-11 — P2-10 phase documentation

Phase 2 is verified. [`docs/architecture/phase-2-lakehouse.md`](architecture/phase-2-lakehouse.md) lists the 2026-11-11 component versions, states that Bronze, Silver, and Gold have no Iceberg partition spec, states that no target file size is configured, and contrasts `WriteMode.FULL_OVERWRITE` (and dbt table replace) with the PostgreSQL `incremental_window` extract. Placeholders `INCREMENTAL_MERGE` and `APPEND_ONLY` still fail if selected. The page cites [`docs/architecture/phase-2-live-run.md`](architecture/phase-2-live-run.md) and does not copy the 55 Gold row counts. `README.md` now leads with what the platform does, what it does not do, and the reproduce path. Stale sentences updated: Bronze D-012 is `approved`; Silver quality points at Gold and the live run; the catalog note points at the Ivy cache. No Compose command was run. No application code changed.

Text check, from the repository root. Both commands exited 0.

```text
rg -n "P2-10|later issues|D-012 stays|downloads the pinned jars|Broader quality" README.md docs/architecture PROJECT_STATUS.md ROADMAP.md
rg -n "phase-2-live-run.md" README.md PROJECT_STATUS.md docs/architecture/phase-2-lakehouse.md
```

The first command printed eight lines, all for the issue id `P2-10`: `ROADMAP.md` lines 175, 190, 192, and 491, and `PROJECT_STATUS.md` lines 68, 74, 75, and 190. `README.md` and `docs/architecture` printed nothing. The phrases `later issues`, `D-012 stays`, `downloads the pinned jars`, and `Broader quality` did not appear. The second command printed nine lines: `docs/architecture/phase-2-lakehouse.md` lines 20 and 77, `PROJECT_STATUS.md` lines 27, 74, 75, and 81, and `README.md` lines 9, 60, and 77. Next issue is P3-01. Phase 3 was not started.

### 2026-11-11 — P2-09 live run

On the local stack, `make test-catalog-probe` printed Spark `2` and Trino `"2"` in 17.7 s. `make live-bronze` matched 18/18 raw counts in 359 s, including geolocation 1_000_163, customers 99_441, DummyJSON 194, and postgres_oltp 3/3/3/5. `make live-silver` returned 20/20 `ok` in 344 s. Silver anchors: geolocation 738_332, DummyJSON 194, postgres_oltp 2/2/2/3, clickstream null `product_id` 16. `make live-gold` printed quarantine `1603` and `dbt build` PASS=287 WARN=0 ERROR=0 SKIP=0 TOTAL=287 in 53.7 s (55 tables + 232 schema tests). `make dbt-test-reconciliation` PASS=56 WARN=0 ERROR=0 SKIP=0 TOTAL=56 in 558 s. On-time 89_936 / 96_470, late orders 6_534, `dim_customers` 96_096, `dim_zip_prefix` 19_015, `dim_date` 3_774, `dim_municipality` 5_571. Those anchors match the 2026-11-05 / 2026-11-06 notes. The Silver checker count is 20 statements after P2-07; the 2026-11-05 note said 22/22. Every Spark submit reported 6 jars already retrieved. `ensure_spark_ivy` is one Makefile line so the sourced `.env` stays in the shell that runs the command. Detail: [`docs/architecture/phase-2-live-run.md`](architecture/phase-2-live-run.md). Partitioning, file sizing, and the phase limitation write-up remain P2-10.

### 2026-11-11 — Site figures

The site scaffold still has no charts. Architecture and Pipeline no longer call the verified lakehouse "not started". `ROADMAP.md` now lists ten figures: nine Superset charts, one per question already in the Gold model, and one JupyterLab image of the same on-time query. P8-04 draws them. P8-06 exports static images and a caption manifest. Phase 9 publishes them on Home, Architecture, and Pipeline. No image file was added.

### 2026-11-11 — Phase 8 is exploration and BI; the site moves to Phase 9

`ROADMAP.md` inserts Phase 8 before the platform report. Phase 8 is JupyterLab on the host and Apache Superset in Compose, both read-only through Trino, and may start after Phase 2 in parallel with Phases 3–7. Issues are P8-01 through P8-05, then the close pair P8-06 (live run, result, and consistency check) and P8-07 (phase documentation and status). The report site is Phase 9 (P9-01–P9-07); its close pair is P9-06 then P9-07. D-017 records the selection and stays `proposed` until the P8-01 PoC against Trino 484. No notebook, image, or Compose service was added. Earlier log lines that call Phase 8 the site mean what is now Phase 9.

### 2026-11-11 — Quarantine and Spark credential exposure

Quarantine staging no longer inlines finding values in SQL. `make stage-quarantine` writes warehouse JSONL and loads it with Spark. Spark runners pass the JDBC password and S3 keys through a properties file. `tests/unit/infrastructure/test_spark_credentials.py` fails if another `docker/spark/*.sh` assigns those keys. The agent rule is `.cursor/rules/05-runtime-data-exposure.mdc`. `uv run ruff check src tests` passed. `uv run ruff format --check src tests` passed. `uv run pytest tests/unit` passed 294. `make stage-quarantine` was not re-run on the live stack. Detail: [`docs/architecture/gold-layer.md`](architecture/gold-layer.md). This entry was archived from the Phase 2 ledger when P2-10 trimmed it.

### 2026-11-11 — P2-07 quality and reconciliation tests

Removed the pinned `live_counts` module. Bronze `--all` writes `.local/bronze-source-counts.tsv` from the raw row counts of that run; `--print-live-checks` requires the file. Silver live checks return `ok` when the Silver count matches the Bronze dedup policy (distinct payload columns for geolocation) and when DummyJSON expansion and clickstream cart/purchase product ids hold. dbt singular tests in `dbt/tests/reconciliation/` cover all 55 Gold tables plus ephemeral `int_order_primary_payment`. Schema tests check type-family prefixes, grain uniqueness, closed value sets, and numeric bounds. Money sums use absolute tolerance `0.01`.

`uv run dbt test --project-dir dbt --profiles-dir dbt` → PASS=288 WARN=0 ERROR=0 SKIP=0 TOTAL=288 in 570.63s, against Gold already built from `odp-raw` / `execution_id=run-001`. Before that run, `brazil_geography.dim_municipality` returned `ICEBERG_MISSING_METADATA`; `DROP TABLE` then `dbt run --select dim_municipality` recreated 5_571 rows. `dbt run --select mart_storefront_user_behavior` recreated 5 rows after session and event aggregates were split. `uv run ruff check src tests` passed. `uv run ruff format --check src tests` passed. `uv run pytest tests/unit` → 285 passed at that point. `make live-bronze` and `make live-silver` were not re-executed. P2-09 remains the live consistency close. The PASS=288 run was before the tests were grouped by schema. After that move, `dbt ls --quiet` counted 56 tests tagged `reconciliation` and 232 tests outside that tag. `make dbt-build` and `make live-gold` pass `--exclude tag:reconciliation`. A `dbt build` without that flag still runs the reconciliation tests. A later `uv run pytest tests/unit` on this tree passed 286. `uv run ruff check src tests` and `uv run ruff format --check src tests` passed again.

### 2026-11-06 — P2-06 dbt Gold verified

Scaffolded `dbt/` with **dbt-trino** `1.11.0` (dbt Core `1.12.6`) against Trino `484` on the Iceberg JDBC catalog. Staged MinIO quarantine JSONL into `iceberg.gold_staging.quarantine_findings` (1_603 rows from the legacy `order_reviews` sidecar). `dbt build` created **55** Gold tables (plus one ephemeral primary-payment helper). Smoke: on-time rate **0.932** on `mart_delivery_performance`; `mart_late_orders` **6_534**; `dim_customers` 96_096; `dim_zip_prefix` 19_015. Compose publishes Trino `TRINO_PORT` (default 8080) for host dbt. Silver remains the Spark writer. D-005 and D-012 approved. `uv run ruff check src tests` passed. `uv run pytest tests/unit` → 272 passed. Detail: [`docs/architecture/gold-layer.md`](architecture/gold-layer.md).

### 2026-11-06 — Gold schemas split by audience

Revised [`docs/architecture/gold-model-design.md`](architecture/gold-model-design.md) and ADR-0012. Source families on contracts stay `olist_analysis` and `product_analysis`. Gold schemas are `marketplace_operations`, `customer_crm`, `seller_acquisition`, `brazil_geography`, `catalog_commerce`, `storefront_behavior`, and `oltp_operations`, plus shared `dim_date`. Delivered orders with both dates are 93.2% on time (89_936 / 96_470) and 6.8% late (6_534). `mart_delivery_performance` publishes the rate. `mart_late_orders` lists each late order. Each schema has `fct_*_quality_findings` for contract violations and business anomalies. Late delivery is not filed as a data error. D-012 stays `proposed`. No dbt model was built.

### 2026-11-06 — P2-05 Gold model design

Documented grain, keys, type 1 refresh, and marts for `olist_analysis`, `product_analysis`, and the PostgreSQL `oltp_` star. Profiled `data/raw` Olist CSVs and Trino `iceberg.silver` (`odp-raw`, `execution_id=run-001`) on this date. Published payments have no refund status: 484 of 625 canceled orders already had `order_approved_at`. Reviewed orders delivered after the estimate average score 2.272 (6_382) against 4.290 on time or early (89_448). IBGE city+UF match after lower, accent strip, and hyphen-to-space: 98_817 / 99_441 customers and 2_986 / 3_095 sellers, with no ambiguous pair. Category supplement for Gold: `pc_gamer` → `gaming_pc`, `portateis_cozinha_e_preparadores_de_alimentos` → `portable_kitchen_appliances_and_food_processors`. Clickstream has no URL and does not join DummyJSON or Olist. SCD Type 2 applications are in that design. Implementation is new roadmap issue P4-08 only. Phase 4 live run and documentation moved to P4-09 and P4-10. D-012 stays `proposed`. No dbt project, no Spark job, no unit-test run.

### 2026-11-06 — P2-08 Spark Ivy cache

Compose Spark mounts `.local/spark-ivy` at `/tmp/.ivy2`. `make spark-ivy-warm` seeds the pinned Iceberg/Hadoop/AWS/JDBC jars. Second warm and a Bronze clickstream `compose run --rm` both reported `6 already retrieved` with no Maven re-download. Host dirs are world-writable for Spark `uid=185`. Detail: `docs/architecture/phase-2-spark-ivy-cache.md`.

### 2026-11-05 — Full-data Bronze and Silver live (`odp-raw`)

Re-verified P2-03 / P2-04 against every object under `ODP_RAW_BUCKET=odp-raw` (not fixture buckets). Write mode is full load + overwrite only (`lakehouse.write_mode`). DummyJSON nested JSON expands via shared `silver.common` helpers; source module maps fields only. `make live-bronze` → Trino 18/18. `make live-silver` → Trino 22/22 (18 counts + DummyJSON shape + clickstream null product_id). Notable Silver deltas vs Bronze: geolocation 1_000_163→738_332; postgres_oltp 3/3/3/5→2/2/2/3. Unit fixtures remain pytest-only. Phase 2 close pair is P2-09 / P2-10; Spark Ivy cache is P2-08 (before the close). Hardcoded `live_counts` → P2-07.

### 2026-11-05 — P2-04 Silver layer

Silver is one Iceberg table per contract on the JDBC catalog, namespace `silver`, format version 2. Columns follow contract types. Modules under `lakehouse.silver` are split by `source_id`. `order_reviews` contract v2 primary key is `(review_id, order_id)`. `make test-silver` wrote the four Bronze live fixtures, promoted Bronze, then Silver. Spark printed `2`, `100`, `2`, `2`. Trino passed 17 checks, including category CSV dedup (3→2) and typed DummyJSON envelope fields. `uv run ruff check src tests` passed. `uv run ruff format --check src tests` passed. `uv run pytest tests/unit` → 261 passed. `docker compose config --services` listed `minio` and `postgres`. D-012 stays `proposed`. Gold is P2-05+.

### 2026-11-01 — snapshot retention moved to Phase 5

Iceberg `INSERT OVERWRITE` keeps old snapshots and their Parquet files. That maintenance is P5-07 in the FinOps phase: it uses the retention named by the P5-05 cost model and sits before the Phase 5 live run (P5-08) and documentation (P5-09). The Phase 5 close pair numbered P5-07/P5-08 on 2026-10-28 is now P5-08/P5-09. No cleanup job was added.

### 2026-11-01 — P2-03 rechecked on the shared Spark runtime

Each Bronze dataset is a separate Spark invocation. `make test-bronze` printed `100`, `2`, `2`, and `3`, then Trino matched 18 checks. The synthetic objects are written by `open_data_platform.lakehouse.fixtures`. `uv run pytest tests/unit` passed. The `site/` pages were left unchanged. D-012 stays `proposed`.

### 2026-10-31 — P2-03 Bronze layer

Bronze is one Iceberg table per contract on the JDBC catalog, namespace `bronze`, format version 2, every column `STRING`. Metadata columns are `source_id`, `execution_id`, `_ingestion_ts`, `raw_object_key`, `record_index`, and `dt`. The final `make test-bronze` used bucket `odp-bronze-live` and four synthetic objects. Spark printed `3`, `100`, `2`, `2`. Trino passed 18 checks, including duplicates, nulls, `São Teste`, and compact JSON text. `uv run ruff check src tests` passed. `uv run ruff format --check src tests` passed. `uv run pytest tests/unit` → 245 passed in 10.96s. `docker compose config --services` listed `minio` and `postgres`. The `site/` pages were left unchanged. D-012 stays `proposed`. Silver is P2-04.

### 2026-10-28 — Phases 0 and 1 stay verified

The close pair (live run, then documentation and status) applies from Phase 2 on. Phases 0 and 1 stay verified on results already in this log, so P0-14, P0-15, P1-12, and P1-13 are removed. Phase 0 evidence is the 2026-10-16 Compose health check, PostgreSQL reachability, MinIO put/get, and green CI. Phase 1 evidence is the DummyJSON live run (7 pages / 194 products), the IBGE live run (5_571 municipalities), the P1-10 MinIO matrix (40 passed, including 1_603 duplicate `review_id` rows), and P1-11. The full Olist Kaggle rerun after the category-file BOM fix remains unrepeated; the staged-file matrix is the recorded Olist result. Next work is P2-01.

### 2026-10-28 — Phase close pattern

`ROADMAP.md` now ends every phase with a live run, result, and consistency check, then a phase documentation and status issue: P0-14/P0-15, P1-12/P1-13, P2-08/P2-09, P3-10/P3-11, P4-08/P4-09, P5-07/P5-08, P6-07/P6-08, P7-08/P7-09, P8-06/P8-07. P1-11 stays the earlier documentation pass. Phase 0 and Phase 1 feature issues keep their recorded status. Those phases are in progress until the new close is verified, so Phase 2 waits. `cd site && npm run build` completed: 6 pages in 1.75s. Unit tests were not re-run. The IDE browser could not open the local preview.

### 2026-10-28 — P1-11 Phase 1 documentation and status

README and `docs/architecture/phase-1-ingestion.md` index the five raw sources, object keys, repeatable overwrite, quarantine, and the three update patterns. Daily and weekly batch remain the P1-06 window (Airflow schedules stay P3-04 and P3-05). Streaming stays Phase 4. Phase 1 acceptance is checked in `ROADMAP.md`. The operational ledger moves to Phase 2, not started, at P2-01. Validation: `uv run ruff check src tests` PASS; `uv run ruff format --check src tests` PASS; `uv run pytest tests/unit` → 216 passed in 10.56s. `cd site && npm ci && npm run build` completed: 6 pages. Node v22.14.0 is below the engine range some transitive packages declare (`@astrojs/prism` >=22.22.0, `undici` >=22.19.0); the build still finished. No live API, Kaggle, or MinIO. No ingestion code change.

### 2026-10-26 — P1-10 live matrix

Ran each Phase 1 source against local MinIO. IBGE and DummyJSON used the public APIs. PostgreSQL used the synthetic fixture, then temporary rows for the error cases, then `DROP SCHEMA oltp` and a reseed. Olist used the gitignored staged CSVs. Injected payloads covered nulls, type mismatches, duplicate keys, structural failures, a clean rerun, and synthetic `fail_batch`. 40 cases passed. No product change. Before the contract fix, that Olist snapshot also quarantined leading-zero ZIP prefixes and closed-deal `True`/`False` and `100.0` cells. `819829d` matches those tokens. A later classification of the same files leaves only the 1_603 duplicate `review_id` rows.

### 2026-10-26 — D-013 approved; sidecar uses the object writer

D-013 is `approved`. Each ingestor calls `prepare_quarantine` and then `write_raw_object` for the raw payload. The quarantine sidecar is a second call to `write_raw_object` with prefix `quarantine`. A clean rerun still deletes that key. `uv run ruff check src tests` PASS; `uv run ruff format --check src tests` PASS; `uv run pytest tests/unit` → 216 passed.

### 2026-10-26 — P1-10 verified

Invalid rows stay in the raw object. A nullable null is not quarantined. A null in a required field, a wrong type, and every row in a repeated primary-key group are written to `quarantine/{source_id}/{dataset}/dt=…/execution_id=…/{object_name}.jsonl` in `ODP_RAW_BUCKET`. The same execution overwrites that sidecar. A clean rerun deletes it. Structural errors still fail before the write and leave earlier objects in place. `on_invalid: fail_batch` writes neither object. No shipped contract uses that policy. Geolocation has no primary key. `timestamp` and `date` are non-empty text, with no calendar check. CSV booleans are `true` and `false`. The PostgreSQL extract quarantines a null primary key instead of aborting. A naive timestamp or a non-finite number still fails before the write. `uv run ruff check src tests` PASS; `uv run ruff format --check src tests` PASS; `uv run pytest tests/unit` → 214 passed. No live API, Kaggle, or MinIO. D-013 stays `proposed`. Reprocess stays P3-07.

### 2026-10-25 — P1-09 withdrawn

BrasilAPI CEP is withdrawn (ADR-0021 / D-021). Olist publishes a 5-digit zip prefix, and geolocation already carries city, state, and coordinates at that grain. IBGE `municipios` covers every Olist state and the normalized municipality name on nearly all customer, seller, and geolocation rows. An 8-digit CEP lookup does not join to that prefix. DummyJSON and IBGE remain the HTTP sources.

### 2026-10-25 — shared GET checked against both live APIs

DummyJSON `/products` responds with no `Content-Encoding`. The body already starts with `{`. `--execution-id p105-live-20261025` wrote 7 plain JSON pages and 194 products, the same catalog size as the 2026-10-24 run. IBGE still sends gzip. `--execution-id p108-live-20261025b` wrote the decoded array, 5_571 municipalities, starting with `[`. Gzip decoding runs only when that header is present.

### 2026-10-25 — P1-08 live run

The first live call returned HTTP 200 with `Content-Encoding: gzip` (239_529 compressed bytes). `urllib` left the body compressed, so the contract check rejected it as non-JSON. `ingestion.http` now decodes gzip before the size check and the write. The rerun `--execution-id p108-live-20261025` wrote 2_372_050 decoded bytes: 5_571 municipalities, unique 7-digit `municipio-id`, 27 `UF-sigla` values, and one null micro/meso pair (`Boa Esperança do Norte`, MT). No contract change. Against the local Olist extract, `UF-sigla` covers every customer, seller, and geolocation state. Exact `municipio-nome` matches zero rows. Normalized names match 98_713 of 99_441 customer rows, 2_988 of 3_095 seller rows, and 993_930 of 1_000_163 geolocation rows. `uv run pytest tests/unit` → 194 passed.

### 2026-10-25 — P1-08 verified

IBGE Localidades: one JSON array of municipalities (`ibge_localidades` / `municipios`) from `GET /api/v1/localidades/municipios?orderBy=nome&view=nivelado`. The GET uses `ingestion.http` with this client's retry and no-redirect policy, and a 64 MiB response cap. The same execution overwrites `municipios.json`; a new `execution_id` keeps the previous object. Contract key mismatches and an empty array fail before the write. Null micro and meso region values, and a null or duplicate `municipio-id`, stay in the object. `uv run pytest tests/unit` → 191 passed; MinIO integration → 1 passed. The public API was not called. Gold geography for `olist_analysis` stays in P2-05. Quarantine stays in P1-10.

### 2026-10-25 — P1-07 verified

Demo storefront clickstream: one JSONL object of user actions from a fictional retail website (`demo_storefront` / `clickstream`). Event time is on the record, spaced at 500 actions per second. The same execution overwrites `events.jsonl`; a new `execution_id` keeps the previous object. Identifiers do not join Olist or DummyJSON. `uv run pytest tests/unit` → 152 passed; MinIO integration → 1 passed. Bronze, Silver, and Gold placement is noted for P2-03, P2-04, and P2-05 and was not built. CDC stays on PostgreSQL. Quarantine stays in P1-10.

### 2026-10-25 — P1-06 verified

PostgreSQL incremental extract for `sellers`, `customers`, `orders`, and `order_items`. The window is `updated_at >= start AND updated_at < end`, both bounds on UTC midnight. A one-day window and a seven-day window call the same function. Each dataset lands as one JSON array named `{dataset}_{start}_{end}.json`. The synthetic seed replaces schema `oltp`. `uv run pytest tests/unit` → 135 passed; Postgres + MinIO integration → 1 passed. Added `psycopg[binary]` 3.3.6. Quarantine, Airflow schedules, and missed-window detection stay in later issues.

### 2026-10-24 — P1-05 live run

The first live call failed with HTTP 403 (`error code: 1010`) because Cloudflare blocks the default urllib user agent. After a `User-Agent` header, the catalog also disagreed with contract v1: 92 of 194 products omit `brand`, and the short final page sets `limit` to the returned count (14, not 30). Contract v2 records that. A rerun with `--execution-id p105-live-20261024` wrote 7 raw pages. `uv run pytest tests/unit` → 105 passed.

### 2026-10-24 — P1-05 verified

DummyJSON `/products` client: `limit`/`skip` pagination, timeout and 429/5xx retries, and one raw JSON page per object (`products-skip-{skip:06d}.json`) under `dummyjson_products`. Structural mismatches fail the batch. `uv run pytest tests/unit` → 103 passed; MinIO integration → 1 passed. The public API was not called. A shorter re-run on the same `execution_id` does not delete later page keys.

### 2026-10-22 — P1-04 verified

Olist historical ingestion: Kaggle download client, safe unzip into gitignored staging, and raw writes for the 11 published CSVs. `closed_deals.seller_id` joins `sellers.seller_id` or `order_items.seller_id` (contract version 2). Each CSV is read and written on its own; a structural failure stops the remaining files. A leading UTF-8 BOM on `product_category_name_translation` is ignored for the header check and kept in the raw bytes. `uv run pytest tests/unit` → 76 passed; MinIO integration → 1 passed.

### 2026-10-22 — P1-04 Olist historical ingestion

Package fetch of `olistbr/brazilian-ecommerce` and `olistbr/marketing-funnel-olist` through `GET /api/v1/datasets/download/{owner}/{slug}`. One slug is extracted and its CSVs are written one at a time before the next slug starts. Published join: `closed_deals.seller_id` = `sellers.seller_id` or `order_items.seller_id` (those three contracts are version 2). Validation: `uv run ruff check src tests` PASS; `uv run ruff format --check src tests` PASS; `uv run pytest tests/unit` → 76 passed; `uv run pytest tests/integration/test_olist_ingest.py` → 1 passed on local MinIO. A live Kaggle run downloaded the e-commerce zip and stopped on the category-file BOM; the full live run after that fix was not re-executed. No Olist rows committed.

### 2026-10-18 — P1-03 verified

YAML contracts under `config/contracts/` for Olist e-commerce, marketing funnel, DummyJSON products, PostgreSQL v1 (`sellers`, `customers`, `orders`, `order_items`), and IBGE `municipios`. Structural loader only. `uv run pytest tests/unit` → 40 passed; ruff clean. D-013 stays `proposed`. Synthetic events and BrasilAPI CEP are out of this set.

### 2026-10-18 — P1-02 raw storage layout and ingestion metadata

Approved D-020 / ADR-0020: auditable raw in object store under `raw/{source_id}/{dataset}/dt=…/execution_id=…/{object_name}`; required metadata `source_id`, `execution_id`, `_ingestion_ts`; local `data/raw/` remains fetch staging only. Package helpers and minimal writer in `src/open_data_platform/ingestion/`. Validation: `uv run ruff check src tests` PASS; `uv run pytest tests/unit` → 21 passed.

### 2026-10-17 — D-019 sources package reviewed

Owner approved DummyJSON (P1-05), IBGE (P1-08), BrasilAPI CEP optional (P1-09). Alternatives in ADR-0019; `docs/sources/` final choices with standardized metadata; Gold domains in ADR-0012; Phase 1 closing = P1-10/P1-11; Phase 3 daily/weekly = P3-04/P3-05. Validation: ruff PASS; pytest unit 6 passed; no vendored CSVs; field-schema check PASS.

### 2026-10-17 — PROJECT_STATUS ledger restructure

Restructured `PROJECT_STATUS.md` for Phase 1+: current-phase progress only; no cumulative Delivered / Verified / full Open decisions tables. Historical entries moved here. Updated `DECISIONS.md` mirroring rule and related pointers.

### 2026-10-17 — P1-01 historical dataset license / redistribution

Recorded CC BY-NC-SA 4.0 terms for Olist Brazilian e-commerce and Marketing Funnel (join on `seller_id`). Approved D-018 / ADR-0018: do not vendor; fetch-from-source under gitignored `data/raw/`. Docs under `docs/sources/`. No dataset files downloaded or committed.

### 2026-10-16 — Platform report site on Starlight + Catppuccin

Replaced the plain HTML scaffold with Astro Starlight and `@catppuccin/starlight` (preview: starlight-themes.netlify.app/catppuccin). Five pages keep Phase 0 verified content; Pipeline remains a stub. Preview: `cd site && npm run dev` (Node.js 22.12+). No public hosting (Phase 8).

### 2026-10-16 — Platform report site scaffold (Phase 0 content)

Added early `site/` scaffold with Phase 0 verified facts. Roadmap incremental rule updated to allow early scaffold; P8-01 marked in progress.

### 2026-10-16 — Phase 8 platform report site on roadmap

Added Phase 8 as the program close-out: multi-page static report (Home, Architecture, Pipeline, Operations, Reference). Documented incremental growth and P7-07 as in-repo case study feeding the site. Updated `ROADMAP.md`, status ledger, `README.md`, and `docs/project-context-for-cursor.md`.

### 2026-10-16 — Phase 0 verified; pin uv 0.13.1

GitHub Actions CI reported green for lint and unit tests after push. Marked Phase 0 verified. Pinned `uv` to `0.13.1` in `pyproject.toml` (`required-version`) and `.github/workflows/ci.yml` to avoid unpinned “latest” installs.

**Phase 0 acceptance (all verified):** clean clone setup; PostgreSQL healthy; MinIO write/read smoke; lint and tests via documented commands; no secrets/live `.env`/private datasets in VCS; CI runs the same lint and test checks.

**Delivered issues P0-01–P0-13:** see `ROADMAP.md` Phase 0. Evidence included MIT license, `AGENTS.md` / rules, status/roadmap/ADRs, Python 3.12 + `uv`, Ruff/pytest, Compose (PostgreSQL 16 + community MinIO), repo hygiene, GitHub Actions, D-001/D-002 approved.

### 2026-10-16 — Compose runtime verified + D-002 AGPL/AIStor note

Documented that MinIO withdrew official images for AIStor while community MinIO remains AGPLv3 (community images OK). Verified `docker compose up -d`: both services healthy; PostgreSQL reachable; MinIO put/get smoke passed.

### 2026-10-16 — P0-08–P0-13 foundation implementation

Added Python 3.12 package (`uv` + `uv.lock`), Ruff/pytest, Compose (PostgreSQL 16.15-bookworm + community MinIO pin), repo hygiene, README/Makefile, GitHub Actions lint/unit CI, and approved D-001/D-002 with image-distribution notes.

### 2026-10-15 — Decision log restructure (D-001–D-017)

Restructured ADRs: removed Phase-0-services, Kubernetes-standalone, and analytical-placeholder ADRs; split compute vs orchestration and observability vs lineage; added medallion, contracts, batch/streaming coexistence, monorepo, discovery, and dataviz. Index in `DECISIONS.md`; detail in `docs/decisions/` ADR-0001–0017. All remain `proposed` (D-001/D-002 later approved).

### 2026-10-15 — P0-07 decision log (summary + ADRs)

`DECISIONS.md` is a short index (status + one-line summary). Full rationale lives in `docs/decisions/`, with `_template.md` and README.

### 2026-10-15 — AWS path as Infrastructure as Code

Scope alignment: Phase 7 delivers the **AWS deployment path as Terraform IaC**—service mapping (MinIO→S3, catalog→Glue, Trino→Athena), modular resources, OIDC/IAM, cost model and case study—with CI `fmt`/`validate`. Local Compose remains the environment for pipeline development and verification. Cloud apply/destroy stay approval-gated.

### 2026-10-15 — Three update patterns (batch + stream)

Scope review: CDC (Phase 4) covers **streaming** only, not daily/weekly PostgreSQL loads. Updated roadmap and status to require three use cases—daily batch (P1-06 + P3-04), weekly batch (P1-06 + P3-05), streaming CDC (Phase 4)—and added P4-07 batch regression after CDC.

### 2026-10-15 — P0-06 roadmap

Added `ROADMAP.md` with issue-level scope for phases 0–8 (IDs `P0-xx` … `P8-xx`), dependencies, phase and issue acceptance criteria, and out-of-scope notes.

### 2026-10-14 — Phase 0 started

Initialized the public repository and the operating model for the program.

| Commit | Description |
| --- | --- |
| `ab3c9a6` | MIT license |
| `dc49fd8` | `AGENTS.md` and contributor workflow |
| `71e96c6` | Engineering rules by concern |

Also delivered: status ledger and program context under `docs/`.
