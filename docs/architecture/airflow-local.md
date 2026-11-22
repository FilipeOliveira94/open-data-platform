# Local Airflow

P3-01 runs Apache Airflow on the Compose `orchestration` profile. `docker compose up` still starts only PostgreSQL and MinIO.

| Component | Setting |
| --- | --- |
| Image | `odp-runtime:airflow3.4.0-spark3.5.8`, built from `docker/runtime` on `apache/airflow:3.4.0-python3.12` (P3-03). P3-01 and P3-02 ran the upstream image directly |
| Executor | `LocalExecutor` |
| Metadata database | `airflow` on the Compose PostgreSQL 16. Not `odp` and not `iceberg` |
| UI | `http://localhost:8081` (`AIRFLOW_PORT`) |
| Task logs | `s3://odp-airflow-logs/task-logs` on the Compose MinIO |

The host port is 8081 because Trino already publishes 8080. Inside the network the API server listens on 8080.

Services: `airflow-db-create`, `airflow-init`, `airflow-apiserver`, `airflow-scheduler`, and `airflow-dag-processor`. There is no Redis, Celery worker, Flower, or triggerer. Celery and Flower are not a later issue. The revisit condition is a written requirement for self-hosted multi-node workers. Amazon MWAA stays the optional AWS counterpart and is not built here.

`platform_health` is the P3-01 smoke DAG: one `BashOperator` that prints `platform_health ready`, with no schedule. `EmptyOperator` is not used. Airflow 3.4 evaluates that operator in the scheduler and does not send it to the executor, so it never writes a task log.

P3-02 adds the ingestion wrappers. They have no schedule. P3-03 adds `transform_lakehouse` (no schedule) and `reconcile_gold` (weekly). P3-04 adds `ingest_postgres_oltp_daily`, the daily schedule on the PostgreSQL extract. Since P3-13 every ingestion DAG emits a raw Asset that schedules `transform_lakehouse`. P3-05 adds `ingest_postgres_oltp_weekly`, a trailing seven-day re-extract for late commits. Phase 4 sets the CDC schedule. P3-06 adds the [window coverage ledger, gap report, and backfill parameters](#window-coverage-gap-report-and-backfill-p3-06).

Since P3-04 every DAG sets `is_paused_upon_creation=True`. A DAG that appears for the first time is paused, and a run created while it is paused stays `queued`. Unpause a DAG before you trigger it:

```bash
docker compose --profile orchestration exec -T airflow-scheduler \
  airflow dags unpause <dag_id>
```

The flag only applies when the metadata database first records the DAG. DAGs that were already in the `airflow` database keep their paused state. There is no need for `docker compose down -v`.

## Start

PostgreSQL and MinIO must be up. The metadata database is created by `airflow-db-create`. The log bucket is created by `make airflow-up` and again by `airflow-init`.

```bash
make airflow-up
```

Local placeholders live in `.env.example`. Compose uses those defaults when `.env` does not override them. The admin user is `AIRFLOW_ADMIN_USER` (`airflow`). The password is `AIRFLOW_ADMIN_PASSWORD` (`odp`). The Fernet key and JWT secret are local placeholders in the same file.

## UI

Open `http://localhost:8081` and sign in with the admin user. The health endpoint is `http://localhost:8081/api/v2/monitor/health`.

## CLI

```bash
docker compose --profile orchestration exec -T airflow-scheduler airflow version
docker compose --profile orchestration exec -T airflow-scheduler airflow dags list
```

Do not print `airflow config get-value database sql_alchemy_conn`. That value contains the database password.

## Task logs

A finished task writes its log to MinIO. The Airflow containers also keep a spool on the `odp_airflow_logs` volume until the upload completes. Ingestion CLIs log each stage with ``logging.info`` on stdout: the stage name, the dataset, the object key, and counts of records, bytes, and quarantine findings. Those lines do not include a record or a field value. Failures use stderr. Airflow records stdout as info and stderr as error, and stores both in the task log.

The MinIO object remains after the three Airflow services stop and after a host reboot, as long as the MinIO volume `odp_minio_data` remains. `docker compose down -v` deletes that volume and the logs with it. A task killed before the upload finishes can lose that attempt.

The connection id is `minio_logs`. The endpoint inside Compose is `http://minio:9000`. Credentials are the existing MinIO root user and password, passed through the container environment. They are not command arguments.

## Stop

```bash
docker compose --profile orchestration stop airflow-apiserver airflow-scheduler airflow-dag-processor
```

Name the services. `docker compose --profile orchestration stop` with no service names also stops PostgreSQL and MinIO, because those two are not on a profile. It does not remove volumes. Spark and Trino stay up; they are on the `lakehouse` profile.

The health payload reports `triggerer` as `down`. This profile does not run a triggerer. `metadatabase`, `scheduler`, and `dag_processor` are the components that should be `healthy`.

Airflow skips masking for a secret shorter than 5 characters. The local placeholder `odp` is shorter, so each CLI call prints that warning. The metadata-database URL in the migrate log is still redacted.

## Ingestion wrappers (P3-02)

P3-02 ran the upstream `apache/airflow:3.4.0-python3.12` image. Since P3-03 the services run the runtime image built on it; the Airflow Python environment is unchanged. That image already imports `boto3` 1.43.117, `psycopg` 3.3.6, and `yaml` 6.0.3. The package is not installed into `site-packages`. Compose mounts `src/`, `config/`, `pyproject.toml`, `tests/fixtures`, and `data/raw` under `/opt/odp/repo` and sets `PYTHONPATH=/opt/odp/repo/src`. Contract, fixture, and Olist root discovery walk parents of `__file__` and reach `/opt/odp/repo`. The whole repository is not mounted, so `.env` and `.git` stay on the host.

There is one PostgreSQL server, the Compose service `postgres`. It holds three databases. `airflow-db-create` is a one-shot `psql` client of that server, not a second server.

| Database | Role |
| --- | --- |
| `odp` | OLTP extract and seed (`POSTGRES_DB`) |
| `airflow` | Airflow metadata (`AIRFLOW_DB`) |
| `iceberg` | Iceberg JDBC catalog (`ICEBERG_CATALOG_DB`) |

Inside the Airflow containers, `POSTGRES_HOST` is `postgres` and `POSTGRES_PORT` is `5432`. That port is the Compose network port. `MINIO_ENDPOINT_URL` is `http://minio:9000`. The host `.env` values `localhost` and the published Postgres port are not copied into these containers. The metadata URL still uses database `airflow`. `AIRFLOW__CORE__DAG_RUN_CONF_OVERRIDES_PARAMS` is `True`, which is also the image default, so `airflow dags trigger -c` JSON overrides DAG params.

Kaggle credentials are the container variables `KAGGLE_API_TOKEN`, `KAGGLE_USERNAME`, and `KAGGLE_KEY`. They are not task arguments.

Each ingestion task runs `python` with an argument vector and `cwd=/opt/odp/repo`. There is no shell. Retries are 2, the delay is 2 minutes, and the execution timeout is 30 minutes. `schedule` is `None` and `catchup` is false; `ingest_postgres_oltp_daily` (P3-04) is the one scheduled ingestion DAG, and it catches up missed days. `airflow/dags/ingest_common.py` holds the task defaults and is listed in `.airflowignore`.

Since P3-04 `run_python_args` starts the child with `start_new_session=True`, so the child and everything it starts (`spark-sql`, Java, dbt) share one process group. When the wait ends, normally or through an exception, the group gets `SIGTERM`; whatever is left after 10 seconds gets `SIGKILL`. The Airflow task timeout (`AirflowTaskTimeout`) and the Airflow kill (`AirflowTaskTerminated`) both derive from `BaseException` in Airflow 3.4 and arrive while the task waits, so they take the same path. `tests/unit/infrastructure/test_run_python_args.py` checks that no child or grandchild survives a timeout, a grandchild that ignores `SIGTERM`, another exception, a non-zero exit, and a zero exit that leaves a grandchild behind.

`execution_id` defaults to the Airflow `run_id`. A new run keeps the previous raw object. Passing `execution_id` in the trigger conf overwrites the keys of that id. The storefront `seed`, `count`, and `event_time_start` stay the documented generator inputs. The PostgreSQL window bounds stay required and are not calculated here.

| DAG | Entrypoint |
| --- | --- |
| `platform_env` | `python /opt/odp/check_ingest_env.py` with steps `imports`, `contracts`, `minio_raw`, `minio_write`, `postgres_oltp` |
| `ingest_olist` | `python -m open_data_platform.ingestion.olist` |
| `ingest_dummyjson_products` | `python -m open_data_platform.ingestion.dummyjson_products` |
| `ingest_ibge_localidades` | `python -m open_data_platform.ingestion.ibge_localidades` |
| `ingest_demo_storefront` | `python -m open_data_platform.ingestion.demo_storefront` |
| `ingest_postgres_oltp` | `python -m open_data_platform.ingestion.postgres_oltp extract` |
| `seed_postgres_oltp` | `python -m open_data_platform.ingestion.postgres_oltp seed` |

`imports` runs first. The other four `platform_env` tasks run in parallel. `minio_raw` heads `ODP_RAW_BUCKET` and does not create it. `minio_write` puts, reads, and deletes `airflow-env/ready.txt` in `MINIO_SMOKE_BUCKET`. `postgres_oltp` checks `current_database()` and that schema `oltp` exists. It does not seed. Output names the database, the bucket, and the contract count. It does not print passwords or object bodies.

`ingest_postgres_oltp` passes `window_start` and `window_end` through. It does not calculate a daily or weekly window and does not trigger `transform_lakehouse`. The daily window is `ingest_postgres_oltp_daily` (below). `seed_postgres_oltp` truncates `oltp.sellers`, `oltp.customers`, `oltp.orders`, and `oltp.order_items`, then loads `tests/fixtures/postgres_oltp/synthetic.json`. It is not upstream of the extract DAG.

`execution_id` defaults to the Airflow `run_id`. A new run keeps the previous raw object. A trigger conf that sets `execution_id` overwrites the keys of that id. The three PostgreSQL extract DAGs default to `<dag_id>__<run_id>` instead (P3-06): run ids repeat across DAGs, and two DAGs that extract the same source must not share a raw prefix (see [Window coverage](#window-coverage-gap-report-and-backfill-p3-06)).

Each DAG must be unpaused first (see above).

```bash
docker compose --profile orchestration exec -T airflow-scheduler \
  airflow dags trigger platform_env
docker compose --profile orchestration exec -T airflow-scheduler \
  airflow dags trigger -c '{"seed":7,"count":100,"event_time_start":"2026-10-18T00:00:00Z"}' \
  ingest_demo_storefront
docker compose --profile orchestration exec -T airflow-scheduler \
  airflow dags trigger -c '{"window_start":"2026-10-17T00:00:00Z","window_end":"2026-10-18T00:00:00Z"}' \
  ingest_postgres_oltp
docker compose --profile orchestration exec -T airflow-scheduler \
  airflow dags trigger seed_postgres_oltp
```

## Verification (2026-11-12)

`make airflow-up` brought `airflow-apiserver`, `airflow-scheduler`, and `airflow-dag-processor` to healthy. `airflow version` printed `3.4.0`. `docker compose config --services` without the profile listed `minio` and `postgres`.

`GET /api/v2/monitor/health` reported `metadatabase`, `scheduler`, and `dag_processor` as `healthy`. `triggerer` was `down`.

`airflow dags list` showed `platform_health`. A manual run `manual__2026-11-12T15:04:09.043021+00:00` finished `ready` as `success` (pid 303, duration 3.704131 seconds). MinIO then held one object:

`task-logs/dag_id=platform_health/run_id=manual__2026-11-12T15:04:09.043021+00:00/task_id=ready/attempt=1.log`

That object was 5502 bytes and contained the fixed line. It did not contain the MinIO password, the Fernet key, or the JWT secret. After `docker compose --profile orchestration stop airflow-apiserver airflow-scheduler airflow-dag-processor`, MinIO still listed that one object. PostgreSQL and MinIO stayed up.

A second `airflow-init` printed `Database migration done!` and `airflow already exists in the db`. The UI login page loaded at `http://localhost:8081/auth/login/`.

An earlier `docker compose --profile orchestration stop` with no service names also stopped PostgreSQL and MinIO. Spark and Trino stayed up. PostgreSQL and MinIO were started again before the log check above. The object was still in the bucket after that MinIO restart.

## Verification (2026-11-12, P3-02)

Staged Olist CSVs were already present (9 ecommerce files and 2 funnel files). `seed_postgres_oltp` run `manual__2026-11-12T17:12:21.152531+00:00` finished `success`. After it, `oltp` held sellers 5, customers 2, orders 2, and order items 2. The `updated_at` span matched the synthetic fixture: sellers `2026-10-16T23:00:00Z` to `2026-10-24T00:00:00Z`, customers `2026-10-17T09:00:00Z` to `2026-10-20T09:00:00Z`, orders `2026-10-17T10:00:00Z` to `2026-10-19T10:00:00Z`, and order items `2026-10-17T11:00:00Z` to `2026-10-19T11:00:00Z`.

`platform_env` run `manual__2026-11-12T17:06:20.317037+00:00` finished `success`. Tasks `imports`, `contracts`, `minio_raw`, `minio_write`, and `postgres_oltp` finished `success`. The smoke object `airflow-env/ready.txt` was absent after `minio_write`.

The ingestion runs finished `success`. Each raw `execution_id` is that run's `run_id`. No quarantine object was written for these runs.

| DAG | Run | Landing |
| --- | --- | --- |
| `ingest_demo_storefront` | `manual__2026-11-12T17:07:18.562508+00:00` | `events.jsonl`, 100 lines |
| `ingest_postgres_oltp` | `manual__2026-11-12T17:07:21.830105+00:00` | customers 1, order items 1, orders 1, sellers 2 |
| `ingest_dummyjson_products` | `manual__2026-11-12T17:07:25.326561+00:00` | 7 pages, 194 products |
| `ingest_ibge_localidades` | `manual__2026-11-12T17:07:28.755226+00:00` | `municipios.json`, 5_571 rows |
| `ingest_olist` | `manual__2026-11-12T17:07:32.636801+00:00` | 11 CSV objects |

The PostgreSQL extract window was `2026-10-17T00:00:00Z` inclusive to `2026-10-18T00:00:00Z` exclusive. `COUNT(*)` on `oltp` for that window was 1, 1, 1, and 2, the same counts as the raw JSON arrays. The five ingestion task logs did not contain the MinIO password, the Kaggle token, the Fernet key, or the JWT secret.

`uv run ruff check src tests airflow docker/airflow` passed. `uv run ruff format --check src tests airflow docker/airflow` passed. `uv run pytest tests/unit` → 324 passed in 13.00s. The DAG contract tests were re-run after the `execution_id` default change: 29 passed.

`ingest_demo_storefront` run `manual__2026-11-12T19:13:59.070074+00:00` finished `success`. The task log recorded the stage lines as `info`: `stage=generated` with `record_count=100` and `byte_count=16071`, then `stage=wrote` for `events.jsonl` with `finding_count=0`. `uv run ruff check src tests airflow docker/airflow` passed. `uv run ruff format --check src tests airflow docker/airflow` passed. `uv run pytest tests/unit` passed 330.

## Transform DAGs (P3-03)

### One runtime image

`docker/runtime/Dockerfile` builds `odp-runtime:airflow3.4.0-spark3.5.8`. The Airflow services and the lakehouse `spark` service run it. There is no other Spark or dbt install.

| Piece | Source |
| --- | --- |
| Airflow `3.4.0` | `apache/airflow:3.4.0-python3.12`, unchanged |
| Spark `3.5.8` and Temurin JRE `17.0.20` | Copied from `spark:3.5.8-java17@sha256:5363cd7d…` (`/opt/spark`, `/opt/java/openjdk`). That image is a build stage only. No service runs it |
| dbt Core `1.12.6`, dbt-trino `1.11.0` | Venv `/opt/dbt-venv`, installed with `--require-hashes` from `docker/runtime/dbt-requirements.txt` |

`dbt-requirements.txt` is `uv export --only-group dbt` from `uv.lock`. The `dbt` group is not in `default-groups`, so `uv sync` does not install dbt on the host. Before P3-03, Make ran Spark in `spark:3.5.8-java17` and dbt on the host through `uv run dbt`. The decision record is the addendum in [ADR-0005](../decisions/0005-compute-and-transform.md).

`make runtime-build` builds the image. `make airflow-up` builds it when it is missing or stale. `docker compose run` builds it when it is missing.

### Medallion build steps

`python -m open_data_platform.lakehouse.medallion_build <step> --run-label <label>` is the one entrypoint. Make passes `make-<UTC timestamp>`, one label per `make` invocation. Airflow passes the run id. Rendered SQL, the Bronze counts, local staging files, and dbt `target/` and logs go to the named volume `odp_lakehouse_work` at `/opt/odp/work/<label>`. The label is also part of every staging key in the warehouse bucket, so a `make` run and a DAG run never share staging ([ADR-0024](../decisions/0024-incremental-lakehouse-refresh.md)).

Since P3-13 the write steps are incremental. A step with nothing to do writes no table, prints why, and exits with `--skip-exit-code` (0 by default, 99 from the DAG). `--full-rebuild` (`make live-rebuild`) rebuilds every table from all complete raw objects; it is never scheduled.

| Step | What it calls |
| --- | --- |
| `bronze` | Creates the catalog database and `lakehouse_state` if missing, promotes new raw objects (`lakehouse.refresh.refresh_bronze`), runs `run_bronze.sh` once per changed dataset, then registers the objects and opens their Bronze check. Skips when nothing is new, no later layer is pending, and no check is open |
| `bronze-checks` | Every open Bronze check ([quality gate](#quality-gate-p3-16)): Trino `COUNT(*)` against the registry row total stored with the write. Records `passed` or `failed` per dataset. Skips when no Bronze check is open |
| `silver` | Renders and runs `run_silver.sh` for each dataset pending for Silver, then clears its flag, opens its Silver check, and marks Gold pending. Fails without writing when a dataset to build has an open Bronze check. Skips when nothing is pending and no Silver check is open |
| `silver-checks` | Every open Silver check. Records the result per dataset. Skips when no Silver check is open |
| `stage-quarantine` | While Gold is pending and no check is open: `lakehouse.quarantine_stage`, then `run_bronze.sh` on that SQL |
| `dbt-build` | While Gold is pending and no check is open: `dbt build --exclude tag:reconciliation`, then clears the Gold flag |
| `dbt-parse` / `dbt-reconcile` | `dbt parse`, `dbt test --select tag:reconciliation` |

`medallion_build` runs each process with an argument vector and no shell. Credentials stay in the container environment. Trino checks use the HTTP protocol at `ODP_TRINO_HOST:ODP_TRINO_PORT` (`trino:8080`). A mismatch prints the statement, the expected value, and the result, then fails the step.

`spark-sql` writes `derby.log` and `metastore_db` to its working directory. The runtime user cannot write the mounted repository, so each Spark script changes to `${TMPDIR:-/tmp}` first.

`tests/unit/lakehouse/test_medallion_build.py` covers the steps without Compose.

### DAGs

| DAG | Schedule | Tasks |
| --- | --- | --- |
| `transform_lakehouse` | None (also triggered by `ingest_postgres_oltp_daily`, P3-04) | `bronze` → `bronze_checks` → `silver` → `silver_checks` → `stage_quarantine` → `dbt_build` |
| `reconcile_gold` | `@weekly` | `dbt_reconcile` |

The tasks use the P3-02 `DEFAULT_ARGS` from `ingest_common.py`: 2 retries, a 2-minute delay, and a 30-minute execution timeout. Since P3-16 the two check tasks have `retries=0`: a failed check is a data result, and it stays open for the next refresh. Both DAGs set `max_active_runs=1` and `catchup=False`. `transform_lakehouse` is linear. All Spark steps share one Ivy cache and one container, so two Spark steps do not run at the same time. Quarantine staging does not read Bronze or Silver. It runs after `silver_checks` only for that reason.

Until P3-04, `reconcile_gold` was unpaused at creation and did not wait for `transform_lakehouse`, so a reconciliation run could read a table that was being replaced. P3-04 closes that with a pool (next section).

Trino (profile `lakehouse`) must be up for the check steps and dbt:

```bash
docker compose --profile lakehouse up -d --wait trino
docker compose --profile orchestration exec -T airflow-scheduler \
  airflow dags trigger transform_lakehouse
docker compose --profile orchestration exec -T airflow-scheduler \
  airflow dags trigger reconcile_gold
```

`tests/unit/infrastructure/test_dagbag.py` loads `airflow/dags` with the pinned Airflow in a `DagBag`. It checks zero import errors, the DAG ids (ten at P3-03, eleven since P3-04), the task order, the shared retries and timeout, and the entrypoint of each task. Since P3-04 it also checks that every DAG is paused at creation, the exact set of `lakehouse_gold` tasks, the daily timetable and window, and the `extract` → `trigger_transform` dependency. Success, retry, and failed-run behavior on a live scheduler is P3-09.

### `lakehouse_gold` pool (P3-04)

`lakehouse_gold` is an Airflow pool with 1 slot. `airflow-init` creates it with `airflow pools set lakehouse_gold 1 …`, which also resets the slot count on every start. A task in a pool that does not exist is never scheduled: the Airflow 3.4 scheduler logs `Tasks using non-existent pool … will not be scheduled`.

| DAG | Tasks in the pool |
| --- | --- |
| `transform_lakehouse` | `bronze`, `silver`, `stage_quarantine`, `dbt_build`: every step that writes tables |
| `reconcile_gold` | `dbt_reconcile` |

`bronze_checks` and `silver_checks` only read through Trino and use `default_pool`. With one slot, a reconciliation run never overlaps a write, and two writes never overlap either. The slot is per task, not per run. Between two pooled transform steps, a waiting `dbt_reconcile` can take the slot. It then reads a lakehouse that is half rebuilt (for example new Bronze and old Silver and Gold), but no table changes while it reads. `tests/unit/infrastructure/test_dagbag.py` checks the exact list of pooled tasks. The `check_contracts` DAG (P10-08) joins the same pool.

The pool was chosen over an Asset that `dbt_build` emits. An Asset only orders runs. A manual reconciliation or a second rebuild can still overlap, and the weekly cadence would become "after every build".

## Verification (2026-11-13, P3-03)

Local Compose with PostgreSQL, MinIO, Trino, and the three Airflow services on `odp-runtime:airflow3.4.0-spark3.5.8`. `airflow dags list` showed `transform_lakehouse` and `reconcile_gold`. `airflow dags list-import-errors` returned no data. Times are UTC, as Airflow records them.

`transform_lakehouse` run `manual__2026-11-14T03:25:52.218916+00:00` finished `success` in 10 min 12 s. Each task succeeded on attempt 1.

| Task | Duration | Result |
| --- | --- | --- |
| `bronze` | 5 min 05 s | Catalog database present. 18 Spark files |
| `bronze_checks` | 1 s | 18 of 18 Trino counts equal to the raw read of this run |
| `silver` | 4 min 07 s | 18 Spark files |
| `silver_checks` | 3 s | 20 of 20 checks `ok` |
| `stage_quarantine` | 14 s | 1_603 findings staged |
| `dbt_build` | 31 s | PASS=287 WARN=0 ERROR=0 (55 table models, 232 schema tests) |

`reconcile_gold` run `manual__2026-11-14T03:36:32.717852+00:00` ran after that build and finished `success`: PASS=56 WARN=0 ERROR=0 in 590 s, inside the 30-minute task timeout. When the DAG first appeared, the scheduler also started `scheduled__2026-11-08T00:00:00+00:00` on its own, as described above. That run passed 56 of 56 against the Gold built before this issue.

Trino after the DAG run, compared with [P2-09](phase-2-live-run.md):

| Anchor | P2-09 | P3-03 DAG run |
| --- | --- | --- |
| Silver geolocation | 738_332 | 738_332 |
| Silver postgres_oltp customers / orders / items / sellers | 2 / 2 / 2 / 3 | 2 / 2 / 2 / 3 |
| `mart_late_orders` | 6_534 | 6_534 |
| On-time / eligible | 89_936 / 96_470 | 89_936 / 96_470 |
| `dim_customers` / `dim_zip_prefix` / `dim_date` / `dim_municipality` | 96_096 / 19_015 / 3_774 / 5_571 | same |
| Quarantine findings staged | 1_603 | 1_603 |
| Gold tables | 55 | 55 |
| `dbt build` / reconciliation | PASS=287 / PASS=56 | PASS=287 / PASS=56 |

Bronze row counts are not the P2-09 values. Bronze geolocation is 2_000_326, against 1_000_163 in P2-09. The raw prefixes now hold two Olist executions (`run-001` and the P3-02 Airflow run), and Bronze keeps every execution under a prefix. `bronze_checks` compares with the raw read of the same run, and Silver dedup returns the P2-09 counts.

The Make path calls the same steps. `make live-bronze` (323 s, 18/18 checks), `make live-silver` (241 s, 20/20), `make live-gold` (43 s, quarantine 1_603 and PASS=287), and `make dbt-test-reconciliation` (558 s, PASS=56) exited 0. `make test-iceberg-poc` printed `2`. `make test-catalog-probe` printed Spark `2` and Trino `"2"`. `make dbt-parse` and `make spark-ivy-warm` exited 0 in the runtime image with the existing `.local/spark-ivy` cache.

The seven task logs in MinIO did not contain the MinIO password, the Fernet key, or the JWT secret. `uv run ruff check src tests airflow docker/airflow` passed. `uv run ruff format --check src tests airflow docker/airflow` passed. `uv run pytest tests/unit` → 348 passed.

## Daily PostgreSQL schedule (P3-04)

`ingest_postgres_oltp_daily` runs `extract`. On success the task emits the `raw_postgres_oltp` Asset, which schedules `transform_lakehouse` (P3-13).

| Setting | Value |
| --- | --- |
| Timetable | `CronDataIntervalTimetable("0 0 * * *", timezone="UTC")` |
| Window | The run's data interval, `[data_interval_start, data_interval_end)` = `[D-1 00:00Z, D 00:00Z)` for the run at `D 00:00Z` (21:00 in Brasília) |
| Catchup | `True`, `start_date` 2026-11-19. After a pause or a scheduler outage, the scheduler creates one run per missed day, oldest first, one at a time. On a fresh database the first run is the 2026-11-19 window. Days before `start_date` are a backfill (P3-06). Until 2026-11-20 05:20Z the DAG had `catchup=False`; the live run below used that setting |
| Settle lag | The `settle` sensor (`TimeDeltaSensor`, `mode="reschedule"`) holds `extract` until `D 00:00Z + 15 min` (P3-16). It does not defer: the stack has no triggerer (D-006). A caught-up window is already past the lag |
| Ingestion timestamp | `--ingestion-ts {{ dag_run.run_after.isoformat() }}` (P3-16): `D 00:00Z` for a scheduled run, the trigger time for a manual one. Every try writes under the same `dt=` |
| `max_active_runs` | 1 |
| Paused at creation | Yes |
| Params | `execution_id` only. No window params |
| After the extract | Asset event `odp-raw://postgres_oltp`. Until P3-13 a `TriggerDagRunOperator` started `transform_lakehouse` |

The timetable is explicit. In Airflow 3.4 `[scheduler] create_cron_data_intervals` defaults to `False`, so a cron string becomes a `CronTriggerTimetable` whose data interval has zero length. The extract rejects a bound that is not UTC midnight, so the timetable stays in UTC. Midnight in São Paulo is 03:00Z.

The extract task renders `--window-start {{ data_interval_start.isoformat() }}` and `--window-end {{ data_interval_end.isoformat() }}`. The DAG does not calculate the window. `validate_window` in the package still checks it. A run without a data interval renders empty bounds, and the extract exits 1 (`window bound must be ISO-8601`) before it connects.

How each kind of run gets its window in Airflow 3.4:

| Run | Window |
| --- | --- |
| Scheduled | The interval that just ended. Run id `scheduled__<D>T00:00:00+00:00`, logical date `D-1` |
| `airflow dags trigger -l <date>` | The last complete UTC day before the trigger time. The CLI infers the interval from `run_after` (now), not from `-l`. `-l` only sets the logical date |
| `airflow dags trigger` without `-l` | No data interval. The extract fails |
| `airflow backfill create --from-date D --to-date D` | `[D, D+1)`. See [Backfill](#backfill-parameters) |

The transform runs only after a successful extract, because only a successful task emits the Asset event. See [Lakehouse refresh trigger](#lakehouse-refresh-trigger-p3-13) for how several events coalesce.

Idempotency:

- A retry or a cleared task keeps the run id, so `execution_id` stays the same. The extract overwrites the same raw keys while the ingestion date is the same. Identical content keeps the same ETag, so Bronze does not promote it again; changed content gets a new ETag, and Bronze replaces that object's rows in one `MERGE`.
- A new run of the same window, for example a manual trigger on the same day as the scheduled run, has a new run id. It writes a new execution next to the previous one. Bronze keeps both and Silver deduplicates by key. This is the P3-02 rule: a new run keeps the previous raw object.
- No window is skipped. The window comes only from the schedule and no watermark is stored, so with `catchup=False` a day the scheduler missed would never be extracted, without any error. `catchup=True` closes that gap. A record of covered windows and a gap report are P3-06.
- Every caught-up run emits an Asset event. Events that arrive while a refresh runs start one follow-up refresh, which promotes every new window at once (P3-13).
- The `dt=` segment comes from the run's `run_after`, not the try's clock (P3-16). Until P3-16 it was the real ingestion date, so a retry that crossed UTC midnight wrote a second complete prefix with the same execution id (review R14).

### Watermark, settle lag, and snapshot (P3-16)

- `schema.sql` adds a `BEFORE UPDATE` trigger, `set_updated_at`, on each `oltp` table. When an `UPDATE` leaves `updated_at` unchanged, the trigger sets it to `clock_timestamp()`, the time of the row change rather than the transaction start. A writer that sets `updated_at` explicitly keeps its value. `INSERT` is untouched; the column is `NOT NULL`.
- The extract checks `pg_trigger` and fails before reading when a table lacks the trigger. `python -m open_data_platform.ingestion.postgres_oltp apply-schema` (idempotent) adds it to an existing database without reseeding.
- The extract refuses a window before `window_end + 15 min` (`coverage.SETTLE_LAG`). Bounds stay at UTC midnight.
- The four datasets of a window are read in one `REPEATABLE READ, READ ONLY` transaction before any write. `orders` and `order_items` come from the same snapshot. A read failure writes nothing.

Residual late commit: a transaction whose `UPDATE` ran before `D 00:00Z` and that commits after `D 00:15Z` has an `updated_at` inside `[D-1, D)` but was not visible to the daily read. The [weekly re-extract](#weekly-postgresql-re-extract-p3-05) reads that day again one day after the week ends, and backfills (P3-06) cover the rest. Streaming CDC (Phase 4) is the exact path.

Since P3-13 `transform_lakehouse` promotes only the new raw objects and rebuilds only the Silver datasets they changed. Measured costs are in [Bronze layer](bronze-layer.md#incremental-refresh-p3-13).

## Weekly PostgreSQL re-extract (P3-05)

`ingest_postgres_oltp_weekly` re-extracts seven days of the OLTP tables through the same P1-06 extract and `postgres_extract_task` as the daily DAG. Purpose, against the daily grain: the daily run at `D 00:00Z` reads `[D-1, D)` once, so a row whose `updated_at` falls in that day but whose transaction commits after the run is never in a daily window. The weekly run reads those days again after they had time to settle.

| Setting | Value |
| --- | --- |
| Timetable | `CronDataIntervalTimetable("0 0 * * 1", timezone="UTC")`, Monday 00:00Z |
| Data interval | `[W-7, W)` for the run at Monday `W 00:00Z` |
| Extract window | The data interval shifted back one day: `[W-8, W-1)`, Sunday 00:00Z to Sunday 00:00Z (Sunday through Saturday). Rendered as `data_interval_start.subtract(days=1)` and `data_interval_end.subtract(days=1)` |
| Catchup | `True`, `start_date` 2026-11-16. The first scheduled run is 2026-11-23 and extracts `[2026-11-15, 2026-11-22)`. Missed weeks run one at a time, oldest first |
| `max_active_runs` | 1 |
| Paused at creation | Yes |
| Execution id | `params.execution_id or (dag.dag_id ~ '__' ~ run_id)` since P3-06. Until then it was `params.execution_id or run_id`, which repeats the daily run id at Monday 00:00Z (both `scheduled__<W>T00:00:00+00:00`) and for backfills |
| After the extract | Asset event `odp-raw://postgres_oltp`, which schedules `transform_lakehouse` (P3-13) |

Why one day of lag. With `[W-7, W)` the last day of the window, Sunday, would have the same 00:00Z edge as its daily run: a Sunday row committed after Monday 00:00Z would miss both. With `[W-8, W-1)` every day in the window had at least one full day after its daily run before the weekly run reads it. Consecutive windows tile without gap or overlap (`test_weekly_windows_tile_without_gaps`). Filipe approved the shift on 2026-11-21; the roadmap text said `[W-7, W)`.

Pipeline design checklist:

| Question | Answer |
| --- | --- |
| Change type | `incremental_window`. The weekly execution repeats rows the daily executions already landed. Silver keeps the latest `updated_at` per key, so a repeated row stays one row; a tie keeps identical values and only the lineage columns may point at the weekly execution |
| Window and watermark | From the data interval only, no stored watermark. A missed week is caught up. Overlap with the daily windows is the point of the DAG. Not covered: a row committed after the weekly run that covers its `updated_at`; backfill (P3-06) and CDC (Phase 4) cover it |
| Re-run and backfill | A cleared run keeps its run id and overwrites the same raw keys; identical content keeps the ETag and Bronze does not promote it again. A new run of the same week writes a new execution; Silver deduplicates. Both proven below |
| Deletes | A hard delete is never extracted. The row stays in Silver and Gold (P3-12). CDC (Phase 4) is the delete path |
| What a run reads | PostgreSQL: seven days of `updated_at`. Bronze: only the weekly objects (P3-13). Silver rebuilds the `postgres_oltp` tables from Bronze, which grows with history (P3-13 behavior). Raw keeps each day twice: once daily, once weekly |
| Reader view | Unchanged: atomic replace (P3-11) |
| Ownership and concurrency | Monday 00:00Z starts the daily and the weekly run together. Both only read PostgreSQL and write different raw prefixes because the execution id carries the DAG id (P3-06; the P3-05 claim that the run ids differ was wrong, see [the collision](#execution-id-collision-found-by-p3-06)). Their two Asset events lead to at most one extra transform run (`batch_asset_events=True`) |
| Trigger | The schedule or a manual trigger with `-l`. Downstream: the raw Asset |
| Failure | A failed extract writes no `_COMPLETE` marker and emits no Asset event. Retries (2) keep the run id and overwrite the same keys. Silver reads every window, complete or not, for `incremental_window`; a partial weekly object only repeats rows |
| Evidence | `scripts/p3_05_weekly_check.sh`, below |

A manual trigger needs `-l`. `airflow dags trigger ingest_postgres_oltp_weekly -l <now>` gets the last complete Monday week before the trigger time as its data interval; on 2026-11-21 that is `[2026-11-09, 2026-11-16)`, so the extract window is `[2026-11-08, 2026-11-15)`. Without `-l` the bounds render empty and the extract fails before it connects.

## Window coverage, gap report, and backfill (P3-06)

[ADR-0025](../decisions/0025-extract-window-coverage-ledger.md). Each successful PostgreSQL extract records one row per dataset in `ingestion_state.extract_coverage` (catalog database `ICEBERG_CATALOG_DB`). The DAGs pass `--frequency`, `--dag-id {{ dag.dag_id }}`, and `--airflow-run-id {{ run_id }}`.

| Column | Meaning |
| --- | --- |
| `source_id`, `dataset`, `execution_id` | Primary key. A cleared or reprocessed run keeps its run id and overwrites its own row; a new run of the same window adds a row |
| `frequency` | `daily` (1-day window), `weekly` (7-day window), or `manual` (the `ingest_postgres_oltp` DAG and a `make` or CLI extract). A `CHECK` rejects a daily or weekly row with the wrong window length, and the extract rejects it before it connects |
| `window_start`, `window_end` | The half-open extract window, UTC midnight |
| `dag_id`, `airflow_run_id` | Null outside Airflow |
| `row_count` | Rows fetched for the window, quarantined ones included |
| `recorded_at` | When the ledger row was written |

The extract writes the rows in one transaction, after every dataset has its `_COMPLETE` marker. A failed extract records nothing, so the window shows as a gap. If the ledger write fails after the raw write, the task fails and the retry rewrites the same raw keys.

### Gap report

`make coverage-report FREQUENCY=daily` (or `weekly`) runs `python -m open_data_platform.ingestion.coverage report --frequency <f>`. The report rebuilds the expected windows of the cadence from its first window up to the last window due at report time (`as_of`, now in the CLI), or the last recorded window end when that is later. A window is due once its end plus the cadence run delay has passed. A DAG that stopped therefore shows every window since then as a gap (P3-16, review R7). Until P3-16 the expected windows ended at the last recorded window, so a stopped DAG reported no gap.

| Cadence | First window | Expected windows | Due at |
| --- | --- | --- | --- |
| `daily` | `[2026-11-19, 2026-11-20)`, the DAG `start_date` | `[D, D+1)` for every day | `D+1 00:15Z` (settle lag) |
| `weekly` | `[2026-11-15, 2026-11-22)`, the first interval shifted back one day | `[W-8, W-1)` for every Monday `W` | `W 00:00Z` (one day after the window end) |

| Finding | Rule | Error |
| --- | --- | --- |
| Gap | An expected window without a row for one of the four datasets | Yes |
| Overlap | Two different recorded windows of the same frequency that intersect (for example a weekly backfill on a non-Monday) | Yes |
| Re-run | The same window recorded by more than one execution | No, counted |
| Before start | A window that starts before the first window (a backfill before `start_date`) | No, listed |

Daily and weekly windows overlap by design (P3-05) and are never compared with each other. `manual` rows are not part of either report. The command prints window dates, dataset names, and counts. It exits 0 when clean, 1 on a gap or an overlap, and 2 when the ledger cannot be read. The cadence start dates live in `coverage.CADENCES`; a unit test fails if they drift from the DAG `START_DATE`.

### Backfill parameters

Airflow 3.4 selects a past window only through `airflow backfill create`. `airflow dags trigger -l` sets the logical date, but the data interval still comes from the trigger time (see [Daily PostgreSQL schedule](#daily-postgresql-schedule-p3-04)).

| DAG | Command | Window per logical date `L` |
| --- | --- | --- |
| `ingest_postgres_oltp_daily` | `airflow backfill create --dag-id ingest_postgres_oltp_daily --from-date A --to-date B` | One run per day `L` in `[A, B]` (both inclusive), window `[L, L+1)` |
| `ingest_postgres_oltp_weekly` | `airflow backfill create --dag-id ingest_postgres_oltp_weekly --from-date A --to-date B` | One run per Monday `L` in `[A, B]`, window `[L-1, L+6)` (Sunday to Saturday) |

| Option | Effect |
| --- | --- |
| `--reprocess-behavior none` (default) | Creates runs only for logical dates without a run. A covered window is skipped |
| `--reprocess-behavior failed` | Also re-runs failed runs |
| `--reprocess-behavior completed` | Also re-runs successful runs. The existing run is cleared and keeps its run id, so the extract overwrites its raw keys and ledger rows |
| `--max-active-runs 1` | Pass it. The backfill does not inherit the DAG's `max_active_runs=1`: without the flag, the six daily runs below started within 60 ms of each other. Parallel extracts are safe (distinct raw keys and ledger rows), but one at a time, oldest first, matches catchup |
| `--dry-run` | Lists the logical dates without creating runs |

Each backfill run emits the `raw_postgres_oltp` Asset. `transform_lakehouse` (`max_active_runs=1`, `batch_asset_events=True`) runs once for the first event, and one follow-up consumes every event queued meanwhile. A backfill of N windows therefore gives far fewer than N refreshes, and each refresh promotes only raw objects not promoted before (ADR-0024).

## Lakehouse refresh trigger (P3-13)

Every ingestion DAG emits an Airflow Asset per raw source it lands, on the success of its ingest or extract task:

| DAG | Assets |
| --- | --- |
| `ingest_olist` | `raw_olist_brazilian_ecommerce`, `raw_olist_marketing_funnel` |
| `ingest_dummyjson_products` | `raw_dummyjson_products` |
| `ingest_ibge_localidades` | `raw_ibge_localidades` |
| `ingest_demo_storefront` | `raw_demo_storefront` |
| `ingest_postgres_oltp`, `ingest_postgres_oltp_daily` | `raw_postgres_oltp` |

The URI is `odp-raw://{source_id}`. `transform_lakehouse` is scheduled on any of them (`AssetAny`) with `max_active_runs=1`. No DAG uses `TriggerDagRunOperator`. A manual `transform_lakehouse` trigger still works.

Each write task of the transform after `bronze` uses the trigger rule `none_failed`, so a skipped step does not stop a layer that is still pending from an earlier failed run. A check task keeps `all_success` and is skipped when its write step was skipped. A step with nothing to do exits 99, which `run_python_args` turns into a skipped task. Since P3-16 a write step does not skip while a check of its layer is open, so the check task still runs; see [Quality gate](#quality-gate-p3-16).

## Quality gate (P3-16)

Until P3-16 a check step checked only the tables its own run wrote. A refresh with no new raw object after a failed check skipped the check, and the `none_failed` rule then built Silver, and later Gold, on data that had failed (review R1). The check state now lives in the promotion registry, table `lakehouse_state.layer_checks`:

| Column | Meaning |
| --- | --- |
| `layer`, `source_id`, `dataset` | Primary key. `layer` is `bronze` or `silver` |
| `status` | `pending` (written, not checked), `failed`, or `passed`. Pending and failed checks are open |
| `expected_rows` | Bronze only: the registry row total stored with the write. A later run re-checks without the earlier run's work dir |
| `marked_by_run`, `marked_at` | The write that opened the check |
| `checked_by_run`, `checked_at` | The check run that recorded `status` |

| Event | Effect |
| --- | --- |
| Bronze registers objects of a dataset | Its Bronze check becomes `pending`, in the transaction that marks Silver pending |
| Silver writes a dataset | Its Silver check becomes `pending`, in the transaction that clears its Silver flag |
| A check step runs | It runs every open check of its layer, one dataset at a time, records `passed` or `failed` for each, and fails if any failed. A write that lands while the check runs leaves the check `pending` |
| `silver` would build a dataset with an open Bronze check | The step fails before writing and names the dataset |
| `stage-quarantine` or `dbt-build` while any check is open | The step fails before writing and names the checks |
| `bronze` or `silver` has nothing to write but a check is open | The step ends as "ran", so the check task after it is not skipped |

A dataset with no row has not been written since the gate existed and has no open check. A Trino error during a check is recorded as `failed`; the next refresh runs it again. The check tasks have `retries=0`.

Pipeline design checklist:

| Question | Answer |
| --- | --- |
| Change type | No `update_pattern` changes. The trigger makes `updated_at` reliable for the `postgres_oltp` datasets |
| Window and watermark | Daily and weekly windows are unchanged. The daily read waits 15 minutes past the window end. A missed window shows as a gap, now also after the last recorded window. A commit later than the lag is left to the weekly re-extract |
| Re-run and backfill | A retry keeps `dt=`, so it overwrites the same keys. Re-running a check on unchanged Bronze gives the same result. `--full-rebuild` opens every check and goes through the same gate |
| Deletes | Not applicable: no delete path changes. A hard delete in OLTP is still invisible to the window extract (P3-12) |
| What a run reads | A re-check is one Trino `COUNT(*)` (Bronze) or the Silver checks of each open dataset. No raw history is read again |
| Reader view | A Bronze table that failed stays queryable; nothing in Silver or Gold is built on it. Atomic replace (P3-11) is unchanged |
| Ownership and concurrency | Only the `transform_lakehouse` steps (`max_active_runs=1`) and the `make live-*` targets write `layer_checks`. A result is stored only if `marked_at` is unchanged, so a mark made during a check survives |
| Trigger | Same Asset schedule. Each later run re-checks whatever is still open |
| Failure | A failed check leaves `failed` in the registry and fails its task with no retry. The next refresh runs it again before Silver or Gold. A blocked write step fails with the dataset and check names |
| Evidence | Unit tests, `pytest tests/integration`, and `scripts/p3_16_gate_check.sh` (below) |

## Verification (2026-11-20, P3-04)

Local Compose with PostgreSQL, MinIO, Trino, and the three Airflow services on `odp-runtime:airflow3.4.0-spark3.5.8`. Times are UTC.

`make airflow-up` ran `airflow-init`, which printed `Pool lakehouse_gold created`. `airflow pools list` showed `lakehouse_gold` with 1 slot next to `default_pool` (128). `airflow dags list-import-errors` returned no data. `airflow dags list` showed `ingest_postgres_oltp_daily` paused, the only DAG new to the metadata database. The ten older DAGs kept their unpaused state, as described above.

`seed_postgres_oltp` run `manual__2026-11-20T03:41:23.189741+00:00` finished `success`. That CLI trigger, without `-l`, had no logical date and no data interval.

### Daily runs

| Run | How it started | Data interval | Extract landing | Result |
| --- | --- | --- | --- | --- |
| `scheduled__2026-11-20T00:00:00+00:00` | `airflow dags unpause` (catchup off: last complete day) | `2026-11-19T00:00Z` → `2026-11-20T00:00Z` | 4 empty arrays (3 bytes each) | `success` |
| `manual__2026-11-20T03:42:53.785961+00:00` | `airflow dags trigger -l 2026-11-20T03:45:00+00:00` | `2026-11-19T00:00Z` → `2026-11-20T00:00Z` (logical date 03:45) | 4 empty arrays | `success` |
| `backfill__2026-10-18T00:00:00+00:00` | `airflow backfill create --from-date 2026-10-17T00:00:00+00:00 --to-date 2026-10-17T00:00:00+00:00` | `2026-10-17T00:00Z` → `2026-10-18T00:00Z` | sellers 2, customers 1, orders 1, order items 1 | `success` |

Each extract log line `stage=extract_started` carried the same `window_start` and `window_end` as the data interval, and `execution_id` equal to the run id. `COUNT(*)` on `oltp` for `[2026-10-17, 2026-10-18)` was 2, 1, 1, and 1, the same as the raw arrays. For `[2026-11-19, 2026-11-20)` it was 0 across the four tables. No quarantine finding was written. Each `trigger_transform` logged `Dag Run triggered successfully.` for `transform_lakehouse`.

### Triggered transform runs

`transform_lakehouse` ran the three triggered runs one after another (`max_active_runs=1`). Every task succeeded on attempt 1.

| Run id | Start → end | Duration | `bronze` | `bronze_checks` | `silver` | `silver_checks` | `stage_quarantine` | `dbt_build` |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| `ingest_postgres_oltp_daily__scheduled__2026-11-20T00:00:00+00:00` | 03:41:50 → 04:02:12 | 20 min 22 s | 5 min 00 s | 4 s | 4 min 07 s | 3 s | 13 s | 31 s |
| `ingest_postgres_oltp_daily__manual__2026-11-20T03:42:53.785961+00:00` | 04:02:12 → 04:11:34 | 9 min 22 s | 4 min 27 s | 1 s | 4 min 01 s | 3 s | 13 s | 29 s |
| `ingest_postgres_oltp_daily__backfill__2026-10-18T00:00:00+00:00` | 04:11:35 → 04:21:26 | 9 min 51 s | 4 min 53 s | 1 s | 4 min 07 s | 3 s | 12 s | 27 s |

Each `dbt_build` printed PASS=287 WARN=0 ERROR=0. The backfill-triggered run passed 18 Bronze checks and 20 Silver checks. After it, Trino showed Silver `postgres_oltp` customers 2, orders 2, order items 2, and sellers 3, the P2-09 values, while Bronze sellers held 11 rows across executions. `gold_staging.quarantine_findings` held 1_603 rows.

### Pool serialization

`reconcile_gold` run `manual__2026-11-20T03:42:11.378124+00:00` was triggered while the first transform's `bronze` ran. `dbt_reconcile` stayed `scheduled` until `bronze` released the slot.

| Task | Start | End |
| --- | --- | --- |
| `transform_lakehouse.bronze` | 03:41:50.8 | 03:46:50.9 |
| `transform_lakehouse.bronze_checks` (default pool) | 03:46:51.6 | 03:46:56.0 |
| `reconcile_gold.dbt_reconcile` | 03:46:51.6 | 03:57:12.4 |
| `transform_lakehouse.silver` | 03:57:14.6 | 04:01:21.8 |

No two `lakehouse_gold` tasks overlapped. `dbt_reconcile` took the slot between `bronze` and `silver`, as the pool section describes, so that transform run took 20 min 22 s instead of about 10. `dbt_reconcile` passed 56 of 56 (PASS=56 WARN=0 ERROR=0).

### Re-execution of the same run

`airflow tasks clear ingest_postgres_oltp_daily -s 2026-10-17T00:00:00+00:00 -e 2026-10-17T00:00:00+00:00 -y` cleared the backfill run. `extract` (attempt 2) wrote the same four keys under `dt=2026-11-20/execution_id=backfill__2026-10-18T00:00:00+00:00/` with the same counts (2/1/1/1). The sellers prefix for `dt=2026-11-20` still held three objects: one per run id. `trigger_transform` reset the existing run `ingest_postgres_oltp_daily__backfill__2026-10-18T00:00:00+00:00` instead of creating a fourth. That run finished `success` again (04:23:23 → 04:33:29, 10 min 06 s; every task on attempt 2). `dbt_build` printed PASS=287. Bronze sellers stayed at 11 rows, Silver `postgres_oltp` stayed 2/2/2/3, and `gold_staging.quarantine_findings` stayed 1_603.

The 33 task-log objects of these runs (8 daily, 24 transform, 1 reconciliation) did not contain the MinIO password, the Fernet key, or the JWT secret. The PostgreSQL placeholder `odp` is shorter than 5 characters and was not scanned.

Unit checks for this issue: `uv run pytest tests/unit` → 364 passed. `uv run ruff check src tests airflow docker/airflow` and `uv run ruff format --check src tests airflow docker/airflow` passed. `make lint` passed. Before the commit, a temporary mutation that dropped `start_new_session` and `killpg` made all six tests in `test_run_python_args.py` fail; the file was restored.

### What differed from the plan

| Item checked | Result |
| --- | --- |
| Cron string in Airflow 3.4 | `create_cron_data_intervals` is `False`; a cron string gives `CronTriggerTimetable` with a zero-length interval. Confirmed. The DAG uses `CronDataIntervalTimetable` |
| Task in a missing pool | The scheduler never schedules it. Confirmed in the 3.4 scheduler source. `airflow-init` creates the pool |
| `AirflowTaskTimeout` hierarchy | Derives from `BaseException`, as does `AirflowTaskTerminated`. Confirmed |
| `airflow dags trigger -l` | Diverged from the plan. The flag sets the logical date only. The data interval comes from the trigger time, so `-l 2026-10-17…` does not select that window. A specific past window used `airflow backfill create` |
| `airflow dags trigger` without `-l` | No logical date and no data interval. The daily extract would fail on empty bounds |

## Verification (2026-11-21, P3-05)

`scripts/p3_05_weekly_check.sh` ran on the local stack (Airflow profile, Trino). `scripts/p3_05_late_commit.py` plants synthetic sellers with bound parameters and prints counts only; the Trino queries hold window bounds and table names, no key or value. Output went to `.local/p3-05/`.

| Step | Result |
| --- | --- |
| On-time seller, `updated_at` 2026-11-13 06:00Z, committed before the daily run | PostgreSQL window `[2026-11-13, 2026-11-14)`: 1 seller |
| `airflow backfill create` daily for 2026-11-13 | `backfill__2026-11-14T00:00:00+00:00` success in 1.5 s. Asset-triggered transform success in 106 s. Silver window: 1 seller; Bronze window: 1 row |
| Late seller, `updated_at` 2026-11-13 12:00Z, committed after the daily run | PostgreSQL window: 2 sellers. Silver window still 1: the daily window missed it |
| `airflow dags trigger ingest_postgres_oltp_weekly -l 2026-11-21T15:51:03+00:00` | `manual__2026-11-21T15:51:08.631343+00:00` success; extract 1.9 s. Raw key `sellers_2026-11-08_2026-11-15.json`. Asset-triggered transform success in 105 s |
| After the weekly run | Silver sellers 5 → 6, 6 distinct keys. Silver window 2 = PostgreSQL window 2. Bronze window 3 rows: the on-time seller from the daily and the weekly execution, the late seller once. Customers, orders, and order items: 2 rows, 2 keys each, unchanged |
| `airflow tasks clear` of the same weekly run | Extract attempt 2 success in 1.1 s, same key. Transform success in 6.7 s. Silver and Bronze counts unchanged |

An earlier pass of the same script on 2026-11-12, a day with no seller before the late commit, also passed. It planted one more synthetic seller (`updated_at` 2026-11-12 12:00Z). The three synthetic sellers stay in PostgreSQL, raw, Silver, and Gold. Their ids start with `p3-05-late-`.

Not run: a scheduled weekly run. The first one is 2026-11-23 00:00Z.

Unit checks: `uv run pytest tests/unit` → 417 passed, with five new DagBag tests for the weekly timetable, intervals, rendered window, tiling, and empty bounds. `make lint` passed.

## Verification (2026-11-21, P3-06)

On the local stack (Airflow, Trino, Spark profile), with the daily and weekly DAGs unpaused. `make coverage-report` ran in the runtime image. Counts and durations come from `ingestion_state.extract_coverage`, `lakehouse_state.bronze_refreshes`, the Airflow `task_instance` table, and Trino. Silver `postgres_oltp` before the first step: sellers 6, customers 2, orders 2, order items 2.

| Step | Result |
| --- | --- |
| Empty ledger | `make coverage-report FREQUENCY=daily`: `expected_windows=0`, clean, exit 0 |
| (a) `backfill create` daily `--from-date 2026-11-20 --to-date 2026-11-20 --reprocess-behavior completed` | The existing `scheduled__2026-11-21T00:00:00+00:00` run was cleared and re-run under the same run id (extract 1.1 s). Four ledger rows for `[2026-11-20, 2026-11-21)`, 0 rows each. The transform promoted 0 objects (same key, same ETag) |
| Gap report after (a) | `gaps=1`: `gap [2026-11-19,2026-11-20) missing=sellers,customers,orders,order_items` (that window ran before the ledger existed). Exit 1 |
| (b) `backfill create` daily `--from-date 2026-11-14 --to-date 2026-11-19 --reprocess-behavior completed` | Six runs (five new, one reprocessed) for `[2026-11-14, 2026-11-20)`. All six started within 60 ms of each other: the backfill does not inherit `max_active_runs=1`. Extracts 1.1 to 1.2 s |
| Refreshes for (b) | One `transform_lakehouse` run, 18:19:58 → 18:21:45 (107 s): Bronze promoted 6 objects per dataset and appended 0 rows (empty windows); Silver 54.8 s, `dbt_build` 30.4 s. Six windows, one refresh |
| Gap report after (b) | Daily clean, exit 0: 2 expected windows, 0 gaps, 0 overlaps, 5 windows before start |
| (d) `backfill create` weekly `--from-date 2026-11-09 --to-date 2026-11-09 --max-active-runs 1` | `backfill__2026-11-16T00:00:00+00:00`, window `[2026-11-08, 2026-11-15)` = `[L-1, L+6)`, sellers 3. It found the collision below |
| Repair: daily 2026-11-15 and weekly 2026-11-09 with `--reprocess-behavior completed` after the execution id fix | Execution ids `ingest_postgres_oltp_daily__backfill__2026-11-16T00:00:00+00:00` and `ingest_postgres_oltp_weekly__backfill__…`. Two events, two refreshes: the weekly event arrived while the first refresh ran and started one follow-up. Bronze sellers 20 → 23 (the 3 weekly rows). Silver sellers stayed 6, the 3 `p3-05-late-` sellers once each |
| (c) `backfill create` daily `--from-date 2026-11-19 --to-date 2026-11-20 --reprocess-behavior completed --max-active-runs 1` | Both covered windows re-run in order under new execution ids (`<dag_id>__<run_id>`). Two refreshes: the first listed before the second extract had finished `order_items`; the follow-up promoted that object. Bronze appended 0 rows |
| After (c) | Silver unchanged: sellers 6, customers 2, orders 2, order items 2. Daily report: 0 gaps, 0 overlaps, `rerun [2026-11-19,2026-11-20)` and `rerun [2026-11-20,2026-11-21)`, clean, exit 0. Weekly report: 0 gaps, 1 window before start, clean |

### Execution id collision found by P3-06

Until this issue, the PostgreSQL DAGs used `params.execution_id or run_id`. Airflow run ids are unique per DAG, not across DAGs. The daily backfill for logical date 2026-11-15 and the weekly backfill for 2026-11-09 were both `backfill__2026-11-16T00:00:00+00:00`, and both ran on ingestion date 2026-11-21. The weekly extract wrote into the daily prefix `raw/postgres_oltp/<dataset>/dt=2026-11-21/execution_id=backfill__2026-11-16T00:00:00+00:00/`. It cleared the daily `_COMPLETE` marker and wrote one that listed only its own object. Bronze treats a prefix whose objects differ from its marker as incomplete, so the 3 weekly sellers were not promoted, while the task ended `success`. The weekly ledger rows also overwrote the daily rows of that execution id. The same collision happens on the scheduled path every Monday 00:00Z: both runs are `scheduled__<W>T00:00:00+00:00`. The first would have been 2026-11-23.

The fix is `POSTGRES_EXECUTION_ID = "{{ params.execution_id or (dag.dag_id ~ '__' ~ run_id) }}"` in `ingest_common.py`, used only by `postgres_extract_task`. The other ingestion DAGs each own one source and keep `run_id`. A DagBag test renders the daily and weekly extract at the same Monday instant and asserts distinct execution ids. The stray prefix from 2026-11-21 stays in raw storage: Bronze ignores it as incomplete, and its daily object was already promoted on 2026-11-21 18:19Z.

Unit checks: `uv run pytest tests/unit` and `make lint` passed (counts in [`docs/status-log.md`](../status-log.md)).

## Verification (2026-11-22, P3-16)

`scripts/p3_16_gate_check.sh` ran on the local stack (PostgreSQL, MinIO, Trino, runtime image with the branch source mounted) on 2026-11-22 03:17Z. Each step is the `medallion_build` CLI that the DAG tasks call.

| Run | Step and result | Seconds |
| --- | --- | --- |
| Setup | Extract on the old schema: exit 1, `oltp tables without the set_updated_at trigger: sellers, customers, orders, order_items` | 3 |
| Setup | `apply-schema`: 4 triggers. Manual extract `[2026-10-19, 2026-10-20)`: 4 objects | 1, 3 |
| r1 | `bronze`: 8 objects promoted, `orders` and `order_items` written (9 rows each). Then `orders.expected_rows` was raised from 9 to 10 in `layer_checks` (the planted failure) | 30 |
| r1 | `bronze-checks`: exit 1. `customers`, `order_items`, `sellers` passed; `orders` failed | 4 |
| r2 | `bronze`: no new raw object, exit 0, "downstream work is pending" | 2 |
| r2 | `bronze-checks`: ran the open check again, exit 1, `orders` failed | 2 |
| r2 | `silver`: exit 1, `silver blocked: bronze check not passed for postgres_oltp.orders (failed)`; no table written | 2 |
| r2 | `dbt-build`: Gold not pending, no model built | 2 |
| r3 | Planted value restored to 9. `bronze-checks`: `orders` passed | 2 |
| r3 | `silver`: 4 tables. `silver-checks`: 4 passed. `stage-quarantine`, then `dbt-build` PASS=287 | 54, 3, 15, 41 |
| r4 | `bronze`: nothing new, nothing pending, skipped. `bronze-checks`: no check open, skipped | 3, 2 |

After r2, Silver was still pending for all four `postgres_oltp` datasets and Gold was not pending. After r3, `pending_layers` was empty and all eight checks were `passed`. The r1 `customers` and `sellers` objects had no new rows (`rows_appended=0`) but were registered, so their checks opened and passed.

The extract changes were tested in `tests/integration/ingestion/test_postgres_oltp_ingest.py` against the scratch database:
- An `UPDATE` that leaves `updated_at` out gets a time at or after the statement.
- An explicit value is kept.
- A commit from another session made between the `orders` and `order_items` reads does not show in the extracted `order_items`.
- A dropped trigger fails the extract.

The existing round-trip test now sets `updated_at` explicitly in its rewrite, because the trigger would otherwise move the row out of its window.

Not verified on a live scheduler: the `settle` sensor and the rendered `--ingestion-ts` on an actual retry. The DagBag tests render the vector and check the sensor settings. Run behavior on a live scheduler is P3-09.
