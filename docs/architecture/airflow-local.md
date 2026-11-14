# Local Airflow

P3-01 runs Apache Airflow on the Compose `orchestration` profile. `docker compose up` still starts only PostgreSQL and MinIO.

| | |
| --- | --- |
| Image | `odp-runtime:airflow3.4.0-spark3.5.8`, built from `docker/runtime` on `apache/airflow:3.4.0-python3.12` (P3-03). P3-01 and P3-02 ran the upstream image directly |
| Executor | `LocalExecutor` |
| Metadata database | `airflow` on the Compose PostgreSQL 16. Not `odp` and not `iceberg` |
| UI | `http://localhost:8081` (`AIRFLOW_PORT`) |
| Task logs | `s3://odp-airflow-logs/task-logs` on the Compose MinIO |

The host port is 8081 because Trino already publishes 8080. Inside the network the API server listens on 8080.

Services: `airflow-db-create`, `airflow-init`, `airflow-apiserver`, `airflow-scheduler`, and `airflow-dag-processor`. There is no Redis, Celery worker, Flower, or triggerer. Celery and Flower are not a later issue. The revisit condition is a written requirement for self-hosted multi-node workers. Amazon MWAA stays the optional AWS counterpart and is not built here.

`platform_health` is the P3-01 smoke DAG: one `BashOperator` that prints `platform_health ready`, with no schedule. `EmptyOperator` is not used. Airflow 3.4 evaluates that operator in the scheduler and does not send it to the executor, so it never writes a task log.

P3-02 adds the ingestion wrappers. They have no schedule. P3-03 adds `transform_lakehouse` (no schedule). P3-04 and P3-05 set the daily and weekly schedules on the PostgreSQL extract. Phase 4 sets the CDC schedule. Backfill DAGs stay a later Phase 3 issue.

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

Each ingestion task runs `python` with an argument vector and `cwd=/opt/odp/repo`. There is no shell. Retries are 2, the delay is 2 minutes, and the execution timeout is 30 minutes. `schedule` is `None` and `catchup` is false. `airflow/dags/ingest_common.py` holds the task defaults and is listed in `.airflowignore`.

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

`ingest_postgres_oltp` passes `window_start` and `window_end` through. It does not calculate a daily or weekly window. `seed_postgres_oltp` truncates `oltp.sellers`, `oltp.customers`, `oltp.orders`, and `oltp.order_items`, then loads `tests/fixtures/postgres_oltp/synthetic.json`. It is not upstream of the extract DAG.

`execution_id` defaults to the Airflow `run_id`. A new run keeps the previous raw object. A trigger conf that sets `execution_id` overwrites the keys of that id.

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

`python -m open_data_platform.lakehouse.medallion_build <step> --run-label <label>` is the one entrypoint. Make passes `--run-label make`. Airflow passes the run id. Rendered SQL, the Bronze raw counts, and dbt `target/` and logs go to the named volume `odp_lakehouse_work` at `/opt/odp/work/<label>`.

| Step | What it calls |
| --- | --- |
| `bronze` | Creates the catalog database if missing, `lakehouse.bronze --all`, then `run_bronze.sh` once per SQL file |
| `bronze-checks` | `bronze_count_checks` on the counts the `bronze` step wrote, through Trino |
| `silver` | `lakehouse.silver --all`, then `run_silver.sh` once per SQL file |
| `silver-checks` | `silver.live_checks()` through Trino |
| `stage-quarantine` | `lakehouse.quarantine_stage`, then `run_bronze.sh` on that SQL |
| `dbt-parse` / `dbt-build` / `dbt-reconcile` | `dbt parse`, `dbt build --exclude tag:reconciliation`, `dbt test --select tag:reconciliation` |

`medallion_build` runs each process with an argument vector and no shell. Credentials stay in the container environment. Trino checks use the HTTP protocol at `ODP_TRINO_HOST:ODP_TRINO_PORT` (`trino:8080`). A mismatch prints the statement, the expected value, and the result, then fails the step.

`spark-sql` writes `derby.log` and `metastore_db` to its working directory. The runtime user cannot write the mounted repository, so each Spark script changes to `${TMPDIR:-/tmp}` first.

`tests/unit/lakehouse/test_medallion_build.py` covers the steps without Compose.

### DAGs

| DAG | Schedule | Tasks |
| --- | --- | --- |
| `transform_lakehouse` | None | `bronze` → `bronze_checks` → `silver` → `silver_checks` → `stage_quarantine` → `dbt_build` |

The tasks use the P3-02 `DEFAULT_ARGS` from `ingest_common.py`: 2 retries, a 2-minute delay, and a 30-minute execution timeout. The DAG sets `max_active_runs=1` and `catchup=False`. `transform_lakehouse` is linear. All Spark steps share one Ivy cache and one container, so two Spark steps do not run at the same time. Quarantine staging does not read Bronze or Silver. It runs after `silver_checks` only for that reason.

Trino (profile `lakehouse`) must be up for the check steps and dbt:

```bash
docker compose --profile lakehouse up -d --wait trino
docker compose --profile orchestration exec -T airflow-scheduler \
  airflow dags trigger transform_lakehouse
```

`tests/unit/infrastructure/test_dagbag.py` loads `airflow/dags` with the pinned Airflow in a `DagBag`. It checks zero import errors, the nine DAG ids, the task order, the shared retries and timeout, and the entrypoint of each task. Success, retry, and failed-run behavior on a live scheduler is P3-09.

