# Local Airflow

P3-01 runs Apache Airflow on the Compose `orchestration` profile. `docker compose up` still starts only PostgreSQL and MinIO.

| | |
| --- | --- |
| Image | `apache/airflow:3.4.0-python3.12` |
| Executor | `LocalExecutor` |
| Metadata database | `airflow` on the Compose PostgreSQL 16. Not `odp` and not `iceberg` |
| UI | `http://localhost:8081` (`AIRFLOW_PORT`) |
| Task logs | `s3://odp-airflow-logs/task-logs` on the Compose MinIO |

The host port is 8081 because Trino already publishes 8080. Inside the network the API server listens on 8080.

Services: `airflow-db-create`, `airflow-init`, `airflow-apiserver`, `airflow-scheduler`, and `airflow-dag-processor`. There is no Redis, Celery worker, Flower, or triggerer. Celery and Flower are not a later issue. The revisit condition is a written requirement for self-hosted multi-node workers. Amazon MWAA stays the optional AWS counterpart and is not built here.

Daily schedules, weekly schedules, backfill, and DAGs that call the package are later Phase 3 issues. The only DAG in this issue is `platform_health`: one `BashOperator` that prints `platform_health ready`, no schedule, paused until someone triggers it. `EmptyOperator` is not used. Airflow 3.4 evaluates that operator in the scheduler and does not send it to the executor, so it never writes a task log.

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

A finished task writes its log to MinIO. The Airflow containers also keep a spool on the `odp_airflow_logs` volume until the upload completes.

The MinIO object remains after the three Airflow services stop and after a host reboot, as long as the MinIO volume `odp_minio_data` remains. `docker compose down -v` deletes that volume and the logs with it. A task killed before the upload finishes can lose that attempt.

The connection id is `minio_logs`. The endpoint inside Compose is `http://minio:9000`. Credentials are the existing MinIO root user and password, passed through the container environment. They are not command arguments.

## Stop

```bash
docker compose --profile orchestration stop airflow-apiserver airflow-scheduler airflow-dag-processor
```

Name the services. `docker compose --profile orchestration stop` with no service names also stops PostgreSQL and MinIO, because those two are not on a profile. It does not remove volumes. Spark and Trino stay up; they are on the `lakehouse` profile.

The health payload reports `triggerer` as `down`. This profile does not run a triggerer. `metadatabase`, `scheduler`, and `dag_processor` are the components that should be `healthy`.

Airflow skips masking for a secret shorter than 5 characters. The local placeholder `odp` is shorter, so each CLI call prints that warning. The metadata-database URL in the migrate log is still redacted.

## Verification (2026-11-12)

`make airflow-up` brought `airflow-apiserver`, `airflow-scheduler`, and `airflow-dag-processor` to healthy. `airflow version` printed `3.4.0`. `docker compose config --services` without the profile listed `minio` and `postgres`.

`GET /api/v2/monitor/health` reported `metadatabase`, `scheduler`, and `dag_processor` as `healthy`. `triggerer` was `down`.

`airflow dags list` showed `platform_health`. A manual run `manual__2026-11-12T15:04:09.043021+00:00` finished `ready` as `success` (pid 303, duration 3.704131 seconds). MinIO then held one object:

`task-logs/dag_id=platform_health/run_id=manual__2026-11-12T15:04:09.043021+00:00/task_id=ready/attempt=1.log`

That object was 5502 bytes and contained the fixed line. It did not contain the MinIO password, the Fernet key, or the JWT secret. After `docker compose --profile orchestration stop airflow-apiserver airflow-scheduler airflow-dag-processor`, MinIO still listed that one object. PostgreSQL and MinIO stayed up.

A second `airflow-init` printed `Database migration done!` and `airflow already exists in the db`. The UI login page loaded at `http://localhost:8081/auth/login/`.

An earlier `docker compose --profile orchestration stop` with no service names also stopped PostgreSQL and MinIO. Spark and Trino stayed up. PostgreSQL and MinIO were started again before the log check above. The object was still in the bucket after that MinIO restart.
