# ADR-0006: Orchestration

| Field | Value |
| --- | --- |
| Status | `approved` |
| Date opened | 2026-10-15 |
| Date decided | 2026-11-12 |
| Owner | Platform architect |
| Index ID | D-006 |
| Related issues | Phase 3 (`P3-01`–`P3-12`), Phase 7 AWS counterparts |
| Supersedes | — |
| Superseded by | — |

## Context / problem

Validated batch flows need scheduling, retries, dependencies, backfills and clear failure behavior. Business logic must stay out of DAG bodies. Compute/transform tooling is decided in [ADR-0005](0005-compute-and-transform.md).

## Decision

Use **Apache Airflow (OSS)** in local Docker Compose for orchestration. Document **Amazon MWAA** and **AWS Step Functions** as optional AWS counterparts for the IaC case study—not required core modules in Phase 7.

The local topology is one pinned image and **LocalExecutor**. P3-01 used `apache/airflow:3.4.0-python3.12` directly. Since P3-03 the services run `odp-runtime:airflow3.4.0-spark3.5.8`, built on that image with Spark and dbt added ([ADR-0005](0005-compute-and-transform.md) addendum). The long-running processes are the API server, the scheduler, and the dag processor, on the Compose profile `orchestration`. The metadata database is `airflow` on the existing PostgreSQL 16. Finished task logs are objects in the MinIO bucket `odp-airflow-logs`.

CeleryExecutor, Redis, and Flower are not part of this platform and are not a planned follow-up. DAG tasks stay thin wrappers. Compute stays in Spark, Trino, and dbt. A Celery worker on the same Compose host does not add a machine. Flower would duplicate the Airflow UI and the Phase 5 Grafana role. Redis after 7.2 is not OSI-licensed.

## Alternatives considered

| Option | Pros | Cons | Why not (if rejected) |
| --- | --- | --- | --- |
| Apache Airflow OSS in Compose (chosen) | Industry standard; retries/backfills; thin DAG pattern fits principles | Compose resource use | — |
| Dagster | Strong software-defined assets | Different mental model; team Airflow familiarity preferred here | Deferred |
| Prefect | Modern Python workflows | Less common lakehouse teaching path in this program | Deferred |
| Mage | Simple notebook-like UX | Less fit for multi-DAG batch + CDC coexistence demos | Rejected |
| Amazon MWAA | Managed Airflow on AWS | Cost; not needed for local verification | Document as AWS counterpart |
| AWS Step Functions | Serverless orchestration | Different paradigm than Airflow DAGs; weaker local parity | Document as optional AWS pattern |
| cron / Make only | Minimal | No dependency graph, backfill UX, or failure visibility | Rejected for Phase 3 goals |
| CeleryExecutor + Flower | Separate worker process and a queue UI | Redis 7.2 license constraint; extra processes on the same host; Flower duplicates the Airflow UI and Phase 5 Grafana | Not a planned follow-up. Revisit only for a written multi-node worker requirement |
| Business SQL/logic inside Airflow operators | Familiar anti-pattern | Untestable; duplicates logic | Rejected by project principles |

## Consequences and trade-offs

- Daily and weekly schedules (D-007) are Airflow concerns over a shared extract. Those schedules are not part of this approval.
- Phase 7 may document MWAA/Step Functions without implementing them as required modules.
- `docker compose up` does not start Airflow. The profile is `orchestration`.
- A profile-wide `stop` with no service names also stops PostgreSQL and MinIO. Stop the three Airflow services by name.
- The health payload reports `triggerer` as `down` because this profile does not run one.

## Evidence / validation

2026-11-12 local run, recorded in [Local Airflow](../architecture/airflow-local.md):

- Image `apache/airflow:3.4.0-python3.12`. `airflow version` printed `3.4.0`.
- `metadatabase`, `scheduler`, and `dag_processor` healthy. `triggerer` down.
- Manual `platform_health` run `manual__2026-11-12T15:04:09.043021+00:00`, task `ready`, state `success`.
- One MinIO object under `task-logs/…/task_id=ready/attempt=1.log` (5502 bytes). The object was still listed after the three Airflow services stopped.
- A second `airflow-init` printed `Database migration done!` and `airflow already exists in the db`.
- UI login page at `http://localhost:8081/auth/login/`.

## Revisit when

Local Airflow proves too heavy for available hardware; re-evaluate a lighter orchestrator. Or a written requirement asks for self-hosted multi-node Airflow workers. That requirement is not in Phases 3–9.
