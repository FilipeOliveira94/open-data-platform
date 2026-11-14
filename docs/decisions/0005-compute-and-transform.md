# ADR-0005: Compute and transform

| Field | Value |
| --- | --- |
| Status | `approved` |
| Date opened | 2026-10-15 |
| Date decided | 2026-11-06 |
| Owner | Platform architect |
| Index ID | D-005 |
| Related issues | Phases 2–4 |
| Supersedes | — |
| Superseded by | — |

## Context / problem

The platform needs scalable batch (and later streaming) processing plus analytics engineering with tests—without burying business logic in orchestration or IaC. Orchestration is a separate decision ([ADR-0006](0006-orchestration.md)).

## Decision

Use **Apache Spark** for heavy engineering transforms (batch and later Structured Streaming). Use **dbt** for Gold/analytical modeling and model tests. Keep reusable logic in the Python package and dbt project.

## Alternatives considered

| Option | Pros | Cons | Why not (if rejected) |
| --- | --- | --- | --- |
| Spark + dbt (chosen) | Clear split: engineering vs analytics models/tests | Two toolchains to pin and document | — |
| Spark as sole transform layer (no dbt) | One engine | Weaker analytics engineering / model-test story | Rejected as sole approach |
| dbt-only transforms | Simple SQL models | Weak for large/raw engineering and CDC processing | Insufficient alone |
| Apache Flink | Strong streaming | Extra ops surface for this scope | Deferred |
| SQL-engine transforms (Trino/Athena CTAS) | Fewer moving parts | Limited for heavy Spark-style jobs and CDC sinks | Possible for some Gold steps only |
| Python-only (pandas/polars) for all scales | Simple locally | Does not scale or map cleanly to lakehouse engines | Rejected as primary |

## Consequences and trade-offs

- Compatibility work concentrates on Spark + lakehouse stack (D-003, D-004).
- DAGs must call package/dbt entrypoints, not reimplement transforms.

## Evidence / validation

P2-06 (2026-11-06): Gold models run with **dbt-trino** `1.11.0` (dbt Core `1.12.6`) against Trino `484` on the Iceberg JDBC catalog. Spark remains the Bronze/Silver writer. `dbt build` created 55 Gold tables. Detail: [`docs/architecture/gold-layer.md`](../architecture/gold-layer.md).

## Addendum (2026-11-13, P3-03): one runtime image

**Status:** evaluated during P3-03 and accepted in this form. Approval is recorded here and in `DECISIONS.md` after the transform DAGs run on this runtime. Evaluated because Airflow had to call Spark and dbt. The decision is unchanged: Spark writes Bronze and Silver, dbt-trino builds Gold. This addendum changes where they run.

**Previous implementation.** Make ran each Spark step in a Compose `spark` service on `spark:3.5.8-java17`, one `docker compose run` per SQL file, after the host rendered the SQL with `uv run python`. dbt-trino was a host dev dependency and `make dbt-build` ran `uv run dbt` against `localhost:8080`. The Airflow image had no Java, Spark, dbt, or Docker access, so a DAG could not call either tool without a second install or the Docker socket.

**New implementation.** `docker/runtime/Dockerfile` builds `odp-runtime:airflow3.4.0-spark3.5.8` on `apache/airflow:3.4.0-python3.12`. It copies `/opt/spark` (Spark `3.5.8`) and the Temurin `17.0.20` JRE from the digest-pinned `spark:3.5.8-java17` image. That image is a build stage only. dbt Core `1.12.6` and dbt-trino `1.11.0` sit in the venv `/opt/dbt-venv`, installed with hashes exported from the `dbt` group of `uv.lock`. The host no longer installs dbt. The Airflow services and the lakehouse `spark` service run this one image. Make and the DAGs call the same entrypoint, `python -m open_data_platform.lakehouse.medallion_build`, with the same Spark scripts and dbt project.

**Why.** One Spark and one dbt install for the whole project, so Make and Airflow cannot drift. No Docker socket in Airflow, which would give the scheduler root-equivalent access to the host. No Spark Thrift or Connect server, which would add a long-running service. The Spark bits are the ones Phase 2 validated.

**Cost.** The image is larger. `docker image inspect` reported 4_839_671_587 bytes for the runtime image and 3_247_536_551 bytes for `apache/airflow:3.4.0-python3.12` on 2026-11-13. Inside the image, `/opt/spark` is 350 MiB and the JRE 269 MiB (`du -sh`). Spark `local[*]` runs in the scheduler container and shares its CPU and memory with Airflow. The Spark and dbt pins now live in the Dockerfile and `uv.lock` instead of a Compose image tag and the host venv.

## Revisit when

PoC shows Spark unfit for the chosen table format; or analytics layer needs a different tool.
