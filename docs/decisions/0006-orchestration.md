# ADR-0006: Orchestration

| Field | Value |
| --- | --- |
| Status | `proposed` |
| Date opened | 2026-10-15 |
| Date decided | — |
| Owner | Platform architect |
| Index ID | D-006 |
| Related issues | Phase 3 (`P3-01`–`P3-11`), Phase 7 AWS counterparts |
| Supersedes | — |
| Superseded by | — |

## Context / problem

Validated batch flows need scheduling, retries, dependencies, backfills and clear failure behavior. Business logic must stay out of DAG bodies. Compute/transform tooling is decided in [ADR-0005](0005-compute-and-transform.md).

## Decision

Use **Apache Airflow (OSS)** in local Docker Compose for orchestration. Document **Amazon MWAA** and **AWS Step Functions** as optional AWS counterparts for the IaC case study—not required core modules in Phase 7.

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
| Business SQL/logic inside Airflow operators | Familiar anti-pattern | Untestable; duplicates logic | Rejected by project principles |

## Consequences and trade-offs

- Daily and weekly schedules (D-007) are Airflow concerns over a shared extract.
- Phase 7 may document MWAA/Step Functions without implementing them as required modules.

## Evidence / validation

None yet — Phase 3.

## Revisit when

Local Airflow proves too heavy for available hardware; re-evaluate managed or lighter orchestrator.
