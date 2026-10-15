# ADR-0005: Compute and transform

| Field | Value |
| --- | --- |
| Status | `proposed` |
| Date opened | 2026-10-15 |
| Date decided | — |
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

None yet — introduced per phase, not in Phase 0.

## Revisit when

PoC shows Spark unfit for the chosen table format; or analytics layer needs a different tool.
