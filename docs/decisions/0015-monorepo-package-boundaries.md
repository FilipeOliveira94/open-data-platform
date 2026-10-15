# ADR-0015: Monorepo and package boundaries

| Field | Value |
| --- | --- |
| Status | `proposed` |
| Date opened | 2026-10-15 |
| Date decided | — |
| Owner | Platform architect |
| Index ID | D-015 |
| Related issues | Cross-cutting; Phase 0+ |
| Supersedes | — |
| Superseded by | — |

## Context / problem

If business logic lives in Airflow DAGs or Terraform, it becomes hard to test, reuse across local/AWS, and review. The repository layout must enforce boundaries.

## Decision

Use a **GitHub monorepo**. Put reusable business and ingestion logic in `src/open_data_platform/` (and dbt models under `dbt/`). Keep Airflow DAGs and Terraform as **thin wrappers** over package entrypoints and modules. Environment-specific configuration stays outside business logic (env vars / config files).

## Alternatives considered

| Option | Pros | Cons | Why not (if rejected) |
| --- | --- | --- | --- |
| Monorepo + package-owned logic (chosen) | Testable; shared local/AWS logic; clear reviews | Requires discipline in DAG/IaC reviews | — |
| Logic inside DAGs | Fast to hack | Untestable; duplicated across envs | Rejected |
| Polyrepo per component | Independent versioning | Coordination overhead for this program size | Rejected |
| Logic in Terraform / provisioners | Infra-centric | Wrong layer; poor data testing | Rejected |

## Consequences and trade-offs

- Do not create empty later-phase directories until those phases start.
- CI should lint/test the package independently of cloud apply.

## Evidence / validation

None yet — package lands in P0-08; boundaries enforced as components appear.

## Revisit when

A component truly needs isolation (e.g. separate release cadence); consider a documented exception, not silent drift.
