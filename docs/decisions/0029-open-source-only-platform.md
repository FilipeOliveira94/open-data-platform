# ADR-0029: Open-source-only platform; no cloud deployment path

| Field | Value |
| --- | --- |
| Status | `approved` |
| Date opened | 2026-11-27 |
| Date decided | 2026-11-27 |
| Owner | Filipe (project owner) |
| Index ID | D-029 |
| Related issues | R1-01; withdrawn: Phase 7 (`P7-01`–`P7-09`), P6-03; rescoped: P5-05 |
| Supersedes | [ADR-0011](0011-iac-and-ci-credentials.md); the AWS half of [ADR-0001](0001-runtime-compose-local-aws-iac.md) |
| Superseded by | — |

## Context / problem

ADR-0001 paired the local Docker Compose runtime with an AWS target delivered as Terraform in Phase 7, and ADR-0011 set the Terraform and OIDC rules for it. No Terraform was ever written. Every verified result in the repository comes from the local open-source stack. Keeping the AWS path in the plan kept an unbuilt cloud mapping in the ADRs, the roadmap, the README, and the site, and made the FinOps work a cost model for infrastructure the project does not run.

## Decision

The platform core is open source only and runs on the local Docker Compose stack. There is no cloud deployment path: Phase 7 and its issues are withdrawn, P6-03 (Terraform CI) is withdrawn, and the ADRs no longer name AWS counterparts. CI stays on GitHub Actions and needs no cloud credentials. FinOps becomes the measured local resource footprint of the stack, with a machine cost to run and a performance analysis built on those measurements (P5-05).

The S3 API stays: MinIO is S3-compatible, and the S3 client libraries (`hadoop-aws`, `aws-java-sdk-bundle`, `fs.s3a.*`, Trino `s3.*`, boto3, the Airflow `aws` connection type) are how the platform talks to MinIO. They are not an AWS dependency.

## Alternatives considered

| Option | Pros | Cons | Why not (if rejected) |
| --- | --- | --- | --- |
| Open source only, local footprint instead of cloud cost (chosen) | Every claim stays measurable on the local stack; no cloud credentials or spend | No documented cloud provisioning path | — |
| Keep the AWS path as Terraform (ADR-0001, ADR-0011) | Shows a cloud mapping | Unbuilt and unverified; maintains docs for services the project does not run | Rejected by the owner, 2026-11-27 |
| Swap AWS for another cloud | Same mapping exercise | Same objection | Rejected |

## Consequences and trade-offs

- Phase numbers stay; Phase 7 is marked Withdrawn so P8–P11 IDs do not change.
- Phase 9 no longer depends on Phase 7.
- Cost figures are local: measured resource use plus stated hardware and electricity assumptions, labeled as estimates. They are not a cloud bill.
- Historical records (`docs/status-log.md`, `docs/project-genesis.md`, `docs/reviews/`, changelogs, and the text of verified Phase 0–1 issues) keep their AWS mentions as history.

## Evidence / validation

- 2026-11-27, R1-01: the roadmap, `AGENTS.md`, `DECISIONS.md`, ADRs, architecture docs, README, and site were updated. A `git grep` for AWS, Terraform, Athena, Glue, OIDC, QuickSight, MWAA, EMR, MSK, CloudWatch, DynamoDB, FinOps, cost model, and Phase 7 terms, run outside the historical records and with the S3 client names filtered out, returns only the withdrawn Phase 7 and P6-03 rows, the R1 table, and changelog or verified-issue history. The command and its result are in the R1-01 PR and in `docs/status-log.md`.

## Revisit when

The owner asks for a cloud deployment path, or a requirement needs a service the local stack cannot run.
