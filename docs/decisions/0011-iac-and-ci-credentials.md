# ADR-0011: IaC and CI credentials

| Field | Value |
| --- | --- |
| Status | `proposed` |
| Date opened | 2026-10-15 |
| Date decided | — |
| Owner | Platform architect |
| Index ID | D-011 |
| Related issues | P0-12, P6-03, P6-06, P7-02–P7-05 |
| Supersedes | — |
| Superseded by | — |

## Context / problem

Long-lived access keys in the repository are a security risk. IaC must be reviewable in CI without applying infrastructure by default.

## Decision

Use **Terraform** for the AWS path, with **GitHub Actions** running `terraform fmt -check` and `validate` (no apply in normal CI). Prefer **OIDC** for short-lived AWS credentials over static keys. Never commit secrets; use `.env.example` for names and placeholders only.

## Alternatives considered

| Option | Pros | Cons | Why not (if rejected) |
| --- | --- | --- | --- |
| Terraform + GHA fmt/validate + OIDC (chosen) | Reviewable IaC; short-lived creds | Apply still needs human approval | — |
| Long-lived IAM user keys in CI | Easy setup | Security anti-pattern | Rejected |
| Apply on every main push | Always “live” | Cost and blast radius | Rejected — approval-gated apply |
| CloudFormation only | Native AWS | Less portable across tooling for this program | Rejected as primary |

## Consequences and trade-offs

- Phase 7 emphasizes readable modules and least-privilege IAM.
- Ask before any operation with meaningful cloud cost.

## Evidence / validation

None yet — CI workflows and Terraform tree in later phases.

## Revisit when

Org policy requires a different OIDC/IAM pattern; adapt modules and docs together.
