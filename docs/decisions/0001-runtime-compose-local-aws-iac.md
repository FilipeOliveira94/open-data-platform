# ADR-0001: Runtime model (Compose local · AWS as IaC)

| Field | Value |
| --- | --- |
| Status | `approved` |
| Date opened | 2026-10-15 |
| Date decided | 2026-10-16 |
| Owner | Platform architect |
| Index ID | D-001 |
| Related issues | P0-13, Phase 7 (`P7-01`–`P7-07`) |
| Supersedes | — |
| Superseded by | — |

## Context / problem

The platform must be reproducible on a laptop (Windows 11 + WSL2 + Docker Desktop) and still provide a credible cloud deployment path. Always-on AWS resources are not required for day-to-day work; the verified local lakehouse maps to AWS services as code.

## Decision

Use **Docker Compose** as the environment for development and end-to-end verification. Deliver the **AWS target architecture as Terraform Infrastructure as Code** in Phase 7 (service mapping, modules, IAM/OIDC, cost model, case study). CI validates Terraform with `fmt` / `validate`; `terraform apply` / `destroy` remain approval-gated.

Phase 0 Compose scope is limited to **PostgreSQL 16** and **MinIO** (see ADR-0002). Later phases add services to the same Compose model when those phases start.

## Alternatives considered

| Option | Pros | Cons | Why not (if rejected) |
| --- | --- | --- | --- |
| Docker Compose + Terraform AWS IaC (chosen) | Local parity; reviewable IaC; low default cost | Must maintain local↔AWS mapping docs | — |
| Docker Swarm | Familiar Docker UX; simple multi-node | Extra ops vs single-host Compose; weak fit for this laptop-first scope | Rejected for local/dev path |
| Kubernetes (kind / k3d / EKS) | Common enterprise pattern | High ops cost; not required for data-pipeline semantics demos | Rejected unless a later requirement appears |
| Podman Compose | Daemonless; rootless-friendly | Team familiarity and Docker Desktop/WSL path already chosen | Deferred |
| Always-on AWS sandbox | Production-like demos | Cost, credentials, slower iteration | Rejected as default |
| Compose only (no AWS IaC path) | Simplest ops | No documented cloud provisioning path | Rejected as sole model |

## Consequences and trade-offs

- Local work stays independent of AWS credentials whenever possible.
- Case study must document local→AWS mapping (MinIO→S3, catalog→Glue, Trino→Athena).
- Cloud readiness is demonstrated by IaC quality and mapping clarity, not by always-on spend.

## Evidence / validation

- `docker-compose.yml` defines pinned PostgreSQL 16 and MinIO services with health checks (P0-10).
- `README.md` and `Makefile` document start/stop and verification commands (P0-11).
- Runtime (2026-10-16): `docker compose up -d` → `odp-postgres` and `odp-minio` **healthy**; `pg_isready -U odp -d odp` accepting connections; MinIO put/get smoke passed.

## Revisit when

Compose resource limits block Phase 4/5 stacks; or a project requirement mandates a different cloud or runtime.

## Notes

Kubernetes non-adoption is recorded here rather than as a standalone ADR.
