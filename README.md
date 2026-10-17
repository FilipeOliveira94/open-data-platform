# Open Data Platform

Reproducible open-source data platform: local development on Windows 11 + WSL2 + Docker Compose, with an AWS deployment path expressed later as Terraform Infrastructure as Code (Phase 7).

**Phase 0** (foundation) is complete: Python 3.12 toolchain, PostgreSQL 16, MinIO, lint/tests, and CI. Next work is Phase 1 ingestion.

## Prerequisites

- WSL2 Ubuntu (or Linux) with Docker Desktop and WSL integration enabled
- [uv](https://docs.astral.sh/uv/) 0.13.1 (pinned in `pyproject.toml` / CI; install matching version)
- Docker Compose v2 (`docker compose version`)
- GNU Make (optional; targets mirror the commands below)

## Quick start

```bash
# 1) Toolchain
uv sync

# 2) Optional local overrides (never commit .env)
cp .env.example .env

# 3) Lint and unit tests (no services required)
uv run ruff check src tests
uv run pytest tests/unit
# or: make lint && make test

# 4) Local services
docker compose up -d
docker compose ps
# or: make up

# 5) PostgreSQL reachability
docker compose exec postgres pg_isready -U odp -d odp

# 6) MinIO write/read smoke
set -a && source .env.example && set +a
uv run pytest tests/integration/test_minio_smoke.py
# or: make test-minio

# 7) Stop services (volumes retained)
docker compose down
# or: make down
```

## Make targets

| Target | Purpose |
| --- | --- |
| `make setup` / `make sync` | Install package and locked deps via `uv` |
| `make lint` | Ruff check |
| `make test` | Unit tests only |
| `make up` / `make down` | Start/stop Compose stack |
| `make test-minio` | Live MinIO put/get smoke |

## Local services (Phase 0)

| Service | Image pin | Ports (default) |
| --- | --- | --- |
| PostgreSQL 16 | `postgres:16.15-bookworm` | `5432` |
| MinIO | `coollabsio/minio:RELEASE.2025-10-15T17-29-55Z` | API `9000`, console `9001` |

**MinIO image note:** MinIO Inc. removed official public container images in favor of commercial **AIStor**. The community MinIO server remains **AGPLv3**, so community-built images are acceptable for local use. Phase 0 pins [`coollabsio/minio`](https://hub.docker.com/r/coollabsio/minio) at upstream release `RELEASE.2025-10-15T17-29-55Z` (see ADR-0002).

Default credentials are placeholders in `.env.example` for local use only.

## CI

GitHub Actions (`.github/workflows/ci.yml`) runs the same lint and unit-test commands on Python 3.12. It does **not** deploy cloud resources or start Compose.

## Documentation

| Document | Role |
| --- | --- |
| [`AGENTS.md`](AGENTS.md) | Agent and contributor operating contract |
| [`ROADMAP.md`](ROADMAP.md) | Issue-level scope and acceptance criteria |
| [`PROJECT_STATUS.md`](PROJECT_STATUS.md) | Delivered vs verified ledger |
| [`DECISIONS.md`](DECISIONS.md) | Architecture decision index |
| [`docs/decisions/`](docs/decisions/) | ADR detail |

## Out of scope for Phase 0

Airflow, Kafka, Spark, Trino, Iceberg, Kubernetes, and the AWS Terraform path (Phase 7).
