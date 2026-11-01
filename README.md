# Open Data Platform

Reproducible open-source data platform: local development on Windows 11 + WSL2 + Docker Compose, with an AWS deployment path expressed later as Terraform Infrastructure as Code (Phase 7).

## Platform report

A short multi-page **platform report** (home, architecture, pipeline, operations, reference) lives under [`site/`](site/) as an Astro [Starlight](https://starlight.astro.build/) app with the [Catppuccin](https://starlight-themes.netlify.app/catppuccin/) theme. Content reflects **verified** work only; public hosting is **Phase 8**. [`PROJECT_STATUS.md`](PROJECT_STATUS.md) remains the source of truth.

| | |
| --- | --- |
| Live report | *Not published yet* (Phase 8) |
| Local preview | `cd site && npm ci && npm run dev` — see [`site/README.md`](site/README.md) (Node.js 22.22+) |
| Scheme | See [`ROADMAP.md`](ROADMAP.md) → Platform report site |
| Status | [`PROJECT_STATUS.md`](PROJECT_STATUS.md) → Platform report site |

When the report is published, the URL will also be set on the GitHub repository **About → Website** field.

**Phase 0** (foundation) is complete: Python 3.12 toolchain, PostgreSQL 16, MinIO, lint/tests, and CI. **Phase 1** (multi-source raw ingestion) is verified. Sources, object keys, repeatable runs, and the three update patterns are indexed in [`docs/architecture/phase-1-ingestion.md`](docs/architecture/phase-1-ingestion.md). **Phase 2** has a verified single-table Spark + Iceberg probe ([P2-01](docs/architecture/phase-2-spark-iceberg-poc.md)) and a verified JDBC catalog plus Trino read ([P2-02](docs/architecture/phase-2-catalog-query.md)). Bronze, Silver, and Gold tables, Airflow schedules, and streaming CDC are later issues.

## Prerequisites

- Windows 11 with WSL2 and an Ubuntu distro (or another Linux host)
- Docker Desktop, running, with WSL integration enabled for that distro
- [uv](https://docs.astral.sh/uv/) **0.13.1** (pinned in `pyproject.toml` and CI)
- Docker Compose v2, included with Docker Desktop (`docker compose version`)
- GNU Make (optional; every Make target below has the equivalent command)
- Node.js 22.22+ only to preview the platform report under `site/`

`uv sync` installs CPython 3.12 for this repository. Ubuntu's `python3` package may be another version. Use `uv run`, not system `python3`.

## Install on a fresh machine

Run these from the Ubuntu terminal as a normal user. They do not require an existing checkout.

### 1. uv 0.13.1

```bash
curl -LsSf https://astral.sh/uv/0.13.1/install.sh | sh
source "$HOME/.local/bin/env"
uv --version
```

`uv --version` must print `uv 0.13.1`. The installer adds `source "$HOME/.local/bin/env"` to `~/.bashrc`, which puts `~/.local/bin` on `PATH`. Open a new terminal if `uv` is not found.

If `curl` is missing: `sudo apt update && sudo apt install -y curl ca-certificates`.

### 2. GNU Make (optional)

```bash
sudo apt update
sudo apt install -y make
make --version
```

### 3. Docker Desktop and WSL

1. Install [Docker Desktop for Windows](https://docs.docker.com/desktop/setup/install/windows-install/).
2. Start Docker Desktop and leave it running. The engine is the `docker-desktop` WSL distro; it stops when the app quits.
3. In Docker Desktop: **Settings → Resources → WSL integration**. Enable the Ubuntu distro, then **Apply & restart**.
4. In Ubuntu, confirm both the client and the server answer:

```bash
docker version
docker compose version
```

## Quick start

```bash
# 1) Toolchain — downloads CPython 3.12 and installs the lockfile into .venv
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
uv run pytest tests/integration/infrastructure/test_minio_smoke.py
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
| `make up` / `make down` | Start/stop Compose stack (PostgreSQL and MinIO only) |
| `make test-minio` | Live MinIO put/get smoke |
| `make test-iceberg-poc` | P2-01 Iceberg probe (MinIO must be healthy; profile `lakehouse`) |
| `make test-catalog-probe` | P2-02 JDBC catalog probe: Spark write, Trino read (PostgreSQL and MinIO must be healthy) |

## Local services (Phase 0)

| Service | Image pin | Ports (default) |
| --- | --- | --- |
| PostgreSQL 16 | `postgres:16.15-bookworm` | `5432` |
| MinIO | `coollabsio/minio:RELEASE.2025-10-15T17-29-55Z` | API `9000`, console `9001` |

**MinIO image note:** MinIO Inc. removed official public container images in favor of commercial **AIStor**. The community MinIO server remains **AGPLv3**, so community-built images are acceptable for local use. Phase 0 pins [`coollabsio/minio`](https://hub.docker.com/r/coollabsio/minio) at upstream release `RELEASE.2025-10-15T17-29-55Z` (see ADR-0002).

Default credentials are placeholders in `.env.example` for local use only.

## Lakehouse probe (P2-01)

Spark is not part of `docker compose up`. The `lakehouse` profile provides one Spark runtime and Trino. Profile `up` leaves Spark idle; a probe passes its script to `docker compose run`. Versions, the Hadoop-catalog limitation, and the 2026-10-28 result are in [`docs/architecture/phase-2-spark-iceberg-poc.md`](docs/architecture/phase-2-spark-iceberg-poc.md).

```bash
docker compose up -d minio
make test-iceberg-poc
```

The catalog in this probe is the Iceberg Hadoop catalog on MinIO. The durable local catalog is the JDBC catalog from P2-02.

## Catalog and Trino (P2-02)

Spark writes `catalog_probe.probe` through the Iceberg JDBC catalog on the Compose PostgreSQL database `iceberg`. Trino `484` reads it. Neither service starts with `docker compose up`. Versions, the command, and the 2026-10-29 result are in [`docs/architecture/phase-2-catalog-query.md`](docs/architecture/phase-2-catalog-query.md).

```bash
make test-catalog-probe
```

## CI

GitHub Actions (`.github/workflows/ci.yml`) runs the same lint and unit-test commands on Python 3.12. It does **not** deploy cloud resources or start Compose.

## Documentation

| Document | Role |
| --- | --- |
| [`AGENTS.md`](AGENTS.md) | Agent and contributor operating contract |
| [`ROADMAP.md`](ROADMAP.md) | Issue-level scope and acceptance criteria (phases 0–8) |
| [`PROJECT_STATUS.md`](PROJECT_STATUS.md) | Current-phase status ledger (verified work + next steps) |
| [`docs/status-log.md`](docs/status-log.md) | Chronological status archive |
| [`DECISIONS.md`](DECISIONS.md) | Architecture decision index |
| [`docs/decisions/`](docs/decisions/) | ADR detail |
| [`docs/architecture/phase-1-ingestion.md`](docs/architecture/phase-1-ingestion.md) | Phase 1 source index, raw keys, repeatable runs, update patterns |
| [`docs/architecture/phase-2-spark-iceberg-poc.md`](docs/architecture/phase-2-spark-iceberg-poc.md) | P2-01 Spark + Iceberg probe: pins, command, result, limitations |
| [`docs/architecture/phase-2-catalog-query.md`](docs/architecture/phase-2-catalog-query.md) | P2-02 JDBC catalog and Trino: pins, command, result, limitations |
| [`docs/sources/`](docs/sources/) | Per-source license, access, and commands |
| [`docs/architecture/`](docs/architecture/) | Raw layout and data contracts |

## Not in the verified platform yet

Bronze, Silver, and Gold tables, Airflow schedules, Kafka, streaming CDC, Kubernetes, the AWS Terraform path (Phase 7), and public hosting of the platform report (Phase 8). P2-01 covers one Iceberg table on the Hadoop catalog. P2-02 covers the same shape of table on the JDBC catalog, read by Trino. The `site/` scaffold previews verified facts locally.
