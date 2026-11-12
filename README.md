# Open Data Platform

A reproducible open-source data platform for local development on Windows 11, WSL2, and Docker Compose. The AWS path is Terraform Infrastructure as Code in Phase 7. This repository does not apply it.

The goal is to show a full batch path that can be rebuilt from a checkout: several sources land as auditable raw files, a Bronze / Silver / Gold lakehouse is built on open components, and Gold metrics are checked against Silver. Schedules, streaming, notebooks, and a public report come later.

## What it does today

Verified on the local stack. Counts and quality results from 2026-11-11 are in the [Phase 2 live run](docs/architecture/phase-2-live-run.md). Versions, partitioning, file sizing, the full-overwrite strategy, and limitations are in [Phase 2 lakehouse](docs/architecture/phase-2-lakehouse.md).

**Raw ingestion.** Five source families land in object storage. Every object carries `source_id`, `execution_id`, and `_ingestion_ts`. Invalid rows stay in the raw object. Findings go to a quarantine sidecar. The same execution overwrites an object. A new execution keeps the previous one. The sources are the Olist e-commerce and marketing-funnel datasets (CC BY-NC-SA 4.0, fetched locally, not vendored), DummyJSON products, a windowed PostgreSQL extract, a fictional storefront clickstream, and IBGE municipalities. Index: [Phase 1 ingestion](docs/architecture/phase-1-ingestion.md).

**Lakehouse.** Spark writes Iceberg tables. Trino reads them. Bronze keeps the source payload and the ingestion metadata. Silver types the columns, expands nested JSON, and deduplicates. Gold is a dbt project on Trino. It covers marketplace delivery and late orders, customer reviews, seller acquisition, Brazil geography, a product catalog, storefront behavior, and a windowed OLTP extract. Quality tests compare Gold keys and sums with Silver. On 2026-11-11, Bronze matched that run's raw counts (18/18), Silver checks returned ok (20/20), and the Gold build and reconciliation tests passed with the same headline anchors as the 2026-11-05 / 2026-11-06 baseline.

**Refresh patterns.** Daily and weekly batch share the PostgreSQL windowed extract. Airflow does not schedule them yet. Streaming change capture is a separate later path and does not replace batch.

## What it does not do yet

Airflow schedules, backfills, Kafka, streaming CDC, Iceberg snapshot expiration, JupyterLab, Superset, an AWS `terraform apply`, and public hosting of the platform report. An early report scaffold under `site/` previews verified facts only. Local preview needs Node.js 22.22+: `cd site && npm ci && npm run dev`. See [`site/README.md`](site/README.md).

## Reproduce it

Prerequisites: Windows 11 with WSL2 Ubuntu (or another Linux host), Docker Desktop with that distro integrated, [uv](https://docs.astral.sh/uv/) 0.13.1, and Docker Compose v2. GNU Make is optional. `uv sync` installs CPython 3.12 for this repository. Use `uv run`, not system `python3`.

```bash
curl -LsSf https://astral.sh/uv/0.13.1/install.sh | sh
source "$HOME/.local/bin/env"
uv --version
```

`uv --version` must print `uv 0.13.1`.

From a checkout:

```bash
uv sync
cp .env.example .env

uv run ruff check src tests
uv run pytest tests/unit

docker compose up -d
docker compose exec postgres pg_isready -U odp -d odp
```

`cp .env.example .env` is optional. Do not commit `.env`. `docker compose up` starts PostgreSQL 16 and MinIO. Spark and Trino are not in that command. The lakehouse targets start them.

Raw objects must already be in the raw bucket (default `odp-raw`). Fetch and load steps are under [`docs/sources/`](docs/sources/). Unit tests do not need those files or the public APIs.

Rebuild the lakehouse from that bucket:

```bash
make test-catalog-probe
make live-bronze
make live-silver
make live-gold
make dbt-test-reconciliation
```

The first Spark target stores jars in `.local/spark-ivy`. Later targets reuse them. `make live-gold` builds Gold and its schema tests. Reconciliation is the separate last command. The stack stays up for several minutes. The recorded result of this path is the [2026-11-11 live run](docs/architecture/phase-2-live-run.md).

Stop services and keep volumes:

```bash
docker compose --profile lakehouse stop
docker compose down
```

GitHub Actions runs the same lint and unit tests on Python 3.12. It does not start Compose and it does not deploy cloud resources.

## Where the detail lives

| Document | Role |
| --- | --- |
| [Phase 1 ingestion](docs/architecture/phase-1-ingestion.md) | Sources, raw keys, repeatable runs, update patterns |
| [Phase 2 lakehouse](docs/architecture/phase-2-lakehouse.md) | Versions, partitioning, file sizing, overwrite strategy, limitations |
| [Phase 2 live run](docs/architecture/phase-2-live-run.md) | 2026-11-11 counts and quality results |
| [Data contracts](docs/architecture/data-contracts.md) | Fields, keys, and null rules |
| [`ROADMAP.md`](ROADMAP.md) | Issue scope and acceptance criteria |
| [`PROJECT_STATUS.md`](PROJECT_STATUS.md) | Current-phase ledger |
| [`DECISIONS.md`](DECISIONS.md) | Architecture decision index |

Contributors and agents follow [`AGENTS.md`](AGENTS.md).
