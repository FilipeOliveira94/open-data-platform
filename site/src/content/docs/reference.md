---
title: Reference
description: Links to the Open Data Platform repository, setup, status, roadmap, and architecture decisions.
---

The repository documents are the source of truth. This site summarizes only facts they record as verified.

## Repository

| Resource | Purpose |
| --- | --- |
| [GitHub repository](https://github.com/FilipeOliveira94/open-data-platform) | Source code, issues, and pull requests |
| [CI workflow](https://github.com/FilipeOliveira94/open-data-platform/blob/main/.github/workflows/ci.yml) | Ruff, the Markdown table check, and unit tests on pushes to `main` and on every pull request |
| [CI runs](https://github.com/FilipeOliveira94/open-data-platform/actions) | Results of the CI workflow |
| [AGENTS.md](https://github.com/FilipeOliveira94/open-data-platform/blob/main/AGENTS.md) | Working rules: architecture principles, data handling, security, and testing |

## Setup

| Resource | Purpose |
| --- | --- |
| [README](https://github.com/FilipeOliveira94/open-data-platform/blob/main/README.md) | Prerequisites, install on a fresh machine, and the commands that run each layer |
| [.env.example](https://github.com/FilipeOliveira94/open-data-platform/blob/main/.env.example) | Variable names and safe placeholders for the local stack |
| [docs/sources/](https://github.com/FilipeOliveira94/open-data-platform/tree/main/docs/sources) | Source notes: how to fetch or generate each dataset |
| [Local Airflow](https://github.com/FilipeOliveira94/open-data-platform/blob/main/docs/architecture/airflow-local.md) | Airflow on the Compose `orchestration` profile: image, executor, UI, and task logs |
| [site/README.md](https://github.com/FilipeOliveira94/open-data-platform/blob/main/site/README.md) | Previewing and building this site locally |

## Status and roadmap

| Resource | Purpose |
| --- | --- |
| [PROJECT_STATUS.md](https://github.com/FilipeOliveira94/open-data-platform/blob/main/PROJECT_STATUS.md) | Current-phase ledger: progress, acceptance, and the latest validation |
| [ROADMAP.md](https://github.com/FilipeOliveira94/open-data-platform/blob/main/ROADMAP.md) | Every phase and issue with its acceptance criteria, and the site figure table |
| [docs/status-log.md](https://github.com/FilipeOliveira94/open-data-platform/blob/main/docs/status-log.md) | Chronological record of each verified issue and its checks |

## Architecture decisions

| Resource | Purpose |
| --- | --- |
| [DECISIONS.md](https://github.com/FilipeOliveira94/open-data-platform/blob/main/DECISIONS.md) | Index of every decision with its current status: proposed, approved, superseded, or rejected |
| [docs/decisions/](https://github.com/FilipeOliveira94/open-data-platform/tree/main/docs/decisions) | The full ADRs: context, options, decision, and consequences |
| [ADR-0029](https://github.com/FilipeOliveira94/open-data-platform/blob/main/docs/decisions/0029-open-source-only-platform.md) | Open-source-only platform on the local stack, with no cloud deployment path |

## Phase documents

| Resource | Purpose |
| --- | --- |
| [Phase 1 ingestion](https://github.com/FilipeOliveira94/open-data-platform/blob/main/docs/architecture/phase-1-ingestion.md) | Sources, traceability, repeatable runs, and update patterns |
| [Phase 2 lakehouse](https://github.com/FilipeOliveira94/open-data-platform/blob/main/docs/architecture/phase-2-lakehouse.md) | Bronze, Silver, and Gold on Iceberg with Spark, Trino, and dbt |
| [Phase 2 live run](https://github.com/FilipeOliveira94/open-data-platform/blob/main/docs/architecture/phase-2-live-run.md) | The recorded end-to-end lakehouse run |
| [Phase 3 orchestration](https://github.com/FilipeOliveira94/open-data-platform/blob/main/docs/architecture/phase-3-orchestration.md) | Airflow schedules, backfills, the quality gate, and quarantine |
| [Phase 3 live run](https://github.com/FilipeOliveira94/open-data-platform/blob/main/docs/architecture/phase-3-live-run.md) | The recorded orchestration run |

The figure manifest (question, value, grain, Gold table, tool, and run id for each site figure) is planned and arrives with P8-08.

Theme: the author's brand palette and the Urbanist font on Starlight. Author: [Filipe Oliveira's portfolio](https://filipeoliveira-hub.com.br).
