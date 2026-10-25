# Open Data Platform — Agent Instructions

## 1. Project Goal

Build a reproducible open-source data platform that supports local development on Windows 11 through WSL2 and Docker Compose, with an AWS deployment path expressed as Terraform Infrastructure as Code.

The platform demonstrates:
- Multiple source ingestion: historical datasets, APIs, PostgreSQL, and a fictional storefront clickstream.
- Batch processing and CDC/streaming.
- A lakehouse with Bronze, Silver and Gold layers.
- Apache Airflow orchestration and dbt analytics.
- Data quality, lineage, observability and FinOps cost modeling.
- Automated testing, CI/CD and reproducible cloud infrastructure as code.

## 2. Working Language

- Use English for code, identifiers, comments, commit messages and technical documentation.
- Communicate with the user in Brazilian Portuguese.
- Explain architectural decisions and trade-offs clearly.

## 3. Mandatory Workflow

For each task:

1. Inspect the existing codebase and current project documentation.
2. Read `PROJECT_STATUS.md`, `ROADMAP.md` and relevant ADRs when they exist.
3. Identify dependencies, assumptions, risks and acceptance criteria.
4. Propose a short implementation plan before making broad changes.
5. Implement the smallest coherent increment.
6. Run relevant tests, linting and validation commands.
7. Inspect the results and fix regressions caused by the changes.
8. Update documentation and `PROJECT_STATUS.md` (current-phase ledger only; append older narrative to `docs/status-log.md` when trimming).
9. Summarize changed files, commands executed, test results, limitations and the next step.

Do not mark a task complete unless its acceptance criteria have been verified.

`PROJECT_STATUS.md` tracks the **current phase** (progress, acceptance, latest validation, active decision gates). Issue inventory lives in `ROADMAP.md`; full ADRs in `DECISIONS.md`; chronological archive in `docs/status-log.md`.

## 4. Architecture Principles

- Prefer simple, modular, reproducible solutions over unnecessary infrastructure.
- Keep business logic separate from Airflow DAGs and infrastructure configuration.
- Make batch pipelines idempotent and support controlled backfills.
- Define explicit data contracts and document schema evolution.
- Design CDC consumers to handle retries, duplicates and deletes.
- Keep raw data auditable and preserve lineage metadata.
- Use environment-specific configuration without duplicating business logic.
- Validate compatibility between storage, table formats, catalogs and query engines.
- Do not introduce a new technology without explaining its purpose and operational cost.

## 5. Python and SQL

- Use type hints and clear interfaces.
- Follow Ruff formatting and linting rules.
- Add unit tests for meaningful business logic.
- Use parameterized SQL and explicit transaction boundaries where appropriate.
- Avoid hard-coded credentials, paths and environment-specific assumptions.
- Pin and lock dependencies.
- Keep tests deterministic and independent of public API availability.

## 6. Data Engineering Requirements

- Record ingestion timestamps, source identifiers and execution IDs where relevant.
- Distinguish event time, processing time and ingestion time.
- Define primary keys and table grain explicitly.
- Handle nulls, duplicate records and invalid schemas intentionally.
- Validate business metrics against source data.
- Document partitioning, file sizing and incremental processing strategies.
- Treat exactly-once end-to-end processing as a claim requiring explicit proof; do not assume it from Kafka or Spark alone.

## 7. Environment and Tooling

- Primary development environment: Windows 11, WSL2 Ubuntu and Docker Desktop.
- Prefer Linux paths inside WSL2.
- Do not assume PowerShell syntax in Linux scripts or Bash syntax in PowerShell commands.
- Use Docker Compose for local services and end-to-end verification.
- Keep local development independent of AWS credentials whenever possible.
- Use Terraform for the AWS deployment path (local→AWS service mapping) and document the required permissions.
- Do not install or upgrade system-wide dependencies without explaining the impact.

## 8. Security and Cost

- Never commit secrets, credentials, access tokens or sensitive datasets.
- Use `.env.example` for variable names and safe placeholders.
- Treat public API responses and downloaded files as untrusted input.
- Prefer short-lived AWS credentials and OIDC for CI/CD.
- Validate Terraform with `fmt`/`validate` in CI; do not run `terraform apply`, `terraform destroy`, delete cloud resources or destructive Docker volume operations without explicit user approval.
- Ask before any operation that can incur meaningful cloud costs.
- Do not invent performance metrics, cost figures, lineage or benchmark results.
- Present the AWS path as Infrastructure as Code that shows how the platform provisions on AWS; do not frame documentation around unpaid cloud usage.

## 9. Testing and Verification

- Run the narrowest relevant tests first.
- Run broader checks before declaring a phase complete.
- Do not claim tests passed if they were not executed.
- Distinguish code validation, integration validation and real end-to-end execution.
- If a dependency or external service is unavailable, report the limitation rather than inventing a successful result.
- Test failure recovery and idempotency, not only the happy path.

## 10. Scope Control

- Work on one issue or one small, coherent task at a time.
- Do not implement multiple future phases in a single change.
- Do not introduce Kubernetes unless an explicit requirement justifies it.
- Avoid duplicate tools for the same responsibility.
- Prefer an explicit TODO or a documented blocker over speculative implementation.

## 11. Completion Report

At the end of every task, report:

- Objective and implementation summary.
- Files created or modified.
- Commands executed.
- Tests passed and failed.
- Acceptance criteria satisfied and remaining.
- Known limitations and risks.
- Suggested next issue.

The user makes final architectural decisions and approves significant changes.

Cursor project rules in `.cursor/rules/` split this contract by concern (`00` principles, `01` Python/testing, `02` data engineering, `03` infrastructure/security, `04` agent workflow). Keep `AGENTS.md` as the canonical source.