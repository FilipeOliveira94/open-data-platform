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
3. Check the relevant roadmap item. Identify dependencies, assumptions, risks and acceptance criteria.
4. For non-trivial work, propose a short implementation plan before editing files.
5. Implement the smallest coherent increment.
6. Run relevant tests, linting and validation commands.
7. Inspect the results and fix regressions caused by the changes.
8. Update documentation and `PROJECT_STATUS.md` (current-phase ledger only; append older narrative to `docs/status-log.md` when trimming).
9. Summarize changed files, commands executed, test results, limitations and the next step.

Do not mark a task complete unless its acceptance criteria have been verified.

Do not silently change architecture, dependencies or acceptance criteria.

Record what was implemented and what the checks showed. Do not record pauses, retries, or other recovery steps that do not change how to operate the result.

An entry point that belongs to the issue and was not run is not verified. Name it and say why before treating the issue as done.

`PROJECT_STATUS.md` tracks the **current phase** (progress, acceptance, latest validation, active decision gates). Do not rebuild cumulative Delivered/Verified tables or the full open-decisions table there. Issue inventory lives in `ROADMAP.md`; full ADRs in `DECISIONS.md`; chronological archive in `docs/status-log.md`.

Do **not** treat `PROJECT_STATUS.md` as durable evidence storage. Architecture docs, ADRs, and `docs/status-log.md` must carry the verified facts themselves. Other docs may point at the status log for chronology; they must not rely on `PROJECT_STATUS.md` as the only (or primary) citation, because that ledger is rewritten each phase.

## 4. Architecture Principles

- Prefer simple, modular, reproducible solutions over unnecessary infrastructure.
- Keep business logic separate from Airflow DAGs and infrastructure configuration.
- Make batch pipelines idempotent and support controlled backfills.
- Define explicit data contracts and document schema evolution.
- Design CDC consumers to handle retries, duplicates and deletes.
- Keep raw data auditable and preserve lineage metadata.
- Use environment-specific configuration without duplicating business logic.
- Validate compatibility between storage, table formats, catalogs and query engines before adopting a combination.
- Do not introduce a new technology without explaining its purpose and operational cost.

## 5. Python and SQL

- Use type hints and clear interfaces.
- Follow Ruff formatting and linting rules.
- Add unit tests for meaningful business logic.
- Use parameterized SQL and explicit transaction boundaries where appropriate.
- Avoid hard-coded credentials, paths and environment-specific assumptions.
- Pin and lock dependencies. Do not leave versions floating.
- Keep tests deterministic and independent of public API availability.

## 6. Data Engineering Requirements

- Record ingestion timestamps, source identifiers and execution IDs where relevant.
- Distinguish event time, processing time and ingestion time.
- Define primary keys and table grain explicitly.
- Handle nulls, duplicate records and invalid schemas intentionally. Do not drop them silently.
- Validate business metrics against source data.
- Document partitioning, file sizing and incremental processing strategies.
- Treat exactly-once end-to-end processing as a claim requiring explicit proof; do not assume it from Kafka or Spark alone.

## 7. Environment and Tooling

- Primary development environment: Windows 11, WSL2 Ubuntu and Docker Desktop.
- Prefer Linux paths inside WSL2.
- Do not assume PowerShell syntax in Linux scripts or Bash syntax in PowerShell commands.
- Use Docker Compose for local services and end-to-end verification.
- Keep local development independent of AWS credentials whenever possible.
- Use Terraform for the AWS deployment path (local→AWS service mapping: MinIO→S3, catalog→Glue, Trino→Athena) and document the required permissions.
- Do not install or upgrade system-wide dependencies without explaining the impact.

## 8. Security and Cost

- Never commit secrets, credentials, access tokens or sensitive datasets. Do not hard-code them in source.
- Not committing a value is not enough. Do not place row values or secrets in query text, process arguments, exception messages, or `print` output. Counts, names, paths, and field names are fine. Write rows to a data file and reference the path. Spark secrets go through `docker/spark/credentials.sh`, not `--conf`. Detail: Runtime data exposure below.
- Use `.env.example` for variable names and safe placeholders.
- Treat public API responses and downloaded files as untrusted input.
- Prefer short-lived AWS credentials and OIDC for CI/CD.
- Validate Terraform with `fmt`/`validate` in CI; do not run `terraform apply`, `terraform destroy`, delete cloud resources or destructive Docker volume operations without explicit user approval.
- Ask before any operation that can incur meaningful cloud costs.
- Do not invent performance metrics, cost figures, lineage or benchmark results.
- Present the AWS path as Infrastructure as Code that shows how the platform provisions on AWS; do not frame documentation around unpaid cloud usage.

### Runtime data exposure

Query text, process arguments, and driver logs are logs too.

- Do not interpolate field values, primary keys, payloads, or free text into SQL that Spark, Trino, or dbt will submit. Counts, dataset names, column names, and object paths are fine. Write the rows to a data file and reference the path, as Bronze does.
- Do not put secrets in `--conf`, argv, or a logged command. Pass them through a mode-600 properties file or an environment variable the library reads itself. `docker/spark/credentials.sh` is the Spark writer. Keep `spark.redaction.regex` covering `password`, `secret`, and `access.key`.
- Exception messages and `print` output name the failing index, field, or dataset. They do not include the cell value or the record.
- A test that compares a fixture may show that fixture in a pytest failure. Do not print live extracts on success. Do not enable dbt `store_failures` unless the failure table is an explicit, reviewed store.
- Any new statement builder needs a unit test where a planted value is absent from the SQL and present only in the data file. `tests/unit/infrastructure/test_spark_credentials.py` fails if a `docker/spark/*.sh` script other than `credentials.sh` assigns `jdbc.password`, `fs.s3a.secret.key`, or `fs.s3a.access.key`.

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

## 12. Issue Close Checklist

Before the closing commit of an issue, check each item and update only what the verified evidence supports:

- `ROADMAP.md`: the issue status, a Changelog line, and Last updated.
- `PROJECT_STATUS.md`: the phase table, Next work consistent with the current acceptance criteria, and Last updated.
- `docs/status-log.md`: a new entry.
- An ADR or `DECISIONS.md` entry changes to `approved` only with evidence from a verified issue.
- `README.md` changes only at phase close.

Prompts for planning, implementing and closing an issue are in `docs/agent-workflow.md`.

## 13. Git

- Commit locally only. Never run `git push`; Filipe reviews and pushes.
- Use Conventional Commits directly on `main`.

AGENTS.md is the single source; CLAUDE.md imports it.