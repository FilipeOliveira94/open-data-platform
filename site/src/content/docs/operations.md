---
title: Operations
description: CI and operational baseline for the Open Data Platform. Observability and AWS IaC are not started.
---

What is in place for quality and automation today, versus work still ahead.

## Verified

| Status | Item |
| --- | --- |
| **Verified** | GitHub Actions runs lint (Ruff) and unit tests on Python 3.12; no cloud deploy |
| **Verified** | No live `.env` or secrets in version control; `.env.example` placeholders only |
| **Verified** | Documented local setup: `uv sync`, Compose up, Postgres ready, MinIO smoke test |

## Not started

| Status | Item |
| --- | --- |
| **Planned** | Observability (Prometheus / Grafana), lineage, cost visibility (Phase 5) |
| **Planned** | CI/CD expansion, resilience and security checks (Phase 6) |
| **Planned** | AWS deployment path as Terraform IaC (Phase 7) |
