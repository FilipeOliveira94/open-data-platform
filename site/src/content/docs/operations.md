---
title: Operations
description: CI and operational baseline for the Open Data Platform. Observability and the resource footprint are not started.
---

What is in place for quality and automation today, versus work still ahead.

## Verified

| Status | Item |
| --- | --- |
| **Verified** | GitHub Actions runs lint (Ruff) and unit tests on Python 3.12; nothing is deployed |
| **Verified** | No live `.env` or secrets in version control; `.env.example` placeholders only |
| **Verified** | Documented local setup: `uv sync`, Compose up, Postgres ready, MinIO smoke test |

## Not started

| Status | Item |
| --- | --- |
| **Planned** | Observability (Prometheus / Grafana), lineage, resource footprint and cost to run (Phase 5) |
| **Planned** | CI/CD expansion, resilience and security checks (Phase 6) |
