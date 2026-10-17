---
title: Pipeline
description: Pipeline layers for the Open Data Platform. Ingestion and lakehouse are not verified yet.
---

Ingestion and lakehouse layers are **not verified yet**. This page will expand as Phases 1–4 land with evidence.

## Planned scope

Phase 1 targets multi-source ingestion: historical dataset (after license check), public API, PostgreSQL incremental extract (daily/weekly windows), and synthetic events.

Later phases cover Bronze / Silver / Gold, Airflow batch schedules, and streaming CDC. None of those pipelines are delivered in the repository today.

## Status

| Status | Item |
| --- | --- |
| **Verified** | Object storage and PostgreSQL available for future writers (Phase 0) |
| **Planned** | Ingestion clients, raw layout conventions, lakehouse, orchestration, CDC |
