# Project Status

Operational ledger for the **current phase**. Issue backlog and acceptance text live in [`ROADMAP.md`](ROADMAP.md). Full decision index: [`DECISIONS.md`](DECISIONS.md). Chronological archive: [`docs/status-log.md`](docs/status-log.md).

| | |
| --- | --- |
| Repository | [FilipeOliveira94/open-data-platform](https://github.com/FilipeOliveira94/open-data-platform) |
| Started | 2026-10-14 |
| Last updated | 2026-10-17 (ledger restructure; P1-01 verified) |
| Current phase | Phase 1 — Multi-source ingestion |
| Phase status | In progress (P1-01 verified) |

**Status values:** not started · in progress · delivered · verified · blocked

**Verified** means acceptance checks were run and evidence is recorded (here and/or in `docs/status-log.md`). Do not treat planned architecture as complete.

**What belongs here:** current objective, current-phase issue progress, phase acceptance checklist, latest validation, short foundation baseline, decisions that gate *this* work, active risks, blockers, next work.

**What does not:** cumulative issue inventories (use `ROADMAP.md`), full ADR tables (use `DECISIONS.md`), long history (use `docs/status-log.md`).

---

## Current objective

**P1-02** — raw storage layout and ingestion metadata conventions.

Phase 1 out of scope: streaming/CDC, Iceberg, Airflow schedules, lakehouse transforms, AWS Terraform apply, Phase 8 hosting polish.

## Next work

1. **P1-02** — raw storage layout and ingestion metadata conventions.
2. **P1-03** — data contracts for planned sources (after P1-02).

---

## Phase 1 progress

| ID | Title | Status | Evidence |
| --- | --- | --- | --- |
| P1-01 | Historical dataset license and redistribution check | Verified | `docs/sources/`; ADR-0018 / D-018; no Olist binaries in git (2026-10-17) |
| P1-02 | Raw storage layout and ingestion metadata conventions | Not started | — |
| P1-03 | Data contracts for planned sources | Not started | — |
| P1-04 | Historical e-commerce (+ marketing funnel) ingestion | Not started | Blocked on P1-01✓, P1-02, P1-03 |
| P1-05 | Paginated public API client | Not started | — |
| P1-06 | PostgreSQL incremental batch extract | Not started | — |
| P1-07 | Synthetic event generator | Not started | — |
| P1-08 | Failure and invalid-record behavior | Not started | — |
| P1-09 | Phase 1 documentation and status | Not started | — |

Acceptance detail: `ROADMAP.md` → Phase 1.

### Phase 1 acceptance

- [ ] Ingestions can be repeated without undue duplication or corruption.
- [ ] Unit tests do not require the public API to be online.
- [ ] Raw data is traceable by source and execution.
- [ ] Contracts and expected fields are documented.
- [ ] Invalid records and errors have explicit, documented behavior.
- [ ] PostgreSQL extract accepts an explicit time/watermark window suitable for daily and weekly batch runs.

---

## Latest validation

| | |
| --- | --- |
| Date | 2026-10-17 |
| Environment | WSL2 / Ubuntu |
| Checks | P1-01: Kaggle listings CC BY-NC-SA 4.0 (e-commerce + marketing funnel); repo scan for vendored csv/olist data artifacts; provenance + ADR-0018 |
| Result | P1-01 verified; no dataset download performed |

---

## Foundation baseline (Phase 0)

Phase 0 is **verified**. Re-check when touching toolchain or Compose:

```bash
uv run ruff check src tests
uv run pytest tests/unit
docker compose up -d && docker compose exec postgres pg_isready -U odp -d odp
set -a && source .env.example && set +a && uv run pytest tests/integration/test_minio_smoke.py
```

Closed Phase 0 issues and narrative: [`ROADMAP.md`](ROADMAP.md) (Phase 0), [`docs/status-log.md`](docs/status-log.md).

---

## Decisions (active for this phase)

Full index: [`DECISIONS.md`](DECISIONS.md). List here only gates for current/near work or decisions approved in this phase.

| ID | Relevance | State |
| --- | --- | --- |
| D-018 | Olist datasets: no vendor; fetch-from-source (`docs/sources/`) | Approved (P1-01) |
| D-013 | Data contracts / no silent drops — gates P1-03+ | Proposed |
| D-015 | Package owns logic; thin DAGs/IaC — gates ingestion layout | Proposed |
| D-001 / D-002 | Compose + MinIO baseline (unchanged) | Approved |

---

## Required update patterns

Three PostgreSQL refresh patterns remain in scope (`ROADMAP.md`). CDC does **not** replace daily/weekly batch.

| Use case | Pattern | Phases | Status |
| --- | --- | --- | --- |
| Daily batch | Windowed incremental extract + Airflow daily schedule | 1 (P1-06), 3 (P3-08) | Not started |
| Weekly batch | Windowed extract + Airflow weekly schedule | 1 (P1-06), 3 (P3-09) | Not started |
| Streaming | Logical replication → Debezium → Kafka → Spark SS | 4 | Not started |

---

## Program roadmap

| Phase | Scope | Status |
| --- | --- | --- |
| 0 | Repository, toolchain, PostgreSQL 16, MinIO, CI | Verified |
| 1 | Multi-source ingestion | In progress (P1-01 verified) |
| 2 | Lakehouse PoC, medallion, dbt | Not started |
| 3 | Airflow daily/weekly, backfills, quality | Not started |
| 4 | Streaming CDC | Not started |
| 5 | Observability, lineage, FinOps model | Not started |
| 6 | CI/CD expansion, resilience, security | Not started |
| 7 | AWS path as Terraform IaC | Not started |
| 8 | Platform report site (hosting + polish) | Not started (early `site/` scaffold exists) |

---

## Risks and limitations

- Olist e-commerce + marketing funnel are **CC BY-NC-SA 4.0**: do not vendor; fetch locally (ADR-0018). MIT covers software/docs only.
- Spark / Iceberg / catalog / query-engine compatibility is unproven until P2-01.
- Exactly-once end-to-end will not be claimed without a written proof.
- `terraform apply` / `destroy` and destructive volume ops need explicit approval.
- Keep daily batch, weekly batch, and streaming CDC distinguishable in design and demos.
- Community MinIO remains AGPLv3; pin documented in ADR-0002.
- Agent shells may lack Docker socket access; host terminal Compose path is source of truth for local ops.

## Blocked

None.

## Platform report site

Early Astro Starlight + Catppuccin scaffold under `site/` (verified facts only). Public hosting is Phase 8. Preview: `cd site && npm run dev` — see `site/README.md`.

| Page | Status |
| --- | --- |
| Home / Architecture / Operations / Reference | Scaffold (mostly Phase 0 facts) |
| Pipeline | Stub |

---

## Log

Recent entries only. Older history: [`docs/status-log.md`](docs/status-log.md).

### 2026-10-17 — Ledger restructure

`PROJECT_STATUS.md` now tracks the current phase only. Cumulative Delivered/Verified/Open-decisions tables removed. Archive: `docs/status-log.md`.

### 2026-10-17 — P1-01 verified

Olist Brazilian e-commerce + Marketing Funnel: CC BY-NC-SA 4.0 recorded; D-018 / ADR-0018 approved (no vendor; fetch-from-source).
