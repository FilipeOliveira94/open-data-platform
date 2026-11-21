# ADR-0007: Daily and weekly batch

| Field | Value |
| --- | --- |
| Status | `approved` (2026-11-21, P3-04 and P3-05 verified) |
| Date opened | 2026-10-15 |
| Date decided | — |
| Owner | Platform architect |
| Index ID | D-007 |
| Related issues | P1-06, P3-04, P3-05 |
| Supersedes | — |
| Superseded by | — |

## Context / problem

Scheduled loads and CDC are different patterns. This platform needs **both** cadence-based batch refreshes and streaming change capture, with clear grain and watermarks. Coexistence rules are in [ADR-0014](0014-batch-vs-streaming-coexistence.md).

## Decision

Implement **one** PostgreSQL incremental extract path parameterized by time/watermark window (P1-06). Drive it with **distinct Airflow schedules**: daily (P3-04) and weekly (P3-05). Document weekly grain and purpose when implemented. CDC ([ADR-0008](0008-streaming-cdc.md)) is complementary, not a replacement.

## Alternatives considered

| Option | Pros | Cons | Why not (if rejected) |
| --- | --- | --- | --- |
| Shared extract + daily and weekly schedules (chosen) | DRY; demonstrates both cadences | Need clear grain docs for weekly | — |
| Daily only | Less DAG surface | Misses weekly grain/demo | Rejected — both required |
| Weekly only | Fewer runs | Misses daily operational pattern | Rejected — both required |
| Separate extract codebases per cadence | Clear schedules | Duplicated logic, drift | Rejected |
| CDC for daily/weekly schedules | One pipeline story | Wrong tool; weakens semantics demo | Rejected |
| Full rebuild every run (no watermark) | Simple logic | Costly; poor idempotent incremental story | Rejected as primary strategy |

## Consequences and trade-offs

- Idempotent re-runs and backfills must be proven for both cadences.
- Docs and demos must keep the three update patterns distinguishable.

## Evidence / validation

Daily half (P3-04), 2026-11-20, recorded in [Local Airflow](../architecture/airflow-local.md) and [`docs/status-log.md`](../status-log.md): `ingest_postgres_oltp_daily` (`CronDataIntervalTimetable("0 0 * * *", "UTC")`, catchup off) passed its data interval to the P1-06 extract. A scheduled run, a manual run, and a one-day backfill for `[2026-10-17, 2026-10-18)` finished `success`. The backfill landed 2/1/1/1 rows, equal to `COUNT(*)` on `oltp` for that window. Each run triggered `transform_lakehouse`, which finished `success` (PASS=287). (The daily DAG has run with catchup on since 2026-11-20 05:20Z.)

Weekly half (P3-05), 2026-11-21, recorded in [Local Airflow](../architecture/airflow-local.md#weekly-postgresql-re-extract-p3-05) and [`docs/status-log.md`](../status-log.md). Grain and purpose: `ingest_postgres_oltp_weekly` (`CronDataIntervalTimetable("0 0 * * 1", "UTC")`, catchup on) re-extracts the trailing seven days `[W-8, W-1)` through the same extract. It does not produce a separate weekly grain. It is the safety net for late commits, rows whose `updated_at` falls in a day the daily run already read. A late seller missed by the daily window reached Silver after a manual weekly run, the row the daily run had landed stayed one row, and a cleared re-run changed no count.

## Revisit when

Business grain for weekly differs enough to need a separate extract strategy (document the fork).
