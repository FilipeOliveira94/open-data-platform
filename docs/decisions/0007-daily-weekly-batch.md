# ADR-0007: Daily and weekly batch

| Field | Value |
| --- | --- |
| Status | `proposed` |
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

None yet — Phase 1 extract + Phase 3 schedules.

## Revisit when

Business grain for weekly differs enough to need a separate extract strategy (document the fork).
