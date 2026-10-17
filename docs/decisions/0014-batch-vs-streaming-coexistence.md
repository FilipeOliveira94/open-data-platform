# ADR-0014: Batch vs streaming coexistence

| Field | Value |
| --- | --- |
| Status | `proposed` |
| Date opened | 2026-10-15 |
| Date decided | — |
| Owner | Platform architect |
| Index ID | D-014 |
| Related issues | Cross-cutting; P1-06, P3-04, P3-05, Phase 4, P4-07 |
| Supersedes | — |
| Superseded by | — |

## Context / problem

Teams often collapse CDC and scheduled loads into one story. This platform must demonstrate **three distinct update patterns** with clear when-to-use guidance.

## Decision

Keep **daily batch**, **weekly batch**, and **streaming CDC** as separate, documented paths. CDC does **not** replace scheduled batch. After CDC is introduced, verify batch paths still work (P4-07). Document lag expectations and reconciliation approach when both paths land overlapping entities.

## Alternatives considered

| Option | Pros | Cons | Why not (if rejected) |
| --- | --- | --- | --- |
| Three distinct patterns (chosen) | Honest ops story; matches roadmap | More pipelines to maintain | — |
| CDC only | One path | Loses cadence/watermark batch demos | Rejected |
| Batch only | Simpler | Misses CDC/streaming requirement | Rejected |
| Dual-write without reconciliation | Looks complete | Silent divergence risk | Rejected |

## Consequences and trade-offs

- D-007 and D-008 implement the paths; this ADR owns the coexistence policy.
- Exactly-once end-to-end remains unclaimed without proof (D-008).

## Evidence / validation

None yet — enforced when Phase 3 and 4 exist.

## Revisit when

Product requirements change lag SLOs; update this ADR and runbooks together.
