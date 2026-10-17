# ADR-0013: Data contracts and invalid-record handling

| Field | Value |
| --- | --- |
| Status | `proposed` |
| Date opened | 2026-10-15 |
| Date decided | — |
| Owner | Platform architect |
| Index ID | D-013 |
| Related issues | Phase 1 (`P1-02`, `P1-03`, `P1-10`), Phase 3 quarantine (`P3-07`) |
| Supersedes | — |
| Superseded by | — |

## Context / problem

Ingestion from multiple sources will produce nulls, duplicates and invalid schemas. Silent drops destroy auditability and make reconciliation impossible.

## Decision

Define **explicit data contracts** (expected fields, types, null rules) under `config/contracts/` (or agreed path). Record ingestion metadata (`_ingestion_ts`, source id, execution id). Handle invalid records intentionally: quarantine and/or fail with documented behavior—**never drop silently**. Schema evolution must be documented when it occurs.

## Alternatives considered

| Option | Pros | Cons | Why not (if rejected) |
| --- | --- | --- | --- |
| Contracts + explicit quarantine/fail (chosen) | Auditable; testable failure paths | More engineering up front | — |
| Schema-on-read only (no contracts) | Fast to start | Ambiguous quality; weak tests | Rejected as sole approach |
| Fail entire batch on any bad row | Strict | Fragile for partial source issues | Allowed only where contract says so |
| No contracts until Gold | Defer work | Corrupt Silver and weak lineage of quality | Rejected |

## Consequences and trade-offs

- Phase 1 writers and Phase 3 quarantine share the same contract language.
- Tests must cover failure paths, not only happy path.

## Evidence / validation

None yet — Phase 1 contracts and tests.

## Revisit when

A source cannot meet contract richness; document a weaker contract rather than inventing fields.
