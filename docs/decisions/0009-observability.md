# ADR-0009: Observability

| Field | Value |
| --- | --- |
| Status | `proposed` |
| Date opened | 2026-10-15 |
| Date decided | — |
| Owner | Platform architect |
| Index ID | D-009 |
| Related issues | Phase 5 (`P5-01`–`P5-03`, `P5-05`–`P5-06`) |
| Supersedes | — |
| Superseded by | — |

## Context / problem

Without metrics and logs tied to **real runs**, operational diagnosis is guesswork. Invented dashboards are not acceptable evidence. Lineage is a separate decision ([ADR-0010](0010-lineage.md)); business dataviz is ([ADR-0017](0017-dataviz.md)).

## Decision

Add **Prometheus** and **Grafana** for local metrics and ops dashboards. On the AWS path, document **Amazon CloudWatch** (and optional third-party APM) as counterparts. Publish a FinOps cost model for the AWS target with explicit assumptions (Cost Explorer / CUR / billing metrics)—never invent spend figures. Pair with structured logging and at least one failure runbook.

## Alternatives considered

| Option | Pros | Cons | Why not (if rejected) |
| --- | --- | --- | --- |
| Prometheus + Grafana locally (chosen) | Open-source; reproducible Compose stack | Self-operated | — |
| Datadog | Polished APM/metrics | Vendor lock-in; weaker offline reproducibility | Optional AWS-path mention, not local core |
| New Relic | Polished APM | Same vendor/reproducibility trade-off | Optional, not local core |
| Amazon CloudWatch | Native AWS logs/metrics/alarms | Not a full local substitute for Prometheus/Grafana | Document as AWS counterpart |
| ELK / OpenSearch stack | Log analytics | Heavy for this program’s Phase 5 scope | Deferred |
| Skip metrics | Less work | Cannot diagnose lag/failures systematically | Rejected |

## Consequences and trade-offs

- Stream-specific metrics may wait until Phase 4 exists.
- Grafana here is for **ops**, not Gold mart BI (see D-017).
- Cost figures must state assumptions and ranges.

## Evidence / validation

None yet — Phase 5.

## Revisit when

Hardware cannot run Prometheus/Grafana alongside the data stack; slim the metric set or use CloudWatch-only on a documented AWS path.
