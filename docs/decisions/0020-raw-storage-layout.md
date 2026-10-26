# ADR-0020: Raw storage layout and ingestion metadata

| Field | Value |
| --- | --- |
| Status | `approved` |
| Date opened | 2026-10-18 |
| Date decided | 2026-10-18 |
| Owner | Platform architect |
| Index ID | D-020 |
| Related issues | P1-02 (gates P1-03+ writers); P2-03 consumes raw into Bronze |
| Supersedes | — |
| Superseded by | — |

## Context / problem

Phase 1 writers need a shared place to land auditable payloads and a shared vocabulary for lineage fields. Without conventions, each source invents paths and metadata, breaking idempotent re-runs and Bronze promotion.

Local Kaggle fetches already use gitignored `data/raw/` (ADR-0018). That path is **staging on disk**, not the lakehouse landing zone. Confusing the two would mix license-sensitive downloads with S3-portable layout.

## Decision

1. **Auditable raw** lives in the object store (MinIO locally, S3 on AWS) under a dedicated bucket (env `ODP_RAW_BUCKET`) and key prefix `raw/`.
2. **`data/raw/`** remains local fetch/staging only (gitignored); it is not the source of truth for lakehouse raw.
3. **Object key layout:**
   `raw/{source_id}/{dataset}/dt={YYYY-MM-DD}/execution_id={execution_id}/{object_name}`
4. **Required ingestion metadata** (object user-metadata and package helpers): `source_id`, `execution_id`, `_ingestion_ts` (UTC ISO-8601).
5. **Idempotency:** the same `(source_id, dataset, dt, execution_id, object_name)` resolves to the same key; re-runs **overwrite** that object. Record-level deduplication is out of scope for raw writers.
6. **Payload format** is opaque bytes at write time (CSV, JSON, etc.). Tabular Bronze/Iceberg is Phase 2 (P2-03).

Package helpers under `src/open_data_platform/ingestion/` own path building, metadata, and the minimal raw writer (ADR-0015).

## Alternatives considered

| Option | Pros | Cons | Why not (if rejected) |
| --- | --- | --- | --- |
| Object-store `raw/` + metadata helpers (chosen) | Auditable; MinIO→S3 portable; testable | Needs discipline vs local `data/raw/` | — |
| Local filesystem only as lakehouse raw | Simple on laptop | Diverges from AWS path; weak object semantics | Rejected (conflicts with D-002) |
| Skip raw; land only as Bronze tables | Fewer layers | Loses opaque replay of source bytes | Rejected for Phase 1 audit story |
| Partition only by `execution_id` (no `dt=`) | Shorter keys | Harder date-scoped listing and ops | Rejected; keep Hive-style `dt=` |

## Consequences and trade-offs

- Downstream contracts (P1-03) and source writers (P1-04+) must use these helpers.
- Bronze (P2-03) reads from this layout and promotes metadata into table columns; it does not replace raw.
- Invalid-record quarantine paths remain P1-10 / P3-07; raw does not silently drop bytes.

## Evidence / validation

- Conventions: [`docs/architecture/raw-storage-layout.md`](../architecture/raw-storage-layout.md)
- Package: `src/open_data_platform/ingestion/`
- Unit tests: `tests/unit/ingestion/` (no live MinIO required)

P1-10 (2026-10-26) filled the quarantine path named above: same bucket, prefix `quarantine/`, raw bytes not rewritten to drop rows. Reprocess stays P3-07. This decision stays `approved`.

## Revisit when

A source cannot fit `{source_id}/{dataset}` naming; multi-bucket raw is required for IAM; or Bronze PoC forces a different partition scheme (document and migrate deliberately).
