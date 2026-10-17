# ADR-0002: Object storage

| Field | Value |
| --- | --- |
| Status | `approved` |
| Date opened | 2026-10-15 |
| Date decided | 2026-10-16 |
| Owner | Platform architect |
| Index ID | D-002 |
| Related issues | P0-10, P0-13, P1-02, P7-01 |
| Supersedes | — |
| Superseded by | — |

## Context / problem

Lakehouse layers need durable object storage with the same architectural role locally and in AWS so path conventions and configuration stay portable.

## Decision

Use **MinIO** locally and **Amazon S3** on the AWS path as the object store for raw and table data. Treat them as the same role in docs and configuration (endpoint/credentials abstracted).

**Licensing and container distribution:** MinIO Inc. stopped publishing official public container images in favor of the commercial **AIStor** distribution. The community MinIO server source remains under **AGPLv3**. Using community-built images of that AGPLv3 codebase for local development and verification is acceptable for this program (no AIStor license required for that path). Amazon S3 remains the AWS object-store counterpart (Phase 7).

**Phase 0 image pin:** Official `minio/minio` and anonymous `quay.io/minio/minio` pulls are unavailable as of late 2026. Local Compose uses the community rebuild `coollabsio/minio:RELEASE.2025-10-15T17-29-55Z` (same upstream MinIO RELEASE lineage, AGPLv3) so the S3-compatible local role remains MinIO. Revisit if official distribution returns or another image is explicitly approved.

## Alternatives considered

| Option | Pros | Cons | Why not (if rejected) |
| --- | --- | --- | --- |
| MinIO ↔ Amazon S3 (chosen) | S3 API locally; clear AWS mapping | Must validate MinIO feature gaps if any; image distribution risk | — |
| Local filesystem only | Zero extra services | Diverges from cloud path; weak object-store semantics | Rejected for lakehouse layers |
| HDFS | Familiar big-data store | Heavy ops; poor fit for S3-centric lakehouse + Athena path | Rejected |
| Google Cloud Storage | Strong object store | Out of AWS target architecture for this program | Rejected (wrong cloud) |
| Azure Blob Storage | Strong object store | Out of AWS target architecture | Rejected (wrong cloud) |
| Ceph / RGW | S3-compatible self-hosted | Heavier than MinIO for laptop Compose | Rejected for local path |
| Real S3 from day one | Production-like | Credentials and cost for every clone | Rejected as default |
| Replace MinIO with LocalStack/Garage | Public images available | Changes the local object-store product and D-002 narrative | Deferred unless MinIO images remain unusable |

## Consequences and trade-offs

- Never hard-code secrets; use env/config for endpoints.
- Phase 7 IAM focuses on S3 bucket layout and least privilege.
- Document the community image pin and AGPLv3 / AIStor distinction in `README.md` and Compose comments.
- Prefer community images that track a known upstream RELEASE tag; do not treat AIStor as a required local dependency.

## Evidence / validation

- Compose service and pin: `docker-compose.yml` (`coollabsio/minio:RELEASE.2025-10-15T17-29-55Z`).
- Package helper + unit tests: `src/open_data_platform/object_storage.py`, `tests/unit/test_object_storage.py`.
- Live put/get smoke: `uv run pytest tests/integration/test_minio_smoke.py` → **1 passed** (2026-10-16); container `odp-minio` healthy.

## Revisit when

API incompatibility between MinIO and required S3 features appears in a PoC; or official MinIO container distribution becomes available again (or an alternative image is approved).
