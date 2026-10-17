# Status log (archive)

Chronological history of program status updates. The operational ledger is [`PROJECT_STATUS.md`](../PROJECT_STATUS.md) (current phase only). Append closed-phase or long narrative entries here when trimming the live ledger; keep `PROJECT_STATUS.md` → Log to recent / current-phase notes.

---

### 2026-10-17 — D-019 sources package reviewed

Owner approved DummyJSON (P1-05), IBGE (P1-08), BrasilAPI CEP optional (P1-09). Alternatives in ADR-0019; `docs/sources/` final choices with standardized metadata; Gold domains in ADR-0012; Phase 1 closing = P1-10/P1-11; Phase 3 daily/weekly = P3-04/P3-05. Validation: ruff PASS; pytest unit 6 passed; no vendored CSVs; field-schema check PASS.

### 2026-10-17 — PROJECT_STATUS ledger restructure

Restructured `PROJECT_STATUS.md` for Phase 1+: current-phase progress only; no cumulative Delivered / Verified / full Open decisions tables. Historical entries moved here. Updated `DECISIONS.md` mirroring rule and related pointers.

### 2026-10-17 — P1-01 historical dataset license / redistribution

Recorded CC BY-NC-SA 4.0 terms for Olist Brazilian e-commerce and Marketing Funnel (join on `seller_id`). Approved D-018 / ADR-0018: do not vendor; fetch-from-source under gitignored `data/raw/`. Docs under `docs/sources/`. No dataset files downloaded or committed.

### 2026-10-16 — Platform report site on Starlight + Catppuccin

Replaced the plain HTML scaffold with Astro Starlight and `@catppuccin/starlight` (preview: starlight-themes.netlify.app/catppuccin). Five pages keep Phase 0 verified content; Pipeline remains a stub. Preview: `cd site && npm run dev` (Node.js 22.12+). No public hosting (Phase 8).

### 2026-10-16 — Platform report site scaffold (Phase 0 content)

Added early `site/` scaffold with Phase 0 verified facts. Roadmap incremental rule updated to allow early scaffold; P8-01 marked in progress.

### 2026-10-16 — Phase 8 platform report site on roadmap

Added Phase 8 as the program close-out: multi-page static report (Home, Architecture, Pipeline, Operations, Reference). Documented incremental growth and P7-07 as in-repo case study feeding the site. Updated `ROADMAP.md`, status ledger, `README.md`, and `docs/project-context-for-cursor.md`.

### 2026-10-16 — Phase 0 verified; pin uv 0.13.1

GitHub Actions CI reported green for lint and unit tests after push. Marked Phase 0 verified. Pinned `uv` to `0.13.1` in `pyproject.toml` (`required-version`) and `.github/workflows/ci.yml` to avoid unpinned “latest” installs.

**Phase 0 acceptance (all verified):** clean clone setup; PostgreSQL healthy; MinIO write/read smoke; lint and tests via documented commands; no secrets/live `.env`/private datasets in VCS; CI runs the same lint and test checks.

**Delivered issues P0-01–P0-13:** see `ROADMAP.md` Phase 0. Evidence included MIT license, `AGENTS.md` / rules, status/roadmap/ADRs, Python 3.12 + `uv`, Ruff/pytest, Compose (PostgreSQL 16 + community MinIO), repo hygiene, GitHub Actions, D-001/D-002 approved.

### 2026-10-16 — Compose runtime verified + D-002 AGPL/AIStor note

Documented that MinIO withdrew official images for AIStor while community MinIO remains AGPLv3 (community images OK). Verified `docker compose up -d`: both services healthy; PostgreSQL reachable; MinIO put/get smoke passed.

### 2026-10-16 — P0-08–P0-13 foundation implementation

Added Python 3.12 package (`uv` + `uv.lock`), Ruff/pytest, Compose (PostgreSQL 16.15-bookworm + community MinIO pin), repo hygiene, README/Makefile, GitHub Actions lint/unit CI, and approved D-001/D-002 with image-distribution notes.

### 2026-10-15 — Decision log restructure (D-001–D-017)

Restructured ADRs: removed Phase-0-services, Kubernetes-standalone, and analytical-placeholder ADRs; split compute vs orchestration and observability vs lineage; added medallion, contracts, batch/streaming coexistence, monorepo, discovery, and dataviz. Index in `DECISIONS.md`; detail in `docs/decisions/` ADR-0001–0017. All remain `proposed` (D-001/D-002 later approved).

### 2026-10-15 — P0-07 decision log (summary + ADRs)

`DECISIONS.md` is a short index (status + one-line summary). Full rationale lives in `docs/decisions/`, with `_template.md` and README.

### 2026-10-15 — AWS path as Infrastructure as Code

Scope alignment: Phase 7 delivers the **AWS deployment path as Terraform IaC**—service mapping (MinIO→S3, catalog→Glue, Trino→Athena), modular resources, OIDC/IAM, cost model and case study—with CI `fmt`/`validate`. Local Compose remains the environment for pipeline development and verification. Cloud apply/destroy stay approval-gated.

### 2026-10-15 — Three update patterns (batch + stream)

Scope review: CDC (Phase 4) covers **streaming** only, not daily/weekly PostgreSQL loads. Updated roadmap and status to require three use cases—daily batch (P1-06 + P3-04), weekly batch (P1-06 + P3-05), streaming CDC (Phase 4)—and added P4-07 batch regression after CDC.

### 2026-10-15 — P0-06 roadmap

Added `ROADMAP.md` with issue-level scope for phases 0–8 (IDs `P0-xx` … `P8-xx`), dependencies, phase and issue acceptance criteria, and out-of-scope notes.

### 2026-10-14 — Phase 0 started

Initialized the public repository and the operating model for the program.

| Commit | Description |
| --- | --- |
| `ab3c9a6` | MIT license |
| `dc49fd8` | `AGENTS.md` and contributor workflow |
| `71e96c6` | Engineering rules by concern |

Also delivered: status ledger and program context under `docs/`.
