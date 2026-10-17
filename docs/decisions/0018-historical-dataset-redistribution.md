# ADR-0018: Historical Olist datasets — no vendor, fetch-from-source

| Field | Value |
| --- | --- |
| Status | approved |
| Date opened | 2026-10-17 |
| Date decided | 2026-10-17 |
| Owner | Program |
| Index ID | D-018 |
| Related issues | P1-01 (blocks P1-04) |
| Supersedes | — |
| Superseded by | — |

## Context / problem

Phase 1 needs a historical e-commerce source. The planned datasets are published by Olist on Kaggle:

- [Brazilian E-Commerce Public Dataset by Olist](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce)
- [Marketing Funnel by Olist](https://www.kaggle.com/datasets/olistbr/marketing-funnel-olist) (joins on `seller_id`)

Both list **CC BY-NC-SA 4.0**. The repository software is **MIT**. Vendoring the CSVs (or derived dumps) into a public MIT tree would mix redistribution terms and risk unauthorized public redistribution of NonCommercial / ShareAlike material. Contributors also must not commit private or large data artifacts.

## Decision

1. Record license and provenance under [`docs/sources/`](../sources/).
2. **Do not** commit Olist dataset binaries or redistributable derived dumps.
3. Obtain data **locally** via the official Kaggle channel (fetch-from-source); store under gitignored paths such as `data/raw/`.
4. Treat both Olist datasets as one historical source family for Phase 1 planning; join details are specified later in contracts/ingestion (P1-03 / P1-04).
5. Unit tests must use synthetic fixtures, not copies of Olist files.

## Alternatives considered

| Option | Pros | Cons | Why not (if rejected) |
| --- | --- | --- | --- |
| Vendor CSVs in git (LFS or raw) | Offline clone works | CC BY-NC-SA vs MIT; public redistribution; repo bloat | Rejected |
| Fetch-from-source + gitignore | License-safe; auditable provenance | Requires Kaggle auth for developers | **Chosen** |
| Replace Olist with a more permissive dataset | Simpler redistribution | Loses planned domain and join story | Not needed; NC use locally is acceptable for this program |

## Consequences and trade-offs

- P1-04 must implement configurable local paths and/or a documented download step; CI unit tests stay offline with fixtures.
- Documentation and the platform report must attribute Olist when describing these sources.
- Commercial redistribution of the licensed material remains out of scope for this repository.

## Evidence / validation

- License listings checked on both Kaggle pages on **2026-10-17** (CC BY-NC-SA 4.0).
- Provenance docs: `docs/sources/olist-brazilian-ecommerce.md`, `docs/sources/olist-marketing-funnel.md`.
- `.gitignore` already excludes `data/raw/`, `data/private/`, `*.parquet`, `*.csv.gz`.
- Verification recorded in `PROJECT_STATUS.md` (P1-01) and `docs/status-log.md`.

## Revisit when

- Olist or Kaggle change license or terms.
- A future requirement needs to ship sample data in-repo (would need a different, clearly licensed subset or synthetic data).
- Legal guidance requires a stricter or looser policy.

## Notes

Conservative NonCommercial / ShareAlike reading is intentional. This ADR does not claim commercial rights to the datasets.
