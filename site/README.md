# Platform report (Starlight, brand theme)

Astro [Starlight](https://starlight.astro.build/) site themed with the author's brand: the violet and pink palette and the Urbanist font from `filipeoliveira-hub-assets/brand/`.

**Purpose:** multi-page platform report (Home, Architecture, Pipeline, Operations, Reference). Content uses only facts verified in `PROJECT_STATUS.md`. Site v1 (Phase 9, P9-05) hosts it on GitHub Pages; from Phase 4 on, each phase's site update issue adds that phase's verified results (`ROADMAP.md` → growth scheme).

## Prerequisites

- Node.js 22.12+ (22.22+ recommended for current Astro engine ranges)
- npm 9+

On this project's WSL setup, Node is installed under `~/.local/node-v22.14.0-linux-x64` with `node` / `npm` / `npx` linked into `~/.local/bin` (already on `PATH` via `~/.local/bin/env` in `.bashrc`). Open a **new** terminal after linking, or run `hash -r` / `source ~/.bashrc` if `npm` is still not found.

Check:

```bash
command -v npm && node -v && npm -v
```

## Preview

From this directory, install the lockfile and start the dev server:

```bash
npm ci
npm run dev
```

Open the URL printed in the terminal (usually `http://localhost:4321/open-data-platform/`, because of the Pages base).

## Build

```bash
npm run build
npm run preview
```

Static output is written to `dist/`. The site is built for the GitHub Pages base `/open-data-platform`, so preview it at `http://localhost:4321/open-data-platform/`. Check the links under that base from the repository root:

```bash
python3 scripts/check_site_links.py site/dist /open-data-platform/
```

## Hosting

The site is published on GitHub Pages at <https://filipeoliveira94.github.io/open-data-platform/>.

- `astro.config.mjs` sets `site` (`https://filipeoliveira94.github.io`) and `base` (`/open-data-platform`), so every page and asset link carries the project path and the build writes `sitemap-index.xml`. `tests/unit/site/test_pages_hosting.py` fails when they drift from the README link.
- [`.github/workflows/pages.yml`](../.github/workflows/pages.yml) runs on every pull request, every push to `main`, and by hand (`workflow_dispatch`). The `build` job installs the lockfile with Node 22, runs `npm run build`, and runs `scripts/check_site_links.py`, which fails the build when a link falls outside the base or points at a file missing from `dist/`.
- Only `main` deploys: the upload and the `deploy` job are skipped on pull requests and other branches. Only the `deploy` job can write to Pages (`pages: write`, `id-token: write`); the rest of the workflow reads the repository. It uses no secrets. One deploy runs at a time and is never cancelled halfway.
- There is no path filter, so every merge to `main` rebuilds and redeploys the whole site. Later site update issues (P4-11, P5-10, P6-10, P8-08, P10-13, P11-19) redeploy by merging to `main`; no extra step.
- One-time repository settings, made by the owner: Settings → Pages → Source set to **GitHub Actions** (without it, the deploy job fails and nothing is published; enable it and re-run the workflow), and About → Website set to the Pages URL.

## Content

| Path | Page |
| --- | --- |
| `src/content/docs/index.mdx` | Home |
| `src/content/docs/architecture.mdx` | Architecture |
| `src/content/docs/pipeline.mdx` | Pipeline |
| `src/content/docs/operations.md` | Operations |
| `src/content/docs/reference.md` | Reference |

## Diagrams

Architecture shows two diagrams and Pipeline one (quality gate and quarantine) through `src/components/DiagramFigure.astro`, which imports the light and dark PNGs from `docs/assets/diagrams/` so docs and site share one copy. To change a diagram, edit the facts in `docs/architecture/` first, then `docs/assets/diagrams/build_diagrams.py`, run it, and export the PNGs with the diagram-design skill's `export_png.py` (scale 2). `tests/unit/site/` fails when the HTML is stale or a page stops importing from `docs/`.

## Architecture map

The home page opens with a 3D map of the target architecture (not a status view; the Home status table says what is verified): `src/components/ArchitectureScene.astro` (markup, styles, accessible text list) and `src/components/architecture-scene/` (`data.ts` holds the components and flows, `scene.ts` is a dependency-free canvas renderer). To add a component or a flow, edit `data.ts` only.

- Brand palette (dark tokens from `filipeoliveira-hub-assets/brand/tokens.css`) and the Urbanist font, self-hosted under `src/assets/fonts/` (SIL OFL 1.1, `OFL.txt`).
- Drag orbits the camera; hover or tap shows a component card; the layer chips highlight a group.
- The animation pauses off-screen and in a hidden tab, and renders one still frame under `prefers-reduced-motion`.

## Theme

- `src/styles/brand.css` maps the `--fi-*` tokens of `filipeoliveira-hub-assets/brand/tokens.css` onto Starlight's `--sl-color-*` variables, light and dark, and loads Urbanist for the whole site. Body, muted and accent text pass WCAG AA; body and accent text on the page background measure at least 7:1. Pink appears only in the Fi mark.
- `src/assets/brand/fi-mark-{light,dark}.svg` is the header logo and `public/favicon.svg` the favicon, both copied from the brand repo. Copy them again when the brand changes.
- `src/components/SocialIcons.astro` overrides Starlight's header links to add a visible Portfolio link (`https://filipeoliveira-hub.com.br`) before the GitHub icon.

## Content rules

- Only facts recorded as **verified** in `PROJECT_STATUS.md` are stated as delivered.
- Site v1 content and GitHub Pages hosting are **Phase 9**. Later results arrive through each phase's site update issue (P4-11, P5-10, P6-10, P8-08, P10-13, P11-19).
- P8-08 exports figures 1–10 and P5-10 adds figure 11; the phase's live run checks them. This scaffold does not invent charts. The figure list is the site figure table in `ROADMAP.md`.
- This tree does not replace repository docs as the source of truth.
