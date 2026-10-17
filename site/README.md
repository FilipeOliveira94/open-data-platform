# Platform report (Starlight + Catppuccin)

Astro [Starlight](https://starlight.astro.build/) site themed with
[Catppuccin for Starlight](https://starlight-themes.netlify.app/catppuccin/)
(`@catppuccin/starlight`).

**Purpose:** multi-page platform report (Home, Architecture, Pipeline, Operations, Reference). Content uses only facts verified in `PROJECT_STATUS.md`. Public hosting remains Phase 8.

## Prerequisites

- Node.js 22.12+ (22.22+ recommended for current Astro engine ranges)
- npm 9+

On this project's WSL setup, Node is installed under `~/.local/node-v22.14.0-linux-x64` with `node` / `npm` / `npx` linked into `~/.local/bin` (already on `PATH` via `~/.local/bin/env` in `.bashrc`). Open a **new** terminal after linking, or run `hash -r` / `source ~/.bashrc` if `npm` is still not found.

Check:

```bash
command -v npm && node -v && npm -v
```

## Preview

From this directory:

```bash
npm install
npm run dev
```

Open the URL printed in the terminal (usually `http://localhost:4321/`).

## Build

```bash
npm run build
npm run preview
```

Static output is written to `dist/`.

## Content

| Path | Page |
| --- | --- |
| `src/content/docs/index.mdx` | Home |
| `src/content/docs/architecture.md` | Architecture |
| `src/content/docs/pipeline.md` | Pipeline |
| `src/content/docs/operations.md` | Operations |
| `src/content/docs/reference.md` | Reference |

Theme options live in `astro.config.mjs`: mocha/latte with mauve accent (same as the [Catppuccin kitchen sink](https://starlight-themes.netlify.app/catppuccin/kitchen-sink/)).

## Content rules

- Only facts recorded as **verified** in `PROJECT_STATUS.md` are stated as delivered.
- Design polish and public hosting are **Phase 8**.
- This tree does not replace repository docs as the source of truth.
