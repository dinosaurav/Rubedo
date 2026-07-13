# marketing/

The Rubedo landing page, deployed to GitHub Pages at
https://dinosaurav.github.io/Rubedo/ (see
[`../.github/workflows/pages.yml`](../.github/workflows/pages.yml)): a Vite +
React app. Content must stay in sync with the root
[`README.md`](../README.md) — that file is the source of truth for what
Rubedo does; this page restates it, never invents beyond it.

No custom domain yet, so the site is served under the `/Rubedo/` project-page
subpath — see the `base` comment in [`vite.config.js`](vite.config.js) for
what to change if that changes.

## Develop

```bash
npm install
npm run dev        # dev server with HMR, http://localhost:5173
npm run lint        # oxlint
```

## Build

```bash
npm run build       # vite build only — the landing page
npm run build:all   # vite build + `uv run mkdocs build` from the repo root
```

The MkDocs documentation site (built from [`../docs/`](../docs/) via
[`../mkdocs.yml`](../mkdocs.yml)) builds **separately** and outputs into
`dist/docs/` (`site_dir: marketing/dist/docs` in `mkdocs.yml`), so the two
sites end up served from one `dist/` directory: the landing page at `/` and
the docs at `/docs/`.

**Important:** Vite's default `emptyOutDir: true` would wipe the entire
`outDir` — including `dist/docs/` — on every `vite build`. That's disabled
in [`vite.config.js`](vite.config.js) (`build.emptyOutDir: false`), so a
plain `npm run build` is safe to run even when `dist/docs/` already exists
from a previous MkDocs build. The trade-off: `vite build` no longer cleans
its own stale output. If you need a fully clean rebuild of both sites:

```bash
rm -rf dist
npm run build:all
```

For a full production deploy, run `npm run build:all` (or `npm run build`
followed by `uv run mkdocs build` from the repo root) so both `dist/` and
`dist/docs/` are current.
