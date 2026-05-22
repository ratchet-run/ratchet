# Website

The Ratchet documentation site, built with [VitePress](https://vitepress.dev/).

## Installation

```bash
npm install
```

## Local development

```bash
npm run dev
```

Starts the VitePress dev server with hot reload at `http://localhost:5173` by default.

## Build

```bash
npm run build
```

Produces a static site in `docs/.vitepress/dist/`.

## Preview the built site

```bash
npm run preview
```

Serves the built `dist/` locally for a final sanity check before deploying.

## Project layout

- `docs/` — source content
  - `*.md` — pages, grouped by section (getting-started, concepts, api-reference, deployment, advanced, troubleshooting, comparison, conformance)
  - `index.md` — landing page (uses VitePress's `layout: home` plus custom slots)
  - `.vitepress/config.ts` — site title, nav, sidebar, theme config, edit-on-GitHub link
  - `.vitepress/theme/` — custom theme: brand CSS, hero slots, footer, 3-state appearance toggle, Umami click tracking, Vue port of `ComparisonMatrix`
  - `public/` — static assets (images, favicons, manifest); copied verbatim to the root of the built site

## Analytics

Umami pageview + replay tracking is gated on the `UMAMI_HOST` and `UMAMI_SITE_ID` environment variables and only enabled when `NODE_ENV=production`. The two `<script>` tags (`script.js` for events, `recorder.js` for session replay) are injected via `docs/.vitepress/config.ts`. Click-event tracking and anchor-view events are wired up in `docs/.vitepress/theme/umamiTracking.ts`.
