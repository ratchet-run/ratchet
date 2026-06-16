// Accessibility scanner for the built VitePress site.
//
// Serves docs/.vitepress/dist over a throwaway HTTP server and runs axe-core
// against every built page in BOTH the light and dark themes, because the two
// themes have independent colour palettes and a contrast failure in one is
// invisible in the other. Fails (exit 1) if axe reports any WCAG 2 A/AA
// violation; prints `incomplete` results separately for manual review without
// failing, since axe flags those when it cannot decide on its own (most often
// contrast computed through a translucent fixed header).
//
// Usage: node a11y/scan.mjs            (scan everything, both themes)
//        node a11y/scan.mjs --json out.json
//
// Assumes the site is already built (`npm run build`).

import { createServer } from 'node:http'
import { readFile, readdir, stat } from 'node:fs/promises'
import { join, extname, relative, dirname } from 'node:path'
import { fileURLToPath } from 'node:url'
import { chromium } from 'playwright'
import { AxeBuilder } from '@axe-core/playwright'

const here = dirname(fileURLToPath(import.meta.url))
const DIST = join(here, '..', 'docs', '.vitepress', 'dist')
const THEMES = ['light', 'dark']
const WCAG_TAGS = ['wcag2a', 'wcag2aa', 'wcag21a', 'wcag21aa']
const CONCURRENCY = 4

// Generated Javadoc is third-party HTML we do not author or control, so it is
// out of scope for this gate. 404.html is never linked.
const EXCLUDE = [/^javadoc\//, /^404\.html$/]

// Documented, justified exceptions. Keep this list short, cite why, and prefer
// fixing the markup over adding an entry. Suppressed counts are always printed.
const ALLOWLIST = [
  {
    rule: 'nested-interactive',
    match: /\.collapsible.*\.item\[role="button"\]/,
    why: 'VitePress default-theme sidebar renders a collapsible group title as role="button" wrapping its link. Upstream component, not ours.',
  },
]
const isAllowed = (id, target) => ALLOWLIST.some((a) => a.rule === id && a.match.test(target))

const MIME = {
  '.html': 'text/html; charset=utf-8',
  '.js': 'text/javascript; charset=utf-8',
  '.css': 'text/css; charset=utf-8',
  '.json': 'application/json; charset=utf-8',
  '.svg': 'image/svg+xml',
  '.png': 'image/png',
  '.jpg': 'image/jpeg',
  '.jpeg': 'image/jpeg',
  '.ico': 'image/x-icon',
  '.woff': 'font/woff',
  '.woff2': 'font/woff2',
  '.map': 'application/json; charset=utf-8',
}

async function walkHtml(dir, base = dir) {
  const out = []
  for (const entry of await readdir(dir, { withFileTypes: true })) {
    const full = join(dir, entry.name)
    if (entry.isDirectory()) {
      out.push(...(await walkHtml(full, base)))
    } else if (extname(entry.name) === '.html') {
      out.push(relative(base, full))
    }
  }
  return out
}

// Resolve a request path to a file inside dist, trying VitePress-style
// fallbacks so client-side asset and page requests both work.
async function resolveFile(pathname) {
  const clean = decodeURIComponent(pathname.split('?')[0])
  const candidates = [clean, `${clean}.html`, join(clean, 'index.html')]
  for (const c of candidates) {
    const p = join(DIST, c)
    if (!p.startsWith(DIST)) continue // no traversal out of dist
    try {
      if ((await stat(p)).isFile()) return p
    } catch {
      /* try next */
    }
  }
  return null
}

function startServer() {
  const server = createServer(async (req, res) => {
    const file = await resolveFile(new URL(req.url, 'http://localhost').pathname)
    if (!file) {
      res.writeHead(404)
      res.end('not found')
      return
    }
    res.writeHead(200, { 'content-type': MIME[extname(file)] ?? 'application/octet-stream' })
    res.end(await readFile(file))
  })
  return new Promise((resolve) => {
    server.listen(0, '127.0.0.1', () => {
      const { port } = server.address()
      resolve({ server, base: `http://127.0.0.1:${port}` })
    })
  })
}

async function scanTheme(browser, base, routes, theme) {
  const context = await browser.newContext()
  // Set the theme the way a real visitor would: VitePress reads this key in an
  // inline pre-paint script, so seeding it before navigation applies the right
  // `.dark` class with no flash and no hydration race (Chromium runs init
  // scripts before page scripts).
  await context.addInitScript(
    ([key, val]) => window.localStorage.setItem(key, val),
    ['vitepress-theme-appearance', theme],
  )

  const findings = []
  const queue = [...routes]

  async function worker() {
    const page = await context.newPage()
    for (let route = queue.shift(); route; route = queue.shift()) {
      await page.goto(`${base}/${route}`, { waitUntil: 'networkidle' })
      const isDark = await page.evaluate(() => document.documentElement.classList.contains('dark'))
      if (isDark !== (theme === 'dark')) {
        throw new Error(`theme mismatch on ${route}: wanted ${theme}, page isDark=${isDark}`)
      }
      const results = await new AxeBuilder({ page }).withTags(WCAG_TAGS).analyze()
      findings.push({ route, theme, violations: results.violations, incomplete: results.incomplete })
    }
    await page.close()
  }

  await Promise.all(Array.from({ length: CONCURRENCY }, worker))
  await context.close()
  return findings
}

function summarize(nodes) {
  return nodes.flatMap((v) =>
    v.nodes.map((n) => ({
      id: v.id,
      impact: v.impact,
      help: v.help,
      target: n.target.join(' '),
      detail: (n.failureSummary ?? '').replace(/\s+/g, ' ').trim().slice(0, 200),
    })),
  )
}

async function main() {
  const routes = (await walkHtml(DIST)).filter((r) => !EXCLUDE.some((re) => re.test(r))).sort()
  if (routes.length === 0) {
    console.error(`No built HTML found under ${DIST}. Run "npm run build" first.`)
    process.exit(2)
  }
  console.log(`Scanning ${routes.length} pages × ${THEMES.length} themes (${THEMES.join(', ')})\n`)

  const { server, base } = await startServer()
  const browser = await chromium.launch()
  const all = []
  try {
    for (const theme of THEMES) {
      all.push(...(await scanTheme(browser, base, routes, theme)))
    }
  } finally {
    await browser.close()
    server.close()
  }

  const violations = []
  const incomplete = []
  let suppressed = 0
  for (const f of all) {
    for (const row of summarize(f.violations)) {
      if (isAllowed(row.id, row.target)) {
        suppressed++
        continue
      }
      violations.push({ route: f.route, theme: f.theme, ...row })
    }
    for (const row of summarize(f.incomplete)) incomplete.push({ route: f.route, theme: f.theme, ...row })
  }

  if (suppressed) {
    console.log(`\n${suppressed} finding(s) suppressed by the documented allowlist:`)
    for (const a of ALLOWLIST) console.log(`  · ${a.rule}: ${a.why}`)
  }

  const jsonFlag = process.argv.indexOf('--json')
  if (jsonFlag !== -1 && process.argv[jsonFlag + 1]) {
    const { writeFile } = await import('node:fs/promises')
    await writeFile(process.argv[jsonFlag + 1], JSON.stringify({ violations, incomplete }, null, 2))
  }

  if (incomplete.length) {
    console.log(`\n${incomplete.length} incomplete result(s) — review manually, not failing the build:`)
    const byRule = new Map()
    for (const i of incomplete) byRule.set(i.id, (byRule.get(i.id) ?? 0) + 1)
    for (const [id, n] of byRule) console.log(`  · ${id}: ${n}`)
  }

  if (violations.length) {
    console.log(`\n✗ ${violations.length} accessibility violation(s):\n`)
    for (const v of violations) {
      console.log(`  [${v.theme}] ${v.route}`)
      console.log(`    ${v.id} (${v.impact}) — ${v.target}`)
      console.log(`    ${v.detail}\n`)
    }
    process.exit(1)
  }

  console.log('\n✓ No accessibility violations in either theme.')
}

main().catch((err) => {
  console.error(err)
  process.exit(2)
})
