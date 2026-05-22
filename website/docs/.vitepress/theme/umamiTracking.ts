// Umami click tracking — adapted from the Docusaurus original at
// website/src/clientModules/umamiTracking.ts. VitePress emits different
// classes (.copy on code blocks, .VPSidebarItem .link in the sidebar) so the
// selectors below target those instead of Docusaurus's.

declare global {
  interface Window {
    umami?: {
      track: (name: string, data?: Record<string, unknown>) => void
    }
  }
}

let installed = false

export function installUmamiTracking() {
  if (typeof window === 'undefined' || installed) return
  installed = true

  document.addEventListener('click', (e) => {
    const el = e.target as Element | null
    if (!el) return

    // Code-block copy button: VitePress renders <button class="copy">
    // inside a <div class="language-java"> (or similar).
    const copyBtn = el.closest<HTMLButtonElement>('button.copy')
    if (copyBtn) {
      const block = copyBtn.closest<HTMLElement>('[class*="language-"]')
      const langClass = Array.from(block?.classList ?? []).find((c) =>
        c.startsWith('language-')
      )
      const language = langClass?.replace('language-', '') ?? 'unknown'
      window.umami?.track('code-copy', {
        language,
        path: window.location.pathname,
      })
      return
    }

    const link = el.closest('a[href]') as HTMLAnchorElement | null
    if (!link) return
    const href = link.getAttribute('href')
    if (!href) return

    // Hero CTA buttons. The Docusaurus home page tagged these with
    // data-umami-event=cta-click; VitePress's home frontmatter doesn't
    // expose data-attributes, so we infer the event from the link target
    // to keep analytics parity with the previous site.
    if (link.classList.contains('VPButton') && link.closest('.VPHero')) {
      const target = inferHeroCtaTarget(href)
      window.umami?.track('cta-click', {
        location: 'hero',
        target,
        path: window.location.pathname,
      })
      return
    }

    // Sidebar click: VitePress sidebar links sit inside .VPSidebarItem
    if (link.closest('.VPSidebarItem')) {
      window.umami?.track('sidebar-click', {
        target: href,
        path: window.location.pathname,
      })
      return
    }

    if (!/^https?:\/\//i.test(href)) return
    try {
      const url = new URL(href)
      if (url.host === window.location.host) return
      window.umami?.track('outbound-click', {
        host: url.host,
        target: url.host + url.pathname,
        path: window.location.pathname,
      })
    } catch {
      // malformed absolute URL — skip silently
    }
  })
}

function inferHeroCtaTarget(href: string): string {
  // Map known hero links back to the same event-target labels the old
  // Docusaurus index.tsx used (data-umami-event-target=...).
  if (href.startsWith('/getting-started/')) return 'get-started'
  if (href.startsWith('/api-reference/')) return 'api-reference'
  return 'unknown'
}

// Anchor-view tracking via VitePress route hash changes.
// Called from theme/index.ts inside a NavigationGuard-style mounted hook.
let lastAnchor = ''
let lastPath = ''
export function trackAnchorView(path: string, hash: string) {
  if (typeof window === 'undefined') return
  if (!hash) return
  if (path === lastPath && hash === lastAnchor) return
  lastPath = path
  lastAnchor = hash
  window.umami?.track('anchor-view', {
    path,
    anchor: hash.replace(/^#/, ''),
  })
}

// 404 tracking. The Docusaurus 404.tsx fired a `not-found` umami event on
// mount. VitePress shows its built-in NotFound layout when no route matches,
// so we detect it via the body class VitePress sets on the 404 page.
let lastNotFoundPath = ''
export function trackNotFoundIfApplicable(path: string) {
  if (typeof window === 'undefined') return
  // Defer to next tick so VitePress has applied page metadata + classes.
  setTimeout(() => {
    const isNotFound =
      document.body.classList.contains('NotFound') ||
      document.querySelector('.NotFound') !== null
    if (!isNotFound) return
    if (path === lastNotFoundPath) return
    lastNotFoundPath = path
    window.umami?.track('not-found', { path })
  }, 0)
}
