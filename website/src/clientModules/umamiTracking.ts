declare global {
  interface Window {
    umami?: {
      track: (name: string, data?: Record<string, unknown>) => void;
    };
  }
}

type RouteUpdateParams = {
  location: {pathname: string; hash: string};
  previousLocation: {pathname: string; hash: string} | null;
};

if (typeof window !== 'undefined') {
  document.addEventListener('click', (e) => {
    const el = e.target as Element | null;
    if (!el) return;

    const copyBtn = el.closest('button[aria-label*="Copy" i]');
    if (copyBtn) {
      const block = copyBtn.closest<HTMLElement>('.theme-code-block');
      const pre = block?.querySelector('pre');
      const langClass = Array.from(pre?.classList ?? []).find((c) =>
        c.startsWith('language-'),
      );
      const language = langClass?.replace('language-', '') ?? 'unknown';
      window.umami?.track('code-copy', {
        language,
        path: window.location.pathname,
      });
      return;
    }

    const link = el.closest('a[href]') as HTMLAnchorElement | null;
    if (!link) return;
    const href = link.getAttribute('href');
    if (!href) return;

    const inSidebar =
      link.classList.contains('menu__link') &&
      !!link.closest('.theme-doc-sidebar-menu');
    if (inSidebar) {
      window.umami?.track('sidebar-click', {
        target: href,
        path: window.location.pathname,
      });
      return;
    }

    if (!/^https?:\/\//i.test(href)) return;
    try {
      const url = new URL(href);
      if (url.host === window.location.host) return;
      window.umami?.track('outbound-click', {
        host: url.host,
        target: url.host + url.pathname,
        path: window.location.pathname,
      });
    } catch {
      // malformed absolute URL — skip silently
    }
  });
}

export function onRouteDidUpdate({
  location,
  previousLocation,
}: RouteUpdateParams) {
  if (typeof window === 'undefined') return;
  if (!location.hash) return;
  if (
    previousLocation &&
    previousLocation.pathname === location.pathname &&
    previousLocation.hash === location.hash
  ) {
    return;
  }
  window.umami?.track('anchor-view', {
    path: location.pathname,
    anchor: location.hash.replace(/^#/, ''),
  });
}
