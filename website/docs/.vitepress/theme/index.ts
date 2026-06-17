import { h } from 'vue'
import type { Theme } from 'vitepress'
import DefaultTheme from 'vitepress/theme'
import HeroCode from './HeroCode.vue'
import HeroInfoTop from './HeroInfoTop.vue'
import HeroTrustStrip from './HeroTrustStrip.vue'
import HeroStarButton from './HeroStarButton.vue'
import HomeUseCasesBand from './HomeUseCasesBand.vue'
import HomeDocsBand from './HomeDocsBand.vue'
import ComparisonMatrix from './ComparisonMatrix.vue'
import SiteFooter from './SiteFooter.vue'
import AppearanceToggle from './AppearanceToggle.vue'
import { installUmamiTracking, trackAnchorView, trackNotFoundIfApplicable } from './umamiTracking'
import './custom.css'

export default {
  extends: DefaultTheme,
  Layout: () => {
    return h(DefaultTheme.Layout, null, {
      'home-hero-info-before': () => h(HeroInfoTop),
      'home-hero-image': () => h(HeroCode),
      'home-hero-info-after': () => h(HeroCode, { compact: true, class: 'hero-code-mobile' }),
      'home-hero-actions-after': () => h(HeroStarButton),
      'home-hero-after': () => h(HeroTrustStrip),
      'home-features-after': () => [h(HomeUseCasesBand), h(HomeDocsBand)],
      'layout-bottom': () => h(SiteFooter),
      'nav-bar-content-after': () => h(AppearanceToggle),
    })
  },
  enhanceApp({ app, router }) {
    app.component('HomeDocsBand', HomeDocsBand)
    app.component('ComparisonMatrix', ComparisonMatrix)
    if (typeof window !== 'undefined') {
      installUmamiTracking()
      // Track hash navigations (anchor-view) on client-side route changes.
      router.onAfterRouteChange = (to) => {
        try {
          const url = new URL(to, window.location.origin)
          trackAnchorView(url.pathname, url.hash)
          trackNotFoundIfApplicable(url.pathname)
        } catch {
          /* noop */
        }
      }
    }
  },
} satisfies Theme
