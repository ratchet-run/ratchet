import type { Plugin } from 'vite'
import { codeToHtml } from 'shiki'

// The Java snippet displayed in the home hero. Pre-rendered to HTML at
// build/serve time so the client doesn't load Shiki for one static block.
const HERO_CODE = `@Inject
JobSchedulerService scheduler;

public void placeOrder(UUID orderId) {
    scheduler.enqueue(() -> orders.process(orderId))
        .withMaxRetries(3)
        .withBackoff(
            BackoffPolicy.EXPONENTIAL,
            Duration.ofSeconds(2))
        .withTags("orders", "fulfillment")
        .submit();
}`

const VIRTUAL_ID = 'virtual:ratchet-hero-code'
const RESOLVED_ID = '\0' + VIRTUAL_ID

let cached: string | null = null

export function heroCodePlugin(): Plugin {
  return {
    name: 'ratchet:hero-code',
    resolveId(id) {
      if (id === VIRTUAL_ID) return RESOLVED_ID
    },
    async load(id) {
      if (id !== RESOLVED_ID) return
      if (cached === null) {
        cached = await codeToHtml(HERO_CODE, {
          lang: 'java',
          themes: { light: 'github-light', dark: 'dracula' },
          defaultColor: false,
        })
      }
      return `export default ${JSON.stringify(cached)}`
    },
  }
}
