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

// A trimmed variant for phones. The full snippet's long lines force a
// horizontal scroll on a narrow column, so this keeps the same story — inject
// the service, submit a method reference, retry, done — in lines short enough
// to read without scrolling or shrinking the type to a squint.
const HERO_CODE_MOBILE = `@Inject
JobSchedulerService scheduler;

scheduler
    .enqueue(() -> ship(orderId))
    .withMaxRetries(3)
    .submit();`

const SNIPPETS: Record<string, string> = {
  'virtual:ratchet-hero-code': HERO_CODE,
  'virtual:ratchet-hero-code-mobile': HERO_CODE_MOBILE,
}

const cache: Record<string, string> = {}

export function heroCodePlugin(): Plugin {
  return {
    name: 'ratchet:hero-code',
    resolveId(id) {
      if (id in SNIPPETS) return '\0' + id
    },
    async load(id) {
      if (!id.startsWith('\0')) return
      const key = id.slice(1)
      const code = SNIPPETS[key]
      if (!code) return
      if (cache[key] === undefined) {
        cache[key] = await codeToHtml(code, {
          lang: 'java',
          themes: { light: 'github-light', dark: 'dracula' },
          defaultColor: false,
        })
      }
      return `export default ${JSON.stringify(cache[key])}`
    },
  }
}
