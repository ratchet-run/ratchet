<script setup lang="ts">
import { ref, onMounted } from 'vue'
import { codeToHtml } from 'shiki'

const code = `@Inject
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

const html = ref('')

onMounted(async () => {
  html.value = await codeToHtml(code, {
    lang: 'java',
    themes: {
      light: 'github-light',
      dark: 'dracula',
    },
    defaultColor: false,
  })
})
</script>

<template>
  <div class="hero-code" v-html="html"></div>
</template>

<style scoped>
.hero-code {
  width: 100%;
  max-width: 520px;
  margin: 0 auto;
}

.hero-code :deep(.shiki) {
  margin: 0;
  padding: 1.35rem;
  border: 1px solid rgba(15, 23, 42, 0.16);
  border-radius: 12px;
  box-shadow: 0 20px 60px rgba(31, 41, 51, 0.18);
  font-size: 0.92rem;
  line-height: 1.62;
  overflow-x: auto;
}

.hero-code :deep(.shiki code) {
  display: block;
}

/* Dual-theme: swap colors via .dark on html.
   Shiki with defaultColor:false emits --shiki-light/--shiki-dark vars.
   Background goes only on the <pre>; per-span only color. */
html:not(.dark) .hero-code :deep(.shiki) {
  background-color: var(--shiki-light-bg) !important;
}
html:not(.dark) .hero-code :deep(.shiki),
html:not(.dark) .hero-code :deep(.shiki span) {
  color: var(--shiki-light) !important;
}

html.dark .hero-code :deep(.shiki) {
  background-color: var(--shiki-dark-bg) !important;
  border-color: rgba(255, 255, 255, 0.08);
  box-shadow: 0 24px 64px rgba(0, 0, 0, 0.45);
}
html.dark .hero-code :deep(.shiki),
html.dark .hero-code :deep(.shiki span) {
  color: var(--shiki-dark) !important;
}

@media (max-width: 640px) {
  .hero-code { display: none; }
}
</style>
