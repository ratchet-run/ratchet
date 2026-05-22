<script setup lang="ts">
// The HTML is pre-rendered through Shiki by heroCodePlugin.ts at build time
// (see docs/.vitepress/heroCodePlugin.ts). Importing the virtual module here
// keeps Shiki out of the client bundle and gives the hero code its final
// markup during SSR so there is no flash of empty content on hydration.
import html from 'virtual:ratchet-hero-code'
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
