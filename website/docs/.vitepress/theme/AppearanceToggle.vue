<script setup lang="ts">
import { ref, onMounted, onBeforeUnmount } from 'vue'

type Mode = 'auto' | 'light' | 'dark'

const KEY = 'vitepress-theme-appearance'
const mode = ref<Mode>('auto')

function applyMode(next: Mode) {
  mode.value = next
  localStorage.setItem(KEY, next)
  const prefersDark = window.matchMedia('(prefers-color-scheme: dark)').matches
  const wantsDark = next === 'dark' || (next === 'auto' && prefersDark)
  document.documentElement.classList.toggle('dark', wantsDark)
}

function cycle() {
  const order: Mode[] = ['auto', 'light', 'dark']
  const next = order[(order.indexOf(mode.value) + 1) % order.length]
  applyMode(next)
}

let mql: MediaQueryList | null = null
const onOsChange = (e: MediaQueryListEvent) => {
  if (mode.value !== 'auto') return
  document.documentElement.classList.toggle('dark', e.matches)
}

function readStoredMode(): Mode {
  const raw = localStorage.getItem(KEY)
  return raw === 'light' || raw === 'dark' || raw === 'auto' ? raw : 'auto'
}

onMounted(() => {
  // Reapply on mount so the DOM .dark class and the toggle UI are guaranteed
  // to agree with localStorage, even if a legacy or unknown value was stored.
  applyMode(readStoredMode())
  mql = window.matchMedia('(prefers-color-scheme: dark)')
  mql.addEventListener('change', onOsChange)
})

onBeforeUnmount(() => {
  mql?.removeEventListener('change', onOsChange)
})

const labels: Record<Mode, string> = {
  auto: 'System (auto)',
  light: 'Light',
  dark: 'Dark',
}

const nextLabel = () => {
  const order: Mode[] = ['auto', 'light', 'dark']
  return labels[order[(order.indexOf(mode.value) + 1) % order.length]]
}
</script>

<template>
  <button
    class="ratchet-appearance"
    :class="`mode-${mode}`"
    type="button"
    :title="`Theme: ${labels[mode]} — click for ${nextLabel()}`"
    :aria-label="`Theme: ${labels[mode]} — click for ${nextLabel()}`"
    @click="cycle"
  >
    <!-- Auto: half-filled circle -->
    <svg v-if="mode === 'auto'" viewBox="0 0 24 24" width="20" height="20" aria-hidden="true">
      <circle cx="12" cy="12" r="9" fill="none" stroke="currentColor" stroke-width="1.6" />
      <path d="M12 3 A9 9 0 0 1 12 21 Z" fill="currentColor" />
    </svg>
    <!-- Light: sun -->
    <svg v-else-if="mode === 'light'" viewBox="0 0 24 24" width="20" height="20" aria-hidden="true">
      <circle cx="12" cy="12" r="4" fill="currentColor" />
      <g stroke="currentColor" stroke-width="1.6" stroke-linecap="round">
        <line x1="12" y1="2.5" x2="12" y2="5" />
        <line x1="12" y1="19" x2="12" y2="21.5" />
        <line x1="2.5" y1="12" x2="5" y2="12" />
        <line x1="19" y1="12" x2="21.5" y2="12" />
        <line x1="5.2" y1="5.2" x2="7" y2="7" />
        <line x1="17" y1="17" x2="18.8" y2="18.8" />
        <line x1="5.2" y1="18.8" x2="7" y2="17" />
        <line x1="17" y1="7" x2="18.8" y2="5.2" />
      </g>
    </svg>
    <!-- Dark: moon -->
    <svg v-else viewBox="0 0 24 24" width="20" height="20" aria-hidden="true">
      <path
        d="M20.5 13.6A8.5 8.5 0 0 1 10.4 3.5 8.5 8.5 0 1 0 20.5 13.6Z"
        fill="currentColor"
      />
    </svg>
  </button>
</template>

<style scoped>
.ratchet-appearance {
  width: 36px;
  height: 36px;
  display: inline-flex;
  align-items: center;
  justify-content: center;
  border-radius: 8px;
  color: var(--vp-c-text-2);
  background: transparent;
  border: 1px solid transparent;
  cursor: pointer;
  transition: color 0.2s, background-color 0.2s, border-color 0.2s;
}

.ratchet-appearance:hover {
  color: var(--vp-c-text-1);
  background: var(--vp-c-default-soft);
}

.ratchet-appearance.mode-auto { color: var(--vp-c-brand-1); }
.ratchet-appearance.mode-light { color: var(--vp-c-brand-1); }
.ratchet-appearance.mode-dark  { color: var(--vp-c-brand-1); }
</style>
