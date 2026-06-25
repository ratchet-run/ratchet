<script setup lang="ts">
// Proof strip rendered directly under the hero (home-hero-after slot). It
// turns the "native, therefore portable" claim into evidence: the published
// conformance matrix, the three TCK tiers, and the licensing/runtime facts a
// Jakarta EE evaluator checks before adopting. Numbers mirror the source of
// truth in docs/conformance/index.md — keep them in sync.
const runtimes = ['WildFly', 'WildFly EE 11', 'Open Liberty', 'Payara', 'GlassFish']
const databases = ['MySQL', 'PostgreSQL', 'Oracle', 'MongoDB']

const facts = [
  { label: '15 verified combinations', detail: '5 runtimes × 3 databases' },
  { label: 'Three TCK tiers', detail: 'Store · API · Jakarta Runtime' },
  { label: 'Apache 2.0', detail: 'no paid tier' },
  { label: 'Java 17+', detail: 'Jakarta EE 10 / 11' },
]
</script>

<template>
  <section class="trust-strip">
    <div class="trust-inner">
      <div class="trust-lead">
        <p class="eyebrow">Standards-based and conformance-tested</p>
        <p class="trust-runtimes">
          <span v-for="r in runtimes" :key="r" class="runtime-chip">{{ r }}</span>
          <span class="trust-times" aria-hidden="true">×</span>
          <span v-for="d in databases" :key="d" class="runtime-chip db">{{ d }}</span>
        </p>
      </div>

      <ul class="fact-row">
        <li v-for="f in facts" :key="f.label">
          <strong>{{ f.label }}</strong>
          <small>{{ f.detail }}</small>
        </li>
      </ul>

      <a
        class="trust-cta"
        href="/conformance/"
        data-umami-event="cta-click"
        data-umami-event-location="hero-trust"
        data-umami-event-target="conformance"
      >
        See the conformance matrix →
      </a>
    </div>
  </section>
</template>

<style scoped>
.trust-strip {
  border-top: 1px solid var(--vp-c-divider);
  border-bottom: 1px solid var(--vp-c-divider);
  background: var(--vp-c-bg-soft);
}

.trust-inner {
  width: min(1152px, calc(100% - 48px));
  margin: 0 auto;
  padding: 1.5rem 0;
  display: grid;
  grid-template-columns: 1.1fr 1.4fr auto;
  gap: 1.5rem 2rem;
  align-items: center;
}

.eyebrow {
  margin: 0 0 0.6rem;
  color: #0f766e;
  font-size: 0.78rem;
  font-weight: 700;
  letter-spacing: 0.02em;
  text-transform: uppercase;
}

.dark .eyebrow {
  color: #5eead4;
}

.trust-runtimes {
  margin: 0;
  display: flex;
  flex-wrap: wrap;
  align-items: center;
  gap: 0.4rem;
}

.runtime-chip {
  padding: 0.18rem 0.55rem;
  font-size: 0.82rem;
  font-weight: 600;
  color: var(--vp-c-text-1);
  background: var(--vp-c-bg);
  border: 1px solid var(--vp-c-divider);
  border-radius: 6px;
  white-space: nowrap;
}

.runtime-chip.db {
  color: var(--vp-c-text-2);
}

.trust-times {
  /* text-2, not text-3: the muted text-3 measured ~2.5:1 on the soft strip
     background, under the WCAG AA floor. aria-hidden in the template keeps this
     decorative separator out of the screen-reader line. */
  color: var(--vp-c-text-2);
  font-weight: 700;
  padding: 0 0.15rem;
}

.fact-row {
  list-style: none;
  margin: 0;
  padding: 0;
  display: grid;
  grid-template-columns: repeat(2, minmax(0, 1fr));
  gap: 0.6rem 1.25rem;
}

.fact-row li {
  display: flex;
  flex-direction: column;
  line-height: 1.25;
}

.fact-row strong {
  font-size: 0.92rem;
  color: var(--vp-c-text-1);
}

.fact-row small {
  font-size: 0.8rem;
  color: var(--vp-c-text-2);
}

.trust-cta {
  justify-self: end;
  font-size: 0.9rem;
  font-weight: 600;
  white-space: nowrap;
  color: var(--vp-c-brand-1);
}

.trust-cta:hover {
  text-decoration: underline;
}

@media (max-width: 960px) {
  .trust-inner {
    grid-template-columns: 1fr;
    gap: 1.25rem;
  }
  .trust-cta {
    justify-self: start;
  }
}
</style>
