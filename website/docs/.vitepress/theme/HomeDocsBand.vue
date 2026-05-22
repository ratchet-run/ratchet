<script setup lang="ts">
const docs = [
  {
    title: 'Install Ratchet',
    href: '/getting-started/installation',
    description: 'Import the BOM, choose a store, apply schema, and produce runtime options.',
  },
  {
    title: 'Run a first job',
    href: '/getting-started/quickstart',
    description: 'A short path from CDI injection to an executed persisted job.',
  },
  {
    title: 'Check the API',
    href: '/api-reference/overview',
    description: 'Current public API, SPI, events, query service, and builder references.',
  },
  {
    title: 'Review conformance',
    href: '/conformance/',
    description: 'Store, API, and Jakarta runtime compatibility reports.',
  },
]
</script>

<template>
  <section class="docs-band">
    <div class="docs-inner">
      <div>
        <p class="eyebrow">Documentation paths</p>
        <h2>Start from the job you need to do.</h2>
      </div>
      <div class="doc-grid">
        <a
          v-for="d in docs"
          :key="d.title"
          :href="d.href"
          class="doc-link"
        >
          <span>{{ d.title }}</span>
          <small>{{ d.description }}</small>
        </a>
      </div>
    </div>
  </section>
</template>

<style scoped>
.docs-band {
  background: var(--vp-c-bg-soft);
  border-top: 1px solid var(--vp-c-divider);
}

.docs-inner {
  width: min(1120px, calc(100% - 2rem));
  margin: 0 auto;
  padding: 3rem 0;
  display: grid;
  grid-template-columns: 0.8fr 1.2fr;
  gap: 2rem;
  align-items: start;
}

.eyebrow {
  margin: 0 0 0.5rem;
  color: #0f766e;
  font-size: 0.82rem;
  font-weight: 700;
  text-transform: uppercase;
}

.dark .eyebrow {
  color: #5eead4;
}

.docs-inner h2 {
  margin: 0;
  font-size: 2rem;
  letter-spacing: 0;
  border-top: 0;
  padding-top: 0;
  line-height: 1.15;
}

@media (max-width: 768px) {
  .docs-inner h2 {
    font-size: 1.5rem;
  }
  .doc-link {
    min-height: auto;
  }
}

.doc-grid {
  display: grid;
  grid-template-columns: repeat(2, minmax(0, 1fr));
  gap: 0.75rem;
}

.doc-link {
  display: block;
  min-height: 9rem;
  padding: 1rem;
  color: inherit;
  background: var(--vp-c-bg);
  border: 1px solid var(--vp-c-divider);
  border-radius: 8px;
  text-decoration: none;
  transition: border-color 0.2s;
}

.doc-link:hover {
  text-decoration: none;
  border-color: var(--vp-c-brand-1);
}

.doc-link span {
  display: block;
  margin-bottom: 0.5rem;
  color: var(--vp-c-text-1);
  font-weight: 700;
}

.doc-link small {
  display: block;
  color: var(--vp-c-text-2);
  font-size: 0.9rem;
  line-height: 1.45;
}

@media (max-width: 996px) {
  .docs-inner,
  .doc-grid {
    grid-template-columns: 1fr;
  }
}
</style>
