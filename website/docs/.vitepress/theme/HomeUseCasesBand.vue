<script setup lang="ts">
const useCases = [
  {
    title: 'Offload work after a request',
    href: '/use-cases/offload-after-request',
    description:
      'Persist follow-up work in the same commit as your business write, then return the response right away.',
    event: 'offload-after-request',
  },
  {
    title: 'Scheduled & recurring jobs',
    href: '/use-cases/scheduled-recurring-jobs',
    description:
      'Cron and delayed jobs that survive a restart and fire on exactly one node. The Quartz replacement.',
    event: 'scheduled-recurring-jobs',
  },
  {
    title: 'Resilient third-party integrations',
    href: '/use-cases/resilient-integrations',
    description:
      'Retry flaky external APIs with backoff, and trip a circuit breaker when a provider goes down.',
    event: 'resilient-integrations',
  },
  {
    title: 'Durable LLM & agent workflows',
    href: '/use-cases/durable-llm-workflows',
    description:
      'Run model calls and multi-step agents as persisted, retrying jobs. Bring your own model client.',
    event: 'durable-llm-workflows',
  },
]
</script>

<template>
  <section class="usecase-band">
    <div class="usecase-inner">
      <div class="usecase-head">
        <p class="eyebrow">Use cases</p>
        <h2>Patterns Ratchet is built for.</h2>
      </div>
      <div class="usecase-grid">
        <a
          v-for="u in useCases"
          :key="u.title"
          :href="u.href"
          class="usecase-link"
          data-umami-event="cta-click"
          data-umami-event-location="use-cases-grid"
          :data-umami-event-target="u.event"
        >
          <span>{{ u.title }}</span>
          <small>{{ u.description }}</small>
        </a>
      </div>
    </div>
  </section>
</template>

<style scoped>
.usecase-band {
  border-top: 1px solid var(--vp-c-divider);
}

.usecase-inner {
  width: min(1120px, calc(100% - 2rem));
  margin: 0 auto;
  padding: 3rem 0;
}

.usecase-head {
  margin-bottom: 1.5rem;
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

.usecase-head h2 {
  margin: 0;
  font-size: 2rem;
  letter-spacing: 0;
  border-top: 0;
  padding-top: 0;
  line-height: 1.15;
}

.usecase-grid {
  display: grid;
  grid-template-columns: repeat(4, minmax(0, 1fr));
  gap: 0.75rem;
}

.usecase-link {
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

.usecase-link:hover {
  text-decoration: none;
  border-color: var(--vp-c-brand-1);
}

.usecase-link span {
  display: block;
  margin-bottom: 0.5rem;
  color: var(--vp-c-text-1);
  font-weight: 700;
}

.usecase-link small {
  display: block;
  color: var(--vp-c-text-2);
  font-size: 0.9rem;
  line-height: 1.45;
}

@media (max-width: 996px) {
  .usecase-grid {
    grid-template-columns: repeat(2, minmax(0, 1fr));
  }
}

@media (max-width: 768px) {
  .usecase-head h2 {
    font-size: 1.5rem;
  }
  .usecase-grid {
    grid-template-columns: 1fr;
  }
  .usecase-link {
    min-height: auto;
  }
}
</style>
