<template>
  <div class="ratchet-hero-prelude">
    <img src="/img/logo.svg" alt="" class="ratchet-hero-logo" aria-hidden="true" />
    <p class="ratchet-hero-eyebrow">Jakarta EE background jobs</p>
  </div>
</template>

<style scoped>
.ratchet-hero-prelude {
  display: flex;
  flex-direction: column;
  align-items: flex-start;
  gap: 0.75rem;
  margin-bottom: 0.75rem;
}

.ratchet-hero-logo {
  width: 76px;
  height: 76px;
}

.ratchet-hero-eyebrow {
  margin: 0;
  color: #0f766e;
  font-size: 0.82rem;
  font-weight: 700;
  letter-spacing: 0.02em;
  text-transform: uppercase;
  line-height: 1;
}

:global(.dark) .ratchet-hero-eyebrow {
  color: #5eead4;
}

@media (max-width: 960px) {
  .ratchet-hero-prelude {
    align-items: center;
  }
  .ratchet-hero-logo {
    width: 56px;
    height: 56px;
  }
}
</style>
