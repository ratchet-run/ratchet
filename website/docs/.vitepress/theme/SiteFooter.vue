<template>
  <footer class="ratchet-site-footer">
    <div class="ratchet-footer">
      <div>
        <h4>Documentation</h4>
        <ul>
          <li><a href="/getting-started/introduction">Getting Started</a></li>
          <li><a href="/comparison/overview">Compare</a></li>
          <li><a href="/api-reference/overview">API Reference</a></li>
          <li><a href="/deployment/overview">Deployment</a></li>
          <li><a href="/conformance/">Conformance</a></li>
          <li><a href="/advanced/customization-platform">Advanced</a></li>
          <li><a href="/troubleshooting/overview">Troubleshooting</a></li>
        </ul>
      </div>
      <div>
        <h4>Community</h4>
        <ul>
          <li><a href="https://github.com/ratchet-run/ratchet">GitHub</a></li>
          <li><a href="https://github.com/ratchet-run/ratchet/issues">Issues</a></li>
          <li><a href="https://github.com/ratchet-run/ratchet/discussions">Discussions</a></li>
        </ul>
      </div>
    </div>
    <div class="ratchet-footer-copy">
      Copyright © {{ year }} Ratchet Project
    </div>
  </footer>
</template>

<script setup>
const year = new Date().getFullYear()
</script>

<style scoped>
.ratchet-site-footer {
  background: #1a1a1a;
  color: #e6e6e6;
}

:global(.dark) .ratchet-site-footer {
  background: #0a0a0a;
}

.ratchet-footer {
  display: grid;
  grid-template-columns: repeat(2, minmax(0, 1fr));
  gap: 2rem;
  padding: 2.5rem 0 1.5rem;
  width: min(1120px, calc(100% - 2rem));
  margin: 0 auto;
}

.ratchet-footer h4 {
  margin: 0 0 0.85rem;
  color: #fbbf24;
  font-size: 0.85rem;
  font-weight: 700;
  letter-spacing: 0.06em;
  text-transform: uppercase;
}

.ratchet-footer ul {
  list-style: none;
  padding: 0;
  margin: 0;
}

.ratchet-footer li { margin: 0.35rem 0; }

.ratchet-footer a {
  color: #d4d4d4;
  text-decoration: none;
  font-size: 0.92rem;
}

.ratchet-footer a:hover {
  color: #fbbf24;
}

.ratchet-footer-copy {
  width: min(1120px, calc(100% - 2rem));
  margin: 0 auto;
  padding: 1rem 0 1.5rem;
  border-top: 1px solid rgba(255, 255, 255, 0.08);
  color: #a3a3a3;
  font-size: 0.85rem;
  text-align: center;
}

@media (min-width: 768px) {
  .ratchet-footer {
    grid-template-columns: repeat(4, minmax(0, 1fr));
  }
}
</style>
