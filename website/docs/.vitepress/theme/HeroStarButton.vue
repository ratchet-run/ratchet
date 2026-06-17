<script setup lang="ts">
// A GitHub-style call to action under the hero buttons. The homepage draws
// traffic that does not always follow through to the repo, so this gives that
// next step its own recognizable button. The umami data-attributes match the
// trust strip and use-case band, so the click lands in analytics as a hero
// cta-click with target=github-star.
const repo = 'https://github.com/ratchet-run/ratchet'
</script>

<template>
  <div class="hero-star">
    <a
      class="hero-star-btn"
      :href="repo"
      target="_blank"
      rel="noopener"
      data-umami-event="cta-click"
      data-umami-event-location="hero"
      data-umami-event-target="github-star"
    >
      <svg class="gh-mark" viewBox="0 0 16 16" width="18" height="18" aria-hidden="true">
        <path
          fill="currentColor"
          d="M8 0C3.58 0 0 3.58 0 8c0 3.54 2.29 6.53 5.47 7.59.4.07.55-.17.55-.38 0-.19-.01-.82-.01-1.49-2.01.37-2.53-.49-2.69-.94-.09-.23-.48-.94-.82-1.13-.28-.15-.68-.52-.01-.53.63-.01 1.08.58 1.23.82.72 1.21 1.87.87 2.33.66.07-.52.28-.87.51-1.07-1.78-.2-3.64-.89-3.64-3.95 0-.87.31-1.59.82-2.15-.08-.2-.36-1.02.08-2.12 0 0 .67-.21 2.2.82a7.65 7.65 0 0 1 2-.27c.68 0 1.36.09 2 .27 1.53-1.04 2.2-.82 2.2-.82.44 1.1.16 1.92.08 2.12.51.56.82 1.27.82 2.15 0 3.07-1.87 3.75-3.65 3.95.29.25.54.73.54 1.48 0 1.07-.01 1.93-.01 2.2 0 .21.15.46.55.38A8.01 8.01 0 0 0 16 8c0-4.42-3.58-8-8-8z"
        />
      </svg>
      <span>Star on GitHub</span>
    </a>
  </div>
</template>

<style scoped>
.hero-star {
  margin-top: 16px;
}

/* Matches the VitePress medium hero button box (40px tall, 20px radius) so it
   lines up with Get Started / API Reference, but in GitHub's own button color
   rather than a site theme color, so it reads as "go to the repo". */
.hero-star-btn {
  display: inline-flex;
  align-items: center;
  gap: 8px;
  height: 40px;
  padding: 0 20px;
  border: 1px solid #24292f;
  border-radius: 20px;
  font-size: 14px;
  font-weight: 600;
  line-height: 1;
  color: #ffffff;
  background-color: #24292f;
  transition:
    color 0.25s,
    background-color 0.25s,
    border-color 0.25s;
}

.hero-star-btn:hover {
  background-color: #32383f;
  border-color: #32383f;
}

/* Pure black would disappear on the dark theme background, so invert it:
   a white button with the dark mark. Both directions clear the AA contrast
   floor the both-theme a11y scan enforces. */
.dark .hero-star-btn {
  color: #24292f;
  background-color: #ffffff;
  border-color: #ffffff;
}

.dark .hero-star-btn:hover {
  background-color: #e6e8eb;
  border-color: #e6e8eb;
}

.gh-mark {
  flex-shrink: 0;
}
</style>
