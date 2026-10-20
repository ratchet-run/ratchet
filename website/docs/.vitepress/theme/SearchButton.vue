<script setup lang="ts">
import VPNavBarSearchButton from 'vitepress/dist/client/theme-default/components/VPNavBarSearchButton.vue'
</script>

<template>
  <!-- Include the visible shortcut in the native name; title names the icon-only mobile button. -->
  <VPNavBarSearchButton :aria-label="undefined" title="Search" />
</template>
