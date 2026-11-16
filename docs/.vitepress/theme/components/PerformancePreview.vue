<script setup lang="ts">
import { withBase } from "vitepress";
</script>

<template>
  <section class="chr-section">
    <h2 class="chr-heading">Performance</h2>
    <p class="chr-lede">
      The benchmark suite compares public APIs, transport boundaries, decoding, streaming, and
      cancellation under one repeatable cloud pipeline. Results are published only with their
      dependency versions and confidence limits.
    </p>

    <div class="chr-status-card">
      <strong>Current status: a fresh baseline is required.</strong>
      <p>
        Published comparison runs used client-v2 0.9.8. The current driver uses 0.11.2 and no
        longer contains the experimental native decoder measured by the last investigation, so
        the old charts are retained as historical evidence rather than presented as current wins.
      </p>
    </div>

    <p class="chr-caveat">
      See <a :href="withBase('/performance/')">the performance status</a> for the completed
      latency-path conclusion, the rerun contract, and links to the historical archive.
    </p>
  </section>
</template>

<style scoped>
.chr-section {
  max-width: 960px;
  margin: 64px auto;
  padding: 0 24px;
}

.chr-heading {
  font-size: 28px;
  font-weight: 600;
  margin-bottom: 12px;
  border-top: none;
}

.chr-lede {
  color: var(--vp-c-text-2);
  max-width: 720px;
  line-height: 1.6;
  margin-bottom: 24px;
}

.chr-status-card {
  border: 1px solid var(--vp-c-divider);
  border-radius: 12px;
  padding: 24px;
  background: var(--vp-c-bg-soft);
  line-height: 1.6;
}

.chr-status-card strong {
  color: var(--chr-accent-cyan);
}

.chr-status-card p {
  margin: 10px 0 0;
  font-size: 14px;
  color: var(--vp-c-text-2);
}

.chr-caveat {
  margin-top: 16px;
  font-size: 13px;
  color: var(--vp-c-text-3);
  max-width: 720px;
}
</style>
