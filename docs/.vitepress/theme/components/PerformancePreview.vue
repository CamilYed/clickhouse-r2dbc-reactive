<script setup lang="ts">
import { withBase } from "vitepress";
import allocationChart from "../../../images/2026-08-23-cloud-matched-pool-allocation.png";
</script>

<template>
  <section class="chr-section">
    <h2 class="chr-heading">Performance</h2>
    <p class="chr-lede">
      Allocation per query through the public R2DBC SPI, an 8-connection pool matched on both
      sides, this driver vs. client-v2's own public async API — cloud-verified on GitHub Actions,
      two independent runs.
    </p>

    <div class="chr-chart-card">
      <img
        :src="allocationChart"
        width="720"
        alt="Allocation per query through the public R2DBC SPI, matched 8-connection pool, this driver vs client-v2"
      />
      <div class="chr-chart-caption">
        <strong>2.7&ndash;2.9x less allocation per query</strong>, and the gap widens as concurrency
        rises — the win that holds up under a fair, cloud-verified comparison. Throughput and
        latency currently favor client-v2 in this same scenario; see the full page for the honest
        breakdown, including a retracted earlier claim.
      </div>
    </div>

    <p class="chr-caveat">
      GitHub Actions `ubuntu-latest`, JMH `trusted` profile (3 forks, 5 warmup iterations),
      confirmed stable across two independent runs. See
      <a :href="withBase('/performance/')">the full performance page</a>
      for methodology, confidence caveats, and every other benchmark family.
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

.chr-chart-card {
  border: 1px solid var(--vp-c-divider);
  border-radius: 12px;
  padding: 24px;
  background: var(--vp-c-bg-soft);
  text-align: center;
}

.chr-chart-card img {
  max-width: 100%;
  height: auto;
  border-radius: 6px;
}

.chr-chart-caption {
  margin-top: 16px;
  font-size: 14px;
  color: var(--vp-c-text-2);
  max-width: 640px;
  margin-left: auto;
  margin-right: auto;
  line-height: 1.55;
}

.chr-chart-caption strong {
  color: var(--chr-accent-cyan);
}

.chr-caveat {
  margin-top: 16px;
  font-size: 13px;
  color: var(--vp-c-text-3);
  max-width: 720px;
}
</style>
