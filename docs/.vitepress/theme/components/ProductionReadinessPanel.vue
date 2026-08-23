<script setup lang="ts">
import { withBase } from "vitepress";

const rows = [
  { mark: "✅", label: "Fixed & verified", count: 13, tone: "good" },
  { mark: "⚠️", label: "Documented, safe limitations", count: 4, tone: "warn" },
  { mark: "❌", label: "Open gaps, not yet addressed", count: 3, tone: "gap" },
  { mark: "\u{1F9EA}", label: "Needs more testing", count: 1, tone: "test" },
];
</script>

<template>
  <section class="chr-section">
    <h2 class="chr-heading">Production readiness</h2>
    <p class="chr-lede">
      Every finding from the driver's own adoption-decision review, triaged honestly rather than
      marketed — the same ✅/⚠️/❌/🧪 vocabulary is used on the dedicated page.
    </p>

    <div class="chr-grid">
      <div v-for="r in rows" :key="r.label" class="chr-stat" :class="'chr-' + r.tone">
        <span class="chr-mark">{{ r.mark }}</span>
        <span class="chr-count">{{ r.count }}</span>
        <span class="chr-label">{{ r.label }}</span>
      </div>
    </div>

    <p class="chr-more">
      <a :href="withBase('/project/production-readiness')">See the full matrix &#8594;</a>
    </p>
  </section>
</template>

<style scoped>
.chr-section {
  max-width: 960px;
  margin: 64px auto 96px;
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

.chr-grid {
  display: grid;
  grid-template-columns: repeat(4, 1fr);
  gap: 16px;
}

@media (max-width: 768px) {
  .chr-grid {
    grid-template-columns: repeat(2, 1fr);
  }
}

.chr-stat {
  border: 1px solid var(--vp-c-divider);
  border-radius: 12px;
  padding: 20px 16px;
  background: var(--vp-c-bg-soft);
  text-align: center;
  display: flex;
  flex-direction: column;
  align-items: center;
  gap: 4px;
}

.chr-mark {
  font-size: 22px;
  line-height: 1;
}

.chr-count {
  font-size: 28px;
  font-weight: 700;
  color: var(--vp-c-text-1);
}

.chr-label {
  font-size: 12.5px;
  color: var(--vp-c-text-2);
  line-height: 1.4;
}

.chr-good {
  border-color: var(--vp-c-brand-1);
}

.chr-test .chr-count {
  color: var(--chr-accent-cyan);
}

.chr-more {
  margin-top: 24px;
  font-size: 14px;
}
</style>
