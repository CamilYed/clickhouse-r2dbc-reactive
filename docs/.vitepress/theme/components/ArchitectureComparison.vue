<script setup lang="ts">
import { withBase } from "vitepress";
</script>

<template>
  <section class="chr-section">
    <h2 class="chr-heading">Why it exists</h2>
    <p class="chr-lede">
      Implementing the R2DBC interfaces is not, by itself, enough to make the execution path
      reactive. This driver reuses
      <a href="https://github.com/ClickHouse/clickhouse-java" target="_blank" rel="noreferrer">client-v2</a>'s
      public row-decoding classes only — its HTTP transport is confirmed blocking by reading the
      source, and is never called.
    </p>

    <div class="chr-compare">
      <div class="chr-card chr-before">
        <span class="chr-tag">Before</span>
        <h3>client-v2, called directly</h3>
        <p>
          <code>internal.HttpAPIClientHelper</code> — classic Apache HttpClient5 I/O. A blocking
          call on a reactive thread, one thread parked per in-flight request.
        </p>
      </div>
      <div class="chr-arrow" aria-hidden="true">&#8594;</div>
      <div class="chr-card chr-after">
        <span class="chr-tag">This driver</span>
        <h3>transport-http, from the socket up</h3>
        <p>
          Its own small, explicit Reactor Netty transport — connection acquisition, streaming
          response chunks, cancellation, all non-blocking. Independent of client-v2 entirely.
        </p>
      </div>
    </div>

    <p class="chr-more">
      <a :href="withBase('/architecture/overview')">Full architecture direction &amp; verified evidence &#8594;</a>
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
  margin-bottom: 32px;
}

.chr-compare {
  display: grid;
  grid-template-columns: 1fr auto 1fr;
  align-items: center;
  gap: 20px;
}

@media (max-width: 640px) {
  .chr-compare {
    grid-template-columns: 1fr;
  }
  .chr-arrow {
    transform: rotate(90deg);
  }
}

.chr-card {
  border: 1px solid var(--vp-c-divider);
  border-radius: 12px;
  padding: 20px 22px;
  background: var(--vp-c-bg-soft);
}

.chr-card h3 {
  font-size: 16px;
  margin: 8px 0 10px;
  border-top: none;
}

.chr-card p {
  font-size: 14px;
  color: var(--vp-c-text-2);
  line-height: 1.55;
  margin: 0;
}

.chr-before {
  opacity: 0.85;
}

.chr-after {
  border-color: var(--vp-c-brand-1);
  background: linear-gradient(160deg, var(--vp-c-brand-soft), var(--vp-c-bg-soft) 65%);
}

.chr-tag {
  display: inline-block;
  font-size: 11px;
  font-weight: 700;
  letter-spacing: 0.06em;
  text-transform: uppercase;
  color: var(--vp-c-text-3);
  margin-bottom: 6px;
}

.chr-after .chr-tag {
  color: var(--vp-c-brand-1);
}

.chr-arrow {
  font-size: 22px;
  color: var(--chr-accent-cyan);
  font-weight: 700;
}

.chr-more {
  margin-top: 24px;
  font-size: 14px;
}
</style>
