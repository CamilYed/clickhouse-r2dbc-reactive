<script setup lang="ts">
const features = [
  {
    title: "Non-blocking transport",
    text: "Built on Reactor Netty from the socket up. client-v2's own HTTP client is confirmed blocking and never called anywhere in this project.",
  },
  {
    title: "Streaming results",
    text: "Response bodies are decoded and emitted as a stream of rows, not aggregated into memory before the caller sees the first one.",
  },
  {
    title: "Bounded backpressure",
    text: "A slow subscriber slows the read from the socket instead of buffering unboundedly — verified with dedicated slow-subscriber contract tests.",
  },
  {
    title: "Real cancellation",
    text: "Disposing a subscription tears down the connection and sends a best-effort KILL QUERY, not just a client-side unsubscribe.",
  },
  {
    title: "R2DBC SPI, verified",
    text: "Runs the official R2DBC Technology Compatibility Kit against a real ClickHouse server, not just a compatibility claim.",
  },
  {
    title: "Spring WebFlux ready",
    text: "A full hexagonal Spring Boot + WebFlux demo module, including the DatabaseClient / BindMarkersFactory gap and how to work around it.",
  },
];
</script>

<template>
  <section class="chr-section">
    <h2 class="chr-heading">What makes it reactive end to end</h2>
    <div class="chr-grid">
      <div v-for="f in features" :key="f.title" class="chr-feature">
        <h3>{{ f.title }}</h3>
        <p>{{ f.text }}</p>
      </div>
    </div>
    <p class="chr-more">
      <a href="/concepts/fully-reactive">What "fully reactive" means, and how each property is tested &#8594;</a>
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
  margin-bottom: 24px;
  border-top: none;
}

.chr-grid {
  display: grid;
  grid-template-columns: repeat(3, 1fr);
  gap: 16px;
}

@media (max-width: 768px) {
  .chr-grid {
    grid-template-columns: repeat(2, 1fr);
  }
}

@media (max-width: 520px) {
  .chr-grid {
    grid-template-columns: 1fr;
  }
}

.chr-feature {
  border: 1px solid var(--vp-c-divider);
  border-radius: 12px;
  padding: 18px 20px;
  background: var(--vp-c-bg-soft);
  transition: border-color 0.15s ease, transform 0.15s ease;
}

.chr-feature:hover {
  border-color: var(--vp-c-brand-1);
  transform: translateY(-2px);
}

.chr-feature h3 {
  font-size: 15px;
  margin: 0 0 8px;
  border-top: none;
  color: var(--vp-c-text-1);
}

.chr-feature h3::before {
  content: "";
  display: inline-block;
  width: 8px;
  height: 8px;
  border-radius: 50%;
  background: var(--vp-c-brand-1);
  margin-right: 8px;
}

.chr-feature p {
  font-size: 13.5px;
  color: var(--vp-c-text-2);
  line-height: 1.55;
  margin: 0;
}

.chr-more {
  margin-top: 20px;
  font-size: 14px;
}
</style>
