import type { Theme } from "vitepress";
import DefaultTheme from "vitepress/theme";
import ArchitectureComparison from "./components/ArchitectureComparison.vue";
import FeatureCards from "./components/FeatureCards.vue";
import PerformancePreview from "./components/PerformancePreview.vue";
import ProductionReadinessPanel from "./components/ProductionReadinessPanel.vue";
import "./custom.css";

// Default VitePress theme (dark/light mode toggle and local search come for free from it),
// plus the homepage components built for Phase 9 PR5 - registered globally so docs/index.md can
// use them directly as tags without a per-page import.
export default {
  extends: DefaultTheme,
  enhanceApp({ app }) {
    app.component("ArchitectureComparison", ArchitectureComparison);
    app.component("FeatureCards", FeatureCards);
    app.component("PerformancePreview", PerformancePreview);
    app.component("ProductionReadinessPanel", ProductionReadinessPanel);
  },
} satisfies Theme;
