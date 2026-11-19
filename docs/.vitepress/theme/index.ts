import type { Theme } from "vitepress";
import DefaultTheme from "vitepress/theme";
import "./custom.css";

// Keep the site on VitePress's standard content model. The default theme provides responsive
// navigation, local search, dark mode, and the homepage feature grid without custom components.
export default {
  extends: DefaultTheme,
} satisfies Theme;
