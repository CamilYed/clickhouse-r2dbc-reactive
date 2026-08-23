import { defineConfig } from "vitepress";
import { withMermaid } from "vitepress-plugin-mermaid";

// VitePress config for the clickhouse-r2dbc-reactive documentation site.
// Renders the existing docs/ tree directly - no content duplication, this is
// the same source of truth used by README.md's "Learn more" links.
//
// Wrapped in withMermaid(...): VitePress does not render ```mermaid fenced blocks natively - left
// unwrapped, they render as a plain syntax-highlighted code block (the bug this fixed). Several
// docs/ pages (architecture/overview.md, README.md's own diagram, the homepage's architecture
// comparison) rely on the fenced block actually becoming a diagram.
export default withMermaid(defineConfig({
  title: "clickhouse-r2dbc-reactive",
  description:
    "A non-blocking R2DBC driver for ClickHouse, built on Reactor Netty from the socket up.",

  // Served from https://camilyed.github.io/clickhouse-r2dbc-reactive/
  base: "/clickhouse-r2dbc-reactive/",

  cleanUrls: true,
  lastUpdated: true,

  srcExclude: ["**/perf-runs/**"],

  // Content pages legitimately link out to files VitePress doesn't render as site pages: root
  // project files (README.md, ROADMAP.md, CLAUDE.md), engineering/roadmap-archive.md, and Java
  // source under the Gradle modules for "here's the actual code" citations. Every docs/ page lives
  // exactly one level under docs/ (docs/<category>/<file>.md), so any link needing two or more
  // "../" segments is, by construction, pointing outside the site rather than at a broken
  // in-site link - safe to ignore here without masking a real dead link between docs pages.
  ignoreDeadLinks: [/\.\.\/\.\.\//],

  head: [["link", { rel: "icon", href: "/clickhouse-r2dbc-reactive/favicon.svg" }]],

  // mermaid (pulled in by withMermaid below) depends on fastdom, a CommonJS-only package, and
  // imports both its main entry and the "fastdom/extensions/fastdom-promised.js" subpath
  // directly (see mermaid's util/fastdom.ts). vitepress-plugin-mermaid's own optimizeDeps.include
  // list predates mermaid's current dependency tree and misses both, so Vite's dev-server
  // dependency scanner never pre-bundles them and serves them unbundled, which fails with
  // "does not provide an export named 'default'" (a plain CJS/ESM interop error, browser console
  // only - docs:build is unaffected). Forcing both specifiers into optimizeDeps.include here
  // fixes the dev server; withMermaid() appends its own list to this one rather than replacing it.
  vite: {
    optimizeDeps: {
      include: ["fastdom", "fastdom/extensions/fastdom-promised.js"],
    },
  },

  themeConfig: {
    logo: "/favicon.svg",

    nav: [
      { text: "Guide", link: "/guide/spring-boot" },
      { text: "Reference", link: "/reference/configuration" },
      { text: "Performance", link: "/performance/" },
      { text: "Architecture", link: "/architecture/overview" },
      {
        text: "Project",
        items: [
          { text: "Production readiness", link: "/project/production-readiness" },
          { text: "Roadmap (GitHub)", link: "https://github.com/CamilYed/clickhouse-r2dbc-reactive/blob/main/ROADMAP.md" },
          { text: "Changelog (GitHub)", link: "https://github.com/CamilYed/clickhouse-r2dbc-reactive/blob/main/CHANGELOG.md" },
        ],
      },
    ],

    sidebar: [
      {
        text: "Concepts",
        items: [{ text: "What \"fully reactive\" means", link: "/concepts/fully-reactive" }],
      },
      {
        text: "Guide",
        items: [{ text: "Using with Spring Boot", link: "/guide/spring-boot" }],
      },
      {
        text: "Architecture",
        items: [{ text: "Architecture direction", link: "/architecture/overview" }],
      },
      {
        text: "Reference",
        items: [
          { text: "Connection configuration", link: "/reference/configuration" },
          { text: "Known limitations", link: "/reference/known-limitations" },
          { text: "R2DBC SPI compatibility", link: "/reference/r2dbc-compatibility" },
        ],
      },
      {
        text: "Operations",
        items: [
          { text: "Connection pooling", link: "/operations/connection-pooling" },
          { text: "Optional: io.r2dbc.pool", link: "/operations/optional-r2dbc-pool" },
        ],
      },
      {
        text: "Performance",
        items: [
          { text: "Overview", link: "/performance/" },
          { text: "Methodology", link: "/performance/methodology" },
          { text: "Results", link: "/performance/results" },
          { text: "Running the benchmarks", link: "/performance/running-benchmarks" },
        ],
      },
      {
        text: "Internals",
        items: [
          { text: "Testing strategy", link: "/internals/testing-strategy" },
          { text: "client-v2 HTTP reference", link: "/internals/client-v2-http-reference" },
        ],
      },
      {
        text: "Project",
        items: [{ text: "Production readiness", link: "/project/production-readiness" }],
      },
    ],

    socialLinks: [
      { icon: "github", link: "https://github.com/CamilYed/clickhouse-r2dbc-reactive" },
      { icon: "linkedin", link: "https://www.linkedin.com/in/jkamil/", ariaLabel: "Kamil Jędrzejuk on LinkedIn" },
    ],

    search: {
      provider: "local",
    },

    editLink: {
      pattern:
        "https://github.com/CamilYed/clickhouse-r2dbc-reactive/edit/main/docs/:path",
      text: "Edit this page on GitHub",
    },

    footer: {
      message:
        'Built by <a href="https://www.linkedin.com/in/jkamil/" target="_blank" rel="noreferrer">Kamil Jędrzejuk</a> · Released under the Apache 2.0 License.',
      copyright: "Not affiliated with or endorsed by ClickHouse, Inc.",
    },

    outline: {
      level: [2, 3],
    },
  },
}));
