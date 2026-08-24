#!/usr/bin/env python3
"""Aggregates every JMH artifact from a mega benchmark sweep into one consolidated report.

Companion to scripts/benchmarks/analyze.py, not a replacement for it: analyze.py is written for
one specific class's shape (PublicApiMatchedPoolThroughputBenchmark - Throughput mode, a
`concurrency` @Param, its own merged-histogram log line) and produces per-run summary.md + charts.
This script is the generic other half - it reads a whole directory of already-downloaded
.github/workflows/benchmark.yml artifacts (any number of benchmark classes, any @Param shape,
any @BenchmarkMode) and produces ONE mega-summary.md across all of them, so a sweep across the
project's ~20 benchmark classes doesn't have to be read one results.json at a time by hand.

Input layout it expects (exactly what `gh run download <run-id> -D <dest>` produces per run, and
what .github/workflows/benchmark.yml's "Assemble artifact directory" step assembles per run):

    <root>/
      benchmark-results-trusted-TrivialQueryBenchmark-1234567/
        results.json      - JMH's own --result-format=JSON output
        metadata.json     - commit/branch/JDK/profile/etc, written by the workflow
        raw-stdout.log    - full `jmh` Gradle task console output
        resource-samples.csv   (trusted profile only)
        jfr/                    (trusted profile only, not parsed here - see analyze.py's own note)
      benchmark-results-trusted-StreamingScanBenchmark-1234571/
        ...

Run it once per run-id to collect a mega sweep into one place, then point this script at the
common parent directory:

    for run_id in 1234567 1234568 1234569; do
      gh -R CamilYed/clickhouse-r2dbc-reactive run download "$run_id" -D ~/Downloads/mega-sweep
    done
    python3 scripts/benchmarks/aggregate-all.py --root ~/Downloads/mega-sweep --out-dir ~/Downloads/mega-sweep

It recurses to find every directory containing a results.json (metadata.json is read if present
alongside it, but not required - a bare results.json still gets aggregated with "unknown" metadata
fields rather than being skipped).

Produces, in --out-dir:
  - mega-summary.md  - one section per benchmark class: environment header, then one table of
                        every (method, @Param combination) with score/error/unit/B-per-op, a
                        thisDriver/clientV2 ratio line when both conventional method names are
                        present for the same @Param combination, an embedded chart, and a flagged
                        warning when a method that appears elsewhere in the class is missing for
                        one @Param combination (the signature of the kind of partial failure this
                        project hit with the ResponseCompression mismatch - see ROADMAP.md).
  - mega-summary.csv - the same rows, flattened, one per (benchmark class, method, @Param
                        combination) - for spreadsheet/pandas use instead of reading prose.
  - charts/<Class>.png            - grouped bar chart, score by @Param combination, one bar per
                                     method - skipped for a class with only one bar total.
  - charts/<Class>-alloc.png      - same shape for B/op (-prof gc secondary metric) - skipped for
                                     a class with no allocation data at all.

Deliberately does not: talk to ClickHouse, invoke Gradle/JMH, parse per-query latency out of
raw-stdout.log (that log line's format/presence varies per benchmark class - see analyze.py's own
MERGED_LATENCY_LINE comment for why that parsing is intentionally kept specific to one class
rather than generalized here), or apply any regression-gate threshold.
"""

from __future__ import annotations

import argparse
import csv
import json
import math
import re
import sys
from datetime import datetime, timezone
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt

CONVENTIONAL_DRIVER_METHODS = {"thisDriver", "clientV2"}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument(
        "--root",
        required=True,
        type=Path,
        help="directory to recurse into looking for results.json files",
    )
    parser.add_argument("--out-dir", required=True, type=Path, help="where to write mega-summary.md/.csv")
    return parser.parse_args()


def load_json(path: Path) -> object:
    with path.open(encoding="utf-8") as f:
        return json.load(f)


def method_name(benchmark_fqn: str) -> str:
    """'io.github...TrivialQueryBenchmark.thisDriver' -> 'thisDriver'."""
    return benchmark_fqn.rsplit(".", 1)[-1]


def class_name(benchmark_fqn: str) -> str:
    """'io.github...TrivialQueryBenchmark.thisDriver' -> 'TrivialQueryBenchmark'."""
    return benchmark_fqn.rsplit(".", 2)[-2]


def params_label(params: dict) -> str:
    if not params:
        return "(no params)"
    return ", ".join(f"{k}={v}" for k, v in sorted(params.items()))


def params_sort_key(label: str) -> tuple:
    """Sorts @Param labels by the numeric value of each 'key=value' component instead of the
    label string itself - plain string sort put poolSize 16/32/4/8 and concurrency 128/32/8 in
    the wrong order (e.g. "128" < "32" < "8" lexically), which read as a rendering bug rather
    than a sort bug. Each value is tried as a float first (tag 0, sorts before any string), and
    falls back to the raw string (tag 1) for genuinely non-numeric @Param values - a mixed label
    still sorts consistently, just alphabetically on its non-numeric components."""
    key = []
    for value in re.findall(r"=([^,]+)", label):
        try:
            key.append((0, float(value)))
        except ValueError:
            key.append((1, value))
    return key or [(1, label)]


def alloc_bytes_per_op(entry: dict) -> float | None:
    secondary = entry.get("secondaryMetrics", {})
    norm = secondary.get("·gc.alloc.rate.norm") or secondary.get("gc.alloc.rate.norm")
    return norm.get("score") if norm is not None else None


def find_artifact_dirs(root: Path) -> list[Path]:
    """Every directory under root that directly contains a results.json - order doesn't matter,
    output is grouped/sorted by benchmark class afterwards regardless of discovery order."""
    return sorted({p.parent for p in root.rglob("results.json")})


class ArtifactError(Exception):
    """results.json exists but couldn't be parsed, or was empty - surfaced as a failed sweep
    member in the report rather than silently dropped, since a build failure (like the
    ResponseCompression mismatch this project hit) can leave a truncated/empty file behind."""


def load_artifact(directory: Path) -> dict:
    results_path = directory / "results.json"
    metadata_path = directory / "metadata.json"

    try:
        results = load_json(results_path)
    except (json.JSONDecodeError, OSError) as e:
        raise ArtifactError(f"could not parse {results_path}: {e}") from e
    if not isinstance(results, list) or not results:
        raise ArtifactError(f"{results_path} has no benchmark entries (build likely failed before completing)")

    metadata = load_json(metadata_path) if metadata_path.exists() else {}

    rows = []
    for entry in results:
        benchmark_fqn = entry["benchmark"]
        primary = entry["primaryMetric"]
        rows.append(
            {
                "class": class_name(benchmark_fqn),
                "method": method_name(benchmark_fqn),
                "params": entry.get("params", {}),
                "mode": entry.get("mode", "unknown"),
                "score": primary["score"],
                "error": primary.get("scoreError"),
                "unit": primary.get("scoreUnit", "unknown"),
                "alloc_bytes_per_op": alloc_bytes_per_op(entry),
            }
        )

    return {"directory": directory, "metadata": metadata, "rows": rows}


def group_by_class(artifacts: list[dict]) -> dict[str, list[dict]]:
    """{benchmark class name: [artifact dicts]} - normally one artifact per class, but a class run
    more than once (e.g. a rerun after a fix) ends up with more than one entry here, each kept and
    rendered separately with its own directory/commit/run-date so a stale and a fixed run are never
    silently merged into one misleading table."""
    grouped: dict[str, list[dict]] = {}
    for artifact in artifacts:
        classes = {row["class"] for row in artifact["rows"]}
        for cls in classes:
            grouped.setdefault(cls, []).append(artifact)
    return grouped


def build_class_section(cls: str, artifact: dict) -> tuple[str, list[dict], dict[str, dict[str, dict]]]:
    metadata = artifact["metadata"]
    rows = [row for row in artifact["rows"] if row["class"] == cls]

    lines = [f"## {cls}", ""]
    lines.append(f"- Source: `{artifact['directory']}`")
    lines.append(f"- Profile: **{metadata.get('profile', 'unknown')}**")
    lines.append(f"- Commit: `{metadata.get('commitSha', 'unknown')}` ({metadata.get('branch', 'unknown')})")
    lines.append(f"- Run date: {metadata.get('runDate', 'unknown')}")
    lines.append(
        f"- JDK: {metadata.get('jdkVersion', 'unknown')}, OS/arch: "
        f"{metadata.get('osName', 'unknown')}/{metadata.get('osArch', 'unknown')}"
    )
    lines.append("")
    lines.append("| method | params | mode | score | error | unit | B/op |")
    lines.append("|---|---|---|---|---|---|---|")

    by_params: dict[str, dict[str, dict]] = {}
    for row in rows:
        label = params_label(row["params"])
        by_params.setdefault(label, {})[row["method"]] = row

    # The set of method names this class is expected to have per @Param combination - not
    # hardcoded to {"thisDriver", "clientV2"}, since some classes use other conventions entirely
    # (e.g. PublicApiPointQueryBenchmark's thisDriverEnabledObservation/thisDriverNoopObservation
    # instead of a plain thisDriver). Derived from whichever combination has the most methods, on
    # the assumption that a partial failure (some forks producing zero entries for one method)
    # only ever shrinks a group, never adds an unexpected method to it.
    all_methods_in_class = max((set(methods) for methods in by_params.values()), key=len, default=set())

    csv_rows = []
    for label in sorted(by_params, key=params_sort_key):
        methods = by_params[label]
        for method_key in sorted(methods):
            row = methods[method_key]
            alloc = f"{row['alloc_bytes_per_op']:.1f}" if row["alloc_bytes_per_op"] is not None else "n/a"
            error = f"±{row['error']:.2f}" if row["error"] is not None else "n/a"
            lines.append(
                f"| {method_key} | {label} | {row['mode']} | {row['score']:.2f} | {error} | "
                f"{row['unit']} | {alloc} |"
            )
            csv_rows.append(
                {
                    "class": cls,
                    "profile": metadata.get("profile", "unknown"),
                    "commit": metadata.get("commitSha", "unknown"),
                    "run_date": metadata.get("runDate", "unknown"),
                    "method": method_key,
                    "params": label,
                    "mode": row["mode"],
                    "score": row["score"],
                    "error": row["error"],
                    "unit": row["unit"],
                    "alloc_bytes_per_op": row["alloc_bytes_per_op"],
                }
            )

        present = set(methods)
        if CONVENTIONAL_DRIVER_METHODS.issubset(present):
            this_driver, client_v2 = methods["thisDriver"], methods["clientV2"]
            if client_v2["score"]:
                ratio = this_driver["score"] / client_v2["score"]
                lines.append("")
                lines.append(
                    f"thisDriver/clientV2 ratio for `{label}`: **{ratio:.2f}** "
                    f"(unit: {this_driver['unit']} - remember to check whether higher or lower is "
                    "better for this metric before reading the ratio as \"faster\")"
                )
                lines.append("")
        missing = all_methods_in_class - present
        if missing:
            lines.append("")
            lines.append(
                f"⚠ only {', '.join(sorted(present))} has a result for `{label}` - "
                f"{', '.join(sorted(missing))} produced no entry (check that run's raw-stdout.log "
                "for a failure)."
            )
            lines.append("")

    lines.append("")
    return "\n".join(lines), csv_rows, by_params


def _render_grouped_bar_chart(
    cls: str,
    by_params: dict[str, dict[str, dict]],
    out_path: Path,
    *,
    value_key: str,
    ylabel: str,
    title_suffix: str,
) -> bool:
    """Shared renderer behind plot_class_chart/plot_class_allocation_chart - same grouped-bar
    shape, parameterized only by which field of each row to plot (primary score vs. B/op) and the
    y-axis label. Returns False (writes nothing) if there's only one bar total - not worth a
    chart. Fixes applied here after reviewing the first real mega-sweep render (2026-08-24):

    - @Param combinations sorted numerically (params_sort_key), not as label strings - a plain
      string sort put poolSize 16/32/4/8 and concurrency 128/32/8 in the wrong order, which read
      as a rendering bug rather than a sort bug.
    - Missing (method, params) combinations are plotted as NaN, not 0 - matplotlib simply omits a
      NaN bar instead of drawing a misleading zero-height one sitting on the axis.
    - The legend is placed fully outside the axes (upper-left anchored just past the right edge)
      instead of matplotlib's auto-placed "best" corner, which routinely overlapped the tallest
      bars when they reached close to the top of the plot.
    - The y-axis switches to log scale when the plotted values span more than ~20x (e.g.
      StreamingScanBenchmark's 10k/100k/1M row tiers) - on a linear scale the smallest bars were
      visually flat against zero and impossible to compare.
    - Figure width now also accounts for the actual rendered length of the @Param labels (not
      just how many there are) - multi-@Param classes like DefaultPoolSlowQueryThroughputBenchmark
      produce long combined labels ("concurrency=32, sleepSeconds=1.0") that got clipped off the
      right edge of the canvas at the old fixed per-bar width.
    - That same per-label-length width scales with the number of @Param combinations too, which
      runs away for classes with *many* long labels (FluxInputStreamBridgeMicrobenchmark's 8
      two-@Param combinations produced a ~44-inch-wide, visually squashed chart with the legend
      stranded far off the right edge). Past a width cap, this switches to vertical (90°) labels
      instead: vertical text needs horizontal room proportional to the bars themselves, not the
      label string length, so width no longer explodes - the extra label height is absorbed by
      growing the figure's *height* instead.
    - Each bar gets its value printed above it (`Bar.bar_label`), so the chart is readable on its
      own without cross-referencing the table below it.
    """
    labels = sorted(by_params, key=params_sort_key)
    methods = sorted({m for group in by_params.values() for m in group})
    if len(labels) * len(methods) <= 1:
        return False

    all_values = [
        row[value_key]
        for group in by_params.values()
        for row in group.values()
        if row.get(value_key) is not None
    ]
    if not all_values:
        return False

    longest_label_chars = max(len(label) for label in labels)
    diagonal_width = max(6.0, len(labels) * max(1.6, longest_label_chars * 0.11))
    max_diagonal_width = 20.0
    if diagonal_width > max_diagonal_width:
        fig_width = max(6.0, len(labels) * max(1.0, len(methods) * 0.6))
        fig_height = 4.5 + longest_label_chars * 0.05
        label_rotation, label_ha = 90, "center"
    else:
        fig_width = diagonal_width
        fig_height = 4.5
        label_rotation, label_ha = 30, "right"
    fig, ax = plt.subplots(figsize=(fig_width, fig_height))

    width = 0.8 / max(len(methods), 1)
    x = range(len(labels))
    for i, method in enumerate(methods):
        offset = (i - (len(methods) - 1) / 2) * width
        values = [by_params[label].get(method, {}).get(value_key) for label in labels]
        plotted = [v if v is not None else math.nan for v in values]
        bars = ax.bar([xi + offset for xi in x], plotted, width=width, label=method)
        ax.bar_label(bars, fmt=lambda v: f"{v:,.0f}" if v == v else "", fontsize=7, padding=2)

    positive_values = [v for v in all_values if v > 0]
    if positive_values and max(positive_values) / min(positive_values) > 20:
        ax.set_yscale("log")
        ylabel = f"{ylabel} (log scale)"

    ax.set_xticks(list(x))
    ax.set_xticklabels(labels, rotation=label_rotation, ha=label_ha)
    ax.set_ylabel(ylabel)
    ax.set_title(f"{cls} - {title_suffix}")
    ax.legend(loc="upper left", bbox_to_anchor=(1.01, 1.0), borderaxespad=0)
    fig.tight_layout()
    fig.savefig(out_path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    return True


def plot_class_chart(cls: str, by_params: dict[str, dict[str, dict]], out_path: Path) -> bool:
    """One grouped bar chart per benchmark class: score by @Param combination, one bar per
    method. Deliberately generic across @BenchmarkMode/units (unlike analyze.py's plots, which
    are written for one specific class's shape) - the y-axis label is just whatever scoreUnit
    that class's entries actually used, and every class gets its own chart rather than one fixed
    set of three metrics. See _render_grouped_bar_chart for the actual rendering."""
    unit = next(
        (row["unit"] for group in by_params.values() for row in group.values()),
        "score",
    )
    return _render_grouped_bar_chart(
        cls, by_params, out_path, value_key="score", ylabel=unit, title_suffix="score by params"
    )


def plot_class_allocation_chart(cls: str, by_params: dict[str, dict[str, dict]], out_path: Path) -> bool:
    """Same shape as plot_class_chart but for B/op (secondary gc.alloc.rate.norm metric) instead
    of the primary score - skipped entirely (no file written) if the class has no -prof gc data
    at all (fast profile runs, or classes that were never run with -prof gc)."""
    return _render_grouped_bar_chart(
        cls,
        by_params,
        out_path,
        value_key="alloc_bytes_per_op",
        ylabel="B/op",
        title_suffix="allocation by params",
    )


def build_report(
    grouped: dict[str, list[dict]], failures: list[str], root: Path, charts_dir: Path
) -> tuple[str, list[dict]]:
    generated_at = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    lines = ["# Mega benchmark summary", ""]
    lines.append(f"- Generated: {generated_at}")
    lines.append(f"- Scanned root: `{root}`")
    lines.append(f"- Benchmark classes found: {len(grouped)}")
    if failures:
        lines.append(f"- Artifact directories that failed to parse: {len(failures)} (see bottom of this file)")
    lines.append("")
    lines.append("## Index")
    lines.append("")
    for cls in sorted(grouped):
        count = len(grouped[cls])
        note = "" if count == 1 else f" ({count} runs)"
        anchor = cls.lower()
        lines.append(f"- [{cls}](#{anchor}){note}")
    lines.append("")

    charts_dir.mkdir(parents=True, exist_ok=True)
    all_csv_rows: list[dict] = []
    for cls in sorted(grouped):
        for artifact in grouped[cls]:
            section, csv_rows, by_params = build_class_section(cls, artifact)
            lines.append(section)
            all_csv_rows.extend(csv_rows)

            score_chart = charts_dir / f"{cls}.png"
            if plot_class_chart(cls, by_params, score_chart):
                lines.append(f"![{cls} score chart](charts/{cls}.png)")
                lines.append("")

            alloc_chart = charts_dir / f"{cls}-alloc.png"
            if plot_class_allocation_chart(cls, by_params, alloc_chart):
                lines.append(f"![{cls} allocation chart](charts/{cls}-alloc.png)")
                lines.append("")

    if failures:
        lines.append("## Artifact directories that failed to parse")
        lines.append("")
        for message in failures:
            lines.append(f"- {message}")
        lines.append("")

    return "\n".join(lines), all_csv_rows


def write_csv(rows: list[dict], out_path: Path) -> None:
    if not rows:
        return
    fieldnames = list(rows[0].keys())
    with out_path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def main() -> int:
    args = parse_args()
    args.out_dir.mkdir(parents=True, exist_ok=True)

    artifact_dirs = find_artifact_dirs(args.root)
    if not artifact_dirs:
        print(f"No results.json found anywhere under {args.root}", file=sys.stderr)
        return 1

    artifacts: list[dict] = []
    failures: list[str] = []
    for directory in artifact_dirs:
        try:
            artifacts.append(load_artifact(directory))
        except ArtifactError as e:
            failures.append(str(e))

    if not artifacts:
        print(f"Found {len(artifact_dirs)} results.json file(s), but none parsed successfully.", file=sys.stderr)
        for message in failures:
            print(f"  - {message}", file=sys.stderr)
        return 1

    grouped = group_by_class(artifacts)
    charts_dir = args.out_dir / "charts"
    summary_md, csv_rows = build_report(grouped, failures, args.root, charts_dir)

    (args.out_dir / "mega-summary.md").write_text(summary_md, encoding="utf-8")
    write_csv(csv_rows, args.out_dir / "mega-summary.csv")

    print(f"Aggregated {len(artifacts)} artifact directory(ies) across {len(grouped)} benchmark class(es).")
    if failures:
        print(f"{len(failures)} artifact directory(ies) failed to parse - see mega-summary.md's bottom section.")
    print(f"Wrote {args.out_dir}/mega-summary.md, {args.out_dir}/mega-summary.csv, and {charts_dir}/*.png")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
