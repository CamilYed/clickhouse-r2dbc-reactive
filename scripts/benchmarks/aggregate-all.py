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
    for label in sorted(by_params):
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


def plot_class_chart(cls: str, by_params: dict[str, dict[str, dict]], out_path: Path) -> bool:
    """One grouped bar chart per benchmark class: score by @Param combination, one bar per
    method. Returns False (and writes nothing) if there's only one bar total - not worth a chart.
    Deliberately generic across @BenchmarkMode/units (unlike analyze.py's plots, which are
    written for one specific class's shape) - the y-axis label is just whatever scoreUnit that
    class's entries actually used, and every class gets its own chart rather than one fixed set
    of three metrics."""
    labels = sorted(by_params)
    methods = sorted({m for group in by_params.values() for m in group})
    if len(labels) * len(methods) <= 1:
        return False

    unit = next(
        (row["unit"] for group in by_params.values() for row in group.values()),
        "score",
    )

    fig, ax = plt.subplots(figsize=(max(6, len(labels) * 1.6), 4))
    width = 0.8 / max(len(methods), 1)
    x = range(len(labels))
    for i, method in enumerate(methods):
        offset = (i - (len(methods) - 1) / 2) * width
        values = [by_params[label].get(method, {}).get("score") for label in labels]
        plotted = [v if v is not None else 0 for v in values]
        ax.bar([xi + offset for xi in x], plotted, width=width, label=method)
    ax.set_xticks(list(x))
    ax.set_xticklabels(labels, rotation=20, ha="right")
    ax.set_ylabel(unit)
    ax.set_title(f"{cls} - score by params")
    ax.legend()
    fig.tight_layout()
    fig.savefig(out_path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    return True


def plot_class_allocation_chart(cls: str, by_params: dict[str, dict[str, dict]], out_path: Path) -> bool:
    """Same shape as plot_class_chart but for B/op (secondary gc.alloc.rate.norm metric) instead
    of the primary score - skipped entirely (no file written) if the class has no -prof gc data
    at all (fast profile runs, or classes that were never run with -prof gc)."""
    labels = sorted(by_params)
    methods = sorted({m for group in by_params.values() for m in group})
    has_alloc = any(
        row.get("alloc_bytes_per_op") is not None for group in by_params.values() for row in group.values()
    )
    if not has_alloc or len(labels) * len(methods) <= 1:
        return False

    fig, ax = plt.subplots(figsize=(max(6, len(labels) * 1.6), 4))
    width = 0.8 / max(len(methods), 1)
    x = range(len(labels))
    for i, method in enumerate(methods):
        offset = (i - (len(methods) - 1) / 2) * width
        values = [by_params[label].get(method, {}).get("alloc_bytes_per_op") for label in labels]
        plotted = [v if v is not None else 0 for v in values]
        ax.bar([xi + offset for xi in x], plotted, width=width, label=method)
    ax.set_xticks(list(x))
    ax.set_xticklabels(labels, rotation=20, ha="right")
    ax.set_ylabel("B/op")
    ax.set_title(f"{cls} - allocation by params")
    ax.legend()
    fig.tight_layout()
    fig.savefig(out_path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    return True


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
