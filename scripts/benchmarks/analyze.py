#!/usr/bin/env python3
"""Turns one JMH run of PublicApiMatchedPoolThroughputBenchmark into a readable report.

Phase 10 Stage 1 (see engineering/roadmap-archive.md's "Phase 10 - Cloud benchmark pipeline"):
this is the analysis half of the cloud benchmark pipeline, invoked by
.github/workflows/benchmark.yml after `./gradlew :clickhouse-r2dbc-reactive-benchmarks:jmh` has
produced JMH's own JSON result file. It never runs the benchmark itself and never talks to
ClickHouse - it only reads three inputs already sitting on disk:

  1. results.json   - JMH's own --result-format=JSON output (the score/error/percentile/
                       secondary-metric source of truth; never parsed from stdout).
  2. raw-stdout.log  - the full console output of the `jmh` Gradle task, captured via `tee` in the
                       workflow. PublicApiMatchedPoolThroughputBenchmark logs a per-query latency
                       percentile line once per measurement iteration via SLF4J
                       (see logLatencySummary in that class) - JMH's own JSON has no per-query
                       latency at all in Throughput mode, only the aggregate score, so this is the
                       only source for p50/p90/p95/p99.
  3. metadata.json   - environment/run metadata the workflow writes before invoking this script
                       (commit SHA, branch, JDK, OS/arch, ClickHouse image, client-v2 version,
                       driver version, fork/warmup/pool-size counts, profile name).

Produces, in --out-dir:
  - summary.md         - one readable report: environment header, then one table per concurrency
                          tier (throughput ops/s, ourDriver/clientV2 ratio, p50/p90/p95/p99 latency,
                          bytes/op allocation when -prof gc data is present).
  - throughput.png      \
  - latency-p99.png      | the three charts the roadmap plan asks for - not a dozen.
  - allocation-bytes.png/

Deliberately does not: talk to ClickHouse, invoke Gradle/JMH itself, compute or apply any
regression-gate threshold, or write anything outside --out-dir. Those are explicitly out of scope
for this first PR (see roadmap-archive.md's "Explicitly out of scope for the first PR").
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt

# PublicApiMatchedPoolThroughputBenchmark.logLatencySummary's SLF4J log line, e.g.:
#   ourDriver per-query latency (us, n=4096): mean=123.4, p50=100, p90=200, p95=250, p99=400, p99.9=600, max=1200
# slf4j-simple prefixes it with "[thread] LEVEL logger - ", which this pattern ignores by
# matching only the part after the last " - " (kept liberal on purpose: the exact SLF4J prefix
# format isn't a contract this script should be coupled to).
LATENCY_LINE = re.compile(
    r"(?P<driver>ourDriver|clientV2) per-query latency .*?n=(?P<n>\d+)\):"
    r" mean=(?P<mean>[\d.]+), p50=(?P<p50>\d+), p90=(?P<p90>\d+), p95=(?P<p95>\d+),"
    r" p99=(?P<p99>\d+), p99\.9=(?P<p999>\d+), max=(?P<max>\d+)"
)

DRIVER_LABELS = {"ourDriver": "ourDriver", "clientV2": "client-v2"}
BAR_COLORS = {"ourDriver": "#2f6fed", "clientV2": "#e8813a"}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--results-json", required=True, type=Path, help="JMH results.json")
    parser.add_argument("--stdout-log", required=True, type=Path, help="captured `jmh` task stdout")
    parser.add_argument("--metadata-json", required=True, type=Path, help="run metadata.json")
    parser.add_argument("--out-dir", required=True, type=Path, help="output directory")
    return parser.parse_args()


def load_json(path: Path) -> object:
    with path.open(encoding="utf-8") as f:
        return json.load(f)


def method_name(benchmark_fqn: str) -> str:
    """'io.github...PublicApiMatchedPoolThroughputBenchmark.ourDriver' -> 'ourDriver'."""
    return benchmark_fqn.rsplit(".", 1)[-1]


def extract_throughput_rows(results: list[dict]) -> dict[int, dict[str, dict]]:
    """{concurrency: {driver: {"score", "error", "unit", "alloc_bytes_per_op"}}}."""
    rows: dict[int, dict[str, dict]] = {}
    for entry in results:
        driver = method_name(entry["benchmark"])
        if driver not in DRIVER_LABELS:
            continue
        concurrency = int(entry["params"]["concurrency"])
        primary = entry["primaryMetric"]
        alloc = None
        secondary = entry.get("secondaryMetrics", {})
        norm = secondary.get("·gc.alloc.rate.norm") or secondary.get("gc.alloc.rate.norm")
        if norm is not None:
            alloc = norm.get("score")
        rows.setdefault(concurrency, {})[driver] = {
            "score": primary["score"],
            "error": primary.get("scoreError"),
            "unit": primary.get("scoreUnit", "ops/s"),
            "alloc_bytes_per_op": alloc,
        }
    return rows


def extract_latency_rows(stdout_text: str) -> dict[str, list[dict]]:
    """{driver: [one dict per logged iteration, in file order]} - no concurrency tag in the log
    line itself, so the caller must zip these against results.json's own ordering (JMH runs
    fork-by-fork, method-by-method, iteration-by-iteration; @Param sweeps run as a full set of
    forks per concurrency value before moving to the next, matching the order results.json lists
    them in - see the run loop assumption documented in build_report)."""
    rows: dict[str, list[dict]] = {"ourDriver": [], "clientV2": []}
    for match in LATENCY_LINE.finditer(stdout_text):
        d = match.groupdict()
        rows[d["driver"]].append(
            {
                "n": int(d["n"]),
                "mean": float(d["mean"]),
                "p50": int(d["p50"]),
                "p90": int(d["p90"]),
                "p95": int(d["p95"]),
                "p99": int(d["p99"]),
                "p99_9": int(d["p999"]),
                "max": int(d["max"]),
            }
        )
    return rows


def average_latency_per_concurrency(
    results: list[dict], latency_rows: dict[str, list[dict]]
) -> dict[int, dict[str, dict]]:
    """Matches logged latency iterations to each (driver, concurrency) benchmark entry by the
    order both JMH's results.json and the stdout log were produced in, then averages every
    measurement iteration's percentiles for that entry. results.json lists one entry per
    (benchmark method, @Param combination) already in run order, and each entry's own
    measurementIterations count says how many latency lines belong to it - that's the anchor,
    since the latency log itself carries no concurrency value."""
    cursors = {"ourDriver": 0, "clientV2": 0}
    out: dict[int, dict[str, dict]] = {}
    for entry in results:
        driver = method_name(entry["benchmark"])
        if driver not in DRIVER_LABELS:
            continue
        concurrency = int(entry["params"]["concurrency"])
        iterations = int(entry.get("measurementIterations", 1)) * int(entry.get("forks", 1))
        start = cursors[driver]
        end = start + iterations
        chunk = latency_rows[driver][start:end]
        cursors[driver] = end
        if not chunk:
            out.setdefault(concurrency, {})[driver] = None
            continue
        averaged = {
            key: sum(row[key] for row in chunk) / len(chunk)
            for key in ("p50", "p90", "p95", "p99", "p99_9", "mean")
        }
        out.setdefault(concurrency, {})[driver] = averaged
    return out


def build_report(
    metadata: dict,
    throughput: dict[int, dict[str, dict]],
    latency: dict[int, dict[str, dict]],
) -> str:
    lines = ["# Benchmark summary", ""]
    lines.append(f"- Benchmark: `{metadata.get('benchmark', 'PublicApiMatchedPoolThroughputBenchmark')}`")
    lines.append(f"- Run date: {metadata.get('runDate', 'unknown')}")
    lines.append(f"- Profile: **{metadata.get('profile', 'unknown')}**")
    lines.append(f"- Commit: `{metadata.get('commitSha', 'unknown')}` ({metadata.get('branch', 'unknown')})")
    lines.append(
        f"- Driver version: {metadata.get('driverVersion', 'unknown')}, "
        f"client-v2 version: {metadata.get('clientV2Version', 'unknown')}"
    )
    lines.append(
        f"- JDK: {metadata.get('jdkVersion', 'unknown')}, OS/arch: "
        f"{metadata.get('osName', 'unknown')}/{metadata.get('osArch', 'unknown')}"
    )
    lines.append(
        f"- Runner: {metadata.get('cpuModel', 'unknown')}, {metadata.get('cpuCores', 'unknown')} cores, "
        f"{metadata.get('ramGb', 'unknown')} GB RAM"
    )
    lines.append(f"- ClickHouse image: `{metadata.get('clickHouseImage', 'unknown')}`")
    lines.append(
        f"- Forks: {metadata.get('forks', 'unknown')}, warmup iterations: "
        f"{metadata.get('warmupIterations', 'unknown')}, pool size: {metadata.get('poolSize', 'unknown')}"
    )
    lines.append("")
    lines.append(
        "> Per the pipeline's trust model (roadmap-archive.md Phase 10): don't trust an absolute "
        "number from a single run on a shared runner - trust the ourDriver/client-v2 ratio, "
        "repeated across several runs."
    )
    lines.append("")
    lines.append(
        "> **p50/p90/p95/p99 below are the mean of each measurement iteration's own HdrHistogram "
        "percentile, not one percentile computed over all samples merged together.** JMH logs one "
        "percentile set per iteration (see `logLatencySummary`); this script averages those "
        "per-iteration values rather than merging the underlying histograms, so a genuine p99 "
        "outlier confined to one iteration can be smoothed out here. The *direction* of a "
        "comparison (which driver is faster) is unaffected, but treat the exact number as a "
        "mean-of-iteration-p99, not a statistically precise global p99, until this is replaced with "
        "a true merged-histogram calculation (tracked in ROADMAP.md)."
    )
    lines.append("")

    for concurrency in sorted(throughput):
        lines.append(f"## concurrency={concurrency}")
        lines.append("")
        lines.append(
            "| driver | throughput (ops/s) | error | p50 (avg-of-iters, us) | p90 (avg-of-iters, us) "
            "| p95 (avg-of-iters, us) | p99 (avg-of-iters, us) | B/op |"
        )
        lines.append("|---|---|---|---|---|---|---|---|")
        tier = throughput[concurrency]
        tier_latency = latency.get(concurrency, {})
        for driver in ("ourDriver", "clientV2"):
            if driver not in tier:
                continue
            t = tier[driver]
            lat = tier_latency.get(driver)
            alloc = f"{t['alloc_bytes_per_op']:.1f}" if t["alloc_bytes_per_op"] is not None else "n/a"
            if lat:
                p50, p90, p95, p99 = (f"{lat[k]:.0f}" for k in ("p50", "p90", "p95", "p99"))
            else:
                p50 = p90 = p95 = p99 = "n/a"
            lines.append(
                f"| {DRIVER_LABELS[driver]} | {t['score']:.1f} | "
                f"{'±' + format(t['error'], '.1f') if t['error'] is not None else 'n/a'} | "
                f"{p50} | {p90} | {p95} | {p99} | {alloc} |"
            )
        if "ourDriver" in tier and "clientV2" in tier and tier["clientV2"]["score"] > 0:
            ratio = tier["ourDriver"]["score"] / tier["clientV2"]["score"]
            lines.append("")
            lines.append(f"ourDriver/client-v2 throughput ratio: **{ratio:.2f}**")
        lines.append("")

    return "\n".join(lines)


def plot_throughput(throughput: dict[int, dict[str, dict]], out_path: Path) -> None:
    concurrencies = sorted(throughput)
    _grouped_bar_chart(
        concurrencies,
        {d: [throughput[c].get(d, {}).get("score") for c in concurrencies] for d in DRIVER_LABELS},
        ylabel="throughput (ops/s)",
        title="PublicApiMatchedPoolThroughputBenchmark - throughput",
        out_path=out_path,
    )


def plot_latency(latency: dict[int, dict[str, dict]], out_path: Path) -> None:
    concurrencies = sorted(latency)
    _grouped_bar_chart(
        concurrencies,
        {
            d: [
                (latency[c].get(d) or {}).get("p99")
                for c in concurrencies
            ]
            for d in DRIVER_LABELS
        },
        ylabel="p99 latency (us)",
        title="PublicApiMatchedPoolThroughputBenchmark - p99 per-query latency",
        out_path=out_path,
    )


def plot_allocation(throughput: dict[int, dict[str, dict]], out_path: Path) -> None:
    concurrencies = sorted(throughput)
    values = {
        d: [throughput[c].get(d, {}).get("alloc_bytes_per_op") for c in concurrencies]
        for d in DRIVER_LABELS
    }
    if all(v is None for series in values.values() for v in series):
        fig, ax = plt.subplots(figsize=(6, 3))
        ax.text(
            0.5,
            0.5,
            "No allocation data (fast profile doesn't run -prof gc)",
            ha="center",
            va="center",
        )
        ax.axis("off")
        fig.savefig(out_path, dpi=150, bbox_inches="tight")
        plt.close(fig)
        return
    _grouped_bar_chart(
        concurrencies,
        values,
        ylabel="allocation (B/op)",
        title="PublicApiMatchedPoolThroughputBenchmark - allocation per query",
        out_path=out_path,
    )


def _grouped_bar_chart(
    concurrencies: list[int],
    series: dict[str, list],
    ylabel: str,
    title: str,
    out_path: Path,
) -> None:
    fig, ax = plt.subplots(figsize=(7, 4))
    width = 0.35
    x = range(len(concurrencies))
    for i, (driver, values) in enumerate(series.items()):
        offset = (i - 0.5) * width
        plotted = [v if v is not None else 0 for v in values]
        ax.bar(
            [xi + offset for xi in x],
            plotted,
            width=width,
            label=DRIVER_LABELS[driver],
            color=BAR_COLORS[driver],
        )
    ax.set_xticks(list(x))
    ax.set_xticklabels([f"c={c}" for c in concurrencies])
    ax.set_ylabel(ylabel)
    ax.set_title(title)
    ax.legend()
    fig.tight_layout()
    fig.savefig(out_path, dpi=150, bbox_inches="tight")
    plt.close(fig)


def main() -> int:
    args = parse_args()
    args.out_dir.mkdir(parents=True, exist_ok=True)

    results = load_json(args.results_json)
    metadata = load_json(args.metadata_json)
    stdout_text = args.stdout_log.read_text(encoding="utf-8", errors="replace")

    throughput = extract_throughput_rows(results)
    if not throughput:
        print(
            f"No PublicApiMatchedPoolThroughputBenchmark entries found in {args.results_json}",
            file=sys.stderr,
        )
        return 1

    latency_rows = extract_latency_rows(stdout_text)
    latency = average_latency_per_concurrency(results, latency_rows)

    summary_md = build_report(metadata, throughput, latency)
    (args.out_dir / "summary.md").write_text(summary_md, encoding="utf-8")

    plot_throughput(throughput, args.out_dir / "throughput.png")
    plot_latency(latency, args.out_dir / "latency-p99.png")
    plot_allocation(throughput, args.out_dir / "allocation-bytes.png")

    print(f"Wrote {args.out_dir}/summary.md, throughput.png, latency-p99.png, allocation-bytes.png")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
