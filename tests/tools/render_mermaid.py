"""Render a mermaid flowchart from a pytest-json-report JSON file.

The renderer reconstructs a *global* execution timeline. When tests ran
sequentially (no xdist), all tests appear in a single lane. When tests ran
in parallel (xdist workers, or any other concurrent execution detected via
overlapping timestamps), tests are grouped into vertical lanes (subgraphs)
showing which tests were running at the same time.

Box fill color is mapped from each test's total duration onto an HSL hue
gradient: green (fast) -> yellow -> red (slow).

Usage:
    python tests/tools/render_mermaid.py
    python tests/tools/render_mermaid.py --input logs/test-runs/report.json \\
                                         --output logs/test-runs/report.md
"""

from __future__ import annotations

import argparse
import json
import sys
from dataclasses import dataclass
from pathlib import Path

DEFAULT_INPUT = Path("logs/test-runs/report.json")
DEFAULT_OUTPUT = Path("logs/test-runs/report.md")


@dataclass(frozen=True)
class TestRecord:
    __test__ = False  # prevent pytest from collecting this dataclass as a test

    nodeid: str
    duration: float
    outcome: str
    start: float | None = None  # wall-clock seconds (epoch); None if unknown
    stop: float | None = None
    worker: str | None = None  # xdist worker id, or "main"; None if unknown


def load_report(path: Path) -> list[TestRecord]:
    """Load the json report and return TestRecords in pytest's recorded order."""
    data = json.loads(path.read_text(encoding="utf-8"))
    raw_tests = data.get("tests", [])
    records: list[TestRecord] = []
    for t in raw_tests:
        nodeid = t.get("nodeid", "?")
        outcome = t.get("outcome", "unknown")
        duration = 0.0
        for phase in ("setup", "call", "teardown"):
            ph = t.get(phase) or {}
            duration += float(ph.get("duration", 0.0) or 0.0)
        meta = t.get("metadata") or {}
        start = meta.get("start")
        stop = meta.get("stop")
        worker = meta.get("worker")
        records.append(
            TestRecord(
                nodeid=nodeid,
                duration=duration,
                outcome=outcome,
                start=float(start) if start is not None else None,
                stop=float(stop) if stop is not None else None,
                worker=worker,
            )
        )
    return records


def hue_for(duration: float, min_d: float, max_d: float) -> float:
    """Map duration to a hue in degrees: 120 (green) for min, 0 (red) for max.

    When max == min (e.g. all tests took the same time, or only one test),
    return 60 (yellow) so the diagram is still readable.
    """
    if max_d <= min_d:
        return 60.0
    ratio = (duration - min_d) / (max_d - min_d)
    ratio = max(0.0, min(1.0, ratio))
    return 120.0 * (1.0 - ratio)


def hsl_to_hex(h: float, s: float = 0.65, l: float = 0.65) -> str:
    """Convert HSL (h in degrees, s/l in 0..1) to a #RRGGBB hex string."""
    import colorsys

    r, g, b = colorsys.hls_to_rgb(h / 360.0, l, s)
    return "#{:02X}{:02X}{:02X}".format(int(r * 255), int(g * 255), int(b * 255))


def color_for(duration: float, min_d: float, max_d: float) -> str:
    return hsl_to_hex(hue_for(duration, min_d, max_d))


def _short_label(nodeid: str) -> str:
    """Shorten a pytest nodeid for display inside a mermaid box."""
    nid = nodeid.replace("::", " ▸ ")
    if len(nid) > 80:
        nid = "…" + nid[-79:]
    return nid.replace('"', "'")


def assign_lanes(records: list[TestRecord]) -> list[list[int]]:
    """Group tests into lanes representing concurrent execution tracks.

    Returns a list of lanes; each lane is a list of indices into ``records``
    (lane order = the order tests ran on that lane).

    Strategy:
      * If every record has a ``worker`` tag, group by worker (each xdist
        worker is its own lane). This is the most accurate.
      * Else, if every record has start/stop timestamps, run a greedy interval
        coloring: walk tests sorted by start time; place each on the
        lowest-indexed lane whose previous test's stop <= this test's start;
        otherwise open a new lane.
      * Else, fall back to a single lane in the order pytest reported them.
    """
    if not records:
        return []

    if all(r.worker for r in records):
        lanes_by_worker: dict[str, list[int]] = {}
        # Sort indices by (worker, start-or-index) so within a lane the order
        # matches execution order.
        indexed = list(enumerate(records))
        indexed.sort(
            key=lambda ir: (
                ir[1].worker or "",
                ir[1].start if ir[1].start is not None else ir[0],
            )
        )
        for i, r in indexed:
            lanes_by_worker.setdefault(r.worker, []).append(i)
        # Stable lane order: by the start time of the first test on each lane.
        return sorted(
            lanes_by_worker.values(),
            key=lambda lane: (
                records[lane[0]].start if records[lane[0]].start is not None else lane[0]
            ),
        )

    if all(r.start is not None and r.stop is not None for r in records):
        order = sorted(range(len(records)), key=lambda i: records[i].start)
        lanes: list[list[int]] = []
        lane_last_stop: list[float] = []
        for i in order:
            placed = False
            for lane_idx, last_stop in enumerate(lane_last_stop):
                if records[i].start >= last_stop:
                    lanes[lane_idx].append(i)
                    lane_last_stop[lane_idx] = records[i].stop
                    placed = True
                    break
            if not placed:
                lanes.append([i])
                lane_last_stop.append(records[i].stop)
        return lanes

    # Fallback: no timing info, single lane in recorded order.
    return [list(range(len(records)))]


def _lane_label(records: list[TestRecord], lane: list[int], lane_idx: int) -> str:
    """Human-friendly label for a lane subgraph."""
    workers = {records[i].worker for i in lane if records[i].worker}
    if len(workers) == 1:
        (only,) = workers
        return f"worker: {only}"
    return f"lane {lane_idx}"


def render_mermaid(records: list[TestRecord]) -> str:
    """Render the mermaid flowchart block (no fences) for the given records."""
    if not records:
        return 'flowchart TD\n    empty["No tests recorded"]\n'

    durations = [r.duration for r in records]
    min_d, max_d = min(durations), max(durations)

    lanes = assign_lanes(records)
    multi_lane = len(lanes) > 1

    def node_id(idx: int) -> str:
        return f"t{idx}"

    lines: list[str] = []
    if multi_lane:
        lines.append("flowchart LR")
    else:
        lines.append("flowchart TD")

    for lane_idx, lane in enumerate(lanes):
        if multi_lane:
            label = _lane_label(records, lane, lane_idx).replace('"', "'")
            lines.append(f'    subgraph lane{lane_idx}["{label}"]')
            lines.append("        direction TB")

        prefix = "        " if multi_lane else "    "

        for idx in lane:
            rec = records[idx]
            label = f"{_short_label(rec.nodeid)}<br/>{rec.duration:.3f}s ({rec.outcome})"
            lines.append(f'{prefix}{node_id(idx)}["{label}"]')

        for prev, nxt in zip(lane, lane[1:]):
            lines.append(f"{prefix}{node_id(prev)} --> {node_id(nxt)}")

        if multi_lane:
            lines.append("    end")

    for idx, rec in enumerate(records):
        fill = color_for(rec.duration, min_d, max_d)
        lines.append(f"    style {node_id(idx)} fill:{fill},stroke:#333,color:#000")

    return "\n".join(lines) + "\n"


def render_markdown(records: list[TestRecord]) -> str:
    """Render the full markdown document (mermaid block + summary table)."""
    lanes = assign_lanes(records)
    parts: list[str] = []
    parts.append("# Test execution report\n")
    parts.append(
        "Auto-generated from `logs/test-runs/report.json` by "
        "`tests/tools/render_mermaid.py`. Do not edit by hand.\n"
    )
    if records:
        if len(lanes) > 1:
            parts.append(
                f"Detected **{len(lanes)} parallel execution lanes** "
                f"({sum(len(l) for l in lanes)} tests). Each subgraph below is a "
                "lane; tests within a lane ran sequentially, tests in different "
                "lanes overlapped in time.\n"
            )
        else:
            parts.append(f"{len(records)} tests, single execution lane (sequential).\n")
    parts.append("## Execution flow\n")
    parts.append("```mermaid\n" + render_mermaid(records) + "```\n")

    parts.append("## Summary\n")
    parts.append("| # | Lane | Test | Duration (s) | Outcome |")
    parts.append("|---|------|------|--------------|---------|")
    # Build a per-test (lane, position) map for the summary table, then sort
    # by execution start so the table reflects global wall-clock order.
    lane_of: dict[int, int] = {}
    for lane_idx, lane in enumerate(lanes):
        for i in lane:
            lane_of[i] = lane_idx
    ordered = sorted(
        range(len(records)),
        key=lambda i: (
            records[i].start if records[i].start is not None else i,
            lane_of.get(i, 0),
        ),
    )
    for rank, i in enumerate(ordered, start=1):
        rec = records[i]
        safe = rec.nodeid.replace("|", "\\|")
        parts.append(
            f"| {rank} | {lane_of.get(i, 0)} | `{safe}` | {rec.duration:.3f} | {rec.outcome} |"
        )
    parts.append("")
    return "\n".join(parts)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--input",
        type=Path,
        default=DEFAULT_INPUT,
        help=f"Path to pytest-json-report JSON (default: {DEFAULT_INPUT})",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=DEFAULT_OUTPUT,
        help=f"Path to write markdown to (default: {DEFAULT_OUTPUT})",
    )
    args = parser.parse_args(argv)

    if not args.input.exists():
        print(
            f"error: input report {args.input} does not exist; run pytest first to generate it.",
            file=sys.stderr,
        )
        return 2

    records = load_report(args.input)
    md = render_markdown(records)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(md, encoding="utf-8")
    lanes = assign_lanes(records)
    print(f"wrote {args.output} ({len(records)} tests, {len(lanes)} lane(s))")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
