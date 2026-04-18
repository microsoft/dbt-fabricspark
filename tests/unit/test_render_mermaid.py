from __future__ import annotations

import json
from pathlib import Path

from tests.tools.render_mermaid import (
    TestRecord,
    assign_lanes,
    color_for,
    hue_for,
    load_report,
    render_markdown,
    render_mermaid,
)


def test_hue_for_min_is_green():
    assert hue_for(1.0, 1.0, 5.0) == 120.0


def test_hue_for_max_is_red():
    assert hue_for(5.0, 1.0, 5.0) == 0.0


def test_hue_for_midpoint_is_yellow():
    assert hue_for(3.0, 1.0, 5.0) == 60.0


def test_hue_for_equal_min_max_returns_mid():
    assert hue_for(2.0, 2.0, 2.0) == 60.0


def test_hue_for_clamps_out_of_range():
    assert hue_for(-10.0, 1.0, 5.0) == 120.0
    assert hue_for(99.0, 1.0, 5.0) == 0.0


def test_color_for_returns_hex():
    c = color_for(1.0, 1.0, 5.0)
    assert c.startswith("#") and len(c) == 7


def test_render_mermaid_contains_nodes_arrows_and_styles():
    records = [
        TestRecord("tests/unit/test_a.py::test_one", 0.10, "passed"),
        TestRecord("tests/unit/test_a.py::test_two", 0.50, "passed"),
        TestRecord("tests/unit/test_b.py::test_three", 1.00, "failed"),
    ]
    out = render_mermaid(records)
    assert out.startswith("flowchart TD")
    assert "t0[" in out and "t1[" in out and "t2[" in out
    assert "t0 --> t1" in out
    assert "t1 --> t2" in out
    assert out.count("style t") == 3
    # fastest should map to green-ish, slowest to red-ish
    assert "fill:#" in out


def test_render_mermaid_handles_empty():
    out = render_mermaid([])
    assert "flowchart TD" in out
    assert "No tests recorded" in out


def test_render_markdown_includes_mermaid_block_and_table():
    records = [TestRecord("tests/unit/test_x.py::test_y", 0.01, "passed")]
    md = render_markdown(records)
    assert "```mermaid" in md
    assert "flowchart TD" in md
    assert "| # | Lane | Test |" in md
    assert "test_x.py::test_y" in md


def test_load_report_roundtrip(tmp_path: Path):
    sample = {
        "tests": [
            {
                "nodeid": "tests/unit/test_a.py::test_one",
                "outcome": "passed",
                "setup": {"duration": 0.01},
                "call": {"duration": 0.10},
                "teardown": {"duration": 0.005},
                "metadata": {"start": 1000.0, "stop": 1000.115, "worker": "main"},
            },
            {
                "nodeid": "tests/unit/test_a.py::test_two",
                "outcome": "failed",
                "setup": {"duration": 0.0},
                "call": {"duration": 0.20},
                "teardown": {"duration": 0.0},
            },
        ]
    }
    p = tmp_path / "report.json"
    p.write_text(json.dumps(sample), encoding="utf-8")
    records = load_report(p)
    assert [r.nodeid for r in records] == [
        "tests/unit/test_a.py::test_one",
        "tests/unit/test_a.py::test_two",
    ]
    assert records[0].outcome == "passed"
    assert abs(records[0].duration - 0.115) < 1e-9
    assert abs(records[1].duration - 0.20) < 1e-9
    assert records[0].start == 1000.0
    assert records[0].worker == "main"
    assert records[1].start is None
    assert records[1].worker is None


def test_assign_lanes_no_metadata_single_lane():
    records = [
        TestRecord("a", 0.1, "passed"),
        TestRecord("b", 0.2, "passed"),
    ]
    assert assign_lanes(records) == [[0, 1]]


def test_assign_lanes_by_worker():
    records = [
        TestRecord("a", 0.1, "passed", start=0.0, stop=0.1, worker="gw0"),
        TestRecord("b", 0.1, "passed", start=0.05, stop=0.15, worker="gw1"),
        TestRecord("c", 0.1, "passed", start=0.2, stop=0.3, worker="gw0"),
    ]
    lanes = assign_lanes(records)
    # Two workers -> two lanes; gw0 contains a then c, gw1 contains b
    assert len(lanes) == 2
    flat = sorted([i for lane in lanes for i in lane])
    assert flat == [0, 1, 2]
    # Find lane containing index 0 (gw0); should also contain 2 in order.
    gw0 = next(lane for lane in lanes if 0 in lane)
    assert gw0 == [0, 2]
    gw1 = next(lane for lane in lanes if 1 in lane)
    assert gw1 == [1]


def test_assign_lanes_by_overlap_when_no_worker():
    # Three tests, two overlap -> 2 lanes
    records = [
        TestRecord("a", 1.0, "passed", start=0.0, stop=1.0),
        TestRecord("b", 1.0, "passed", start=0.5, stop=1.5),  # overlaps a
        TestRecord("c", 1.0, "passed", start=2.0, stop=3.0),  # after both
    ]
    lanes = assign_lanes(records)
    assert len(lanes) == 2
    # a and c should be in the same lane (greedy reuses earliest free lane)
    lane_with_a = next(lane for lane in lanes if 0 in lane)
    assert 2 in lane_with_a


def test_assign_lanes_sequential_no_overlap_single_lane():
    records = [
        TestRecord("a", 1.0, "passed", start=0.0, stop=1.0),
        TestRecord("b", 1.0, "passed", start=1.0, stop=2.0),
        TestRecord("c", 1.0, "passed", start=2.0, stop=3.0),
    ]
    assert assign_lanes(records) == [[0, 1, 2]]


def test_render_mermaid_multilane_uses_subgraphs_and_lr():
    records = [
        TestRecord("a", 0.1, "passed", start=0.0, stop=0.1, worker="gw0"),
        TestRecord("b", 0.1, "passed", start=0.0, stop=0.1, worker="gw1"),
    ]
    out = render_mermaid(records)
    assert out.startswith("flowchart LR")
    assert "subgraph lane0" in out
    assert "subgraph lane1" in out
    assert "direction TB" in out
    assert out.count("end") >= 2
