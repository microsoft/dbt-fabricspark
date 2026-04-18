# tests/tools

Helper tools for working with this repo's test outputs.

## `render_mermaid.py`

Renders a [mermaid](https://mermaid.js.org/) **flowchart** in a markdown file
that visualizes the most recent test run: each test is a box (labelled with
its nodeid, total duration, and outcome), arrows connect tests in the order
they ran, and box fill color is mapped from duration onto a hue gradient —
**green = fastest, red = slowest**.

### Inputs

The renderer reads a JSON file produced by [`pytest-json-report`](https://github.com/numirias/pytest-json-report). 

That plugin is wired into this repo's `pyproject.toml` `addopts`, so **every `pytest` invocation already drops** 
`logs/test-runs/report.json` automatically.

### Generating the diagram

Mermaid generation is intentionally **manual**:

```bash
uv run python tests/tools/render_mermaid.py
```

This writes `logs/test-runs/report.md`. Open it in any markdown viewer and view in [Mermaid Flow](https://www.mermaidflow.app/editor).

### CLI flags

| Flag       | Default                      | Purpose                             |
| ---------- | ---------------------------- | ----------------------------------- |
| `--input`  | `logs/test-runs/report.json` | Path to the pytest-json-report JSON |
| `--output` | `logs/test-runs/report.md`   | Path to write the markdown to       |

### Output

A single markdown file containing:

1. A `mermaid` code block showing test execution. When tests ran sequentially,
   it's a single top-down chain (`flowchart TD`). When tests ran in parallel, 
   it's a left-to-right chart. Otherwise lanes are inferred from overlapping 
   start/stop timestamps via greedy interval coloring.

2. A summary table (`#`, lane, test, duration, outcome) sorted by global
   wall-clock start time.

Per-test `start` / `stop` / `worker` metadata is recorded by a tiny
`pytest_json_runtest_metadata` hook in `tests/conftest.py` (cost: two
timestamp reads per test).