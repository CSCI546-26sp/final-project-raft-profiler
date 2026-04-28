# CS 546 Final Project — Raft Profiler

Course workspace containing **raft-profiler**: a post-hoc **critical path profiler for [Ray](https://www.ray.io/)** jobs.

## Contents

| Path | Purpose |
|------|--------|
| [`raft-profiler/`](raft-profiler/) | Library, benchmarks, HTML dashboard generator |
| [`raft-profiler/README.md`](raft-profiler/README.md) | Setup, how to run benchmarks, limitations |
| [`raft-profiler/report/`](raft-profiler/report/) | Course report (`report.tex`; build PDF locally or on Overleaf) |

## Quick start

Python **3.10+**. From `raft-profiler/`:

```bash
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt
python benchmarks/mapreduce.py
```

On macOS/Linux: `source .venv/bin/activate`.

Generated dashboards usually go under `raft-profiler/output/` (ignored by git unless committed).

## Idea

Ray’s UI shows timelines and utilization, not which tasks bounded wall-clock time. This project records ref edges at submission, combines them with State API timings, computes the weighted longest path (critical path), and renders a static HTML report.

Course details and citations are in `report/report.tex`.
