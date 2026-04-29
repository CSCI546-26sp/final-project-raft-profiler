# CS 546 Final Project — Raft Profiler

Course workspace containing **ray-profiler**: a post-hoc **critical path profiler for [Ray](https://www.ray.io/)** jobs.

## Contents

| Path | Purpose |
|------|--------|
| [`ray-profiler/`](ray-profiler/) | Library, benchmarks, HTML dashboard generator |
| [`ray-profiler/README.md`](ray-profiler/README.md) | Setup, how to run benchmarks, limitations |
| [`ray-profiler/report/`](ray-profiler/report/) | Course report (`report.tex`; build PDF locally or on Overleaf) |


## Quick start

Python **3.10+**. From `ray-profiler/`:

```bash
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt
python benchmarks/mapreduce.py
```

Generated dashboards usually go under `ray-profiler/output/` 

## Idea

Ray’s UI shows timelines and utilization, not which tasks bounded wall-clock time. This project records ref edges at submission, combines them with State API timings, computes the weighted longest path (critical path), and renders a static HTML report.

Course details and citations are in `ray-profiler/report/report.pdf`.
