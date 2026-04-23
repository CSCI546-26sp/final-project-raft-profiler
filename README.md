# CS 546 Final Project — Raft Profiler

This repository is the course final for **CS 546**: a small workspace that contains **raft-profiler**, a post-hoc **critical path profiler for [Ray](https://www.ray.io/)** jobs.

## What lives here

| Path | Purpose |
|------|--------|
| [`raft-profiler/`](raft-profiler/) | Profiler library, HTML dashboard, and benchmark scripts |
| [`raft-profiler/README.md`](raft-profiler/README.md) | Full design notes, API usage, benchmark commands, and limitations |

## Quick start

Use **Python 3.10+** (Ray-supported). From the `raft-profiler` directory:

```bash
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt
python benchmarks\mapreduce.py
```

On macOS or Linux, activate the venv with `source .venv/bin/activate`.

Generated dashboards and JSON artifacts go under `raft-profiler/output/` (ignored by git).

## Why this project

Ray’s dashboard shows timelines and utilization, but it does not answer: *which tasks actually bounded wall-clock time?* The profiler builds a dependency graph from **ObjectRef** edges captured at task submission, combines it with Ray’s **State API** timings, and computes the **longest path** through the DAG. That path is the critical path: the set of tasks whose cumulative delay determines when the job finished.

## License and course use

Created for CS 546 coursework. See the profiler README for integration details and known limitations (Ray Core–centric capture, post-hoc timing only).
