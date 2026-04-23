# raft-profiler

A critical path profiler for Ray jobs. It answers one question Ray's dashboard doesn't: which tasks actually determined when your job finished?

## Repository layout

```
raft-profiler/
  profiler/           # Library: dependency capture, critical path, dashboard
  benchmarks/         # Example Ray workloads (run from this directory)
  requirements.txt    # Dependency set for reproducible installs
  output/             # Generated HTML/JSON (gitignored)
```

## Setup

Install pinned dependencies (recommended for grading or reproduction):

```bash
pip install -r requirements.txt
```

Or install Ray directly:

```bash
pip install ray[default]
```

For the Modin benchmark only:

```bash
pip install modin[ray]
```

Ray shows you task timelines and resource usage. But if your job took 12 seconds, it doesn't tell you whether speeding up any particular task would have helped. This profiler does. It finds the longest chain of dependent tasks through the execution DAG — the critical path — and highlights it. Tasks not on that chain could be 10x faster and it wouldn't change a thing.

## How it works

At submission time, the profiler wraps `ray.remote` to intercept every task call and record which ObjectRefs were passed as arguments. This gives it the actual data dependencies (not just the spawn parent that Ray's State API tracks). After the job finishes, it pulls timing from the State API, builds the dependency DAG, runs a topological DP to find the longest path, and writes a dashboard.

Integration is two lines:

```python
import ray
from profiler import profile, print_critical_path

ray.init()
profile()          # call before defining any @ray.remote tasks

# ... your existing code, unchanged ...

print_critical_path()   # call at the end
```

## Running the benchmarks

All benchmarks are in `benchmarks/` and should be run from the project root.

**Map-reduce** — reducers depend on all mappers via ObjectRefs:
```bash
python benchmarks/mapreduce.py
python benchmarks/mapreduce.py --inject-fault 1 --fault-delay 2.0
```

**Batch prediction** — one loader feeds N parallel predictors. Tests shared bottleneck vs single-branch bottleneck:
```bash
python benchmarks/batch_prediction.py --fault-type loader   --fault-delay 2.0
python benchmarks/batch_prediction.py --fault-type predictor --fault-delay 2.0
```

**Tree reduce** — binary tree of aggregations, log₂(N) depth:
```bash
python benchmarks/tree_reduce.py --inject-fault 0 --fault-delay 2.0
```

**RL straggler** — rollout workers feed a learner. One seed runs 35x longer:
```bash
python benchmarks/rl_straggler.py
```

**Pi estimation** — fully parallel (no deps). Critical path is always just the slowest task:
```bash
python benchmarks/pi_estimation.py
```

**Modin data skew** — groupby on a skewed dataset run through Modin:
```bash
python benchmarks/modin_skew.py
```

Dashboards are written to `output/`.

## What the dashboard shows

Three tabs:

- **Timeline** — Gantt view. Critical path tasks are red, everything else is blue. Each bar is split into waiting time (before the task started executing) and compute time.
- **DAG** — Force-directed graph of the dependency structure. Critical path edges are red. You can drag nodes around.
- **Breakdown table** — Sortable table with waiting/compute split per task, CP value, and a mini bar chart.

## Limitations

**Ray Core only.** The profiler captures dependencies by inspecting ObjectRef arguments at the call site. Ray Tune, Ray AIR, and Ray Serve submit tasks internally without exposing ObjectRefs at the user level, so the dependency graph can't be built automatically for those frameworks.

**Post-hoc only.** Timing data comes from the State API after the job finishes. There's no live/streaming view.

**Nested containers.** Dependencies inside dicts or deeply nested structures won't be detected. The profiler walks one level into lists and tuples.
