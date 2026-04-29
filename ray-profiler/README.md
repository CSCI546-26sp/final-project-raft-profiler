# ray-profiler

A critical path profiler for Ray jobs. It answers one question Ray's dashboard doesn't: which tasks actually determined when your job finished?

## Repository layout

From the course repo root, the profiler project lives under **`ray-profiler/`**. That directory is laid out as follows:

```
ray-profiler/
  profiler/           # Python package: instrumentation, critical path, HTML dashboard
  benchmarks/         # Runnable Ray workloads (map-reduce, batch prediction, etc.)
  report/             # Course report
  output/             # Generated HTML and JSON (gitignored)
  requirements.txt
```

`output/` holds generated HTML and JSON (gitignored). The course report LaTeX lives under `report/`. Sample or captured timing data may appear as `run_profile.json` at the project root when you run the profiler.

## Setup

```bash
pip install -r requirements.txt
```

Or install Ray directly:

```bash
pip install ray[default]
```

For the Modin benchmark:

```bash
pip install modin[ray]
```

For the Daft benchmark, install Daft per [Daft docs](https://docs.daft.ai/) and use the dependencies your `daft-run.py` expects.

Ray shows task timelines and resource usage but not whether speeding up a particular task would shorten the job. This profiler finds the longest chain of dependent tasks (the critical path) and highlights it. Tasks not on that chain could be much faster without changing completion time under the captured dependency model.

## How it works

At submission time the profiler wraps `ray.remote`, records `ObjectRef` arguments as dependency edges, then after the job finishes pulls timings from Ray's State API, builds the graph, runs a longest-path DP, and writes HTML.

Integration:

```python
import ray
from profiler import profile, print_critical_path

ray.init()
profile()

print_critical_path()
```

Optional: decorate hot helpers with `track_function` (see `user_fn_tracker.py`) so the dashboard can show user Python functions per task.

## Running the benchmarks

Run from the `ray-profiler` directory (the folder that contains `profiler/`, `benchmarks/`, and `requirements.txt`).

**Map-reduce**

```bash
python benchmarks/mapreduce.py
python benchmarks/mapreduce.py --inject-fault 1 --fault-delay 2.0
```

**Batch prediction**

```bash
python benchmarks/batch_prediction.py --fault-type loader --fault-delay 2.0
python benchmarks/batch_prediction.py --fault-type predictor --fault-delay 2.0
```

**Tree reduce**

```bash
python benchmarks/tree_reduce.py --inject-fault 0 --fault-delay 2.0
```

**RL straggler**

```bash
python benchmarks/rl_straggler.py
```

**Pi estimation**

```bash
python benchmarks/pi_estimation.py
```

**Modin skew**

```bash
python benchmarks/modin_skew.py
```

**Daft on Ray**

```bash
python benchmarks/daft-run.py
```

**Parameter server (demo)**

```bash
python benchmarks/parameter_server.py --mode sync
python benchmarks/parameter_server.py --mode async --inject-fault 1 --fault-delay 2.0
```

Dashboards default under `output/` when benchmarks pass `output_html=...`; some scripts use paths like `output/mapreduce_dashboard.html`.

## What the dashboard shows

- **Timeline** — Gantt-style bars; critical-path tasks emphasized; segments for waiting vs compute where breakdown data exists.
- **DAG** — Force-directed dependency graph; critical-path edges highlighted.
- **Breakdown table** — Sortable columns including optional **User fn** (from `track_function`) and straggler hints where applicable.

## Limitations

Ray Core emphasis: dependencies come from `ObjectRef` arguments at user-visible `remote` calls. Frameworks that hide refs may yield incomplete graphs.

Timing is collected after the job completes via the State API (post-hoc snapshot).

Nested dicts are not walked for refs; lists and tuples are unpacked one level.

Third-party pipelines (e.g. Daft, Modin internals) may overlap driver-side tasks with worker compute; interpret CP together with timelines and labels.
