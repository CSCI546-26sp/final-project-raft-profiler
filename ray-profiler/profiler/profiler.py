import ray
import time
import os
from pathlib import Path
from typing import Optional
from ray.util.state import list_tasks, list_workers

from .critical_path import compute_critical_path
from .runtime_breakdown import breakdown_all_tasks, parse_breakdown
from .straggler_detection import detect_stragglers
from .what_if import what_if_speedup
from .live_profiler import LiveProfiler
from .run_comparison import save_run
from .dashboard import generate_dashboard
from .ray_dashboard import dashboard_port
from .user_fn_tracker import (
    ensure_tracker,
    enable_pickle_by_value,
    fetch_records,
    is_tracker_task,
)

_dep_graph      = {}
_active         = False
_live_profiler  = None


def _patch_remote_options(options_fn):
    def patched(*args, **kwargs):
        dep_ids = []
        for arg in list(args) + list(kwargs.values()):
            if isinstance(arg, ray.ObjectRef):
                dep_ids.append(arg.task_id().hex())
            elif isinstance(arg, (list, tuple)):
                for item in arg:
                    if isinstance(item, ray.ObjectRef):
                        dep_ids.append(item.task_id().hex())

        ref = options_fn(*args, **kwargs)

        if isinstance(ref, ray.ObjectRef):
            _dep_graph[ref.task_id().hex()] = dep_ids
        elif isinstance(ref, (list, tuple)):
            for r in ref:
                if isinstance(r, ray.ObjectRef):
                    _dep_graph[r.task_id().hex()] = dep_ids

        return ref
    return patched


def _wrap_remote_callable(remote_callable):
    original_remote = remote_callable.remote
    remote_callable.remote = _patch_remote_options(original_remote)

    original_options = remote_callable.options
    def patched_options(*a, **kw):
        opts = original_options(*a, **kw)
        opts.remote = _patch_remote_options(opts.remote)
        return opts
    remote_callable.options = patched_options

    return remote_callable


def _patch_ray_remote():
    original_ray_remote = ray.remote

    def patched_ray_remote(fn=None, **kwargs):
        if fn is not None:
            result = original_ray_remote(fn)
        else:
            result = original_ray_remote(**kwargs)
            if fn is not None:
                result = result(fn)
            else:
                inner = result
                def double_wrap(f):
                    r = inner(f)
                    return _wrap_remote_callable(r)
                return double_wrap

        return _wrap_remote_callable(result)

    ray.remote = patched_ray_remote


def profile(live: bool = True, poll_interval: float = 2.0):
    global _active, _live_profiler
    _patch_ray_remote()
    _active = True
    print("[profiler] active: tracking ObjectRef dependencies automatically")              
    enable_pickle_by_value()                                                              
    ensure_tracker()
    if live:
        _live_profiler = LiveProfiler(_dep_graph, poll_interval=poll_interval)
        _live_profiler.start()

def print_critical_path(
    wait:        float = 2,
    output_html: str   = "ray_profiler_dashboard.html",
    save_json:   str   = "run_profile.json",
    job_label:   str   = "Ray Job",
):
    if not _active:
        print("[profiler] not active: call profile() first")
        return

    if _live_profiler is not None:
        _live_profiler.stop()

    print("\n[profiler] collecting final task timing from State API...")
    time.sleep(wait)

    all_tasks = list_tasks(detail=True, limit=10_000)                                                          
    try:
        all_workers = list_workers(detail=True, limit=10_000)
        worker_pid: dict[str, int] = {}
        for w in all_workers:
            wid = getattr(w, "worker_id", None)
            wpid = getattr(w, "pid", None)
            if wid and wpid is not None:
                worker_pid[wid] = int(wpid)
    except Exception:
        worker_pid = {}
    timing = {}
    for t in all_tasks:
        func_name = getattr(t, "func_or_class_name", None) or "unknown"
                                                                       
        if is_tracker_task(func_name):
            continue
                                                                           
                                                                       
        display_name = getattr(t, "name", None) or func_name
        if t.start_time_ms and t.end_time_ms:
            wid = getattr(t, "worker_id", None)
            timing[t.task_id] = {
                "name":      display_name,
                "func_name": func_name,
                "start_ms":  t.start_time_ms,
                "end_ms":    t.end_time_ms,
                "exec_ms":   t.end_time_ms - t.start_time_ms,
                "worker_id": wid,
                "worker_pid": worker_pid.get(wid) if wid else None,
            }

    if not timing:
        print("[profiler] no task timing data found")
        return                                                         
    user_fn_records = fetch_records()
    attached = 0
    have_any_pid = any(info["worker_pid"] is not None for info in timing.values())
    if user_fn_records:                
        records_by_tid: dict[str, list[dict]] = {tid: [] for tid in timing}
        for r in user_fn_records:
            r_pid = r.get("pid")
            r_start, r_end = r["start_ms"], r["end_ms"]
            candidates: list[tuple[str, float]] = []
            for tid, info in timing.items():
                if have_any_pid and info["worker_pid"] is not None:
                    if r_pid is None or info["worker_pid"] != r_pid:
                        continue
                                                                         
                                                                    
                                                               
                if (info["start_ms"] - 5) <= r_start and r_end <= (info["end_ms"] + 5):
                    candidates.append((tid, info["end_ms"] - info["start_ms"]))
            if not candidates:
                                                                     
                best_ovl = 0.0
                best_tid: Optional[str] = None
                for tid, info in timing.items():
                    if have_any_pid and info["worker_pid"] is not None:
                        if r_pid is None or info["worker_pid"] != r_pid:
                            continue
                    ovl = min(r_end, info["end_ms"]) - max(r_start, info["start_ms"])
                    if ovl > best_ovl:
                        best_ovl = ovl
                        best_tid = tid
                if best_tid is not None:
                    records_by_tid[best_tid].append(r)
                continue
                                                 
            candidates.sort(key=lambda c: c[1])
            records_by_tid[candidates[0][0]].append(r)

        for tid, recs in records_by_tid.items():
            if not recs:
                continue
            info = timing[tid]
            by_name: dict[str, dict] = {}
            for r in recs:
                d = by_name.setdefault(r["fn_name"], {
                    "name": r["fn_name"], "source": r["source"],
                    "elapsed_ms": 0.0, "calls": 0,
                })
                d["elapsed_ms"] += (r["end_ms"] - r["start_ms"])
                d["calls"] += 1
            ranked = sorted(by_name.values(), key=lambda d: -d["elapsed_ms"])
            for d in ranked:
                d["elapsed_ms"] = round(d["elapsed_ms"], 2)
            info["user_fns"] = ranked
            info["top_user_fn"] = ranked[0]
            attached += 1
    pid_match = "pid+interval" if have_any_pid else "interval-only"
    print(f"[profiler] user-fn records: {len(user_fn_records)} fetched, "
          f"attached to {attached}/{len(timing)} task(s) [{pid_match}]")

    def resolve(dep_hex):
        for tid in timing:
            if tid.startswith(dep_hex) or dep_hex.startswith(tid):
                return tid
        return None

    resolved_deps = {}
    for tid in timing:
        raw = _dep_graph.get(tid, [])
        resolved = [r for r in (resolve(d) for d in raw) if r is not None]
        resolved_deps[tid] = resolved

    cp = compute_critical_path(timing, resolved_deps)

    if not cp.path:
        print("[profiler] could not compute critical path")
        return

                       
    task_events_map = {}
    for t in all_tasks:
        if not t.task_id or t.task_id not in timing:
            continue
        events = []
        if hasattr(t, "events") and t.events:
            for ev in t.events:
                if hasattr(ev, "state_name"):
                    state_str = f"{{'state': '{ev.state_name}', 'created_ms': {ev.creation_time_ms}}}"
                elif isinstance(ev, dict):
                    state_str = str(ev)
                else:
                    state_str = str(ev)
                events.append({"state": state_str, "created_ms": None})
        task_events_map[t.task_id] = events

    breakdowns = {}
    for tid, events in task_events_map.items():
        if events:
            breakdowns[tid] = parse_breakdown(tid, events)

                     
    print("\nAll Tasks\n")
    print(f"  {'Task':<35} {'Exec (ms)':>10}  {'CP (ms)':>10}  {'Critical?'}")
    print(f"  {'_'*35} {'_'*10}  {'_'*10}  {'_'*9}")

    for tid in sorted(timing, key=lambda t: timing[t]["start_ms"]):
        info = timing[tid]
        marker = "yes" if tid in cp.path_set else ""
        print(f"  {info['name']:<35} {info['exec_ms']:>10.0f}  "
              f"{cp.cp_value.get(tid, 0):>10.0f}  {marker}")

                         
    print("\nCritical Path\n")
    print(f"  Total length: {cp.cp_value[cp.terminal]:.0f} ms\n")

    for i, tid in enumerate(cp.path):
        info = timing[tid]
        label = "START" if i == 0 else "     "
        suffix = "  < END" if i == len(cp.path) - 1 else ""
        bd = breakdowns.get(tid)
        if bd:
            bd_str = (f"  [dep {bd.dependency_ms:.0f}ms | queue {bd.queue_ms:.0f}ms"
                      f" | overhead {bd.overhead_ms:.0f}ms | compute {bd.compute_ms:.0f}ms]")
        else:
            bd_str = ""
        print(f"  {label} > {info['name']:<35} ({info['exec_ms']:.0f} ms){bd_str}{suffix}")
        top = info.get("top_user_fn")
        if top:
            print(f"          ran your `{top['name']}` "
                  f"({top['source']}) for {top['elapsed_ms']:.0f}ms"
                  f" across {top['calls']} call(s)")

    print(f"\n  {len(cp.path)} task(s) on the critical path")
    print(f"  {len(timing) - len(cp.path)} task(s) are NOT on the critical path")
    print(f"  Speeding up off-path tasks will NOT reduce total runtime.\n")

                         
    stragglers = detect_stragglers(timing, cp.path)

    if stragglers:
        print("Stragglers Detected\n")
        for s in stragglers:
            print(f"  ! {s.func_name} [{s.task_id[:8]}]  "
                  f"took {s.exec_ms:.0f}ms, median for {s.func_name} is "
                  f"{s.median_ms:.0f}ms ({s.ratio:.1f}x slower, "
                  f"{s.sibling_count} instances)")
        print()
    else:
        print("No stragglers detected on the critical path\n")

                      
    print("What-If Analysis (2x speedup)\n")
    for tid in cp.path:
        info = timing[tid]
        wif = what_if_speedup(timing, resolved_deps, cp, tid, speedup=2.0)
        shift_marker = " ! path shifted!" if wif.path_changed else ""
        print(f"  {info['name']:<30} {info['exec_ms']:>8.0f}ms > "
              f"{wif.modified_exec_ms:>8.0f}ms  |  "
              f"saves {wif.saved_ms:.0f}ms{shift_marker}")
    print()

               
    root_dir = Path(__file__).parent.parent
    if not Path(output_html).is_absolute():
        output_html = str(root_dir / output_html)

    generate_dashboard(
        timing=timing, resolved_deps=resolved_deps,
        cp=cp, breakdowns=breakdowns, stragglers=stragglers,
        output_path=output_html, job_label=job_label,
    )
    print(f"[profiler] open dashboard: file://{os.path.abspath(output_html)}")

                             
    if save_json:
        save_path = str(root_dir / save_json) if not Path(save_json).is_absolute() else save_json
        save_run(
            path=save_path, timing=timing, resolved_deps=resolved_deps,
            cp_path=cp.path, cp_total_ms=cp.cp_value.get(cp.terminal, 0),
            breakdowns=breakdowns,
        )


               
def run_profiled(fn=None, output_html="ray_profiler_dashboard.html",
                 save_json="run_profile.json", job_label="Ray Job"):
    def decorator(func):
        def wrapper(*args, **kwargs):
            ray.init(include_dashboard=True, dashboard_port=dashboard_port())
            profile()
            time.sleep(3)
            try:
                result = func(*args, **kwargs)
            finally:
                print_critical_path(
                    output_html=output_html,
                    save_json=save_json,
                    job_label=job_label,
                )
                ray.shutdown()
            return result
        return wrapper

    if fn is not None:
        return decorator(fn)
    return decorator