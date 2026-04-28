from __future__ import annotations
from collections import deque, defaultdict
from dataclasses import dataclass, field


@dataclass
class CriticalPathResult:
    path:      list[str]               
    cp_value:  dict[str, float]        
    cp_prev:   dict[str, str | None]   
    terminal:  str                     
    path_set:  set[str]                


def _topological_sort(
    timing:        dict[str, dict],
    resolved_deps: dict[str, list[str]],
) -> list[str]:
    in_degree:  dict[str, int]         = {tid: 0 for tid in timing}
    dependents: dict[str, list[str]]   = defaultdict(list)

    for tid, deps in resolved_deps.items():
        if tid not in timing:
            continue
        for dep in deps:
            if dep in timing:
                in_degree[tid] += 1
                dependents[dep].append(tid)

    queue = deque(tid for tid in timing if in_degree[tid] == 0)
    order: list[str] = []

    while queue:
        tid = queue.popleft()
        order.append(tid)
        for child in dependents[tid]:
            in_degree[child] -= 1
            if in_degree[child] == 0:
                queue.append(child)

    if len(order) != len(timing):
        raise RuntimeError("Cycle detected in task dependency graph. Cannot compute critical path.")
    
    return order


def compute_critical_path(
    timing:        dict[str, dict],
    resolved_deps: dict[str, list[str]],
) -> CriticalPathResult:
    if not timing:
        return CriticalPathResult([], {}, {}, "", set())

    topo_order = _topological_sort(timing, resolved_deps)

    cp_value: dict[str, float]       = {}
    cp_prev:  dict[str, str | None]  = {}

    job_start = min(t["start_ms"] for t in timing.values())

    for tid in topo_order:
        exec_ms  = timing[tid]["exec_ms"]
        start_ms = timing[tid]["start_ms"]
        best_dep    = None
        best_dep_cp = 0.0
        
        # Step 1A: Try to find explicit dependencies first (Modin / Ray Data)
        for dep in resolved_deps.get(tid, []):
            if dep in cp_value and cp_value[dep] > best_dep_cp:
                best_dep_cp = cp_value[dep]
                best_dep    = dep
        
        # Step 1B: The Fallback Heuristic (Daft / Hidden Actors)
        if best_dep is None:
            min_time_gap = float('inf')
            for prev_tid, prev_timing in timing.items():
                if prev_tid != tid and prev_timing["end_ms"] <= start_ms:
                    gap = start_ms - prev_timing["end_ms"]
                    if gap < min_time_gap:
                        min_time_gap = gap
                        best_dep = prev_tid
            
            if best_dep is not None:
                best_dep_cp = cp_value.get(best_dep, 0.0)
                if tid not in resolved_deps:
                    resolved_deps[tid] = []
                if best_dep not in resolved_deps[tid]:
                    resolved_deps[tid].append(best_dep)

        # Step 2: Calculate Scheduling Delay based on the FINAL best_dep
        if best_dep is not None:
            gap = start_ms - timing[best_dep]["end_ms"]
            scheduling_delay = max(0.0, gap) 
        else:
            # If STILL None, it's the absolute first task in the entire job
            scheduling_delay = max(0.0, start_ms - job_start)
            
        cp_value[tid] = exec_ms + best_dep_cp + scheduling_delay
        cp_prev[tid]  = best_dep

    terminal = max(cp_value, key=cp_value.get)
    path: list[str] = []
    cur = terminal
    while cur is not None:
        path.append(cur)
        cur = cp_prev.get(cur)
    path.reverse()

    return CriticalPathResult(
        path     = path,
        cp_value = cp_value,
        cp_prev  = cp_prev,
        terminal = terminal,
        path_set = set(path),
    )
