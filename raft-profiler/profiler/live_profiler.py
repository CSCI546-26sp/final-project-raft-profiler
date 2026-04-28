"""
Live streaming critical path: background thread polls Ray's State API
and recomputes the critical path as tasks complete.
"""

from __future__ import annotations
import threading
import time
from ray.util.state import list_tasks

from .critical_path import compute_critical_path, CriticalPathResult


class LiveProfiler:
    def __init__(self, dep_graph: dict, poll_interval: float = 2.0):
        self._dep_graph = dep_graph
        self._poll_interval = poll_interval
        self._seen_task_ids: set[str] = set()
        self._latest_cp: CriticalPathResult | None = None
        self._latest_timing: dict[str, dict] = {}
        self._poll_count: int = 0
        self._stop_event = threading.Event()
        self._thread: threading.Thread | None = None

    def start(self):
        self._thread = threading.Thread(target=self._poll_loop, daemon=True)
        self._thread.start()
        print(f"[live] background profiler started (polling every {self._poll_interval}s)")

    def stop(self) -> dict:
        self._stop_event.set()
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=5.0)
        print(f"[live] stopped after {self._poll_count} poll cycles")
        self._do_one_poll(final=True)
        resolved_deps = self._resolve_deps(self._latest_timing)
        return {
            "timing": self._latest_timing,
            "resolved_deps": resolved_deps,
            "cp": self._latest_cp,
        }

    def _poll_loop(self):
        self._stop_event.wait(timeout=3.0)
        while not self._stop_event.is_set():
            self._do_one_poll(final=False)
            self._stop_event.wait(timeout=self._poll_interval)

    def _do_one_poll(self, final: bool = False):
        try:
            all_tasks = list_tasks(detail=True, limit=10_000)
        except Exception:
            return
        timing = {}
        for t in all_tasks:
            if t.start_time_ms and t.end_time_ms:
                timing[t.task_id] = {
                    "name": t.func_or_class_name or "unknown",
                    "start_ms": t.start_time_ms,
                    "end_ms": t.end_time_ms,
                    "exec_ms": t.end_time_ms - t.start_time_ms,
                }
        if not timing: return
        current_ids = set(timing.keys())
        new_ids = current_ids - self._seen_task_ids
        if not new_ids and not final: return
        self._seen_task_ids = current_ids
        self._latest_timing = timing
        self._poll_count += 1
        resolved_deps = self._resolve_deps(timing)
        cp = compute_critical_path(timing, resolved_deps)
        if not cp.path: return
        self._latest_cp = cp
        total_ms = cp.cp_value.get(cp.terminal, 0)
        terminal_name = timing[cp.terminal]["name"]
        tag = "FINAL" if final else f"#{self._poll_count}"
        print(f"  [live {tag}] {len(timing)} tasks done | "
              f"CP: {len(cp.path)} tasks, {total_ms:.0f}ms | "
              f"bottleneck: {terminal_name}")

    def _resolve_deps(self, timing):
        def resolve(dep_hex):
            for tid in timing:
                if tid.startswith(dep_hex) or dep_hex.startswith(tid):
                    return tid
            return None
        resolved = {}
        for tid in timing:
            raw = self._dep_graph.get(tid, [])
            resolved_list = [r for r in (resolve(d) for d in raw) if r is not None]
            resolved[tid] = resolved_list
        return resolved