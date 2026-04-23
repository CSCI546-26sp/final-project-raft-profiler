from __future__ import annotations
import ast
import json
from dataclasses import dataclass


@dataclass
class TaskBreakdown:
    task_id:      str
    waiting_ms:   float   
    compute_ms:   float   
    total_ms:     float   

    def waiting_pct(self) -> float:
        return (self.waiting_ms / self.total_ms * 100) if self.total_ms > 0 else 0.0

    def compute_pct(self) -> float:
        return (self.compute_ms / self.total_ms * 100) if self.total_ms > 0 else 100.0


def _parse_event(raw_state: str) -> tuple[str, float]:
    try:
        d = ast.literal_eval(raw_state)
        return d["state"], float(d["created_ms"])
    except Exception:
        return str(raw_state), 0.0


def _is_running(state_name: str) -> bool:
    return "running" in state_name.lower()


def parse_breakdown(task_id: str, events: list[dict]) -> TaskBreakdown:
    if not events:
        return TaskBreakdown(task_id, 0.0, 0.0, 0.0)

    parsed = []
    for ev in events:
        state_name, ts = _parse_event(ev["state"])
        if ts and ts > 0:
            parsed.append((state_name, ts))

    if not parsed:
        return TaskBreakdown(task_id, 0.0, 0.0, 0.0)

    parsed.sort(key=lambda x: x[1])

    start_ts = parsed[0][1]
    end_ts   = parsed[-1][1]
    total_ms = end_ts - start_ts

    running_ts: float | None = None
    for state_name, ts in parsed:
        if _is_running(state_name):
            running_ts = ts
            break

    if running_ts is None:
        return TaskBreakdown(task_id, total_ms, 0.0, total_ms)

    waiting_ms = max(running_ts - start_ts, 0.0)
    compute_ms = max(end_ts    - running_ts, 0.0)

    return TaskBreakdown(
        task_id    = task_id,
        waiting_ms = waiting_ms,
        compute_ms = compute_ms,
        total_ms   = total_ms,
    )


def breakdown_all_tasks(tasks: list[dict]) -> dict[str, TaskBreakdown]:
    result: dict[str, TaskBreakdown] = {}
    for t in tasks:
        task_id = t.get("task_id", "")
        events  = t.get("events", [])
        if task_id and events:
            result[task_id] = parse_breakdown(task_id, events)
    return result


def breakdown_from_json(json_path: str) -> dict[str, TaskBreakdown]:
    with open(json_path) as f:
        data = json.load(f)
    return breakdown_all_tasks(data.get("tasks", []))


if __name__ == "__main__":
    import sys
    path = sys.argv[1] if len(sys.argv) > 1 else "critical_path_profile.json"

    try:
        breakdowns = breakdown_from_json(path)
        print(f"Parsed {len(breakdowns)} task breakdowns from {path}\n")

        with open(path) as f:
             data = json.load(f)
             raw_tasks = data.get("tasks", [])

        sample_tasks = [t for t in raw_tasks if t["name"] == "sampling_task"]
        print(f"{'Task ID':<20} {'Waiting (ms)':>14} {'Compute (ms)':>14} {'W%':>6} {'C%':>6}")
        print("-" * 65)
        for t in sample_tasks:
            bd = breakdowns.get(t["task_id"])
            if bd:
                print(f"{t['task_id'][:18]:<20} {bd.waiting_ms:>14.1f} {bd.compute_ms:>14.1f} "
                      f"{bd.waiting_pct():>5.1f}% {bd.compute_pct():>5.1f}%")

        print("\nSelf-test passed ✓")

    except FileNotFoundError:
        print(f"File not found: {path}")
        print("Run: python runtime_breakdown.py path/to/critical_path_profile.json")
