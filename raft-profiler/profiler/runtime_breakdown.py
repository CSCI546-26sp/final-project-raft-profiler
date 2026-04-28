"""
Per-Task Runtime Breakdown (Fine-Grained)
Splits task time into: dependency wait, queue wait, overhead, and compute.
"""

from __future__ import annotations
import ast
from dataclasses import dataclass


def _categorize_state(state_name: str) -> str:
    s = state_name.upper()
    if "PENDING_ARGS" in s:   return "dependency"
    if "PENDING_NODE" in s:   return "queue"
    if "SUBMITTED" in s:      return "overhead"
    if "RUNNING" in s:        return "compute"
    return "other"


@dataclass
class TaskBreakdown:
    task_id:        str
    dependency_ms:  float
    queue_ms:       float
    overhead_ms:    float
    compute_ms:     float
    waiting_ms:     float    # = dependency + queue + overhead
    total_ms:       float

    def waiting_pct(self) -> float:
        return (self.waiting_ms / self.total_ms * 100) if self.total_ms > 0 else 0.0

    def compute_pct(self) -> float:
        return (self.compute_ms / self.total_ms * 100) if self.total_ms > 0 else 100.0

    def dependency_pct(self) -> float:
        return (self.dependency_ms / self.total_ms * 100) if self.total_ms > 0 else 0.0

    def queue_pct(self) -> float:
        return (self.queue_ms / self.total_ms * 100) if self.total_ms > 0 else 0.0

    def overhead_pct(self) -> float:
        return (self.overhead_ms / self.total_ms * 100) if self.total_ms > 0 else 0.0


def _parse_event(raw_state: str) -> tuple[str, float]:
    try:
        d = ast.literal_eval(raw_state)
        return d["state"], float(d["created_ms"])
    except Exception:
        return str(raw_state), 0.0


def parse_breakdown(task_id: str, events: list[dict]) -> TaskBreakdown:
    if not events:
        return TaskBreakdown(task_id, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0)

    parsed = []
    for ev in events:
        state_name, ts = _parse_event(ev["state"])
        if ts and ts > 0:
            parsed.append((state_name, ts))

    if not parsed:
        return TaskBreakdown(task_id, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0)

    parsed.sort(key=lambda x: x[1])

    buckets = {"dependency": 0.0, "queue": 0.0, "overhead": 0.0, "compute": 0.0, "other": 0.0}

    for i in range(len(parsed) - 1):
        state_name = parsed[i][0]
        gap_ms = max(parsed[i + 1][1] - parsed[i][1], 0.0)
        category = _categorize_state(state_name)
        buckets[category] += gap_ms

    dependency_ms = buckets["dependency"]
    queue_ms      = buckets["queue"]
    overhead_ms   = buckets["overhead"]
    compute_ms    = buckets["compute"]
    waiting_ms    = dependency_ms + queue_ms + overhead_ms
    total_ms      = waiting_ms + compute_ms + buckets["other"]

    return TaskBreakdown(
        task_id=task_id, dependency_ms=dependency_ms, queue_ms=queue_ms,
        overhead_ms=overhead_ms, compute_ms=compute_ms,
        waiting_ms=waiting_ms, total_ms=total_ms,
    )


def breakdown_all_tasks(tasks: list[dict]) -> dict[str, TaskBreakdown]:
    result: dict[str, TaskBreakdown] = {}
    for t in tasks:
        task_id = t.get("task_id", "")
        events = t.get("events", [])
        if task_id and events:
            result[task_id] = parse_breakdown(task_id, events)
    return result