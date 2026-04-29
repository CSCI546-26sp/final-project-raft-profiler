"""
What-if analysis: modify a task's duration and recompute the critical path
to see if the bottleneck shifts.
"""

from __future__ import annotations
import copy
from dataclasses import dataclass

from .critical_path import compute_critical_path, CriticalPathResult


@dataclass
class WhatIfResult:
    task_id:           str
    task_name:         str
    original_exec_ms:  float
    modified_exec_ms:  float
    original_total_ms: float
    modified_total_ms: float
    saved_ms:          float
    path_changed:      bool
    original_path:     list[str]
    modified_path:     list[str]
    modified_cp:       CriticalPathResult

    @property
    def summary(self) -> str:
        lines = []
        lines.append(f"What-if: {self.task_name} [{self.task_id[:8]}] "
                      f"changed from {self.original_exec_ms:.0f}ms "
                      f"to {self.modified_exec_ms:.0f}ms\n")
        lines.append(f"  Original total: {self.original_total_ms:.0f}ms")
        lines.append(f"  Modified total: {self.modified_total_ms:.0f}ms")
        lines.append(f"  Time saved:     {self.saved_ms:.0f}ms")
        if self.saved_ms <= 0:
            lines.append(f"\n  No improvement.")
        elif self.path_changed:
            lines.append(f"\n  Bottleneck shifted to different tasks.")
        else:
            lines.append(f"\n  Direct improvement. All {self.saved_ms:.0f}ms saved.")
        return "\n".join(lines)


def what_if_analysis(timing, resolved_deps, original_cp, task_id, new_exec_ms):
    if task_id not in timing:
        raise ValueError(f"Task {task_id} not found in timing data")
    original_exec_ms = timing[task_id]["exec_ms"]
    task_name = timing[task_id]["name"]
    modified_timing = copy.deepcopy(timing)
    modified_timing[task_id]["exec_ms"] = new_exec_ms
    modified_cp = compute_critical_path(modified_timing, resolved_deps)
    original_total = original_cp.cp_value.get(original_cp.terminal, 0)
    modified_total = modified_cp.cp_value.get(modified_cp.terminal, 0)
    saved = original_total - modified_total
    path_changed = (original_cp.path != modified_cp.path)
    return WhatIfResult(
        task_id=task_id, task_name=task_name,
        original_exec_ms=original_exec_ms, modified_exec_ms=new_exec_ms,
        original_total_ms=original_total, modified_total_ms=modified_total,
        saved_ms=saved, path_changed=path_changed,
        original_path=original_cp.path, modified_path=modified_cp.path,
        modified_cp=modified_cp,
    )


def what_if_speedup(timing, resolved_deps, original_cp, task_id, speedup=2.0):
    if task_id not in timing:
        raise ValueError(f"Task {task_id} not found in timing data")
    original_ms = timing[task_id]["exec_ms"]
    new_ms = original_ms / speedup
    return what_if_analysis(timing, resolved_deps, original_cp, task_id, new_ms)