"""
Straggler detection: flags tasks on the critical path whose execution
time is abnormally high compared to other invocations of the same function.
"""

from __future__ import annotations
from dataclasses import dataclass
from collections import defaultdict


@dataclass
class StragglerInfo:
    task_id:       str
    func_name:     str
    exec_ms:       float
    median_ms:     float
    mad_ms:        float
    ratio:         float
    sibling_count: int


def _median(values: list[float]) -> float:
    s = sorted(values)
    n = len(s)
    if n == 0: return 0.0
    if n % 2 == 1: return s[n // 2]
    return (s[n // 2 - 1] + s[n // 2]) / 2


def _mad(values: list[float], med: float) -> float:
    if not values: return 0.0
    deviations = [abs(v - med) for v in values]
    return _median(deviations)


def detect_stragglers(
    timing:            dict[str, dict],
    critical_path_ids: list[str],
    threshold:         float = 3.0,
) -> list[StragglerInfo]:
    groups: dict[str, list[float]] = defaultdict(list)
    for tid, info in timing.items():
        groups[info["name"]].append(info["exec_ms"])

    stats: dict[str, tuple[float, float, int]] = {}
    for func_name, times in groups.items():
        med = _median(times)
        mad = _mad(times, med)
        stats[func_name] = (med, mad, len(times))

    stragglers: list[StragglerInfo] = []
    for tid in critical_path_ids:
        if tid not in timing: continue
        info = timing[tid]
        func_name = info["name"]
        exec_ms = info["exec_ms"]
        med, mad, count = stats.get(func_name, (0, 0, 0))
        if count < 2: continue
        effective_mad = max(mad, 1.0)
        distance = (exec_ms - med) / effective_mad
        if distance > threshold:
            ratio = exec_ms / med if med > 0 else float("inf")
            stragglers.append(StragglerInfo(
                task_id=tid, func_name=func_name, exec_ms=exec_ms,
                median_ms=med, mad_ms=mad, ratio=ratio, sibling_count=count,
            ))
    return stragglers