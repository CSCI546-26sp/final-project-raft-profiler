from __future__ import annotations
import json
from dataclasses import dataclass
from collections import defaultdict


def save_run(path, timing, resolved_deps, cp_path, cp_total_ms, breakdowns=None):
    bd_data = {}
    if breakdowns:
        for tid, bd in breakdowns.items():
            bd_data[tid] = {
                "dependency_ms": getattr(bd, "dependency_ms", 0),
                "queue_ms": getattr(bd, "queue_ms", 0),
                "overhead_ms": getattr(bd, "overhead_ms", 0),
                "compute_ms": bd.compute_ms,
                "waiting_ms": bd.waiting_ms,
                "total_ms": bd.total_ms,
            }
    data = {
        "timing": timing, "resolved_deps": resolved_deps,
        "cp_path": cp_path, "cp_total_ms": cp_total_ms, "breakdowns": bd_data,
    }
    with open(path, "w") as f:
        json.dump(data, f, indent=2)
    print(f"[compare] run saved to {path}")


def load_run(path):
    with open(path) as f:
        return json.load(f)


@dataclass
class FunctionDiff:
    func_name: str
    before_avg_ms: float
    after_avg_ms: float
    change_ms: float
    change_pct: float
    before_count: int
    after_count: int


@dataclass
class ComparisonResult:
    before_total_ms: float
    after_total_ms: float
    saved_ms: float
    path_changed: bool
    before_cp_funcs: list[str]
    after_cp_funcs: list[str]
    function_diffs: list[FunctionDiff]

    @property
    def summary(self) -> str:
        lines = ["Run Comparison\n"]
        lines.append(f"  Before: {self.before_total_ms:.0f}ms")
        lines.append(f"  After:  {self.after_total_ms:.0f}ms")
        if self.saved_ms > 0:
            lines.append(f"  Saved:  {self.saved_ms:.0f}ms")
        elif self.saved_ms < 0:
            lines.append(f"  Slower: {-self.saved_ms:.0f}ms")
        if self.path_changed:
            lines.append(f"\n  Critical path shifted!")
            lines.append(f"    Before: {' > '.join(self.before_cp_funcs)}")
            lines.append(f"    After:  {' > '.join(self.after_cp_funcs)}")
        lines.append(f"\n  {'Function':<25} {'Before':>10} {'After':>10} {'Change':>10}")
        lines.append(f"  {'_'*25} {'_'*10} {'_'*10} {'_'*10}")
        for fd in sorted(self.function_diffs, key=lambda x: x.change_ms):
            if fd.change_ms < 0:
                marker = f"  {-fd.change_ms:.0f}ms faster"
            elif fd.change_ms > 0:
                marker = f"  {fd.change_ms:.0f}ms slower"
            else:
                marker = f"  same"
            lines.append(f"  {fd.func_name:<25} {fd.before_avg_ms:>9.0f}ms {fd.after_avg_ms:>9.0f}ms {marker}")
        return "\n".join(lines)


def compare_runs(before_path, after_path):
    before = load_run(before_path)
    after = load_run(after_path)

    def group_by_func(timing):
        groups = defaultdict(list)
        for tid, info in timing.items():
            groups[info["name"]].append(info["exec_ms"])
        return groups

    before_groups = group_by_func(before["timing"])
    after_groups = group_by_func(after["timing"])
    all_funcs = set(before_groups.keys()) | set(after_groups.keys())

    function_diffs = []
    for func_name in sorted(all_funcs):
        bt = before_groups.get(func_name, [])
        at = after_groups.get(func_name, [])
        ba = sum(bt) / len(bt) if bt else 0
        aa = sum(at) / len(at) if at else 0
        function_diffs.append(FunctionDiff(
            func_name=func_name, before_avg_ms=ba, after_avg_ms=aa,
            change_ms=aa - ba, change_pct=(aa - ba) / ba * 100 if ba > 0 else 0,
            before_count=len(bt), after_count=len(at),
        ))

    def cp_func_names(run_data):
        return [run_data["timing"][tid]["name"] for tid in run_data["cp_path"] if tid in run_data["timing"]]

    return ComparisonResult(
        before_total_ms=before["cp_total_ms"], after_total_ms=after["cp_total_ms"],
        saved_ms=before["cp_total_ms"] - after["cp_total_ms"],
        path_changed=cp_func_names(before) != cp_func_names(after),
        before_cp_funcs=cp_func_names(before), after_cp_funcs=cp_func_names(after),
        function_diffs=function_diffs,
    )


if __name__ == "__main__":
    import sys
    if len(sys.argv) != 3:
        print("Usage: python -m profiler.run_comparison before.json after.json")
        sys.exit(1)
    result = compare_runs(sys.argv[1], sys.argv[2])
    print(result.summary)