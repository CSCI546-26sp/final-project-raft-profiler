import ray
import time
import numpy as np
import argparse
import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from profiler import profile, print_critical_path

parser = argparse.ArgumentParser()
parser.add_argument("--inject-fault", type=int, default=-1)
parser.add_argument("--fault-delay",  type=float, default=2.0)
parser.add_argument("--num-leaves",   type=int, default=8)
parser.add_argument("--array-size",   type=int, default=500_000)
args = parser.parse_args()

assert args.num_leaves > 0 and (args.num_leaves & (args.num_leaves - 1)) == 0

ray.init(include_dashboard=True, dashboard_port=8265)
profile()
time.sleep(3)


@ray.remote
def leaf_task(leaf_id, array_size, fault_delay):
    if fault_delay > 0:
        print(f"  [leaf {leaf_id}] FAULT — sleeping {fault_delay}s")
        time.sleep(fault_delay)
    return np.random.normal(size=array_size)


@ray.remote
def aggregate(left, right):
    return left + right


print(f"\nTree reduction: {args.num_leaves} leaves")
if args.inject_fault >= 0:
    print(f"Fault injected into leaf {args.inject_fault} ({args.fault_delay}s delay)")
print()

current_level = [
    leaf_task.remote(i, args.array_size, args.fault_delay if i == args.inject_fault else 0.0)
    for i in range(args.num_leaves)
]

level_num = 0
print(f"Level {level_num} (leaves): {len(current_level)} tasks")

while len(current_level) > 1:
    next_level = []
    for i in range(0, len(current_level), 2):
        ref = aggregate.remote(current_level[i], current_level[i + 1])
        next_level.append(ref)
    level_num += 1
    print(f"Level {level_num}:          {len(next_level)} tasks")
    current_level = next_level

root_ref = current_level[0]
result = ray.get(root_ref)
print(f"\nResult: shape={result.shape}, mean={result.mean():.4f}")

total_tasks = args.num_leaves + (args.num_leaves - 1)
print(f"Total tasks: {total_tasks}")
print(f"Expected CP depth: {level_num + 1} tasks")

print_critical_path(output_html="output/tree_reduce_dashboard.html", job_label="Tree Reduce")
ray.shutdown()
