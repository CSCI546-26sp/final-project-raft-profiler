import ray
import time
import random
import argparse
import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from profiler import profile, print_critical_path

parser = argparse.ArgumentParser()
parser.add_argument("--inject-fault", type=int, default=-1,
                    help="Index of mapper to slow down (0-based). -1 = no fault.")
parser.add_argument("--fault-delay",  type=float, default=2.0)
parser.add_argument("--num-mappers",  type=int, default=4)
parser.add_argument("--num-reducers", type=int, default=2)
args = parser.parse_args()

ray.init(num_cpus=2, include_dashboard=True, dashboard_port=8265)
profile()
time.sleep(3)


@ray.remote
def mapper(mapper_id, num_reducers, fault_delay):
    if fault_delay > 0:
        print(f"  [mapper {mapper_id}] FAULT — sleeping {fault_delay}s")
        time.sleep(fault_delay)
    pairs = [(random.randint(0, num_reducers - 1), random.random())
             for _ in range(500_000)]
    partitions = [[] for _ in range(num_reducers)]
    for key, val in pairs:
        partitions[key].append(val)
    return partitions


@ray.remote
def reducer(reducer_id, *all_mapper_outputs):
    values = []
    for mapper_output in all_mapper_outputs:
        values.extend(mapper_output[reducer_id])
    return {"reducer_id": reducer_id, "count": len(values), "sum": sum(values)}


print(f"\nMap-reduce: {args.num_mappers} mappers, {args.num_reducers} reducers")
if args.inject_fault >= 0:
    print(f"Fault injected into mapper {args.inject_fault} ({args.fault_delay}s delay)\n")

mapper_refs = [
    mapper.remote(i, args.num_reducers,
                  args.fault_delay if i == args.inject_fault else 0.0)
    for i in range(args.num_mappers)
]

reducer_refs = [
    reducer.remote(j, *mapper_refs)
    for j in range(args.num_reducers)
]

results = ray.get(reducer_refs)
print("\nResults:")
for r in results:
    print(f"  reducer {r['reducer_id']}: {r['count']} values, sum={r['sum']:.2f}")

print_critical_path(output_html="output/mapreduce_dashboard.html", job_label="Map-Reduce")
ray.shutdown()
