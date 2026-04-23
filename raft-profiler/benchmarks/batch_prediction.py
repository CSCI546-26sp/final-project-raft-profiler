import ray
import time
import random
import argparse
import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from profiler import profile, print_critical_path

parser = argparse.ArgumentParser()
parser.add_argument("--fault-type", type=str, choices=["none", "loader", "predictor"], default="none")
parser.add_argument("--fault-delay", type=float, default=2.0)
parser.add_argument("--num-batches", type=int, default=5)
args = parser.parse_args()

ray.init(include_dashboard=True, dashboard_port=8265)
profile()
time.sleep(2)


@ray.remote
def loader(num_batches, fault_type, fault_delay):
    if fault_type == "loader":
        print(f"  [Loader] FAULT — sleeping {fault_delay}s")
        time.sleep(fault_delay)
    else:
        time.sleep(0.2)
    return [f"data_chunk_{i}" for i in range(num_batches)]


@ray.remote
def predictor(batch_id, data_chunk, fault_type, fault_delay):
    if fault_type == "predictor" and batch_id == 0:
        print(f"  [Predictor {batch_id}] FAULT — sleeping {fault_delay}s")
        time.sleep(fault_delay)
    else:
        jitter = random.uniform(0.05, 0.15)
        time.sleep(0.1 + jitter)
    return f"Prediction for {data_chunk}"


if __name__ == "__main__":
    print(f"\nBatch Prediction: 1 Loader → {args.num_batches} parallel Predictors")
    print(f"Fault injection: {args.fault_type.upper()}\n")

    batch_refs = loader.options(
        name="dataset_loader",
        num_returns=args.num_batches
    ).remote(args.num_batches, args.fault_type, args.fault_delay)

    prediction_refs = []
    for i in range(args.num_batches):
        ref = predictor.options(name=f"predictor_{i}").remote(
            i, batch_refs[i], args.fault_type, args.fault_delay
        )
        prediction_refs.append(ref)

    ray.get(prediction_refs)
    print("\n[App] All predictions complete!")

    print_critical_path(
        output_html=f"output/batch_{args.fault_type}_dashboard.html",
        job_label=f"Batch Prediction ({args.fault_type.upper()} fault)"
    )
    ray.shutdown()
