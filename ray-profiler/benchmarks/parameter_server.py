"""
Distributed parameter-server training loop (sync / async), patterned after:
https://docs.ray.io/en/latest/ray-core/examples/plot_parameter_server.html

Uses NumPy-only fake gradients so the benchmark runs without PyTorch/MNIST.
For production ML training on Ray, prefer Ray Train.
"""
from __future__ import annotations

import argparse
import os
import sys
import time

import numpy as np
import ray

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from profiler import print_critical_path, profile, track_function, dashboard_port

parser = argparse.ArgumentParser(description="Parameter server benchmark (Ray profiler demo)")
parser.add_argument("--num-workers", type=int, default=4)
parser.add_argument("--dim", type=int, default=4096, help="Fake weight vector dimension")
parser.add_argument("--lr", type=float, default=0.01)
parser.add_argument("--iterations", type=int, default=30)
parser.add_argument(
    "--mode",
    type=str,
    choices=("sync", "async"),
    default="sync",
    help="Synchronous ( barrier each step ) vs asynchronous Hogwild-style updates.",
)
parser.add_argument("--inject-fault", type=int, default=-1, help="Slow gradient on worker index; -1 = none")
parser.add_argument("--fault-delay", type=float, default=2.0)
args = parser.parse_args()


@track_function
def fake_backward(weights: np.ndarray, worker_id: int, fault_worker: int, fault_delay: float) -> np.ndarray:
    """Simulate local backward pass (heavy-ish math + optional straggler)."""
    if worker_id == fault_worker:
        print(f"  [worker {worker_id}] FAULT — sleeping {fault_delay}s")
        time.sleep(fault_delay)
    rng = np.random.default_rng(worker_id + 17)
    batch = rng.standard_normal(weights.shape, dtype=np.float64)
                                                                
    grad = weights - batch
    return grad.astype(np.float64)


@ray.remote
class ParameterServer:

    def __init__(self, dim: int, lr: float):
        self.dim = dim
        self.lr = lr
        self.weights = np.zeros(dim, dtype=np.float64)

    def get_weights(self) -> np.ndarray:
        return self.weights.copy()

    def apply_gradients(self, *gradients: np.ndarray) -> np.ndarray:
        time.sleep(0.2)
        stacked = np.stack(gradients, axis=0)
        summed = stacked.sum(axis=0)
        self.weights -= self.lr * summed
        return self.weights.copy()


@ray.remote
class DataWorker:
    def __init__(self, worker_id: int, dim: int, fault_worker: int, fault_delay: float):
        self.worker_id = worker_id
        self.dim = dim
        self.fault_worker = fault_worker
        self.fault_delay = fault_delay

    def compute_gradients(self, weights: np.ndarray) -> np.ndarray:
        return fake_backward(weights, self.worker_id, self.fault_worker, self.fault_delay)


def run_sync(ps, workers: list, iterations: int):
    """All workers compute gradients each step; PS aggregates then applies."""
    current_weights = ps.get_weights.remote()
    for i in range(iterations):
        grads = [
            w.compute_gradients.options(name=f"worker_{j}_grad").remote(current_weights)
            for j, w in enumerate(workers)
        ]
        current_weights = ps.apply_gradients.remote(*grads)
        if i % 10 == 0 or i == iterations - 1:
            w = ray.get(current_weights)
            print(f"  iter {i:4d}  ||w||={np.linalg.norm(w):.4f}")
    return current_weights


def run_async(ps, workers: list, iterations: int, updates_total: int):
    """Gradient-driven pipeline: apply each finished gradient immediately (tutorial async pattern)."""
    current_weights = ps.get_weights.remote()
    pending: dict = {}

    worker_index = {id(w): i for i, w in enumerate(workers)}

    def launch(worker):
        idx = worker_index[id(worker)]
        ref = worker.compute_gradients.options(name=f"worker_{idx}_grad_async").remote(
            current_weights
        )
        pending[ref] = worker

    for w in workers:
        launch(w)

    for step in range(updates_total):
        ready, _ = ray.wait(list(pending.keys()), num_returns=1)
        ready_ref = ready[0]
        worker = pending.pop(ready_ref)
        current_weights = ps.apply_gradients.remote(ready_ref)
        launch(worker)
        if step % max(1, updates_total // 5) == 0 or step == updates_total - 1:
            w_arr = ray.get(current_weights)
            print(f"  update {step:4d}  ||w||={np.linalg.norm(w_arr):.4f}")

    return current_weights


if __name__ == "__main__":
    ray.init(include_dashboard=True, dashboard_port=dashboard_port())
    profile()
    time.sleep(2)

    print(
        f"\nParameter server  mode={args.mode}  workers={args.num_workers}  "
        f"iters={args.iterations}  dim={args.dim}"
    )
    if args.inject_fault >= 0:
        print(f"Fault: worker {args.inject_fault} delayed by {args.fault_delay}s\n")

    ps = ParameterServer.remote(args.dim, args.lr)
    workers = [
        DataWorker.remote(i, args.dim, args.inject_fault, args.fault_delay)
        for i in range(args.num_workers)
    ]

    t0 = time.perf_counter()
    if args.mode == "sync":
        final_weights_ref = run_sync(ps, workers, args.iterations)
    else:
        updates = args.iterations * args.num_workers
        final_weights_ref = run_async(ps, workers, args.iterations, updates)

    _ = ray.get(final_weights_ref)
    elapsed = time.perf_counter() - t0
    print(f"\n[App] Done in {elapsed:.2f}s")

    suffix = args.mode
    fault = f"_fault{args.inject_fault}" if args.inject_fault >= 0 else ""
    print_critical_path(
        output_html=f"output/parameter_server_{suffix}{fault}_dashboard.html",
        job_label=f"Parameter Server ({args.mode})",
    )
    ray.shutdown()
