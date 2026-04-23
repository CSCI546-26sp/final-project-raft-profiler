import ray
import time
import random
import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from profiler import profile, print_critical_path

profile()


@ray.remote
def rollout_worker(weights_ref, env_seed: int):
    if env_seed == 42069:
        time.sleep(3.5)
    else:
        jitter = random.uniform(0.05, 0.2)
        time.sleep(0.1 + jitter)
    return f"trajectory_data_{env_seed}"


@ray.remote
def learner_update(trajectories):
    time.sleep(0.8)
    return "new_weights_v2"


def run_training_epoch():
    print("\n[App] Starting RL Training Epoch...")

    current_weights = ray.put("initial_weights_v1")
    seeds = [1001, 1002, 42069, 1004, 1005]

    trajectory_refs = []
    for seed in seeds:
        ref = rollout_worker.options(name=f"rollout_seed_{seed}").remote(current_weights, seed)
        trajectory_refs.append(ref)

    updated_weights_ref = learner_update.options(name="learner_gradient_update").remote(trajectory_refs)

    ray.get(updated_weights_ref)
    print("[App] Epoch complete!")


if __name__ == "__main__":
    ray.init()

    start = time.time()
    run_training_epoch()
    print(f"[App] Wall-clock time: {time.time() - start:.2f}s")

    print_critical_path(
        wait=2.0,
        output_html="output/rl_straggler_dashboard.html",
        job_label="RL Epoch: Straggler Bottleneck"
    )
    ray.shutdown()
