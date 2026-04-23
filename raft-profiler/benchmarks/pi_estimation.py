import ray
import math
import time
import random
import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from ray.util.state import list_tasks
from profiler import profile, print_critical_path

ray.init(include_dashboard=True, dashboard_port=8265)
profile()
time.sleep(3)


@ray.remote
class ProgressActor:
    def __init__(self, total_num_samples):
        self.total_num_samples = total_num_samples
        self.num_samples_completed_per_task = {}

    def report_progress(self, task_id, num_samples_completed):
        self.num_samples_completed_per_task[task_id] = num_samples_completed

    def get_progress(self):
        return sum(self.num_samples_completed_per_task.values()) / self.total_num_samples


@ray.remote
def sampling_task(num_samples, task_id, progress_actor):
    num_inside = 0
    for i in range(num_samples):
        x, y = random.uniform(-1, 1), random.uniform(-1, 1)
        if math.hypot(x, y) <= 1:
            num_inside += 1
        if (i + 1) % 1_000_000 == 0:
            progress_actor.report_progress.remote(task_id, i + 1)
    progress_actor.report_progress.remote(task_id, num_samples)
    return num_inside


NUM_SAMPLING_TASKS = 10
NUM_SAMPLES_PER_TASK = 10_000_000
TOTAL_NUM_SAMPLES = NUM_SAMPLING_TASKS * NUM_SAMPLES_PER_TASK

progress_actor = ProgressActor.remote(TOTAL_NUM_SAMPLES)

results = []
for i in range(NUM_SAMPLING_TASKS):
    ref = sampling_task.remote(NUM_SAMPLES_PER_TASK, i, progress_actor)
    results.append(ref)

while True:
    progress = ray.get(progress_actor.get_progress.remote())
    print(f"Progress: {int(progress * 100)}%")
    if progress == 1:
        break
    time.sleep(1)

total_num_inside = sum(ray.get(results))
pi = (total_num_inside * 4) / TOTAL_NUM_SAMPLES
print(f"\nEstimated π: {pi}")

print_critical_path(output_html="output/pi_estimation_dashboard.html", job_label="Pi Estimation")
ray.shutdown()
