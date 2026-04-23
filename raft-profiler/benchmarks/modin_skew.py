import ray
import numpy as np
import time
import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from profiler import profile, print_critical_path
ray.init(num_cpus=4, ignore_reinit_error=True)
profile()
import modin.pandas as pd
data = {
    "category": ["A"] * 100 + ["B"] * 1 + ["C"] * 1,
    "value": np.random.rand(102),
}
df = pd.DataFrame(data)
def heavy_work(group):
    if group.name == "A":
        time.sleep(5)  
    return group.sum()

print("[demo] Starting heavy skewed GroupBy...")
result = df.groupby("category").apply(heavy_work)

print_critical_path(
    output_html="output/massive_skew_gap.html",
    job_label="Massive Computational Skew"
)