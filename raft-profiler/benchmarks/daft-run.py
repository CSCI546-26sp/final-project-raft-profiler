try:
    import ray
except ModuleNotFoundError as exc:
    raise SystemExit(
        "Missing dependency 'ray'. Install with: pip install ray"
    ) from exc

try:
    import numpy as np
except ModuleNotFoundError as exc:
    raise SystemExit(
        "Missing dependency 'numpy'. Install with: pip install numpy"
    ) from exc
import time
import sys
import os

# Ensure the profiler from your parent directory is accessible
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from profiler import profile, print_critical_path

# 1. Setup Ray and the Trap
# Using 4 CPUs makes the 1-on-3 CPU imbalance very obvious in the HTML
ray.init(num_cpus=4, ignore_reinit_error=True)
profile()

# 2. Import Daft (3rd Party Rust-based Engine)
try:
    import daft
except ModuleNotFoundError as exc:
    raise SystemExit(
        "Missing dependency 'daft'. Install with: pip install getdaft"
    ) from exc

print("[demo] Initializing Daft on Ray...")

# 3. Create a Skewed Dataset
# 99% of the keys are "1", which will force a single Reducer to do all the work
data = {
    "id": [1] * 990 + list(range(2, 12)),
    "value": np.random.rand(1000)
}
df = daft.from_pydict(data)

# 4. Inject a "Computationally Heavy" UDF
# This simulates complex processing that will live on the Critical Path
@daft.udf(return_dtype=daft.DataType.float64())
def heavy_transformation(val):
    # val is a Daft Series. To simulate the sleep for the bottleneck, 
    # we sleep once per batch.
    time.sleep(5) 
    
    # `val` is a Daft Series, so scalar arithmetic here raises a type error
    # in this runtime; return the batch unchanged after simulated work.
    return val

print("[demo] Building and Executing the Logical Plan...")

# We apply a transformation and then a SORT. 
# Sort is a 'Shuffle' operation, which is the "Final Boss" for a profiler.
df = df.with_column("processed", heavy_transformation(df["value"]))
df = df.sort("id")

# .collect() triggers the actual Ray tasks
results = df.collect()

print("[demo] Operation complete. Generating Critical Path Analysis...")

# 5. Export the insights
print_critical_path(
    output_html="output/daft_3rdparty_skew.html",
    job_label="Daft 3rd-Party Query Profiling"
)