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

                                                              
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from profiler import profile, print_critical_path, dashboard_port

                           
                                                                      
ray.init(num_cpus=4, include_dashboard=True, dashboard_port=dashboard_port(), ignore_reinit_error=True)
profile()

                                              
try:
    import daft
except ModuleNotFoundError as exc:
    raise SystemExit(
        "Missing dependency 'daft'. Install with: pip install getdaft"
    ) from exc

print("[demo] Initializing Daft on Ray...")

                            
                                                                               
data = {
    "id": [1] * 990 + list(range(2, 12)),
    "value": np.random.rand(1000)
}
df = daft.from_pydict(data)

                                         
                                                                       
@daft.udf(return_dtype=daft.DataType.float64())
def heavy_transformation(val):
                                                                      
                              
    time.sleep(5) 
    
                                                                           
                                                                       
    return val

print("[demo] Building and Executing the Logical Plan...")

                                             
                                                                          
df = df.with_column("processed", heavy_transformation(df["value"]))
df = df.sort("id")

                                          
results = df.collect()

print("[demo] Operation complete. Generating Critical Path Analysis...")

                        
print_critical_path(
    output_html="output/daft_3rdparty_skew.html",
    job_label="Daft 3rd-Party Query Profiling"
)