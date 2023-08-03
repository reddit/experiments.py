from prometheus_client import Counter

experiments_client_counter = Counter(
    "experiments_py_client_total",
    "Count of successful/failed Experiments.py operations (with error_type) in reddit-experiments package",
    ["operation", "success", "error_type", "pkg_version"],
)
