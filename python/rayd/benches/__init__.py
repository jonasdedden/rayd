"""Reference benchmarks for rayd. Single-node, Python-driven, end-to-end.

Run individually:

    python -m rayd.benches.bench_task_throughput
    python -m rayd.benches.bench_task_latency
    python -m rayd.benches.bench_put_get

Or `make bench` runs all three.

Each script prints a short summary table to stdout. They're not
collected by pytest (`testpaths` in `pyproject.toml` excludes this
directory) — benchmarks are slow and result-noisy by nature.
"""
