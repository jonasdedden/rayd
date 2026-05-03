# Grafana dashboards

## `rayd-overview.json`

Cluster-wide overview covering all 26 metrics rayd exposes across its
four `/metrics` endpoints (GCS, raylet, plasma server, driver). 19
panels grouped into 4 collapsible rows:

1. **Cluster health · rayd-gcs** — nodes alive/total, jobs, actors,
   GCS RPC rates (heartbeat / register / `WatchNodes` events).
2. **Object transport · rayd-raylet** — Pull/Push/RegisterObject/
   GetObjectLocations rates, spill restores, directory size, and the
   `NodeIndex` fast-path hit ratio (Phase 4.3.3c-F).
3. **Object store · rayd-plasma** — arena bytes (used vs total),
   plasma RPC rates, object count, utilization gauge.
4. **Driver activity · rayd-py** — task lifecycle (submitted /
   completed / failed), put/get rates, live `ObjectRef` count, and
   a derived task failure-ratio panel.

### Importing

Grafana → Dashboards → New → Import → upload JSON file. When prompted
for a datasource, pick your Prometheus instance — the dashboard
references it via the templated `${DS_PROMETHEUS}` variable, so it
works across multi-datasource setups.

### Variables

- `Prometheus` — datasource selector (auto-populated from your
  Grafana config).
- `Instance` — multi-select filter against the `instance` label that
  Prometheus stamps on every series. Useful when you scrape multiple
  raylets or drivers in one cluster. Defaults to `All`.

### Scraping config

Each component opts in independently via its own `--metrics-bind`
flag or env var. A minimal Prometheus `scrape_configs` block looks
like:

```yaml
scrape_configs:
  - job_name: rayd-gcs
    static_configs:
      - targets: ["gcs-host:9100"]
  - job_name: rayd-raylet
    static_configs:
      - targets: ["node1:9101", "node2:9101"]
  - job_name: rayd-plasma
    static_configs:
      - targets: ["node1:9102", "node2:9102"]
  - job_name: rayd-driver
    static_configs:
      - targets: ["driver-host:9103"]
```

The dashboard assumes default Prometheus `instance` labelling
(`host:port`); no relabel rules required.
