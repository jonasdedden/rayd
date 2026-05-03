# Local Grafana dashboard test setup

Bring up a small rayd cluster + dockerised Prometheus + Grafana so the
[`rayd-overview.json`](../rayd-overview.json) dashboard renders against
real data on your laptop. Useful for reviewing dashboard changes
before committing them and for sanity-checking that every metric the
project advertises actually appears in the graphs.

## Prerequisites

- Docker + `docker compose` (any reasonably recent version).
- `uv` for the Python venv.
- A working Rust toolchain (the run script does a release build of
  `rayd-cli`).

The whole stack uses `network_mode: host`, so the docker containers
reach the rayd processes on `localhost` directly. Linux + macOS work
the same. Windows + WSL hosts need Docker Desktop with host
networking enabled (or you can rewrite the `localhost` targets in
`prometheus.yml` to `host.docker.internal`).

The compose file uses fully-qualified image names
(`docker.io/prom/prometheus:...`, `docker.io/grafana/grafana:...`)
so it works the same under Docker and rootless Podman. Podman
doesn't auto-prefix unqualified short names; with bare
`prom/prometheus`, the first `up -d` would error with `did not
resolve to an alias and no unqualified-search registries are
defined`.

## One-shot bring-up

From the repo root:

    ./dashboards/dev/run.sh

The script:

1. builds `rayd-cli` (release) and the Python extension (`maturin develop`);
2. starts `rayd plasma-server` + `rayd gcs` in the background, each
   with `--metrics-bind` pointed at a distinct loopback port;
3. brings up Prometheus + Grafana via `docker compose`;
4. tails the rayd logs and waits for `Ctrl-C`.

Open Grafana at <http://localhost:3000>. Anonymous-admin login is on,
so no credentials prompt — go to **Dashboards → rayd → rayd · cluster
overview**. The dashboard's `Instance` variable defaults to *All*,
showing every component.

## Generating load

In a second shell:

    cd <repo>
    export RAYD_GCS_ADDRESS=127.0.0.1:60000
    export RAYD_PLASMA_SOCKET=/tmp/rayd-dev.sock
    export RAYD_METRICS_BIND=127.0.0.1:9103
    export RAYD_RAYLET_METRICS_BIND=127.0.0.1:9101
    uv run python dashboards/dev/workload.py

The workload submits 16 tasks, 4 puts, and 4 gets every 0.5 s, with a
~10% intentional task-failure rate so the failure-ratio panel shows
non-zero values. Press `Ctrl-C` to stop.

## What runs where

| Endpoint | URL | Driven by |
|---|---|---|
| GCS gRPC | `localhost:60000` | `rayd gcs` (host) |
| GCS `/metrics` | `localhost:9100` | `rayd gcs --metrics-bind` |
| Raylet `/metrics` | `localhost:9101` | driver-attached raylet (env var below) |
| Plasma `/metrics` | `localhost:9102` | `rayd plasma-server --metrics-bind` |
| Driver `/metrics` | `localhost:9103` | `RAYD_METRICS_BIND` (driver) |
| Prometheus | `localhost:9090` | container, `network_mode: host` |
| Grafana | `localhost:3000` | container, anonymous admin |

Every scrape target in `prometheus.yml` is stamped with
`rayd_cluster: rayd-dev` so the dashboard's `$rayd_cluster` template
variable has something to filter on. The `rayd_` prefix avoids
colliding with the generic `cluster` label that platform Prometheus
instances typically use to identify a *k8s* cluster — so a central
Grafana watching N k8s clusters, each running its own rayd
deployment, can compose this dashboard's `$rayd_cluster` filter on
top of its existing `$cluster` filter. In production a single
Prometheus often scrapes multiple rayd deployments via
`relabel_configs` extracting the rayd cluster name from a k8s
annotation; the dashboard works the same way against either source.

The raylet is the one that takes a moment of explaining: there is no
separate `rayd start --address` worker in this setup. The Python
driver embeds a raylet (one per process), and that raylet honors the
new `RAYD_RAYLET_METRICS_BIND` env var to expose its `/metrics`. So
the same `python workload.py` invocation drives both the driver-side
counters AND the raylet-side counters that the dashboard reads.

## What you should see (sanity checks)

After a minute of running `workload.py`:

- **Cluster health row**: `Nodes alive` = 1, `Jobs running` = 1.
  Heartbeats arrive every 2 s (`rate(rayd_gcs_heartbeat_received_total[$__rate_interval])`
  ≈ 0.5/s) and `WatchNodes` event publishes are 0/s at steady state
  (only spike on register/drain).
- **Object transport row**: `RegisterObject/s` non-zero (driver
  registers each `put`'s self-entry); `Pull/s` zero in single-node mode
  (no remote-owner refs); `NodeIndex hit ratio` ~100% green.
- **Plasma row**: `arena_bytes_used` grows then plateaus as
  `rayd.put` payloads age out; `objects` reflects live ref count;
  `create/get/delete /s` track the workload bursts.
- **Driver row**: `submitted/completed/failed` trace each other with
  ~10% failure ratio (the `_maybe_fail` task is configured for it);
  `puts/gets /s` track the burst rate; `Live ObjectRefs` plateau is
  set by the burst window (refs drop after `get_settled`).

If any panel is blank, check:

1. `docker compose logs prometheus` for scrape errors (most commonly
   a target unreachable).
2. The rayd logs at `dashboards/dev/.run/{gcs,plasma}.log`.
3. The driver's stderr for `RAYD_RAYLET_METRICS_BIND` parse warnings.

## Tear-down

`Ctrl-C` in the `run.sh` shell. The trap stops the docker containers,
SIGTERMs the rayd processes (with a 0.5 s grace period), and removes
the plasma UDS socket so the next run starts from a clean slate.

If something goes sideways and the trap doesn't fire:

    cd dashboards/dev && docker compose down
    pkill -f 'target/release/rayd'
    rm -f /tmp/rayd-dev.sock

## Editing the dashboard

`grafana-provisioning/dashboards/rayd.yml` sets `allowUiUpdates: true`
and `updateIntervalSeconds: 10`, so:

- Edit panels in Grafana → **Save dashboard** → Grafana persists the
  change inside the container only (won't survive a `docker compose
  down`).
- To export a tweaked dashboard back to the repo: in Grafana, **Share
  → Export → Save to file**, then copy the resulting JSON over
  `dashboards/rayd-overview.json`. The committed JSON references its
  datasource via `${DS_PROMETHEUS}` for portability — keep that
  variable when re-exporting (Grafana's "Export for sharing
  externally" toggle preserves it).
