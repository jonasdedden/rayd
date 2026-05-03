#!/usr/bin/env bash
# Bring up a small local rayd cluster + dockerised Prometheus +
# Grafana for testing the rayd-overview dashboard.
#
# Layout once running:
#
#   rayd gcs              ← localhost:60000   (gRPC)
#                            metrics: localhost:9100  → /metrics
#   rayd plasma-server    ← /tmp/rayd-dev.sock (UDS)
#                            metrics: localhost:9102  → /metrics
#   driver (workload.py)  ← driver `/metrics`: localhost:9103
#                            embeds a raylet whose metrics:
#                            localhost:9101  → /metrics
#   prometheus (docker)   ← scrapes all four every 5s
#                            UI:      http://localhost:9090
#   grafana    (docker)   ← anonymous-admin login
#                            UI:      http://localhost:3000
#                            dashboard: rayd · cluster overview
#
# Usage:
#   ./dashboards/dev/run.sh                # bring up + tail logs
#   in another shell:
#     uv run python dashboards/dev/workload.py
#
# Ctrl-C in the run.sh shell tears everything down.

set -euo pipefail

ROOT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/../.." && pwd)"
DEV_DIR="$ROOT_DIR/dashboards/dev"
RUN_DIR="$DEV_DIR/.run"
mkdir -p "$RUN_DIR"

GCS_ADDR="127.0.0.1:60000"
GCS_METRICS_ADDR="127.0.0.1:9100"
RAYLET_METRICS_ADDR="127.0.0.1:9101"
PLASMA_METRICS_ADDR="127.0.0.1:9102"
DRIVER_METRICS_ADDR="127.0.0.1:9103"
PLASMA_SOCKET="/tmp/rayd-dev.sock"

# ── 1. Ensure binaries are built ──────────────────────────────────────
echo "==> building rayd-cli (release)"
( cd "$ROOT_DIR" && cargo build --release -p rayd-cli )

if [[ ! -f "$ROOT_DIR/.venv/bin/python" ]]; then
  echo "==> creating venv via uv"
  ( cd "$ROOT_DIR" && uv venv && uv sync --group dev )
fi

echo "==> building Python extension (debug, editable)"
( cd "$ROOT_DIR" && uv run maturin develop --uv >/dev/null )

# ── 2. Cleanup function ──────────────────────────────────────────────
PIDS=()

cleanup() {
  echo
  echo "==> shutting down"
  ( cd "$DEV_DIR" && docker compose down >/dev/null 2>&1 ) || true
  for pid in "${PIDS[@]}"; do
    if kill -0 "$pid" 2>/dev/null; then
      kill "$pid" 2>/dev/null || true
    fi
  done
  # Give them a moment to drain, then SIGKILL stragglers.
  sleep 0.5
  for pid in "${PIDS[@]}"; do
    if kill -0 "$pid" 2>/dev/null; then
      kill -9 "$pid" 2>/dev/null || true
    fi
  done
  rm -f "$PLASMA_SOCKET"
}
trap cleanup EXIT INT TERM

# ── 3. Stale-state cleanup so re-runs are idempotent ─────────────────
rm -f "$PLASMA_SOCKET"
( cd "$DEV_DIR" && docker compose down >/dev/null 2>&1 ) || true

# ── 4. Start rayd services ───────────────────────────────────────────
echo "==> starting rayd plasma-server"
"$ROOT_DIR/target/release/rayd" plasma-server "$PLASMA_SOCKET" \
    --capacity-mb 256 \
    --metrics-bind "$PLASMA_METRICS_ADDR" \
    >"$RUN_DIR/plasma.log" 2>&1 &
PIDS+=($!)

echo "==> starting rayd gcs"
"$ROOT_DIR/target/release/rayd" gcs \
    --bind "$GCS_ADDR" \
    --metrics-bind "$GCS_METRICS_ADDR" \
    >"$RUN_DIR/gcs.log" 2>&1 &
PIDS+=($!)

# Brief settle so the server ports bind before the driver tries to
# attach. 0.5s is enough on a fast laptop; the GCS log will surface
# any startup failures via the trap below.
sleep 0.5

# ── 5. Start Prometheus + Grafana via docker ─────────────────────────
echo "==> starting prometheus + grafana (docker compose)"
( cd "$DEV_DIR" && docker compose up -d )

# ── 6. Print attachment instructions ─────────────────────────────────
cat <<EOF

==================================================================
rayd dev cluster up.

  GCS:          $GCS_ADDR
  Plasma:       $PLASMA_SOCKET
  Prometheus:   http://localhost:9090
  Grafana:      http://localhost:3000  (anonymous admin)
                  → Dashboards → "rayd" → "rayd · cluster overview"

To submit a workload (in a separate terminal):

  cd $ROOT_DIR
  export RAYD_GCS_ADDRESS=$GCS_ADDR
  export RAYD_PLASMA_SOCKET=$PLASMA_SOCKET
  export RAYD_METRICS_BIND=$DRIVER_METRICS_ADDR
  export RAYD_RAYLET_METRICS_BIND=$RAYLET_METRICS_ADDR
  uv run python dashboards/dev/workload.py

Ctrl-C this shell to tear everything down.
==================================================================

EOF

# ── 7. Tail the Rust-side logs while we wait for shutdown ────────────
tail -F "$RUN_DIR/gcs.log" "$RUN_DIR/plasma.log"
