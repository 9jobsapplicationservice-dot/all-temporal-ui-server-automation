#!/usr/bin/env sh
set -eu

export PIPELINE_WORKSPACE_ROOT="${PIPELINE_WORKSPACE_ROOT:-/app}"
export PIPELINE_ROOT="${PIPELINE_ROOT:-/app/data/pipeline}"
export PIPELINE_PYTHON="${PIPELINE_PYTHON:-python3}"
export PIPELINE_TEMPORAL_AUTO_START="${PIPELINE_TEMPORAL_AUTO_START:-true}"
export DISPLAY="${DISPLAY:-:99}"
export PORT="${PORT:-3000}"

mkdir -p "$PIPELINE_ROOT" /app/data/chrome /tmp/chrome-profile

if ! pgrep Xvfb >/dev/null 2>&1; then
  echo "Starting Xvfb..."
  Xvfb "$DISPLAY" -screen 0 "${XVFB_RESOLUTION:-1920x1080x24}" >/tmp/xvfb.log 2>&1 &
fi

if [ "${PIPELINE_TEMPORAL_AUTO_START:-true}" = "true" ]; then
  if ! pgrep temporal >/dev/null 2>&1; then
    echo "Starting Temporal Server..."
    temporal server start-dev \
      --db-filename /app/data/temporal.db \
      --ui-port 8233 \
      --ip 0.0.0.0 \
      >/tmp/temporal.log 2>&1 &
    
    # Wait for temporal to be ready
    echo "Waiting for Temporal Server to listen on 7233..."
    for i in $(seq 1 30); do
      if nc -z localhost 7233 >/dev/null 2>&1; then
        echo "Temporal Server is ready."
        break
      fi
      sleep 1
    done
  fi

  if ! pgrep -f "python3 -m pipeline.temporal_worker" >/dev/null 2>&1; then
    echo "Starting Temporal Worker..."
    python3 -m pipeline.temporal_worker >/tmp/temporal_worker.log 2>&1 &
    echo "Temporal Worker started in background."
  fi
fi

echo "Starting Next.js App on port $PORT..."
exec npm --prefix "/app/sendemailwith-code/email-automation-nodejs" start
