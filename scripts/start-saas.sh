#!/usr/bin/env sh
set -eu

export PIPELINE_WORKSPACE_ROOT="${PIPELINE_WORKSPACE_ROOT:-/app}"
export PIPELINE_ROOT="${PIPELINE_ROOT:-/app/data/pipeline}"
export PIPELINE_PYTHON="${PIPELINE_PYTHON:-python3}"
export DIRECT_EXECUTION_MODE="${DIRECT_EXECUTION_MODE:-true}"
export PIPELINE_DATA_DIR="${PIPELINE_DATA_DIR:-/app/data/pipeline}"
export DISPLAY="${DISPLAY:-:99}"
export PORT="${PORT:-3000}"
export PIPELINE_MODE=true

mkdir -p "$PIPELINE_DATA_DIR" /app/data/chrome /tmp/chrome-profile

if [ "$DIRECT_EXECUTION_MODE" = "true" ]; then
  echo "DIRECT_EXECUTION_MODE=true"
  echo "PIPELINE_DATA_DIR=$PIPELINE_DATA_DIR"
fi

if ! pgrep Xvfb >/dev/null 2>&1; then
  echo "Starting Xvfb..."
  Xvfb "$DISPLAY" -screen 0 "${XVFB_RESOLUTION:-1920x1080x24}" >/tmp/xvfb.log 2>&1 &
fi

echo "Starting Next.js App on port $PORT..."
exec npm --prefix "/app/sendemailwith-code/email-automation-nodejs" start
