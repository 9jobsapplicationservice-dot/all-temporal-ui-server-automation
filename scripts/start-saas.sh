#!/usr/bin/env sh
set -eu

export PIPELINE_WORKSPACE_ROOT="${PIPELINE_WORKSPACE_ROOT:-/app}"
export PIPELINE_ROOT="${PIPELINE_ROOT:-/data/pipeline}"
export PIPELINE_PYTHON="${PIPELINE_PYTHON:-python3}"
export PIPELINE_TEMPORAL_AUTO_START="${PIPELINE_TEMPORAL_AUTO_START:-true}"
export DISPLAY="${DISPLAY:-:99}"
export PORT="${PORT:-3000}"

mkdir -p "$PIPELINE_ROOT" /data/chrome /tmp/chrome-profile

if ! pgrep Xvfb >/dev/null 2>&1; then
  Xvfb "$DISPLAY" -screen 0 "${XVFB_RESOLUTION:-1920x1080x24}" >/tmp/xvfb.log 2>&1 &
fi

exec npm --prefix "/app/sendeamilwith code/email-automation-nodejs" start
