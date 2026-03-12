#!/usr/bin/env bash

set -euo pipefail

BASE_DIR="/Users/openclaw/Documents/Playground/tender-watch"
PORT="8765"
URL="http://127.0.0.1:$PORT/"
OPEN_URL="$URL?t=$(date +%s)"
LOG_DIR="$BASE_DIR/logs"
PID_FILE="$BASE_DIR/data/dashboard_server.pid"
SERVER_LOG="$LOG_DIR/dashboard_server.log"

mkdir -p "$LOG_DIR" "$BASE_DIR/data"

if [[ -f "$PID_FILE" ]]; then
  OLD_PID="$(cat "$PID_FILE" || true)"
  if [[ -n "${OLD_PID:-}" ]] && kill -0 "$OLD_PID" 2>/dev/null; then
    if curl -fsS "$URL" >/dev/null 2>&1; then
      open "$OPEN_URL"
      echo "dashboard=$OPEN_URL"
      exit 0
    fi
  fi
fi

cd "$BASE_DIR"
nohup python3 -u "$BASE_DIR/dashboard_server.py" >>"$SERVER_LOG" 2>&1 </dev/null &
echo $! > "$PID_FILE"

for _ in {1..20}; do
  if curl -fsS "$URL" >/dev/null 2>&1; then
    open "$OPEN_URL"
    echo "dashboard=$OPEN_URL"
    exit 0
  fi
  sleep 0.5
done

echo "dashboard_start_failed" >&2
echo "log=$SERVER_LOG" >&2
exit 1
