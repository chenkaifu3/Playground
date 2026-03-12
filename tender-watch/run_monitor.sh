#!/usr/bin/env bash

set -euo pipefail

BASE_DIR="/Users/openclaw/Documents/Playground/tender-watch"
MODE="${1:-incremental}"
PROFILE="${2:-non_hunan_expressway_maintenance_design}"
TS="$(date '+%Y%m%d_%H%M%S')"
LOG_DIR="$BASE_DIR/logs"
RUN_LOG="$LOG_DIR/run_${MODE}_${PROFILE}_${TS}.log"

case "$MODE" in
  incremental|snapshot|retry|retry_high_value|retry_long_tail)
    ;;
  *)
    echo "Unsupported mode: $MODE" >&2
    echo "Usage: ./run_monitor.sh [incremental|snapshot|retry|retry_high_value|retry_long_tail]" >&2
    exit 1
    ;;
esac

mkdir -p "$LOG_DIR"

cd "$BASE_DIR"
source .venv/bin/activate

{
  echo "[$(date '+%F %T')] mode=$MODE"
  echo "[$(date '+%F %T')] profile=$PROFILE"
  python collector.py "$MODE" --profile "$PROFILE"
} | tee "$RUN_LOG"

echo "run_log=$RUN_LOG"
