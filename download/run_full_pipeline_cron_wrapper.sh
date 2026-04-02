#!/bin/bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PIPELINE_SCRIPT="$SCRIPT_DIR/run_full_pipeline.sh"

# Cron runs with a minimal PATH; prepend the project conda env and common system bins.
export PATH="/home/Daniele/miniforge3/envs/icon/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:$PATH"

LOG_BASE_DIR="/sat_data/icon/log_con_dowload"
CRON_LOG_DIR="$LOG_BASE_DIR/cron"
LOCK_FILE="/tmp/icon_glori_pipeline.lock"
END_DATE_UTC="2026-09-30"

mkdir -p "$CRON_LOG_DIR"

TODAY_UTC="$(date -u +%F)"
if [[ "$TODAY_UTC" > "$END_DATE_UTC" ]]; then
    echo "[$(date -u '+%Y-%m-%d %H:%M:%S UTC')] Scheduler window ended ($END_DATE_UTC). Exiting." >> "$CRON_LOG_DIR/cron_runner.log"
    exit 0
fi

exec 9>"$LOCK_FILE"
if ! flock -n 9; then
    echo "[$(date -u '+%Y-%m-%d %H:%M:%S UTC')] Previous run still active, skipping." >> "$CRON_LOG_DIR/cron_runner.log"
    exit 0
fi

RUN_STAMP="$(date -u +%Y%m%d_%H%M%S)"
RUN_LOG="$CRON_LOG_DIR/cron_run_${RUN_STAMP}.log"

{
    echo "======================================================================"
    echo "Cron-triggered pipeline run started: $(date -u '+%Y-%m-%d %H:%M:%S UTC')"
    echo "Pipeline script: $PIPELINE_SCRIPT"
    echo "======================================================================"

    if ! bash "$PIPELINE_SCRIPT" --run-once; then
        echo "Pipeline run FAILED at $(date -u '+%Y-%m-%d %H:%M:%S UTC')"
        exit 1
    fi

    echo "Pipeline run completed successfully at $(date -u '+%Y-%m-%d %H:%M:%S UTC')"
} >> "$RUN_LOG" 2>&1

echo "[$(date -u '+%Y-%m-%d %H:%M:%S UTC')] Run finished. Log: $RUN_LOG" >> "$CRON_LOG_DIR/cron_runner.log"
