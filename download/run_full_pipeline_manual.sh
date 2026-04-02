#!/bin/bash

################################################################################
# run_full_pipeline_manual.sh
#
# Manual on-demand pipeline for full ICON GRIB downloads.
#
# This pipeline is separate from the cron-driven daily pipeline.
# It downloads all available ICON-D2 and ICON-EU variables for init 00 and 12
# into *_full folders, then uploads the raw GRIB files directly to the
# case-study buckets.
################################################################################

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LOG_DATE="$(date -u +%Y%m%d)"
LOG_BASE_DIR="/sat_data/icon/log_con_dowload"
LOG_DIR="$LOG_BASE_DIR/$LOG_DATE/manual_full"
PYTHON_BIN="python3"
CONTINUE_ON_ERROR=false

mkdir -p "$LOG_DIR"
MAIN_LOG="$LOG_DIR/pipeline_run.log"
exec 1> >(tee -a "$MAIN_LOG")
exec 2>&1

echo "================================================================================"
echo "ICON-GLORI Manual Full GRIB Pipeline"
echo "================================================================================"
echo "Started: $(date)"
echo "Log directory: $LOG_DIR"
echo ""

echo "Checking prerequisites..."
if ! $PYTHON_BIN -c "import boto3" 2>/dev/null; then
    echo "WARNING: Python boto3 module not found. Upload will fail."
fi

echo "✓ All prerequisites met"
echo ""

PIPELINE_START_TIME=$(date +%s)
STEPS_FAILED=()
STEPS_SUCCEEDED=()

STEP_LOG="$LOG_DIR/step1_download_full.log"
echo "================================================================================"
echo "STEP 1: Full download (1_download_d2_eu_dwd_full.sh)"
echo "================================================================================"
if bash "$SCRIPT_DIR/1_download_d2_eu_dwd_full.sh" > "$STEP_LOG" 2>&1; then
    echo "✓ Full download completed successfully"
    STEPS_SUCCEEDED+=("Full Download")
else
    echo "✗ Full download failed (check $STEP_LOG)"
    STEPS_FAILED+=("Full Download")
    if [ "$CONTINUE_ON_ERROR" = false ]; then
        echo "Stopping pipeline due to error."
        exit 1
    fi
fi
echo ""

STEP_LOG="$LOG_DIR/step2_upload_raw.log"
echo "================================================================================"
echo "STEP 2: Upload raw GRIB files to case-study buckets"
echo "================================================================================"
if $PYTHON_BIN "$SCRIPT_DIR/4_upload_grib_case_study.py" > "$STEP_LOG" 2>&1; then
    echo "✓ Raw GRIB upload completed successfully"
    STEPS_SUCCEEDED+=("Raw GRIB Upload")
else
    echo "✗ Raw GRIB upload failed (check $STEP_LOG)"
    STEPS_FAILED+=("Raw GRIB Upload")
    if [ "$CONTINUE_ON_ERROR" = false ]; then
        echo "Stopping pipeline due to error."
        exit 1
    fi
fi
echo ""

PIPELINE_END_TIME=$(date +%s)
PIPELINE_DURATION=$((PIPELINE_END_TIME - PIPELINE_START_TIME))

echo "================================================================================"
echo "PIPELINE SUMMARY"
echo "================================================================================"
echo "Duration: $((PIPELINE_DURATION / 60))m $((PIPELINE_DURATION % 60))s"
echo ""

echo "✓ Manual full GRIB pipeline executed."
if [ ${#STEPS_FAILED[@]} -gt 0 ]; then
    echo "✗ Steps failed (${#STEPS_FAILED[@]}):"
    for step in "${STEPS_FAILED[@]}"; do
        echo "  - $step"
    done
    echo ""
    echo "Check log files in $LOG_DIR for details."
    exit 1
fi

echo "Finished: $(date)"
echo "================================================================================"
