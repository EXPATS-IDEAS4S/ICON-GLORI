#!/bin/bash

################################################################################
# run_full_pipeline.sh
#
# ICON-GLORI Download & Processing Pipeline
#
# Orchestrates the complete workflow:
# 0. Download from DWD (0_download_d2_eu_dwd.sh)
# 1. Convert GRIB to NetCDF (2_convert_grib_to_nc.sh)
# 2. Merge NetCDF files (3_merge_nc_files.py)
# 3. Upload to S3 buckets (4_upload_data_bucket.py)
# 4. Cleanup temporary files (5_cleanup.sh)
#
# Each step is optional and can be skipped via config flags.
# All steps must succeed for cleanup to proceed (unless CONTINUE_ON_ERROR=true).
#
# Usage:
#    bash run_full_pipeline.sh
#
# Configuration:
#    Edit the SETTINGS section below to customize behavior.
################################################################################

# -------- SETTINGS --------
LOG_DIR="logs"
CONTINUE_ON_ERROR=false      # Continue processing even if a step fails
SKIP_DOWNLOAD=false          # Skip step 0: download
SKIP_CONVERT=false           # Skip step 2: GRIB → NC conversion
SKIP_MERGE=false             # Skip step 3: merge NC files
SKIP_UPLOAD=false            # Skip step 4: upload to buckets
SKIP_CLEANUP=true            # Skip step 5: cleanup (default=skip for safety)
KEEP_GRIB_ON_CLEANUP=true    # If cleanup enabled: keep original GRIB files
PYTHON_BIN="python3"         # Python executable
# --------------------------

set -e

# Create log directory
mkdir -p "$LOG_DIR"

# Main log file
MAIN_LOG="$LOG_DIR/pipeline_run.log"
exec 1> >(tee -a "$MAIN_LOG")
exec 2>&1

echo "================================================================================"
echo "ICON-GLORI Download & Processing Pipeline"
echo "================================================================================"
echo "Started: $(date)"
echo "Log directory: $LOG_DIR"
echo ""

# Check prerequisites
echo "Checking prerequisites..."
if ! command -v cdo &> /dev/null; then
    echo "ERROR: CDO (Climate Data Operators) not found. Please install it."
    exit 1
fi

if ! $PYTHON_BIN -c "import xarray" 2>/dev/null; then
    echo "ERROR: Python xarray module not found. Please install it."
    exit 1
fi

if ! $PYTHON_BIN -c "import boto3" 2>/dev/null; then
    echo "WARNING: Python boto3 module not found. S3 upload will fail."
fi

echo "✓ All prerequisites met"
echo ""

# Initialize counters
PIPELINE_START_TIME=$(date +%s)
STEPS_FAILED=()
STEPS_SUCCEEDED=()

# Step 0: Download
if [ "$SKIP_DOWNLOAD" = false ]; then
    echo "================================================================================"
    echo "STEP 0: Download data from DWD (0_download_d2_eu_dwd.sh)"
    echo "================================================================================"
    
    STEP_LOG="$LOG_DIR/step0_download.log"
    if bash 0_download_d2_eu_dwd.sh > "$STEP_LOG" 2>&1; then
        echo "✓ Download completed successfully"
        STEPS_SUCCEEDED+=("Download")
    else
        echo "✗ Download failed (check $STEP_LOG)"
        STEPS_FAILED+=("Download")
        if [ "$CONTINUE_ON_ERROR" = false ]; then
            echo "Stopping pipeline due to error. Set CONTINUE_ON_ERROR=true to proceed."
            exit 1
        fi
    fi
    echo ""
else
    echo "STEP 0: Skipped (SKIP_DOWNLOAD=true)"
    echo ""
fi

# Step 2: Convert GRIB to NC
if [ "$SKIP_CONVERT" = false ]; then
    echo "================================================================================"
    echo "STEP 2: Convert GRIB files to NetCDF (2_convert_grib_to_nc.sh)"
    echo "================================================================================"
    
    STEP_LOG="$LOG_DIR/step2_convert.log"
    if bash 2_convert_grib_to_nc.sh > "$STEP_LOG" 2>&1; then
        echo "✓ Conversion completed successfully"
        STEPS_SUCCEEDED+=("GRIB→NC Conversion")
    else
        echo "✗ Conversion failed (check $STEP_LOG)"
        STEPS_FAILED+=("GRIB→NC Conversion")
        if [ "$CONTINUE_ON_ERROR" = false ]; then
            echo "Stopping pipeline due to error."
            exit 1
        fi
    fi
    echo ""
else
    echo "STEP 2: Skipped (SKIP_CONVERT=true)"
    echo ""
fi

# Step 3: Merge NC files
if [ "$SKIP_MERGE" = false ]; then
    echo "================================================================================"
    echo "STEP 3: Merge NetCDF files (3_merge_nc_files.py)"
    echo "================================================================================"
    
    STEP_LOG="$LOG_DIR/step3_merge.log"
    if $PYTHON_BIN 3_merge_nc_files.py merged_nc nc_tmp > "$STEP_LOG" 2>&1; then
        echo "✓ Merge completed successfully"
        STEPS_SUCCEEDED+=("NetCDF Merge")
    else
        echo "✗ Merge failed (check $STEP_LOG)"
        STEPS_FAILED+=("NetCDF Merge")
        if [ "$CONTINUE_ON_ERROR" = false ]; then
            echo "Stopping pipeline due to error."
            exit 1
        fi
    fi
    echo ""
else
    echo "STEP 3: Skipped (SKIP_MERGE=true)"
    echo ""
fi

# Step 4: Upload to buckets
if [ "$SKIP_UPLOAD" = false ]; then
    echo "================================================================================"
    echo "STEP 4: Upload to S3 buckets (4_upload_data_bucket.py)"
    echo "================================================================================"
    
    STEP_LOG="$LOG_DIR/step4_upload.log"
    if $PYTHON_BIN 4_upload_data_bucket.py merged_nc > "$STEP_LOG" 2>&1; then
        echo "✓ Upload completed successfully"
        STEPS_SUCCEEDED+=("S3 Upload")
    else
        echo "✗ Upload failed (check $STEP_LOG)"
        STEPS_FAILED+=("S3 Upload")
        if [ "$CONTINUE_ON_ERROR" = false ]; then
            echo "Stopping pipeline due to error."
            exit 1
        fi
    fi
    echo ""
else
    echo "STEP 4: Skipped (SKIP_UPLOAD=true)"
    echo ""
fi

# Step 5: Cleanup
if [ "$SKIP_CLEANUP" = false ]; then
    echo "================================================================================"
    echo "STEP 5: Cleanup temporary files (5_cleanup.sh)"
    echo "================================================================================"
    
    STEP_LOG="$LOG_DIR/step5_cleanup.log"
    CLEANUP_ARGS=""
    if [ "$KEEP_GRIB_ON_CLEANUP" = true ]; then
        CLEANUP_ARGS="--keep-grib"
    fi
    
    if bash 5_cleanup.sh $CLEANUP_ARGS > "$STEP_LOG" 2>&1; then
        echo "✓ Cleanup completed successfully"
        STEPS_SUCCEEDED+=("Cleanup")
    else
        echo "⚠ Cleanup had warnings (check $STEP_LOG)"
        STEPS_FAILED+=("Cleanup (non-fatal)")
    fi
    echo ""
else
    echo "STEP 5: Skipped (SKIP_CLEANUP=true)"
    echo ""
fi

# Final summary
PIPELINE_END_TIME=$(date +%s)
PIPELINE_DURATION=$((PIPELINE_END_TIME - PIPELINE_START_TIME))

echo "================================================================================"
echo "PIPELINE SUMMARY"
echo "================================================================================"
echo "Duration: $((PIPELINE_DURATION / 60))m $((PIPELINE_DURATION % 60))s"
echo ""

if [ ${#STEPS_SUCCEEDED[@]} -gt 0 ]; then
    echo "✓ Steps succeeded (${#STEPS_SUCCEEDED[@]}):"
    for step in "${STEPS_SUCCEEDED[@]}"; do
        echo "  - $step"
    done
    echo ""
fi

if [ ${#STEPS_FAILED[@]} -gt 0 ]; then
    echo "✗ Steps failed (${#STEPS_FAILED[@]}):"
    for step in "${STEPS_FAILED[@]}"; do
        echo "  - $step"
    done
    echo ""
    echo "Check log files in $LOG_DIR for details."
    exit 1
fi

echo "✓ All pipeline steps completed successfully!"
echo "Finished: $(date)"
echo "================================================================================"
