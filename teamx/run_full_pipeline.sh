#!/bin/bash

################################################################################
# ICON GLORI TeamX - Full Data Processing Pipeline
################################################################################
# This script orchestrates the complete workflow:
# 1. Download data from server for a given date
# 2. Convert GRIB to NetCDF on regular lat/lon grid
# 3. Merge NetCDF files
# 4. Upload to S3 bucket
# 5. Cleanup temporary directories
#
# Usage: bash run_full_pipeline.sh [date_list_file]
# Example: bash run_full_pipeline.sh date_list.txt
################################################################################

# -------- SETTINGS --------
PYTHON_BIN="/home/Daniele/miniforge3/envs/vissl/bin/python"  # Full path to Python executable
DATE_LIST_FILE="${1:-date_list.txt}"  # Use first argument or default to date_list.txt
LOG_DIR="./logs"
CONTINUE_ON_ERROR=false  # Set to true to continue processing even if a step fails
SKIP_DOWNLOAD=false      # Set to true to skip download step (useful for reprocessing)
SKIP_CONVERT=false       # Set to true to skip conversion step
SKIP_MERGE=false         # Set to true to skip merge step
SKIP_UPLOAD=false        # Set to true to skip upload step
SKIP_DELETE_TEMP=false     # Set to true to skip cleanup of temporary directories
# --------------------------

# Create log directory
mkdir -p "$LOG_DIR"

# Check if date list file exists
if [ ! -f "$DATE_LIST_FILE" ]; then
    echo "ERROR: Date list file not found: $DATE_LIST_FILE"
    echo "Usage: bash run_full_pipeline.sh [date_list_file]"
    exit 1
fi

# Count total dates
TOTAL_DATES=$(wc -l < "$DATE_LIST_FILE")

echo "=============================================="
echo "ICON GLORI TeamX - Full Pipeline"
echo "=============================================="
echo "Date list file: $DATE_LIST_FILE"
echo "Total dates in list: $TOTAL_DATES"
echo "Log directory: $LOG_DIR"
echo ""

# Initialize counters
SUCCESS_COUNT=0
FAILED_COUNT=0
FAILED_DATES=()

# Get start time
PIPELINE_START_TIME=$(date +%s)

# Read dates from file and process each
DATE_INDEX=0
while IFS= read -r DATE || [ -n "$DATE" ]; do
    # Skip empty lines and comments
    [[ -z "$DATE" || "$DATE" =~ ^#.* ]] && continue
    
    DATE_INDEX=$((DATE_INDEX + 1))
    echo ""
    echo "=========================================="
    echo "Processing date [$DATE_INDEX/$TOTAL_DATES]: $DATE"
    echo "=========================================="
    
    # Create date-specific log file
    DATE_LOG="$LOG_DIR/${DATE}.log"
    echo "Processing date: $DATE" > "$DATE_LOG"
    echo "Started at: $(date)" >> "$DATE_LOG"
    echo "" >> "$DATE_LOG"
    
    # Track if this date succeeded
    DATE_FAILED=false
    STEP_START_TIME=$(date +%s)
    
    # =====================================
    # STEP 1: Download folder from server
    # =====================================
    if [ "$SKIP_DOWNLOAD" = false ]; then
        echo ""
        echo "Step 1/4: Downloading data for $DATE..."
        echo "----------------------------------------"
        echo "Command: bash 1_download_folder.sh $DATE"
        
        if bash 1_download_folder.sh "$DATE" >> "$DATE_LOG" 2>&1; then
            echo "✓ Download completed successfully"
        else
            echo "✗ Download failed (exit code: $?)"
            DATE_FAILED=true
            [ "$CONTINUE_ON_ERROR" = false ] && echo "Stopping pipeline for this date." || echo "Continuing to next step..."
            [ "$CONTINUE_ON_ERROR" = false ] && continue
        fi
    else
        echo "Step 1/4: Download - SKIPPED"
    fi
    
    # =====================================
    # STEP 2: Convert GRIB to NetCDF
    # =====================================
    if [ "$SKIP_CONVERT" = false ]; then
        echo ""
        echo "Step 2/4: Converting GRIB to NetCDF for $DATE..."
        echo "----------------------------------------"
        echo "Command: bash 2_convert_gz_grib_to_reglatlon_nc.sh $DATE"
        
        if bash 2_convert_gz_grib_to_reglatlon_nc.sh "$DATE" >> "$DATE_LOG" 2>&1; then
            echo "✓ Conversion completed successfully"
        else
            echo "✗ Conversion failed (exit code: $?)"
            DATE_FAILED=true
            [ "$CONTINUE_ON_ERROR" = false ] && echo "Stopping pipeline for this date." || echo "Continuing to next step..."
            [ "$CONTINUE_ON_ERROR" = false ] && continue
        fi
    else
        echo "Step 2/4: Conversion - SKIPPED"
    fi
    
    # =====================================
    # STEP 3: Merge NetCDF files
    # =====================================
    if [ "$SKIP_MERGE" = false ]; then
        echo ""
        echo "Step 3/4: Merging NetCDF files for $DATE..."
        echo "----------------------------------------"
        echo "Command: $PYTHON_BIN 3_merge_nc_files.py $DATE"
        
        if "$PYTHON_BIN" 3_merge_nc_files.py "$DATE" >> "$DATE_LOG" 2>&1; then
            echo "✓ Merge completed successfully"
        else
            echo "✗ Merge failed (exit code: $?)"
            DATE_FAILED=true
            [ "$CONTINUE_ON_ERROR" = false ] && echo "Stopping pipeline for this date." || echo "Continuing to next step..."
            [ "$CONTINUE_ON_ERROR" = false ] && continue
        fi
    else
        echo "Step 3/4: Merge - SKIPPED"
    fi
    
    # =====================================
    # STEP 4: Upload to S3 bucket
    # =====================================
    if [ "$SKIP_UPLOAD" = false ]; then
        echo ""
        echo "Step 4/4: Uploading to S3 bucket for $DATE..."
        echo "----------------------------------------"
        echo "Command: $PYTHON_BIN 4_upload_data_bucket.py $DATE"
        
        if "$PYTHON_BIN" 4_upload_data_bucket.py "$DATE" >> "$DATE_LOG" 2>&1; then
            echo "✓ Upload completed successfully"
        else
            echo "✗ Upload failed (exit code: $?)"
            DATE_FAILED=true
            [ "$CONTINUE_ON_ERROR" = false ] && echo "Stopping pipeline for this date." || echo "Continuing to next step..."
            [ "$CONTINUE_ON_ERROR" = false ] && continue
        fi
    else
        echo "Step 4/4: Upload - SKIPPED"
    fi
    
    #=====================================
    #STEP 5: Delete temporary directories (grib_tmp and nc_tmp)
    #=====================================
    if [ "$SKIP_DELETE_TEMP" = false ]; then
        echo ""
        echo "Step 5/5: Deleting temporary directories for $DATE..."
        echo "----------------------------------------"
        echo "Command: bash 5_cleanup_temp_dirs.sh $DATE"
        
        if bash 5_cleanup_temp_dirs.sh "$DATE" >> "$DATE_LOG" 2>&1; then
            echo "✓ Temporary directories cleaned up successfully"
        else
            echo "✗ Cleanup of temporary directories failed (exit code: $?)"
            DATE_FAILED=true
            [ "$CONTINUE_ON_ERROR" = false ] && echo "Stopping pipeline for this date." || echo "Continuing to next step..."
            [ "$CONTINUE_ON_ERROR" = false ] && continue
        fi
    else
        echo "Step 5/5: Cleanup of temporary directories - SKIPPED"
    fi
    
    # Calculate processing time for this date
    STEP_END_TIME=$(date +%s)
    STEP_DURATION=$((STEP_END_TIME - STEP_START_TIME))
    
    # Update counters
    if [ "$DATE_FAILED" = false ]; then
        SUCCESS_COUNT=$((SUCCESS_COUNT + 1))
        echo ""
        echo "✓✓✓ Date $DATE processed successfully in ${STEP_DURATION}s ✓✓✓"
    else
        FAILED_COUNT=$((FAILED_COUNT + 1))
        FAILED_DATES+=("$DATE")
        echo ""
        echo "✗✗✗ Date $DATE failed ✗✗✗"
    fi
    
    echo "Completed at: $(date)" >> "$DATE_LOG"
    echo "Duration: ${STEP_DURATION} seconds" >> "$DATE_LOG"
    
done < "$DATE_LIST_FILE"

# Calculate total pipeline time
PIPELINE_END_TIME=$(date +%s)
TOTAL_DURATION=$((PIPELINE_END_TIME - PIPELINE_START_TIME))
HOURS=$((TOTAL_DURATION / 3600))
MINUTES=$(((TOTAL_DURATION % 3600) / 60))
SECONDS=$((TOTAL_DURATION % 60))

# Print final summary
echo ""
echo "=========================================="
echo "Pipeline Completed!"
echo "=========================================="
echo "Dates in list: $TOTAL_DATES"
echo "Dates processed: $DATE_INDEX"
echo "Successful: $SUCCESS_COUNT"
echo "Failed: $FAILED_COUNT"
echo "Total time: ${HOURS}h ${MINUTES}m ${SECONDS}s"
echo ""

if [ ${FAILED_COUNT} -gt 0 ]; then
    echo "Failed dates:"
    for failed_date in "${FAILED_DATES[@]}"; do
        echo "  - $failed_date"
    done
    echo ""
    echo "Check logs in $LOG_DIR for details."
    exit 1
else
    echo "All dates processed successfully!"
    exit 0
fi

#2047377