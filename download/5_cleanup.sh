#!/bin/bash

# ============================================================================
# 5_cleanup.sh
#
# Clean up temporary directories after successful pipeline run.
#
# Removes:
# - nc_tmp/ (temporary NC files before merging)
# - merged_nc/ (merged files after upload)
# - Optionally removes original GRIB files from /sat_data/icon/ based on config
#
# Usage: bash 5_cleanup.sh [--keep-grib]
# ============================================================================

set -e

# -------- SETTINGS --------
NC_TMP_DIR="nc_tmp"
MERGED_NC_DIR="merged_nc"
GRIB_BASE="/sat_data/icon"
LOG_DIR="logs"
KEEP_GRIB=false  # Set to true to preserve original GRIB files
# --------------------------

# Parse command-line arguments
if [ "$1" = "--keep-grib" ]; then
    KEEP_GRIB=true
fi

mkdir -p "$LOG_DIR"

LOGFILE="$LOG_DIR/cleanup.log"
exec > >(tee -a "$LOGFILE") 2>&1

echo "==== Cleanup $(date -u) ===="
echo "Keep GRIB files: $KEEP_GRIB"
echo ""

TOTAL_REMOVED=0
TOTAL_ERRORS=0

# Function to safely remove directory
remove_directory() {
    local dir="$1"
    local desc="$2"
    
    if [ -d "$dir" ]; then
        echo -n "Removing $desc ($dir) ... "
        if rm -rf "$dir" 2>/dev/null; then
            echo "✓"
            TOTAL_REMOVED=$((TOTAL_REMOVED + 1))
        else
            echo "✗ (permission denied or in use)"
            TOTAL_ERRORS=$((TOTAL_ERRORS + 1))
        fi
    else
        echo "ⓘ $desc not found: $dir"
    fi
}

# Remove temporary NC directory
remove_directory "$NC_TMP_DIR" "Temporary NC files"

# Remove merged NC directory
remove_directory "$MERGED_NC_DIR" "Merged NC files"

# Optionally remove original GRIB files
if [ "$KEEP_GRIB" = false ]; then
    echo ""
    echo "Removing original GRIB files from download cache..."
    
    # This is useful to free space; GRIB files are still available on DWD server
    for icon_type in "icon_d2" "icon_eu"; do
        grib_dir="$GRIB_BASE/$icon_type"
        if [ -d "$grib_dir" ]; then
            echo -n "  Cleaning $grib_dir ... "
            if rm -rf "$grib_dir"/* 2>/dev/null; then
                echo "✓"
                TOTAL_REMOVED=$((TOTAL_REMOVED + 1))
            else
                echo "✗ (permission denied)"
                TOTAL_ERRORS=$((TOTAL_ERRORS + 1))
            fi
        fi
    done
fi

echo ""
echo "==== Cleanup Summary ===="
echo "Directories removed: $TOTAL_REMOVED"
echo "Errors: $TOTAL_ERRORS"

# Show disk space after cleanup
echo ""
echo "Disk usage after cleanup:"
df -h /sat_data 2>/dev/null | tail -1 || df -h / | tail -1

echo "==== DONE $(date -u) ===="

if [ "$TOTAL_ERRORS" -gt 0 ]; then
    exit 1
fi
