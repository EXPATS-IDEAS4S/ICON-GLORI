#!/bin/bash

# ============================================================================
# 2_convert_grib_to_nc.sh
#
# Convert GRIB files to NetCDF format using CDO.
#
# - Reads GRIB files from /sat_data/icon/icon_d2 and /sat_data/icon/icon_eu
# - Converts to regular lat-lon NetCDF
# - Stores NC files in temporary directory: nc_tmp/icon_d2 and nc_tmp/icon_eu
#
# Usage: bash 2_convert_grib_to_nc.sh [icon_type]
# If icon_type not specified, processes both icon-d2 and icon-eu
# ============================================================================

set -e

# -------- SETTINGS --------
INPUT_BASE="/sat_data/icon"
NC_TMP_BASE="nc_tmp"
LOG_DIR="logs"
VARIABLES="synmsg_bt_cl_ir10.8,synmsg_bt_cl_wv6.2,clct"  # Variables to extract
# --------------------------

mkdir -p "$LOG_DIR"
mkdir -p "$NC_TMP_BASE"

LOGFILE="$LOG_DIR/convert_grib_to_nc.log"
exec > >(tee -a "$LOGFILE") 2>&1

echo "==== GRIB → NC Conversion $(date -u) ===="

# Determine which icon types to process
if [ -z "$1" ]; then
    ICON_TYPES=("icon_d2" "icon_eu")
else
    ICON_TYPES=("$1")
fi

TOTAL_CONVERTED=0
FAILED_FILES=0

for ICON_TYPE in "${ICON_TYPES[@]}"; do
    echo ""
    echo "Processing ICON type: $ICON_TYPE"
    
    INPUT_DIR="$INPUT_BASE/$ICON_TYPE"
    OUTPUT_DIR="$NC_TMP_BASE/$ICON_TYPE"
    
    if [ ! -d "$INPUT_DIR" ]; then
        echo "Warning: Input directory not found: $INPUT_DIR"
        continue
    fi
    
    mkdir -p "$OUTPUT_DIR"
    
    # Find all bz2-compressed GRIB files
    GRIB_FILES=$(find "$INPUT_DIR" -name "*.bz2" | sort)
    FILE_COUNT=$(echo "$GRIB_FILES" | grep -c . || echo 0)
    
    if [ "$FILE_COUNT" -eq 0 ]; then
        echo "No GRIB files found in $INPUT_DIR"
        continue
    fi
    
    echo "Found $FILE_COUNT GRIB files to convert"
    
    for gzfile in $GRIB_FILES; do
        filename=$(basename "$gzfile")
        base="${filename%.bz2}"
        
        # Extract step, variable, and date from path
        # Example: /sat_data/icon/icon_d2/12/clct/2026-03-31/icon-d2_germany_regular-lat-lon_single-level_2026033100_007_2d_clct.grib2.bz2
        step=$(echo "$gzfile" | awk -F'/' '{print $(NF-3)}')
        var=$(echo "$gzfile" | awk -F'/' '{print $(NF-2)}')
        date=$(echo "$gzfile" | awk -F'/' '{print $(NF-1)}')
        
        nc_subdir="$OUTPUT_DIR/${step}/${var}/${date}"
        mkdir -p "$nc_subdir"
        
        echo -n "  Converting $filename ... "
        
        # Create temp directory for processing
        temp_work="/tmp/grib_work_$$"
        mkdir -p "$temp_work"
        cd "$temp_work" || exit 1
        
        # Decompress GRIB
        if ! bzcat "$gzfile" > "$base" 2>/dev/null; then
            echo "FAILED (decompress error)"
            FAILED_FILES=$((FAILED_FILES + 1))
            cd /
            rm -rf "$temp_work"
            continue
        fi
        
        # Convert GRIB to NetCDF, selecting only the requested variables
        if ! cdo -f nc -P 4 selname,"$VARIABLES" "$base" "${base}.nc" 2>/dev/null; then
            echo "FAILED (CDO convert error)"
            FAILED_FILES=$((FAILED_FILES + 1))
            cd /
            rm -rf "$temp_work"
            continue
        fi
        
        # Move to output directory
        if mv "${base}.nc" "$nc_subdir/" 2>/dev/null; then
            echo "OK ✓"
            TOTAL_CONVERTED=$((TOTAL_CONVERTED + 1))
        else
            echo "FAILED (move error)"
            FAILED_FILES=$((FAILED_FILES + 1))
        fi
        
        cd /
        rm -rf "$temp_work"
    done
done

echo ""
echo "==== Conversion Summary ===="
echo "Files converted: $TOTAL_CONVERTED"
echo "Failed files: $FAILED_FILES"
echo "==== DONE $(date -u) ===="

if [ "$FAILED_FILES" -gt 0 ]; then
    exit 1
fi
