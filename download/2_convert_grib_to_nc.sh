#!/bin/bash

# ============================================================================
# 2_convert_grib_to_nc.sh
#
# Convert GRIB files to NetCDF format using CDO.
#
# - Reads GRIB files from /sat_data/icon/icon_d2 and /sat_data/icon/icon_eu
# - Converts each GRIB file directly to NetCDF (no variable selection/regridding)
# - Stores NC files in temporary directory: nc_tmp/icon_d2 and nc_tmp/icon_eu
#
# Usage: bash 2_convert_grib_to_nc.sh [icon_type]
# If icon_type not specified, processes both icon-d2 and icon-eu
# ============================================================================

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# -------- SETTINGS --------
INPUT_BASE="/sat_data/icon"
NC_TMP_BASE="$INPUT_BASE"
LOG_DATE="$(date -u +%Y%m%d)"
LOG_DIR="/sat_data/icon/log_con_dowload/$LOG_DATE"
FAIL_ON_ANY_ERROR=false   # true: exit non-zero if any file fails
# --------------------------

mkdir -p "$LOG_DIR"
mkdir -p "$NC_TMP_BASE"

LOGFILE="$LOG_DIR/convert_grib_to_nc.log"
exec > >(tee -a "$LOGFILE") 2>&1

echo "==== GRIB -> NC Conversion $(date -u) ===="

# Determine which icon types to process
if [ -z "$1" ]; then
    ICON_TYPES=("icon_d2" "icon_eu")
else
    ICON_TYPES=("$1")
fi

TOTAL_CONVERTED=0
FAILED_FILES=0
CDO_ERR_LOG="$LOG_DIR/cdo_errors.log"
FAILED_FILES_LOG="$LOG_DIR/failed_grib_files.log"
: > "$CDO_ERR_LOG"
: > "$FAILED_FILES_LOG"

for ICON_TYPE in "${ICON_TYPES[@]}"; do
    echo ""
    echo "Processing ICON type: $ICON_TYPE"

    INPUT_DIR="$INPUT_BASE/$ICON_TYPE"
    OUTPUT_DIR="$NC_TMP_BASE/$ICON_TYPE/nc_tmp"

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
        step=$(echo "$gzfile" | awk -F'/' '{print $(NF-3)}')
        var=$(echo "$gzfile" | awk -F'/' '{print $(NF-2)}')
        date=$(echo "$gzfile" | awk -F'/' '{print $(NF-1)}')

        nc_subdir="$OUTPUT_DIR/${step}/${var}/${date}"
        mkdir -p "$nc_subdir"

        echo -n "  Converting $filename ... "

        temp_work="/tmp/grib_work_$$"
        mkdir -p "$temp_work"
        cd "$temp_work" || exit 1

        if ! bzcat "$gzfile" > "$base" 2>/dev/null; then
            echo "FAILED (decompress error)"
            echo "$(date -u '+%Y-%m-%d %H:%M:%S UTC') | DECOMPRESS_ERROR | $gzfile" >> "$FAILED_FILES_LOG"
            FAILED_FILES=$((FAILED_FILES + 1))
            cd /
            rm -rf "$temp_work"
            continue
        fi

        # Plain GRIB -> NetCDF conversion; no selname filtering.
        if ! cdo -f nc -P 4 copy "$base" "${base}.nc" 2>>"$CDO_ERR_LOG"; then
            echo "FAILED (CDO convert error)"
            echo "$(date -u '+%Y-%m-%d %H:%M:%S UTC') | CDO_ERROR | $gzfile" >> "$FAILED_FILES_LOG"
            FAILED_FILES=$((FAILED_FILES + 1))
            cd /
            rm -rf "$temp_work"
            continue
        fi

        if mv "${base}.nc" "$nc_subdir/" 2>/dev/null; then
            echo "OK"
            TOTAL_CONVERTED=$((TOTAL_CONVERTED + 1))
        else
            echo "FAILED (move error)"
            echo "$(date -u '+%Y-%m-%d %H:%M:%S UTC') | MOVE_ERROR | $gzfile" >> "$FAILED_FILES_LOG"
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
echo "CDO errors log: $CDO_ERR_LOG"
echo "Failed files log: $FAILED_FILES_LOG"
echo "==== DONE $(date -u) ===="

if [ "$TOTAL_CONVERTED" -eq 0 ]; then
    echo "ERROR: No files were converted successfully."
    exit 1
fi

if [ "$FAIL_ON_ANY_ERROR" = true ] && [ "$FAILED_FILES" -gt 0 ]; then
    echo "ERROR: Conversion had failed files and FAIL_ON_ANY_ERROR=true."
    exit 1
fi

if [ "$FAILED_FILES" -gt 0 ]; then
    echo "WARNING: Partial conversion completed with $FAILED_FILES failed files."
fi
