#!/bin/bash

# ============================================================================
# 5_cleanup.sh
#
# Clean up temporary directories after successful pipeline run.
#
# Removes:
# - /sat_data/icon/icon_d2/nc_tmp and /sat_data/icon/icon_eu/nc_tmp
# - /sat_data/icon/icon_d2/merged_nc and /sat_data/icon/icon_eu/merged_nc
# - Optionally removes only GRIB files (*.bz2, *.grib, *.grib2), not whole folders
#
# Usage: bash 5_cleanup.sh [--keep-grib|--remove-grib]
# ============================================================================

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LOG_DATE="$(date -u +%Y%m%d)"

# -------- SETTINGS --------
GRIB_BASE="/sat_data/icon"
ICON_TYPES=("icon_d2" "icon_eu")
LOG_DIR="$SCRIPT_DIR/logs/$LOG_DATE"
PIPELINE_LOG_DIR="/sat_data/icon/log_con_dowload/$LOG_DATE"
KEEP_GRIB=false  # Safe default: preserve original GRIB files
# --------------------------

# Parse command-line arguments
for arg in "$@"; do
    case "$arg" in
        --keep-grib)
            KEEP_GRIB=true
            ;;
        --remove-grib)
            KEEP_GRIB=false
            ;;
        *)
            echo "ERROR: Unknown argument: $arg"
            echo "Usage: bash 5_cleanup.sh [--keep-grib|--remove-grib]"
            exit 1
            ;;
    esac
done

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

# Remove per-model temporary and merged directories only
for icon_type in "${ICON_TYPES[@]}"; do
    remove_directory "$GRIB_BASE/$icon_type/nc_tmp" "Temporary NC files for $icon_type"
    remove_directory "$GRIB_BASE/$icon_type/merged_nc" "Merged NC files for $icon_type"
done

# Remove only the downloaded DWD index artifact from centralized logs.
CONTENT_LOG_FILE="$PIPELINE_LOG_DIR/content.log.bz2"
if [ -f "$CONTENT_LOG_FILE" ]; then
    echo -n "Removing DWD content index artifact ($CONTENT_LOG_FILE) ... "
    if rm -f "$CONTENT_LOG_FILE" 2>/dev/null; then
        echo "✓"
        TOTAL_REMOVED=$((TOTAL_REMOVED + 1))
    else
        echo "✗ (permission denied or in use)"
        TOTAL_ERRORS=$((TOTAL_ERRORS + 1))
    fi
else
    echo "ⓘ DWD content index artifact not found: $CONTENT_LOG_FILE"
fi

# Optionally remove original GRIB files
if [ "$KEEP_GRIB" = false ]; then
    echo ""
    echo "Removing original GRIB files (*.bz2, *.grib, *.grib2) from cache..."

    for icon_type in "${ICON_TYPES[@]}"; do
        grib_dir="$GRIB_BASE/$icon_type"
        if [ -d "$grib_dir" ]; then
            echo -n "  Cleaning GRIB files in $grib_dir ... "
            removed_count=$(find "$grib_dir" -type f \( -name "*.bz2" -o -name "*.grib" -o -name "*.grib2" \) -print -delete 2>/dev/null | wc -l)
            find "$grib_dir" -type d -empty -delete 2>/dev/null || true

            if [ "$removed_count" -ge 0 ]; then
                echo "✓ ($removed_count files removed)"
                TOTAL_REMOVED=$((TOTAL_REMOVED + removed_count))
            else
                echo "✗ (permission denied)"
                TOTAL_ERRORS=$((TOTAL_ERRORS + 1))
            fi
        else
            echo "ⓘ GRIB base not found for $icon_type: $grib_dir"
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
