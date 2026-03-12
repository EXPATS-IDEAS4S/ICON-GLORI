#!/bin/bash

# Script to clean up temporary directories used in the ICON data processing workflow
# This removes all files and folders from specified temporary directories

# -------- SETTINGS --------
GRIB_TMP_DIR="/sat_data/icon_teamx/grib_tmp"
NC_TMP_DIR="/sat_data/icon_teamx/nc_tmp"
# --------------------------

echo "============================================"
echo "Cleanup Script for ICON TeamX Temp Directories"
echo "============================================"
echo ""
echo "Cleaning directories:"
echo "  1. $GRIB_TMP_DIR"
echo "  2. $NC_TMP_DIR"
echo ""

# Clean grib_tmp directory
if [ -d "$GRIB_TMP_DIR" ]; then
    echo "Cleaning $GRIB_TMP_DIR ..."
    rm -rf "$GRIB_TMP_DIR"/*
    if [ $? -eq 0 ]; then
        echo "✓ $GRIB_TMP_DIR cleaned successfully"
    else
        echo "✗ Failed to clean $GRIB_TMP_DIR (permission denied?)"
    fi
else
    echo "! $GRIB_TMP_DIR does not exist, skipping"
fi

echo ""

# Clean nc_tmp directory
if [ -d "$NC_TMP_DIR" ]; then
    echo "Cleaning $NC_TMP_DIR ..."
    rm -rf "$NC_TMP_DIR"/*
    if [ $? -eq 0 ]; then
        echo "✓ $NC_TMP_DIR cleaned successfully"
    else
        echo "✗ Failed to clean $NC_TMP_DIR (permission denied?)"
    fi
else
    echo "! $NC_TMP_DIR does not exist, skipping"
fi

echo ""
echo "Cleanup completed!"
echo ""

# Show disk space after cleanup
echo "Disk space after cleanup:"
df -h /sat_data | tail -1
