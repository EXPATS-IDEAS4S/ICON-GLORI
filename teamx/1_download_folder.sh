#!/bin/bash

# -------- SETTINGS --------
HOST="acinn-data.uibk.ac.at"
USER="teamx_icon"
PASS='Ter0Drav$'

#read date from command line argument
if [ -z "$1" ]; then
    echo "Usage: $0 YYYYMMDD_HH"
    exit 1
fi

REMOTE_DIR="/$1"  # Remote folder to download
OUTPUT_DIR="/sat_data/icon_teamx/grib_tmp/$1"  # Local folder to save downloaded files
# --------------------------

mkdir -p "$OUTPUT_DIR"

echo "Downloading files to: $OUTPUT_DIR"
echo "Skipping files that already exist..."

wget -r -nH --cut-dirs=1 \
     --no-clobber \
     --ftp-user="$USER" \
     --ftp-password="$PASS" \
     -P "$OUTPUT_DIR" \
     ftp://$HOST$REMOTE_DIR/

echo "Download completed."