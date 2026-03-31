#!/bin/bash

# ----------------------------------------------------------------------------
# download_d2_eu_dwd.sh
#
# Incremental downloader for DWD ICON Open Data.
#
# What it does:
# - Reads DWD content.log.bz2 index.
# - Filters entries by ICON type, forecast step, variable, and grid.
# - Builds download URLs for all matching files currently listed by DWD.
# - Downloads files into /sat_data/icon/icon_d2 or /sat_data/icon/icon_eu,
#   using subfolders: step/variable/date.
# - Optionally skips existing files when OVERWRITE=false.
#
# Log/artifact files in LOG_DIR:
# - download.log:
#   Full stdout/stderr execution log for this script.
# - failed_downloads.txt:
#   List of URLs that failed during the download loop.
# - content.log.bz2:
#   Local copy of DWD index used by this run.
# - my_content.log:
#   Filtered subset of content.log.bz2 matching configured icon/step/var/grid.
# - updated_files.txt:
#   Final URL list for all currently matching files.
# ----------------------------------------------------------------------------

set -e

LOG_DIR="logs"
LOGFILE="$LOG_DIR/download.log"
FAILED_DOWNLOADS_FILE="$LOG_DIR/failed_downloads.txt"
CONTENT_LOG_FILE="$LOG_DIR/content.log.bz2"
FILTERED_LOG_FILE="$LOG_DIR/my_content.log"
UPDATED_FILES_FILE="$LOG_DIR/updated_files.txt"
OVERWRITE=false

mkdir -p "$LOG_DIR"
exec > >(tee -a $LOGFILE) 2>&1

echo "==== RUN $(date -u) ===="

CONTENT_LOG_URL="https://opendata.dwd.de/weather/nwp/content.log.bz2"

ICON_TYPES=("icon-d2" "icon-eu")
VARIABLES=("synmsg_bt_cl_ir10.8" "synmsg_bt_cl_wv6.2" "clct")
STEPS=("00" "12")      #("00" "03" "06" "09" "12" "15" "18" "21")
GRID="regular-lat-lon" #choose between "regular-lat-lon" and "icosahedral"

echo "Overwrite enabled: ${OVERWRITE}"

> "$FAILED_DOWNLOADS_FILE"

# download index
wget -q "$CONTENT_LOG_URL" -O "$CONTENT_LOG_FILE"

# filter
> "$FILTERED_LOG_FILE"
for icon_type in "${ICON_TYPES[@]}"; do
  for var in "${VARIABLES[@]}"; do
    for step in "${STEPS[@]}"; do
      PATTERN="/${icon_type}/grib/${step}/${var}/.*_${GRID}_"
      bzgrep "$PATTERN" "$CONTENT_LOG_FILE" >> "$FILTERED_LOG_FILE" || true
    done
  done
done

# build URL list for all matched files
URL_BASE="${CONTENT_LOG_URL%/content.log.bz2}"
awk -F'|' -v base="$URL_BASE" '
  {
    path=$1
    gsub(/^\.\//, "", path)
    print base "/" path
  }
' "$FILTERED_LOG_FILE" > "$UPDATED_FILES_FILE"

echo "Files matched filters: $(wc -l < "$UPDATED_FILES_FILE")"

echo "Files to download: $(wc -l < "$UPDATED_FILES_FILE")"

SUCCESS_COUNT=0
FAIL_COUNT=0
SKIP_COUNT=0

# download with structure
while IFS= read -r url; do
  [ -z "$url" ] && continue

    fname=$(basename "$url")
  var=$(echo "$url" | awk -F'/' '{print $(NF-1)}')
  step=$(echo "$url" | awk -F'/' '{print $(NF-2)}')
  icon_type=$(echo "$url" | awk -F'/' '{for (i=1; i<=NF; i++) if ($i ~ /^icon-(d2|eu)$/) {print $i; exit}}')
  if [ -z "$icon_type" ]; then
    echo "Skipping URL with unknown icon type: $url"
    continue
  fi
  icon_dir=${icon_type//-/_}

  date=$(echo "$fname" | grep -oE '[0-9]{10}' | head -n1)
  if [ -n "$date" ]; then
    date="${date:0:4}-${date:4:2}-${date:6:2}"
  else
    date=$(date -u +%F)
  fi

  outdir="/sat_data/icon/${icon_dir}/${step}/${var}/${date}"
  mkdir -p "$outdir"

  target_file="$outdir/$fname"
  if [ -f "$target_file" ] && [ "${OVERWRITE,,}" != "true" ]; then
    SKIP_COUNT=$((SKIP_COUNT + 1))
    continue
  fi

  if [ "${OVERWRITE,,}" = "true" ]; then
    WGET_ARGS=(-q)
  else
    WGET_ARGS=(-q -c)
  fi

  if wget "${WGET_ARGS[@]}" "$url" -O "$target_file"; then
    SUCCESS_COUNT=$((SUCCESS_COUNT + 1))
  else
    echo "$url" >> "$FAILED_DOWNLOADS_FILE"
    echo "Download failed, skipping: $url"
    FAIL_COUNT=$((FAIL_COUNT + 1))
    continue
  fi

done < "$UPDATED_FILES_FILE"

echo "Downloaded successfully: $SUCCESS_COUNT"
echo "Skipped existing files: $SKIP_COUNT"
echo "Failed downloads: $FAIL_COUNT"

echo "==== DONE $(date -u) ===="

if [ "$FAIL_COUNT" -gt 0 ]; then
  echo "Some downloads failed. See $FAILED_DOWNLOADS_FILE"
  exit 1
fi