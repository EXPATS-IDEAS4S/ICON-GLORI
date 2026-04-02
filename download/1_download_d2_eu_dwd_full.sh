#!/bin/bash

# ----------------------------------------------------------------------------
# 1_download_d2_eu_dwd_full.sh
#
# Standalone full downloader for DWD ICON Open Data.
#
# What it does:
# - Crawls DWD directory listings for ICON-D2 and ICON-EU.
# - Downloads all available variables for init times 00 and 12.
# - Saves files into /sat_data/icon/icon_d2_full or icon_eu_full,
#   using subfolders: step/variable/date.
# - Skips missing/corrupted downloads and keeps going.
#
# Log/artifact files in LOG_DIR:
# - download.log:
#   Full stdout/stderr execution log for this script.
# - failed_downloads.txt:
#   List of URLs that failed during the download loop.
# - content.log.bz2:
#   Local copy of DWD index used by this run.
# - filtered_content.log:
#   Filtered subset of content.log.bz2 matching icon/step/grid.
# - updated_files.txt:
#   Final URL list for all currently matching files.
# ----------------------------------------------------------------------------

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LOG_DATE="$(date -u +%Y%m%d)"
LOG_DIR="/sat_data/icon/log_con_dowload/$LOG_DATE"
LOGFILE="$LOG_DIR/download_full.log"
FAILED_DOWNLOADS_FILE="$LOG_DIR/failed_downloads_full.txt"
CONTENT_LOG_FILE="$LOG_DIR/content.log.bz2"
FILTERED_LOG_FILE="$LOG_DIR/filtered_content_full.log"
UPDATED_FILES_FILE="$LOG_DIR/updated_files_full.txt"
OVERWRITE=false

mkdir -p "$LOG_DIR"
exec > >(tee -a "$LOGFILE") 2>&1

BASE_URL="https://opendata.dwd.de/weather/nwp"
ICON_TYPES=("icon-d2" "icon-eu")
STEPS=("00" "12")

echo "==== FULL DOWNLOAD RUN $(date -u) ===="
echo "Overwrite enabled: ${OVERWRITE}"

> "$FAILED_DOWNLOADS_FILE"

list_directory_entries() {
  local url="$1"
  wget -qO- "$url" 2>/dev/null \
    | sed -n 's/.*href="\([^"]*\)".*/\1/p'
}

collect_variable_dirs() {
  local url="$1"
  list_directory_entries "$url" \
    | grep '/$' \
    | grep -v '^../$' \
    | sed 's:/$::'
}

collect_file_urls() {
  local url="$1"
  list_directory_entries "$url" \
    | grep -v '^../$' \
    | grep '\.bz2$' \
    | sed "s#^#${url}#"
}

: > "$UPDATED_FILES_FILE"
for icon_type in "${ICON_TYPES[@]}"; do
  for step in "${STEPS[@]}"; do
    step_url="$BASE_URL/$icon_type/grib/$step/"
    while IFS= read -r var; do
      [ -z "$var" ] && continue
      var_url="$step_url$var/"
      collect_file_urls "$var_url" >> "$UPDATED_FILES_FILE"
    done < <(collect_variable_dirs "$step_url")
  done
done

sort -u -o "$UPDATED_FILES_FILE" "$UPDATED_FILES_FILE"

# Keep only regular-lat-lon grid files for full download.
grep 'regular-lat-lon' "$UPDATED_FILES_FILE" > "${UPDATED_FILES_FILE}.tmp" || true
mv "${UPDATED_FILES_FILE}.tmp" "$UPDATED_FILES_FILE"

echo "Files to download: $(wc -l < "$UPDATED_FILES_FILE")"

SUCCESS_COUNT=0
FAIL_COUNT=0
SKIP_COUNT=0

while IFS= read -r url; do
  [ -z "$url" ] && continue

  fname=$(basename "$url")
  var=$(echo "$url" | awk -F'/' '{print $(NF-2)}')
  step=$(echo "$url" | awk -F'/' '{print $(NF-3)}')
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

  outdir="/sat_data/icon/${icon_dir}_full/${step}/${var}/${date}"
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

if [ "$SUCCESS_COUNT" -eq 0 ]; then
  echo "ERROR: No files downloaded successfully."
  exit 1
fi

if [ "$FAIL_COUNT" -gt 0 ]; then
  echo "WARNING: Some downloads failed (see $FAILED_DOWNLOADS_FILE). Continuing with successful downloads."
fi
