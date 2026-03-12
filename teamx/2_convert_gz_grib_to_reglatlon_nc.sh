#!/bin/bash
source /home/Daniele/miniforge3/etc/profile.d/conda.sh
conda activate icon

#read date from command line argument
if [ -z "$1" ]; then
    echo "Usage: $0 YYYYMMDD_HH"
    exit 1
fi

# === USER SETTINGS ===
FOLDER="/sat_data/icon_teamx/grib_tmp/$1"
GRID_FOLDER="/sat_data/icon_teamx/domain"
GRIB_DIR="$FOLDER"
NC_DIR="$FOLDER/nc_tmp"
GRID_FILE="$GRID_FOLDER/grid_500m.txt"         # Target lat-lon grid description (TEAMX 500m grid file)
WEIGHTS_FILE="$GRID_FOLDER/weights_500m.nc"    # Weight file for regridding
GRID_INFO_FILE="$GRID_FOLDER/domain2_DOM02.nc" # ICON domain file
UNSTRUCTURED_GRID="$GRID_FOLDER/unstructured_grid.nc"
VAR_NAME="SYNMSG_BT_CL_IR10.8,CLCT"  # Variable name to select from GRIB files
# =====================

# Create output folder if missing
mkdir -p "$NC_DIR"
cd "$GRIB_DIR" || { echo "Error: cannot cd into $GRIB_DIR"; exit 1; }

# --- Extract ICON unstructured grid once ---
if [ ! -f "$UNSTRUCTURED_GRID" ]; then
    echo "Extracting unstructured grid (grid 2) from $GRID_INFO_FILE ..."
    cdo -selgrid,2 "$GRID_INFO_FILE" "$UNSTRUCTURED_GRID"
    echo "Saved ICON grid to $UNSTRUCTURED_GRID"
else
    echo "Unstructured grid file already exists: $UNSTRUCTURED_GRID"
fi

# --- Generate weights once using ICON unstructured grid as source ---
if [ ! -f "$WEIGHTS_FILE" ]; then
    echo "Generating remapping weights (this may take some time) ..."
    cdo -P 4 gennn,"$GRID_FILE" "$UNSTRUCTURED_GRID" "$WEIGHTS_FILE"
    echo "Weights stored in $WEIGHTS_FILE"
else
    echo "Weights file already exists: $WEIGHTS_FILE"
fi

# --- Loop over all GRIB files ---
for gzfile in *.gz; do
    [ -e "$gzfile" ] || continue
    base="${gzfile%.gz}"

    echo "Processing $gzfile ..."

    # 1. Unzip GRIB
    gunzip -c "$gzfile" > "$base"

    # 2. Convert GRIB → NetCDF
    #cdo -P 4 -f nc copy "$base" "${base}.nc"
    cdo -P 4 -f nc selname,"$VAR_NAME" "$base" "${base}.nc"

    # 3. Attach correct ICON grid
    cdo setgrid,"$UNSTRUCTURED_GRID" "${base}.nc" "${base}_grid.nc"

    # 4. Remap to target grid
    cdo -P 4 remap,"$GRID_FILE","$WEIGHTS_FILE" "${base}_grid.nc" "$NC_DIR/${base}.nc"
    if [ $? -ne 0 ]; then
        echo "Error converting $gzfile — skipping."
        rm -f "$base" "${base}.nc" "${base}_grid.nc"
        continue
    fi

    # 5. Cleanup
    rm -f "$base" "${base}.nc" "${base}_grid.nc"
    echo "Created $NC_DIR/${base}.nc"
done

echo "All files processed successfully."

