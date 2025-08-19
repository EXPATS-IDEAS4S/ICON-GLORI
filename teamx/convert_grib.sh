#!/bin/bash

FOLDER="/work/dcorradi/icon_output/teamx/acinn-data.uibk.ac.at/20250630_00"
GRIB_DIR="$FOLDER/grib"
NC_DIR="$FOLDER/nc"

# Check arguments
if [ -z "$FOLDER" ]; then
    echo "Usage: $0 /path/to/main_folder"
    exit 1
fi

# Check folders
if [ ! -d "$GRIB_DIR" ]; then
    echo "Error: GRIB folder '$GRIB_DIR' not found."
    exit 1
fi

mkdir -p "$NC_DIR"

cd "$GRIB_DIR" || exit 1

for gzfile in *.gz; do
    [ -e "$gzfile" ] || continue  # Skip if no .gz files found

    base="${gzfile%.gz}"

    echo "Processing $gzfile ..."

    # 1. Unzip to GRIB file
    gunzip -c "$gzfile" > "$base"

    # 2. Convert GRIB to NetCDF in nc folder
    cdo -f nc copy "$base" "$NC_DIR/${base}.nc"
    if [ $? -ne 0 ]; then
        echo "Error converting $gzfile — keeping original files."
        rm -f "$base"  # Remove failed GRIB
        continue
    fi

    # 3. Remove both GRIB and original gz after successful conversion
    rm -f "$base" "$gzfile"

    echo "Created $NC_DIR/${base}.nc and deleted $gzfile"
done

echo "All files processed."
