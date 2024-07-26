#!/bin/bash

# Define input and output directories
input_dir="/data/sat/msg/icon_output/marche_flood_15-09-22/grib"
output_dir="/data/sat/msg/icon_output/marche_flood_15-09-22/netcdf"
file_pattern="*.grb"  # Pattern to match files

# Create output directory if it doesn't exist
mkdir -p "$output_dir"

# Loop through files matching the pattern in the input directory
for file in "$input_dir"/$file_pattern; do
  # Extract the base name of the file (without directory and extension)
  base_name=$(basename "$file" .grib)
  
  # Define the output file name
  output_file="$output_dir/$base_name.nc" #TODO change this to remove grib in the nc file
  
  # Convert GRIB to NetCDF using cdo
  cdo -f nc copy "$file" "$output_file"
  
  # Check if the conversion was successful
  if [ $? -eq 0 ]; then
    echo "Converted $file to $output_file successfully."
  else
    echo "Failed to convert $file."
  fi
done
