#!/usr/bin/env python3
"""
Merge NC files and extract a specific variable.

Reads NC files from a directory, extracts SYNMSG_BT_CL_IR10.8 variable,
merges all files, and saves to output directory.
"""

import os
import glob
import sys
import gzip
import shutil
from pathlib import Path

import xarray as xr
import numpy as np


def merge_nc_files(input_dir, output_dir, variable_name=["SYNMSG_BT_CL_IR10.8"], date="20250401_00"):
    """
    Merge NC files and extract specific variable(s).
    
    Parameters:
    -----------
    input_dir : str
        Directory containing NC files to merge
    output_dir : str
        Directory where merged file will be saved
    variable_name : str or list
        Variable name(s) to extract (comma-separated string or list)
        Example: "SYNMSG_BT_CL_IR10.8" or "SYNMSG_BT_CL_IR10.8,CLCT" or ["SYNMSG_BT_CL_IR10.8", "CLCT"]
    """
    
    # Parse comma-separated variable names or accept list directly
    if isinstance(variable_name, list):
        var_list = variable_name
    else:
        var_list = [v.strip() for v in variable_name.split(',')]
    
    print("=" * 60)
    print("NC File Merger")
    print("=" * 60)
    print(f"Input directory:  {input_dir}")
    print(f"Output directory: {output_dir}")
    print(f"Variables:        {', '.join(var_list)}")
    print()
    
    # Check input directory exists
    if not os.path.isdir(input_dir):
        print(f"ERROR: Input directory does not exist: {input_dir}")
        return False
    
    # Create output directory if it doesn't exist
    os.makedirs(output_dir, exist_ok=True)
    
    # Find all NC files in input directory
    nc_files = sorted(glob.glob(os.path.join(input_dir, "*.nc")))
    
    if not nc_files:
        print(f"ERROR: No NC files found in {input_dir}")
        return False
    
    print(f"Found {len(nc_files)} NC files")
    print()
    
    # Open and process files that contain the variable
    datasets_with_var = []
    files_with_var = []
    files_skipped = []
    
    for nc_file in nc_files:
        filename = os.path.basename(nc_file)
        try:
            ds = xr.open_dataset(nc_file)
            
            # Check which requested variables exist in this file
            found_vars = [v for v in var_list if v in ds.data_vars]
            
            if found_vars:
                datasets_with_var.append(ds)
                files_with_var.append(filename)
                print(f"✓ {filename} - Contains: {', '.join(found_vars)}")
            else:
                files_skipped.append(filename)
                ds.close()
                print(f"✗ {filename} - No requested variables found, skipping")
                
        except Exception as e:
            files_skipped.append(filename)
            print(f"✗ {filename} - Error reading file: {e}")
    
    print()
    print(f"Found variable in {len(files_with_var)} files")
    print(f"Skipped {len(files_skipped)} files")
    
    if not datasets_with_var:
        print("\nERROR: No files contain the variable!")
        return False
    
    print()
    print("Merging datasets...")
    
    # Extract requested variables that exist in each dataset
    data_vars = []
    for ds in datasets_with_var:
        # Keep only the variables that exist in this dataset
        existing_vars = [v for v in var_list if v in ds.data_vars]
        if existing_vars:
            var_data = ds[existing_vars]
            data_vars.append(var_data)
    
    # Merge along the first available dimension
    try:
        # Try to merge along time dimension (common for climate/weather data)
        if "time" in data_vars[0].dims:
            merged_ds = xr.concat(data_vars, dim="time")
            print(f"✓ Merged along 'time' dimension")
        else:
            # Get first dimension
            first_dim = list(data_vars[0].dims)[0]
            merged_ds = xr.concat(data_vars, dim=first_dim)
            print(f"✓ Merged along '{first_dim}' dimension")
    except Exception as e:
        print(f"ERROR: Failed to merge datasets: {e}")
        return False
    
    # Close all opened datasets
    for ds in datasets_with_var:
        ds.close()
    
    # Create output filename
    var_suffix = '_'.join(var_list) if len(var_list) <= 3 else f"{len(var_list)}vars"
    output_file = os.path.join(output_dir, f"merged_{var_suffix}_{date}.nc")
    
    print()
    print(f"Saving merged file: {output_file}")
    
    try:
        # Apply zlib compression (level 9) to all data variables in the output NetCDF.
        encoding = {
            var_name: {"zlib": True, "complevel": 9}
            for var_name in merged_ds.data_vars
        }

        merged_ds.to_netcdf(
            output_file,
            encoding=encoding,
            unlimited_dims=['time'] if 'time' in merged_ds.dims else None
        )
        merged_ds.close()
        print("✓ File saved successfully with zlib compression (level 9)")
    except Exception as e:
        print(f"ERROR: Failed to save file: {e}")
        return False

    # # Also create a gzip-compressed copy (.nc.gz) while keeping the .nc file.
    # gz_output_file = f"{output_file}.gz"
    # try:
    #     with open(output_file, "rb") as f_in, gzip.open(gz_output_file, "wb", compresslevel=9) as f_out:
    #         shutil.copyfileobj(f_in, f_out)
    #     print(f"✓ Gzip file created: {gz_output_file}")
    # except Exception as e:
    #     print(f"ERROR: Failed to create gzip file: {e}")
    #     return False
    
    # Print summary
    print()
    print("=" * 60)
    print("Summary")
    print("=" * 60)
    print(f"Output file: {output_file}")
    print(f"File size: {os.path.getsize(output_file) / (1024**2):.2f} MB")
    #print(f"Gzip file: {gz_output_file}")
    #print(f"Gzip size: {os.path.getsize(gz_output_file) / (1024**2):.2f} MB")
    
    # Show variable info
    merged_info = xr.open_dataset(output_file)
    for var in var_list:
        if var in merged_info.data_vars:
            print(f"{var} shape: {merged_info[var].shape}")
            print(f"{var} dtype: {merged_info[var].dtype}")
    merged_info.close()
    print()
    
    return True


if __name__ == "__main__":
    # Check for date argument
    if len(sys.argv) < 2:
        print("Usage: python 3_merge_nc_files.py <DATE> [INPUT_DIR] [OUTPUT_DIR] [VARIABLE]")
        print("Example: python 3_merge_nc_files.py 20250401_00")
        sys.exit(1)
    
    # Get date from command line
    DATE = sys.argv[1]
    
    # Default paths based on date
    INPUT_DIR = f"/sat_data/icon_teamx/grib_tmp/{DATE}/nc_tmp"
    OUTPUT_DIR = f"/sat_data/icon_teamx/nc_tmp/{DATE}"
    VARIABLE = ["SYNMSG_BT_CL_IR10.8", "CLCT"]  # List of variable names to extract (can be modified as needed)

    os.makedirs(OUTPUT_DIR, exist_ok=True)
    
    # Allow additional command-line arguments
    if len(sys.argv) > 2:
        INPUT_DIR = sys.argv[2]
    if len(sys.argv) > 3:
        OUTPUT_DIR = sys.argv[3]
    if len(sys.argv) > 4:
        VARIABLE = sys.argv[4]
    
    print(f"Processing date: {DATE}")
    success = merge_nc_files(INPUT_DIR, OUTPUT_DIR, VARIABLE, DATE)
    sys.exit(0 if success else 1)
