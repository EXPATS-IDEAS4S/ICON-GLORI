#!/usr/bin/env python3
"""
Merge NC files and extract a specific variable.

Reads NC files from a directory, extracts SYNMSG_BT_CL_IR10.8 variable,
merges all files, and saves to output directory.
"""

import os
import glob
import sys

import xarray as xr
def _to_bool(value):
    """Parse common truthy/falsey string values into bool."""
    if isinstance(value, bool):
        return value
    if value is None:
        return False

    parsed = str(value).strip().lower()
    if parsed in {"1", "true", "t", "yes", "y", "on"}:
        return True
    if parsed in {"0", "false", "f", "no", "n", "off"}:
        return False
    raise ValueError(f"Cannot parse boolean value from: {value}")


def _merge_variable_subset(datasets_with_var, var_list, output_dir, date):
    """Merge a subset of variables across datasets and save one output file."""
    data_vars = []
    concat_dim = None

    # Determine concat dimension once from first matching dataset.
    for ds in datasets_with_var:
        existing_vars = [v for v in var_list if v in ds.data_vars]
        if not existing_vars:
            continue
        candidate = ds[existing_vars]
        if "time" in candidate.dims:
            concat_dim = "time"
        else:
            concat_dim = list(candidate.dims)[0]
        break

    if concat_dim is None:
        print(f"ERROR: Could not determine merge dimension for: {', '.join(var_list)}")
        return None

    for ds in datasets_with_var:
        existing_vars = [v for v in var_list if v in ds.data_vars]
        if existing_vars:
            subset = ds[existing_vars]

            # Duplicate coordinate values on non-concat dimensions (for example,
            # vertical "height") can break xarray alignment during concat.
            # Drop those indexes so concat can stack raw dimension order.
            index_dims_to_drop = [
                dim for dim in subset.dims
                if dim != concat_dim and dim in subset.indexes
            ]
            if index_dims_to_drop:
                subset = subset.drop_indexes(index_dims_to_drop)

            data_vars.append(subset)

    if not data_vars:
        print(f"ERROR: No datasets with requested variables: {', '.join(var_list)}")
        return None

    try:
        merged_ds = xr.concat(
            data_vars,
            dim=concat_dim,
            join="override",
            coords="minimal",
            compat="override",
        )
        print(f"✓ Merged along '{concat_dim}' dimension")
    except Exception as e:
        print(f"ERROR: Failed to merge datasets for {', '.join(var_list)}: {e}")
        return None

    var_suffix = '_'.join(var_list) if len(var_list) <= 3 else f"{len(var_list)}vars"
    output_file = os.path.join(output_dir, f"merged_{var_suffix}_{date}.nc")

    print(f"Saving merged file: {output_file}")
    try:
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
        return output_file
    except Exception as e:
        print(f"ERROR: Failed to save file: {e}")
        return None


def merge_nc_files(
    input_dir,
    output_dir,
    variable_name=["SYNMSG_BT_CL_IR10.8"],
    date="20250401_00",
    separate_files_per_variable=False,
):
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
    print(f"Separate outputs: {separate_files_per_variable}")
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

    output_files = []
    if separate_files_per_variable:
        for var in var_list:
            print()
            print(f"Merging variable: {var}")
            merged_output = _merge_variable_subset(datasets_with_var, [var], output_dir, date)
            if merged_output is None:
                print(f"ERROR: Failed merge for variable: {var}")
                for ds in datasets_with_var:
                    ds.close()
                return False
            output_files.append(merged_output)
    else:
        merged_output = _merge_variable_subset(datasets_with_var, var_list, output_dir, date)
        if merged_output is None:
            for ds in datasets_with_var:
                ds.close()
            return False
        output_files.append(merged_output)
    
    # Close all opened datasets
    for ds in datasets_with_var:
        ds.close()
    
    print()
    # Print summary
    print("=" * 60)
    print("Summary")
    print("=" * 60)
    for output_file in output_files:
        print(f"Output file: {output_file}")
        print(f"File size: {os.path.getsize(output_file) / (1024**2):.2f} MB")

        merged_info = xr.open_dataset(output_file)
        for var in merged_info.data_vars:
            print(f"{var} shape: {merged_info[var].shape}")
            print(f"{var} dtype: {merged_info[var].dtype}")
        merged_info.close()
    print()
    
    return True


if __name__ == "__main__":
    # Check for date argument
    if len(sys.argv) < 2:
        print("Usage: python 3_merge_nc_files.py <DATE> [INPUT_DIR] [OUTPUT_DIR] [VARIABLE] [SEPARATE_FILES_PER_VARIABLE]")
        print("Example: python 3_merge_nc_files.py 20250401_00")
        sys.exit(1)
    
    # Get date from command line
    DATE = sys.argv[1]
    
    # Default paths based on date
    INPUT_DIR = f"/sat_data/icon/icon_teamx/grib_tmp/{DATE}/nc_tmp"
    OUTPUT_DIR = f"/sat_data/icon/icon_teamx/nc_tmp/{DATE}"
    VARIABLE = ["rwmr", "snmr", "grle"]  # List of variable names to extract (can be modified as needed)
    CONFIG = {
        "SEPARATE_FILES_PER_VARIABLE": True,
    }

    os.makedirs(OUTPUT_DIR, exist_ok=True)
    
    # Allow additional command-line arguments
    if len(sys.argv) > 2:
        INPUT_DIR = sys.argv[2]
    if len(sys.argv) > 3:
        OUTPUT_DIR = sys.argv[3]
    if len(sys.argv) > 4:
        VARIABLE = sys.argv[4]
    if len(sys.argv) > 5:
        CONFIG["SEPARATE_FILES_PER_VARIABLE"] = _to_bool(sys.argv[5])
    
    print(f"Processing date: {DATE}")
    success = merge_nc_files(
        INPUT_DIR,
        OUTPUT_DIR,
        VARIABLE,
        DATE,
        separate_files_per_variable=CONFIG["SEPARATE_FILES_PER_VARIABLE"],
    )
    sys.exit(0 if success else 1)
