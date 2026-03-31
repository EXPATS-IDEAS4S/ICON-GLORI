#!/usr/bin/env python3
"""
3_merge_nc_files.py

Merge NetCDF files per icon type and initialization time.

- Groups NC files by icon type (icon_d2, icon_eu) and init time (00, 12)
- Per group, concatenates along the forecast dimension (or time if available)
- Saves merged files with compression to final output directory

Usage:
    python 3_merge_nc_files.py [output_dir]
"""

import os
import glob
import sys
from pathlib import Path
from collections import defaultdict

import xarray as xr
import numpy as np


def merge_nc_files_by_init(output_base="merged_nc", nc_tmp_base="nc_tmp"):
    """
    Merge NC files grouped by icon type and init time.
    
    Parameters:
    -----------
    output_base : str
        Base output directory for merged files
    nc_tmp_base : str
        Base input directory containing converted NC files
    """
    
    print("=" * 70)
    print("NetCDF File Merger (Icon Type + Init Time Grouping)")
    print("=" * 70)
    print(f"Input base:  {nc_tmp_base}")
    print(f"Output base: {output_base}")
    print()
    
    os.makedirs(output_base, exist_ok=True)
    
    # Dictionary to track files by (icon_type, init_time)
    groups = defaultdict(list)
    
    # Icon types and init times to process
    ICON_TYPES = ["icon_d2", "icon_eu"]
    INIT_TIMES = ["00", "12"]
    
    # Scan for NC files and group them
    for icon_type in ICON_TYPES:
        input_dir = os.path.join(nc_tmp_base, icon_type)
        
        if not os.path.isdir(input_dir):
            print(f"Warning: Input directory not found: {input_dir}")
            continue
        
        # Find all NC files for this icon type
        nc_files = sorted(glob.glob(os.path.join(input_dir, "**/*.nc"), recursive=True))
        
        if not nc_files:
            print(f"No NC files found for {icon_type}")
            continue
        
        print(f"Found {len(nc_files)} NC files for {icon_type}")
        
        # Group files by init time extracted from path or filename
        for nc_file in nc_files:
            # Try to extract init time from path: e.g., nc_tmp/icon_d2/00/var/date/file.nc
            parts = nc_file.split(os.sep)
            init_time = None
            
            # Look for step (00, 12, etc.) in path
            for part in parts:
                if part in INIT_TIMES:
                    init_time = part
                    break
            
            if init_time is None:
                # Fallback: try to extract from filename timestamp (e.g., 2026033100 → init=00)
                try:
                    for part in parts:
                        if len(part) >= 10 and part[-10:-8].isdigit():
                            potential_init = part[-2:]
                            if potential_init in INIT_TIMES:
                                init_time = potential_init
                                break
                except:
                    pass
            
            if init_time is None:
                print(f"  Warning: Could not determine init time for {os.path.basename(nc_file)}, skipping")
                continue
            
            key = (icon_type, init_time)
            groups[key].append(nc_file)
    
    # Process each group
    total_merged = 0
    failed_groups = 0
    
    for (icon_type, init_time), nc_files in sorted(groups.items()):
        print()
        print(f"Merging {icon_type} init={init_time} ({len(nc_files)} files)")
        
        # Load all datasets
        datasets = []
        for nc_file in nc_files:
            try:
                ds = xr.open_dataset(nc_file)
                datasets.append(ds)
            except Exception as e:
                print(f"  Error reading {os.path.basename(nc_file)}: {e}")
                continue
        
        if not datasets:
            print(f"  ERROR: No valid datasets for {icon_type} init={init_time}")
            failed_groups += 1
            continue
        
        # Merge datasets
        try:
            # Detect merge dimension (usually 'time' but could be indexed)
            merge_dim = None
            if "time" in datasets[0].dims:
                merge_dim = "time"
            else:
                # Use first dimension
                merge_dim = list(datasets[0].dims)[0] if datasets[0].dims else None
            
            if merge_dim:
                merged_ds = xr.concat(datasets, dim=merge_dim)
                print(f"  ✓ Merged along '{merge_dim}' dimension")
            else:
                # If no common dimension, just use the first dataset
                merged_ds = datasets[0]
                print(f"  ⓘ No merge dimension found, using first dataset only")
            
            # Close all datasets
            for ds in datasets:
                ds.close()
            
            # Define output file
            output_dir = os.path.join(output_base, icon_type)
            os.makedirs(output_dir, exist_ok=True)
            output_file = os.path.join(output_dir, f"merged_{icon_type}_init{init_time}.nc")
            
            # Save with compression
            encoding = {
                var_name: {"zlib": True, "complevel": 9}
                for var_name in merged_ds.data_vars
            }
            
            merged_ds.to_netcdf(
                output_file,
                encoding=encoding,
                unlimited_dims=["time"] if "time" in merged_ds.dims else None
            )
            merged_ds.close()
            
            file_size_mb = os.path.getsize(output_file) / (1024**2)
            print(f"  ✓ Saved: {os.path.basename(output_file)} ({file_size_mb:.2f} MB)")
            total_merged += 1
            
        except Exception as e:
            print(f"  ERROR: Failed to merge {icon_type} init={init_time}: {e}")
            failed_groups += 1
            continue
    
    # Summary
    print()
    print("=" * 70)
    print(f"Merged groups: {total_merged}")
    print(f"Failed groups: {failed_groups}")
    print("=" * 70)
    
    return failed_groups == 0


if __name__ == "__main__":
    output_dir = sys.argv[1] if len(sys.argv) > 1 else "merged_nc"
    nc_tmp_dir = sys.argv[2] if len(sys.argv) > 2 else "nc_tmp"
    
    success = merge_nc_files_by_init(output_dir, nc_tmp_dir)
    sys.exit(0 if success else 1)
