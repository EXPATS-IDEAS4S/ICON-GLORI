#!/usr/bin/env python3
"""
3_merge_nc_files.py

Merge NetCDF files from /sat_data/icon layout.

- Scans: /sat_data/icon/icon_<type>/nc_tmp/<init>/<var>/<date>/*.nc
- Groups files by icon type and init time
- Concatenates files per variable, then merges all variables into one dataset
- Saves merged files with compression in: /sat_data/icon/icon_*/merged_nc/

Usage:
    python 3_merge_nc_files.py [date] [sat_base]

Examples:
    python 3_merge_nc_files.py
    python 3_merge_nc_files.py 2026-03-31
    python 3_merge_nc_files.py 2026-03-31 /sat_data/icon
"""

import os
import glob
import sys
from collections import defaultdict

import numpy as np
import xarray as xr


def deduplicate_time(ds):
    """Keep first occurrence of duplicate timestamps, preserving order."""
    if "time" not in ds.dims:
        return ds

    time_values = ds["time"].values
    _, unique_idx = np.unique(time_values, return_index=True)
    unique_idx = np.sort(unique_idx)
    if len(unique_idx) == ds.sizes["time"]:
        return ds
    return ds.isel(time=unique_idx)


def merge_nc_files_by_init(target_date="2026-03-31", sat_base="/sat_data/icon"):
    """
    Merge NC files grouped by icon type and init time.
    
    Parameters:
    -----------
    target_date : str
        Date folder to process (format: YYYY-MM-DD)
    sat_base : str
        Base sat_data path containing icon_* directories
    """
    
    print("=" * 70)
    print("NetCDF File Merger (/sat_data/icon layout)")
    print("=" * 70)
    print(f"Date:        {target_date}")
    print(f"Sat base:    {sat_base}")
    print()

    # Dictionary to track files by (icon_type, init_time) and variable name
    groups = defaultdict(lambda: defaultdict(list))

    icon_dirs = [
        os.path.join(sat_base, "icon_d2"),
        os.path.join(sat_base, "icon_eu"),
    ]

    for icon_dir in icon_dirs:
        icon_type = os.path.basename(icon_dir)
        nc_tmp_dir = os.path.join(icon_dir, "nc_tmp")

        if not os.path.isdir(nc_tmp_dir):
            print(f"Warning: Input directory not found: {nc_tmp_dir}")
            continue

        date_pattern = os.path.join(nc_tmp_dir, "*", "*", target_date, "*.nc")
        nc_files = sorted(glob.glob(date_pattern))

        if not nc_files:
            print(f"No NC files found for {icon_type} on date {target_date}")
            continue

        print(f"Found {len(nc_files)} NC files for {icon_type} on {target_date}")

        for nc_file in nc_files:
            rel_parts = os.path.relpath(nc_file, nc_tmp_dir).split(os.sep)
            if len(rel_parts) < 4:
                print(f"  Warning: Unexpected path structure, skipping: {nc_file}")
                continue

            init_time = rel_parts[0]
            var_name = rel_parts[1]

            key = (icon_type, init_time)
            groups[key][var_name].append(nc_file)

    # Process each group
    total_merged = 0
    failed_groups = 0

    for (icon_type, init_time), var_groups in sorted(groups.items()):
        print()
        print(
            f"Merging {icon_type} init={init_time} "
            f"({len(var_groups)} variables)"
        )

        all_opened = []
        per_var_datasets = []

        try:
            for var_name, nc_files in sorted(var_groups.items()):
                print(f"  Variable {var_name}: {len(nc_files)} files")

                datasets = []
                for nc_file in sorted(nc_files):
                    try:
                        ds = xr.open_dataset(nc_file)
                        datasets.append(ds)
                        all_opened.append(ds)
                    except Exception as e:
                        print(f"    Error reading {os.path.basename(nc_file)}: {e}")
                        continue

                if not datasets:
                    print(f"    Warning: no readable files for variable {var_name}, skipping")
                    continue

                merge_dim = "time" if "time" in datasets[0].dims else None
                if merge_dim is None:
                    merge_dim = list(datasets[0].dims)[0] if datasets[0].dims else None

                if merge_dim:
                    var_ds = xr.concat(datasets, dim=merge_dim)
                    print(f"    ✓ Concatenated on '{merge_dim}'")
                else:
                    var_ds = datasets[0]
                    print("    ⓘ No concat dimension found, using first file")

                if "time" in var_ds.dims:
                    before_count = int(var_ds.sizes["time"])
                    var_ds = deduplicate_time(var_ds)
                    after_count = int(var_ds.sizes["time"])
                    if after_count != before_count:
                        print(f"    ⓘ Removed duplicate time entries: {before_count} -> {after_count}")

                per_var_datasets.append(var_ds)

            if not per_var_datasets:
                print(f"  ERROR: No valid variable datasets for {icon_type} init={init_time}")
                failed_groups += 1
                continue

            # Keep full union of timestamps across variables.
            # Variables that do not have a given timestamp will contain NaN there.
            time_datasets = [ds for ds in per_var_datasets if "time" in ds.dims]
            if time_datasets:
                union_times = np.unique(np.concatenate([ds_t["time"].values for ds_t in time_datasets]))
                print(f"  Union time steps across variables: {len(union_times)}")

            merged_ds = xr.merge(per_var_datasets, join="outer", compat="override")
            print("  ✓ Merged all variables into a single dataset")

            # Define output file
            output_dir = os.path.join(sat_base, icon_type, "merged_nc")
            os.makedirs(output_dir, exist_ok=True)
            output_file = os.path.join(
                output_dir,
                f"merged_{icon_type}_init{init_time}_{target_date}.nc"
            )

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

            for ds in per_var_datasets:
                ds.close()
            for ds in all_opened:
                ds.close()

            file_size_mb = os.path.getsize(output_file) / (1024**2)
            print(f"  ✓ Saved: {os.path.basename(output_file)} ({file_size_mb:.2f} MB)")
            total_merged += 1

        except Exception as e:
            print(
                f"  ERROR: Failed to merge {icon_type} "
                f"init={init_time}: {e}"
            )

            for ds in per_var_datasets:
                try:
                    ds.close()
                except Exception:
                    pass
            for ds in all_opened:
                try:
                    ds.close()
                except Exception:
                    pass

            failed_groups += 1
            continue

    # Summary
    print()
    print("=" * 70)
    print(f"Merged groups: {total_merged}")
    print(f"Failed groups: {failed_groups}")
    print("=" * 70)
    
    if total_merged == 0:
        print("ERROR: No groups were merged successfully.")
        return False
    
    if failed_groups > 0:
        print(f"WARNING: {failed_groups} group(s) failed to merge. Continuing with {total_merged} successful merge(s).")
    
    return True


if __name__ == "__main__":
    date_arg = sys.argv[1] if len(sys.argv) > 1 else "2026-03-31"
    sat_base_arg = sys.argv[2] if len(sys.argv) > 2 else "/sat_data/icon"

    success = merge_nc_files_by_init(date_arg, sat_base_arg)
    sys.exit(0 if success else 1)
