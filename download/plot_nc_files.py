#!/usr/bin/env python3
"""
plot_nc_files.py

Explore and plot NetCDF files (raw or merged).

Modes:
  raw (default):
    - Scans: /sat_data/icon/icon_<type>/nc_tmp/<init>/<var>/<date>/*.nc
    - Picks random samples per var/init
    - Output: /sat_data/icon/icon_<type>/img/<init>/<var>/<date>/

  merged:
    - Scans: /sat_data/icon/icon_<type>/merged_nc/merged_*.nc
    - Can filter by date/init
    - Output: /sat_data/icon/icon_<type>/plots_merged/

Usage:
    python plot_nc_files.py
    python plot_nc_files.py --mode raw --date 2026-03-31 --samples 3
    python plot_nc_files.py --mode merged --date 2026-03-31
    python plot_nc_files.py --mode merged --init 00
"""

import glob
import math
import os
import random
from typing import Optional

import matplotlib.pyplot as plt
import numpy as np
import xarray as xr

# ============================================================================
# CONFIGURATION
# ============================================================================
CONFIG = {
    # Processing mode: "raw" or "merged"
    "mode": "merged",
    
    # Date to process (format: YYYY-MM-DD)
    "date": "2026-03-31",
    
    # Base path containing icon_d2 and icon_eu directories
    "sat_base": "/sat_data/icon",
    
    # For raw mode: number of random files to sample per var/init
    "samples": 3,
    
    # Random seed for reproducibility (raw mode)
    "seed": 42,
    
    # Filter by init time (None = all, or comma-separated like "00" or "00,12")
    "init": None,
    
    # Maximum number of time panels to show in multiplots
    "max_times": 12,
}
# ============================================================================


def find_time_dim(da: xr.DataArray) -> Optional[str]:
    for dim in da.dims:
        if dim.lower() == "time":
            return dim
    for dim in da.dims:
        if dim in da.coords and np.issubdtype(da[dim].dtype, np.datetime64):
            return dim
    return None


def pick_2d_slice(da: xr.DataArray) -> xr.DataArray:
    """Reduce a DataArray to 2D by selecting first index on extra dims."""
    while da.ndim > 2:
        da = da.isel({da.dims[0]: 0})
    return da


def write_structure_report(ds: xr.Dataset, out_txt: str) -> None:
    lines = []
    lines.append("Dataset summary")
    lines.append("=" * 60)
    lines.append(f"Dimensions: {dict(ds.sizes)}")
    lines.append("")
    lines.append("Coordinates:")
    for c in ds.coords:
        lines.append(f"- {c}: dims={ds[c].dims}, shape={tuple(ds[c].shape)}, dtype={ds[c].dtype}")
    lines.append("")
    lines.append("Data variables:")
    for v in ds.data_vars:
        da = ds[v]
        lines.append(f"- {v}: dims={da.dims}, shape={tuple(da.shape)}, dtype={da.dtype}")
        if "units" in da.attrs:
            lines.append(f"  units={da.attrs['units']}")
    lines.append("")

    with open(out_txt, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))


def plot_variable(da: xr.DataArray, out_png: str, max_times: int) -> None:
    time_dim = find_time_dim(da)

    if time_dim is None:
        fig, ax = plt.subplots(figsize=(7, 5), constrained_layout=True)
        da2d = pick_2d_slice(da)
        im = ax.imshow(da2d.values, origin="lower", cmap="viridis")
        ax.set_title(f"{da.name} (no time dimension)")
        ax.set_xlabel(da2d.dims[-1])
        ax.set_ylabel(da2d.dims[-2])
        plt.colorbar(im, ax=ax, fraction=0.046, pad=0.04)
        fig.savefig(out_png, dpi=140)
        plt.close(fig)
        return

    n_times = int(da.sizes[time_dim])
    n_show = min(n_times, max_times)
    ncols = min(4, n_show)
    nrows = int(math.ceil(n_show / ncols))

    fig, axes = plt.subplots(nrows, ncols, figsize=(4 * ncols, 3.5 * nrows), constrained_layout=True)
    axes = np.atleast_1d(axes).ravel()

    vmin = float(da.min(skipna=True).values)
    vmax = float(da.max(skipna=True).values)
    if not np.isfinite(vmin) or not np.isfinite(vmax) or vmin == vmax:
        vmin, vmax = None, None

    for i in range(n_show):
        ax = axes[i]
        da_t = da.isel({time_dim: i})
        da2d = pick_2d_slice(da_t)
        im = ax.imshow(da2d.values, origin="lower", cmap="viridis", vmin=vmin, vmax=vmax)
        t_val = da[time_dim].isel({time_dim: i}).values
        ax.set_title(f"{da.name} | t={t_val}")
        ax.set_xlabel(da2d.dims[-1])
        ax.set_ylabel(da2d.dims[-2])
        plt.colorbar(im, ax=ax, fraction=0.046, pad=0.04)

    for j in range(n_show, len(axes)):
        axes[j].axis("off")

    if n_times > n_show:
        fig.suptitle(f"{da.name}: showing first {n_show}/{n_times} time steps")

    fig.savefig(out_png, dpi=140)
    plt.close(fig)


def process_file(nc_file: str, out_dir: str, max_times: int) -> int:
    os.makedirs(out_dir, exist_ok=True)
    file_tag = os.path.splitext(os.path.basename(nc_file))[0]
    produced = 0

    with xr.open_dataset(nc_file) as ds:
        struct_path = os.path.join(out_dir, f"{file_tag}__structure.txt")
        write_structure_report(ds, struct_path)
        produced += 1

        for var_name in sorted(ds.data_vars):
            da = ds[var_name]
            plot_path = os.path.join(out_dir, f"{file_tag}__{var_name}.png")
            try:
                plot_variable(da, plot_path, max_times=max_times)
                produced += 1
            except Exception as e:
                print(f"      Warning: failed plotting {var_name} in {file_tag}: {e}")

    return produced


def main_raw_mode(config: dict) -> int:
    """Process raw nc_tmp files with random sampling."""
    rng = random.Random(config["seed"])
    icon_types = ["icon_d2", "icon_eu"]
    init_times = ["00", "12"]
    total_files = 0
    total_outputs = 0

    print(f"Mode: raw | date={config['date']} samples={config['samples']} seed={config['seed']}")

    for icon_type in icon_types:
        base_nc = os.path.join(config["sat_base"], icon_type, "nc_tmp")
        base_img = os.path.join(config["sat_base"], icon_type, "img")

        if not os.path.isdir(base_nc):
            print(f"Warning: missing directory {base_nc}")
            continue

        for init_time in init_times:
            init_dir = os.path.join(base_nc, init_time)
            if not os.path.isdir(init_dir):
                print(f"Warning: missing init folder {init_dir}")
                continue

            var_dirs = [d for d in sorted(glob.glob(os.path.join(init_dir, "*"))) if os.path.isdir(d)]
            if not var_dirs:
                print(f"Warning: no variable folders in {init_dir}")
                continue

            for var_dir in var_dirs:
                var_name = os.path.basename(var_dir)
                pattern = os.path.join(var_dir, config["date"], "*.nc")
                files = sorted(glob.glob(pattern))

                if not files:
                    continue

                n_pick = min(config["samples"], len(files))
                picked = rng.sample(files, n_pick)
                out_dir = os.path.join(base_img, init_time, var_name, config["date"])

                print(
                    f"{icon_type} init={init_time} var={var_name}: "
                    f"picked {n_pick}/{len(files)} files"
                )

                for nc_file in picked:
                    try:
                        produced = process_file(nc_file, out_dir, max_times=config["max_times"])
                        total_outputs += produced
                        total_files += 1
                    except Exception as e:
                        print(f"    Error processing {nc_file}: {e}")

    print(f"Sampled files processed: {total_files}")
    print(f"Artifacts saved:         {total_outputs}")
    return 0


def main_merged_mode(config: dict) -> int:
    """Process merged_nc files."""
    icon_types = ["icon_d2", "icon_eu"]
    init_filters = None
    if config["init"]:
        init_filters = set(config["init"].split(","))

    total_files = 0
    total_outputs = 0

    print(f"Mode: merged | date={config['date']} init_filter={init_filters}")

    for icon_type in icon_types:
        merged_dir = os.path.join(config["sat_base"], icon_type, "merged_nc")
        base_plots = os.path.join(config["sat_base"], icon_type, "plots_merged")

        if not os.path.isdir(merged_dir):
            print(f"Warning: missing directory {merged_dir}")
            continue

        pattern = os.path.join(merged_dir, "*.nc")
        files = sorted(glob.glob(pattern))

        if not files:
            print(f"Warning: no merged files found in {merged_dir}")
            continue

        for nc_file in files:
            basename = os.path.basename(nc_file)

            if config["date"] and config["date"] not in basename:
                continue

            if init_filters:
                if not any(f"init{init_str}" in basename for init_str in init_filters):
                    continue

            print(f"{icon_type}: processing {basename}")
            out_dir = base_plots

            try:
                produced = process_file(nc_file, out_dir, max_times=config["max_times"])
                total_outputs += produced
                total_files += 1
            except Exception as e:
                print(f"  Error processing {basename}: {e}")

    print(f"Merged files processed: {total_files}")
    print(f"Artifacts saved:        {total_outputs}")
    return 0


def main() -> int:
    print("=" * 70)
    print("NC Explorer (structure + plots)")
    print("=" * 70)

    if CONFIG["mode"] == "raw":
        return main_raw_mode(CONFIG)
    elif CONFIG["mode"] == "merged":
        return main_merged_mode(CONFIG)
    else:
        print(f"ERROR: Unknown mode {CONFIG['mode']}")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
