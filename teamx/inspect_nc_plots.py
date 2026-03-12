#!/usr/bin/env python3
"""
Inspect merged NC file and create visualizations.

Validates merged file contents and produces plots for variable inspection.
"""

import os
import sys
import tempfile
import shutil
from pathlib import Path

import xarray as xr
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.colors as mcolors
from PIL import Image

import cartopy.crs as ccrs
import cartopy.feature as cfeature
HAS_CARTOPY = True



def inspect_and_plot_nc(merged_file, output_dir=None, variable_name="SYNMSG_BT_CL_IR10.8"):
    """
    Inspect NC file and create plots.
    
    Parameters:
    -----------
    merged_file : str
        Path to merged NC file
    output_dir : str
        Directory to save plots (default: same directory as merged_file)
    variable_name : str
        Variable name to inspect
    """
    
    print("=" * 70)
    print("NC File Inspector & Plotter")
    print("=" * 70)
    print(f"File: {merged_file}")
    print(f"Variable: {variable_name}")
    print()
    
    # Check file exists
    if not os.path.isfile(merged_file):
        print(f"ERROR: File not found: {merged_file}")
        return False
    
    # Set output directory
    if output_dir is None:
        output_dir = os.path.dirname(merged_file) or "."
    
    os.makedirs(output_dir, exist_ok=True)
    
    print(f"Plots output directory: {output_dir}")
    print()
    
    # Load dataset
    print("Step 1: Loading dataset...")
    try:
        ds = xr.open_dataset(merged_file)
        print("✓ Dataset loaded successfully")
    except Exception as e:
        print(f"ERROR: Failed to load file: {e}")
        return False
    
    # Check variable exists
    print()
    print("Step 2: Checking variable...")
    if variable_name not in ds.data_vars:
        print(f"ERROR: Variable '{variable_name}' not found in dataset")
        print(f"Available variables: {list(ds.data_vars.keys())}")
        ds.close()
        return False
    print(f"✓ Variable found: {variable_name}")
    
    # Get variable info
    var = ds[variable_name]
    print()
    print("Step 3: Variable Information")
    print("-" * 70)
    print(f"Shape: {var.shape}")
    print(f"Dimensions: {var.dims}")
    print(f"Data type: {var.dtype}")
    print(f"Attributes: {var.attrs}")
    print()
    
    # Statistics
    print("Step 4: Variable Statistics")
    print("-" * 70)
    stats = {
        "Min": float(var.min().values),
        "Max": float(var.max().values),
        "Mean": float(var.mean().values),
        "Median": float(var.median().values),
        "Std Dev": float(var.std().values),
    }
    for stat_name, stat_val in stats.items():
        print(f"{stat_name:12s}: {stat_val:12.4f}")
    print()
    
    # Count valid/invalid data
    valid_count = int((~np.isnan(var.values)).sum())
    total_count = int(np.prod(var.shape))
    invalid_count = total_count - valid_count
    print(f"Valid data points: {valid_count} / {total_count}")
    print(f"Invalid/NaN points: {invalid_count}")
    print()
    
    # Create plots
    print("Step 5: Creating plots...")
    print("-" * 70)
    
    plot_files = []
    
    try:
        # Plot 1: Histogram of values
        print("  Creating histogram...")
        fig, ax = plt.subplots(figsize=(10, 6))
        
        # Flatten and remove NaN values
        data_flat = var.values.flatten()
        data_flat = data_flat[~np.isnan(data_flat)]
        
        ax.hist(data_flat, bins=50, edgecolor='black', alpha=0.7)
        ax.set_xlabel(variable_name, fontsize=12)
        ax.set_ylabel('Frequency', fontsize=12)
        ax.set_title(f'Histogram: {variable_name}', fontsize=14, fontweight='bold')
        ax.grid(True, alpha=0.3)
        
        hist_file = os.path.join(output_dir, f"{variable_name}_histogram.png")
        plt.savefig(hist_file, dpi=150, bbox_inches='tight')
        plt.close()
        print(f"    ✓ Saved: {os.path.basename(hist_file)}")
        plot_files.append(hist_file)
        
    except Exception as e:
        print(f"    ✗ Failed to create histogram: {e}")
    
    try:
        # Plot 2: Spatial map (static) or animated GIF (all time steps)
        print("  Creating spatial map / animation...")

        # Try to find lat/lon coordinates
        lat_name = None
        lon_name = None
        for name in ['lat', 'latitude', 'Latitude', 'LAT', 'nav_lat']:
            if name in ds.coords:
                lat_name = name
                break
        for name in ['lon', 'longitude', 'Longitude', 'LON', 'nav_lon']:
            if name in ds.coords:
                lon_name = name
                break

        # Create custom colormap to handle NaN
        cmap = plt.cm.viridis.copy()
        cmap.set_bad(color='white')

        # Common color scale for all frames
        global_vmin = float(np.nanmin(var.values))
        global_vmax = float(np.nanmax(var.values))
        levels = np.linspace(global_vmin, global_vmax, 50)

        # Build GIF if time is available
        if 'time' in var.dims and var.shape[0] > 1:
            frame_files = []
            temp_dir = tempfile.mkdtemp(prefix="nc_plot_frames_")

            try:
                n_frames = int(var.sizes['time'])
                for t in range(n_frames):
                    data_slice = var.isel(time=t)
                    time_val = str(ds['time'].values[t]) if 'time' in ds.coords else f"{t}"
                    frame_path = os.path.join(temp_dir, f"frame_{t:04d}.png")

                    if HAS_CARTOPY and lat_name and lon_name:
                        lat = ds[lat_name].values
                        lon = ds[lon_name].values
                        fig = plt.figure(figsize=(14, 10))
                        ax = fig.add_subplot(1, 1, 1, projection=ccrs.PlateCarree())
                        im = ax.contourf(
                            lon,
                            lat,
                            data_slice.values,
                            transform=ccrs.PlateCarree(),
                            levels=levels,
                            cmap=cmap,
                        )
                        ax.coastlines(resolution='50m', linewidth=0.5)
                        ax.add_feature(cfeature.BORDERS, linewidth=0.5)
                        ax.add_feature(cfeature.LAND, alpha=0.3)
                        ax.add_feature(cfeature.OCEAN, alpha=0.3)
                        ax.gridlines(draw_labels=True, linewidth=0.5, alpha=0.5)
                        plt.colorbar(im, ax=ax, label=variable_name, shrink=0.8)
                        ax.set_title(
                            f"Spatial Map (Cartopy): {variable_name} | t={t} | {time_val}",
                            fontsize=13,
                            fontweight='bold',
                        )
                    else:
                        fig, ax = plt.subplots(figsize=(12, 8))
                        im = ax.imshow(
                            data_slice.values,
                            cmap=cmap,
                            aspect='auto',
                            interpolation='nearest',
                            origin='lower',
                            vmin=global_vmin,
                            vmax=global_vmax,
                        )
                        plt.colorbar(im, ax=ax, label=variable_name)
                        ax.set_title(
                            f"Spatial Map: {variable_name} | t={t} | {time_val}",
                            fontsize=13,
                            fontweight='bold',
                        )
                        ax.set_xlabel('Longitude / X dimension', fontsize=12)
                        ax.set_ylabel('Latitude / Y dimension', fontsize=12)

                    plt.savefig(frame_path, dpi=120, bbox_inches='tight')
                    plt.close()
                    frame_files.append(frame_path)

                gif_file = os.path.join(output_dir, f"{variable_name}_spatial_map.gif")
                frames = [Image.open(fp) for fp in frame_files]
                frames[0].save(
                    gif_file,
                    save_all=True,
                    append_images=frames[1:],
                    duration=400,
                    loop=0,
                )
                for fr in frames:
                    fr.close()

                print(f"    ✓ Saved: {os.path.basename(gif_file)} ({n_frames} frames)")
                plot_files.append(gif_file)

            finally:
                shutil.rmtree(temp_dir, ignore_errors=True)

        else:
            # If there is no time dimension, keep a static map.
            if 'time' in var.dims:
                data_slice = var.isel(time=0)
                title_suffix = " (Time 0)"
            else:
                if len(var.shape) >= 2:
                    data_slice = var.isel({var.dims[0]: 0}) if len(var.shape) > 2 else var
                    title_suffix = " (First Slice)"
                else:
                    data_slice = var
                    title_suffix = ""

            if HAS_CARTOPY and lat_name and lon_name:
                lat = ds[lat_name].values
                lon = ds[lon_name].values
                fig = plt.figure(figsize=(14, 10))
                ax = fig.add_subplot(1, 1, 1, projection=ccrs.PlateCarree())
                im = ax.contourf(
                    lon,
                    lat,
                    data_slice.values,
                    transform=ccrs.PlateCarree(),
                    levels=levels,
                    cmap=cmap,
                )
                ax.coastlines(resolution='50m', linewidth=0.5)
                ax.add_feature(cfeature.BORDERS, linewidth=0.5)
                ax.add_feature(cfeature.LAND, alpha=0.3)
                ax.add_feature(cfeature.OCEAN, alpha=0.3)
                ax.gridlines(draw_labels=True, linewidth=0.5, alpha=0.5)
                plt.colorbar(im, ax=ax, label=variable_name, shrink=0.8)
                ax.set_title(f'Spatial Map (Cartopy): {variable_name}{title_suffix}', fontsize=14, fontweight='bold')
                map_file = os.path.join(output_dir, f"{variable_name}_spatial_map_cartopy.png")
            else:
                fig, ax = plt.subplots(figsize=(12, 8))
                im = ax.imshow(
                    data_slice.values,
                    cmap=cmap,
                    aspect='auto',
                    interpolation='nearest',
                    origin='lower',
                    vmin=global_vmin,
                    vmax=global_vmax,
                )
                plt.colorbar(im, ax=ax, label=variable_name)
                ax.set_title(f'Spatial Map: {variable_name}{title_suffix}', fontsize=14, fontweight='bold')
                ax.set_xlabel('Longitude / X dimension', fontsize=12)
                ax.set_ylabel('Latitude / Y dimension', fontsize=12)
                map_file = os.path.join(output_dir, f"{variable_name}_spatial_map.png")

            plt.savefig(map_file, dpi=150, bbox_inches='tight')
            plt.close()
            print(f"    ✓ Saved: {os.path.basename(map_file)}")
            plot_files.append(map_file)

    except Exception as e:
        print(f"    ✗ Failed to create spatial map / GIF: {e}")
    
    try:
        # Plot 3: Time series (if time dimension exists)
        if 'time' in var.dims and var.shape[0] > 1:
            print("  Creating time series...")
            
            # Compute mean along spatial dimensions
            time_series = var.mean(dim=[d for d in var.dims if d != 'time']).values
            
            fig, ax = plt.subplots(figsize=(12, 6))
            ax.plot(time_series, marker='o', linestyle='-', linewidth=2, markersize=4)
            ax.set_xlabel('Time Index', fontsize=12)
            ax.set_ylabel(f'Mean {variable_name}', fontsize=12)
            ax.set_title(f'Time Series: {variable_name}', fontsize=14, fontweight='bold')
            ax.grid(True, alpha=0.3)
            
            ts_file = os.path.join(output_dir, f"{variable_name}_timeseries.png")
            plt.savefig(ts_file, dpi=150, bbox_inches='tight')
            plt.close()
            print(f"    ✓ Saved: {os.path.basename(ts_file)}")
            plot_files.append(ts_file)
        
    except Exception as e:
        print(f"    ✗ Failed to create time series: {e}")
    
    try:
        # Plot 4: Statistics summary
        print("  Creating statistics summary...")
        
        fig, axes = plt.subplots(2, 2, figsize=(12, 10))
        fig.suptitle(f'{variable_name} - Statistics Summary', fontsize=14, fontweight='bold')
        
        # Box plot
        ax = axes[0, 0]
        ax.boxplot(data_flat, vert=True)
        ax.set_ylabel(variable_name)
        ax.set_title('Box Plot')
        ax.grid(True, alpha=0.3)
        
        # Statistics text
        ax = axes[0, 1]
        ax.axis('off')
        stats_text = f"""
Variable: {variable_name}

Shape: {var.shape}
Dims: {var.dims}

Statistics:
  Min:    {stats['Min']:.4f}
  Max:    {stats['Max']:.4f}
  Mean:   {stats['Mean']:.4f}
  Median: {stats['Median']:.4f}
  Std:    {stats['Std Dev']:.4f}

Data Quality:
  Valid:   {valid_count}
  Invalid: {invalid_count}
  Total:   {total_count}
        """
        ax.text(0.1, 0.5, stats_text, fontsize=11, family='monospace',
                verticalalignment='center', bbox=dict(boxstyle='round', facecolor='wheat', alpha=0.5))
        
        # Cumulative distribution
        ax = axes[1, 0]
        sorted_data = np.sort(data_flat)
        ax.plot(sorted_data, np.linspace(0, 1, len(sorted_data)), linewidth=2)
        ax.set_xlabel(variable_name)
        ax.set_ylabel('Cumulative Probability')
        ax.set_title('Cumulative Distribution')
        ax.grid(True, alpha=0.3)
        
        # Data availability
        ax = axes[1, 1]
        labels = ['Valid', 'Invalid']
        sizes = [valid_count, invalid_count]
        colors = ['#66c2a5', '#fc8d62']
        ax.pie(sizes, labels=labels, colors=colors, autopct='%1.1f%%', startangle=90)
        ax.set_title('Data Availability')
        
        plt.tight_layout()
        stats_file = os.path.join(output_dir, f"{variable_name}_statistics.png")
        plt.savefig(stats_file, dpi=150, bbox_inches='tight')
        plt.close()
        print(f"    ✓ Saved: {os.path.basename(stats_file)}")
        plot_files.append(stats_file)
        
    except Exception as e:
        print(f"    ✗ Failed to create statistics plot: {e}")
    
    # Summary
    ds.close()
    
    print()
    print("=" * 70)
    print("Inspection Complete")
    print("=" * 70)
    print(f"Plots saved: {len(plot_files)}")
    for pf in plot_files:
        print(f"  - {os.path.basename(pf)}")
    print()
    
    return True


if __name__ == "__main__":
    # Default paths
    DATE = "20250401_00"
    VARIABLE = "SYNMSG_BT_CL_IR10.8"
    MERGED_FILE = f"/sat_data/icon_teamx/nc_tmp/{DATE}/merged_{VARIABLE}_{DATE}.nc"
    OUTPUT_DIR = "/home/Daniele/codes/ICON-GLORI/teamx/plots"
    
    
    # Allow command-line arguments
    if len(sys.argv) > 1:
        MERGED_FILE = sys.argv[1]
    if len(sys.argv) > 2:
        OUTPUT_DIR = sys.argv[2]
    if len(sys.argv) > 3:
        VARIABLE = sys.argv[3]
    
    success = inspect_and_plot_nc(MERGED_FILE, OUTPUT_DIR, VARIABLE)
    sys.exit(0 if success else 1)
