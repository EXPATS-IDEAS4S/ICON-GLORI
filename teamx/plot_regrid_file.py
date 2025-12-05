import xarray as xr
import matplotlib.pyplot as plt
import cartopy.crs as ccrs
import cartopy.feature as cfeature
import numpy as np
from glob import glob
import os

from plot_utils import regrid_coarsen_safe, create_fig, convert_crops_to_images

# === USER FLAGS ===
APPLY_CLOUD_MASK = True
CLOUD_THRESHOLD = 50.0   # % total cloud cover

APPLY_REGRID = True
TARGET_XSIZE, TARGET_YSIZE = 128, 128  # regrid resolution

APPLY_CROP = False #centered and squared
CROP_LONMIN, CROP_LONMAX = 9.25, 12.5 #7.0, 15.0
CROP_LATMIN, CROP_LATMAX = 45.25, 48.5

PLOT_MINIMAL = True
SAVE_REDUCED_NC = False

COLOR_MODE = 'greyscale'  # 'RGB' or 'greyscale'
FORMAT = 'tif'

# === PATHS ===
date = "20250630_00"
nc_file_folder = f"/data/trade_pc/ICON/icon_teamx/{date}/nc"

if APPLY_CROP:
    save_folder = f"/data/trade_pc/ICON/icon_teamx/{date}/img_ml_cropped"
    nc_processed_folder = f"/data/trade_pc/ICON/icon_teamx/{date}/nc_processed_cropped"
else:
    save_folder = f"/data/trade_pc/ICON/icon_teamx/{date}/img_ml"
    nc_processed_folder = f"/data/trade_pc/ICON/icon_teamx/{date}/nc_processed"


os.makedirs(save_folder, exist_ok=True)
os.makedirs(nc_processed_folder, exist_ok=True)

# Variables of interest
var_names = ["SYNMSG_BT_CL_IR10.8"]
units = ["K"]
vmin = [200]
vmax = [300]
cmap = "Greys"

# === MAIN LOOP ===
for nc_file in sorted(glob(f"{nc_file_folder}/*.nc")):
    # Select only necessary variables
    ds_full = xr.open_dataset(nc_file)
    datetime = ds_full.time.values
    print(f"Processing {nc_file} at time {datetime}")
    #get latmin, latmax, lonmin, lonmax values
    # latmin, latmax = ds_full["lat"].min().values, ds_full["lat"].max().values
    # lonmin, lonmax = ds_full["lon"].min().values, ds_full["lon"].max().values
    # print(f"Processing {nc_file} with lat [{latmin}, {latmax}] and lon [{lonmin}, {lonmax}]")
    # exit()
    needed_vars = var_names + (["CLCT"] if APPLY_CLOUD_MASK else [])

    existing_vars = [v for v in needed_vars if v in ds_full]

    if not existing_vars:
        print(f"No required variables found in {nc_file}, skipping.")
        ds_full.close()
        continue

    ds = ds_full[existing_vars]


    # --- Cloud mask ---
    if APPLY_CLOUD_MASK and "CLCT" in ds:
        for i, var_name in enumerate(var_names):
            if var_name not in ds:
                continue
            cloud_mask = ds["CLCT"].squeeze() >= CLOUD_THRESHOLD
            ds[var_name] = ds[var_name].where(cloud_mask, vmax[i])

    # --- Crop (optional) ---
    if APPLY_CROP:
        ds = ds.sel(lat=slice(CROP_LATMIN, CROP_LATMAX),
                    lon=slice(CROP_LONMIN, CROP_LONMAX))

    # --- Regrid (optional) ---
    if APPLY_REGRID:
        ds = regrid_coarsen_safe(
            ds,
            target_xsize=TARGET_XSIZE,
            target_ysize=TARGET_YSIZE,
            dim_lon="lon",
            dim_lat="lat",
            agg="mean",
            crop=True
        )
        # Safety check
        assert ds.dims["lat"] == TARGET_YSIZE, f"Regrid failed for lat: {ds.dims['lat']} != {TARGET_YSIZE}"
        assert ds.dims["lon"] == TARGET_XSIZE, f"Regrid failed for lon: {ds.dims['lon']} != {TARGET_XSIZE}"

    # Extract lon/lat after processing
    lon, lat = ds["lon"].values, ds["lat"].values

    # --- Plot variables ---
    for i, var_name in enumerate(var_names):
        if var_name not in ds:
            print(f"Variable {var_name} not found in {nc_file}")
            continue

        da = ds[var_name].squeeze()

        if PLOT_MINIMAL:
            # da is your 2D array, shape (ny, nx)
            # fig, ax = plt.subplots(figsize=(8, 6))
            # ax.imshow(da, cmap="Greys", vmin=vmin[i], vmax=vmax[i], origin="lower")
            # ax.axis("off")

            # # save fixed size in pixels
            # fig.savefig(f"{minimal_plot_folder}/{var_name}_{os.path.basename(nc_file)}.png",
            #             dpi=1,  # 1 pixel per data array element
            #             bbox_inches="tight", pad_inches=0)
            # plt.close(fig)
            filename =  f"{datetime[0]}_{var_name}" #os.path.basename(nc_file).replace(".nc", f"{datetime}_{var_name}")
            convert_crops_to_images(da, TARGET_XSIZE, TARGET_YSIZE, filename, FORMAT,
                                save_folder, cmap, vmin[i], vmax[i], COLOR_MODE, apply_cma=APPLY_CLOUD_MASK)
        else:
            fig, ax = plt.subplots(figsize=(10, 8), subplot_kw={"projection": ccrs.PlateCarree()})
            im = ax.pcolormesh(lon, lat, da, cmap="Greys", shading="auto", vmin=vmin[i], vmax=vmax[i])
            plt.colorbar(im, ax=ax, shrink=0.5, label=f"{var_name} [{units[i]}]")
            ax.coastlines(resolution="10m")
            ax.add_feature(cfeature.BORDERS, linestyle=":", linewidth=1, edgecolor="orange")
            ax.set_title(f"{var_name} from {os.path.basename(nc_file)}")
            fig.savefig(f"/data/trade_pc/ICON/icon_teamx/{date}/img/{var_name}_{os.path.basename(nc_file)}.png", bbox_inches="tight")
            print(f"Saved plot for {var_name} from {os.path.basename(nc_file)}")
            plt.close(fig)

    # --- Save reduced NetCDF ---
    if SAVE_REDUCED_NC:
        reduced_ds = ds[var_names]
        if APPLY_CLOUD_MASK and "CLCT" in ds:
            reduced_ds["CLCT"] = ds["CLCT"]
        path_to_save = os.path.join(nc_processed_folder, os.path.basename(nc_file))
        reduced_ds.to_netcdf(path_to_save)

    ds.close()





#Variables in file: ['height_bnds', 'plev_2_bnds', 'plev_3_bnds', 'plev_4_bnds', 'depth_bnds', 
# 'depth_2_bnds', 'lev_5_bnds', 'alt_bnds', 'alt_2_bnds', 'depth_4_bnds', 'u', 'ASWDIFD_S', 'ASWDIR_S', 
# 'RAIN_GSP', 'RAIN_CON', 'SNOW_GSP', 'SNOW_CON', 'GRAU_GSP', 'tp', 'PRR_GSP', 'lssrwe', 'PRG_GSP', 
# 'tcwv', 'TQC', 'TQI', 'tcolr', 'tcols', 'TQG', 'CAPE_ML', 'CIN_ML', 'DBZ_CMAX', 'DBZ_850', 'LPI', 
# 'SDI_2', 'HBAS_SC', 'HTOP_SC', 'ECHOTOP', 'HZEROCL', 'SNOWLMT', 'SYNMSG_BT_CL_IR10.8', 
# 'SYNMSG_BT_CL_WV6.2', 'z', 't', 'r', 'u_2', 'v', 'w', 'v_2', 'wz', 't_2', 'pres', 'q', 
# 'clwmr', 'QI', 'rwmr', 'snmr', 'grle', 'ccl', 'Q_SEDIM', 'tke', 'CLCL', 'CLCM', 'CLCH', 'CLCT', 'CLCT_MOD', 
# 'CLDEPTH', 'HTOP_DC', 'al', 'ASWDIFU_S', 'ALHFL_S', 'ASHFL_S', 'APAB_S', 'ASOB_S', 'ASOB_T', 'ATHB_S', 
# 'ATHB_T', 'AUMFL_S', 'AVMFL_S', 'nswrf', 'nswrf_2', 'param198.4.0', 'prmsl', 'sp', '2r', 'QV_S', 'RUNOFF_S', 
# 'RUNOFF_G', '2t', 'TMAX_2M', 'TMIN_2M', '2d', 'T_G', '10u', '10v', 'VMAX_10M', '10u_2', '10v_2', 'TWATER', 
# 'cnwat', 'sr', 'TQC_DIA', 'TQI_DIA', 'TQV_DIA', 'ceil', 'LPI_MAX', 'DBZ_CTMAX', 'vis', 'TCOND_MAX', 'TCOND10_MX', 
# 'UH_MAX', 'VORW_CTMAX', 'W_CTMAX', 'sde', 'rsn', 'T_SNOW', 'sd', 'FRESHSNW', 'snowc', 'T_SO', 'W_SO', 'W_SO_ICE', 
# 'SMI', 'icetk', 'ist', 'TCH', 'cd', 'ltlt', 'T_WML_LK', 'lblt', 'C_T_LK', 'H_ML_LK']