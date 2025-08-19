import xarray as xr
import matplotlib.pyplot as plt
import cartopy.crs as ccrs
import cartopy.feature as cfeature
from glob import glob
import numpy as np
from cartopy.mpl.gridliner import LONGITUDE_FORMATTER, LATITUDE_FORMATTER
import cartopy.mpl.ticker as cticker

# === USER SETTINGS ===
nc_file_folder = "/work/dcorradi/icon_output/teamx/acinn-data.uibk.ac.at/20250630_00/nc"  
var_names = ["t2m", '2r', 'CLCT', 'SYNMSG_BT_CL_IR10.8', 'SYNMSG_BT_CL_WV6.2']
save_folder = "/work/dcorradi/icon_output/teamx/acinn-data.uibk.ac.at/20250630_00/img"
grid_file = "/work/dcorradi/icon_output/teamx/acinn-data.uibk.ac.at/domain/grid_500m.txt"

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

# --- Parse the grid_500m.txt ---

grid_info = {}

with open(grid_file, 'r') as f:
    for line in f:
        line = line.strip()
        if line.startswith("#") or line == "":
            continue
        if '=' in line:
            key, val = line.split('=')
            key = key.strip()
            val = val.split('#')[0].strip()  # remove anything after #
            try:
                grid_info[key] = float(val)
            except ValueError:
                grid_info[key] = val.strip('"')

# --- Step 2: Build lat/lon arrays ---
xfirst = grid_info['xfirst']
xinc   = grid_info['xinc']
xsize  = int(grid_info['xsize'])

yfirst = grid_info['yfirst']
yinc   = grid_info['yinc']
ysize  = int(grid_info['ysize'])

lon = np.linspace(xfirst, xfirst + (xsize-1)*xinc, xsize)
lat = np.linspace(yfirst, yfirst + (ysize-1)*yinc, ysize)

#Find all nc files in folder
nc_files = sorted(glob(f"{nc_file_folder}/*.nc"))

# Loop over the data files
for nc_file in nc_files:

    # --- Load data ---
    ds = xr.open_dataset(nc_file)

    print("Variables in file:", list(ds.data_vars))

    # Loop over the selected variabels
    for var_name in var_names:
        if var_name not in ds:
            print(f"Variable '{var_name}' not found in {nc_file}")
            continue
        
        da = ds[var_name]
        
        # --- Select first timestep if time dimension exists ---
        time = nc_file.split('/')[-1].replace('.nc', '')
        if "time" in da.dims:
            da = da.isel(time=0)
            time = da.time.values

        # --- Plot ---
        da = da.squeeze()  # removes dimensions of size 1
        fig, ax = plt.subplots(figsize=(10,8), subplot_kw={'projection': ccrs.PlateCarree()})

        # use the lon/lat from the txt file for plotting
        im = ax.pcolormesh(lon, lat, da, cmap="coolwarm", shading='auto')
        cbar = plt.colorbar(im, ax=ax, orientation='vertical', fraction=0.046, pad=0.04, shrink=0.7)
        cbar.ax.tick_params(labelsize=14)
        cbar.set_label(var_name, fontsize=14)
        ax.coastlines(resolution="10m")
        ax.add_feature(cfeature.BORDERS, linestyle=":", linewidth=1)
        ax.add_feature(cfeature.LAND, facecolor="lightgray")
        ax.add_feature(cfeature.OCEAN, facecolor="lightblue")

        # Set up longitude and latitude tick formatters
        ax.xaxis.set_major_formatter(cticker.LongitudeFormatter())
        ax.yaxis.set_major_formatter(cticker.LatitudeFormatter())
        #increase size of the axis labels
        ax.xaxis.label.set_size(14)
        ax.yaxis.label.set_size(14)

        # Optionally, add gridlines
        gl = ax.gridlines(draw_labels=True)
        gl.top_labels = False
        gl.right_labels = False


        ax.set_title(f"{var_name} from {str(time).split('.')[0]}", fontsize=16, fontweight='bold')
        fig.savefig(f"{save_folder}/{var_name}_{nc_file.split('/')[-1].replace('.nc', '.png')}", bbox_inches='tight')   
        plt.close(fig)
