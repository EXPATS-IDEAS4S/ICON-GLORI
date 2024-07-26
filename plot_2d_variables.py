"""
Open and Plot the example ICON model output
"""

import xarray as xr
import matplotlib.pyplot as plt
import cartopy.crs as ccrs
from glob import glob
import os
import sys
import matplotlib.colors as mcolors

sys.path.append('/home/dcorradi/Documents/Codes/NIMROD/')
from figures.quality_check_functions import create_gif_from_folder
from compare.comparison_function import get_max_min

folder_path = '/data/sat/msg/icon_output/marche_flood_15-09-22/netcdf/'
nc_filename = 'icon_*.nc'
out_path = '/home/dcorradi/Documents/Data/ICON-GLORI/Marche_Flood_22/Fig/'

#get all filenames in folder
all_nc_files = sorted(glob(folder_path+nc_filename)) #const file is the last one
print(all_nc_files) 

list_variables = ['tmax_2m', 'tmin_2m', 
                    'sst', '2t', '2d', '10u', '10v', 'vmax_10m', 'tp', 
                    'rain_con', 'snow_con', 'tprate', 'sot', 'qv', 'sot_2', 
                    'pmsl', 'sp', 'tqv', 'synmsg_bt_cl_wv6.2', 
                    'synmsg_bt_cl_ir3.9', 'synmsg_bt_cl_ir10.8', 
                    'synmsg_rad_cl_wv6.2', 'synmsg_rad_cl_ir3.9', 'synmsg_rad_cl_ir10.8', 
                    'cp', '2r', 'rwmr', 'snmr', 'qv_2', 
                    'clwmr', 'u', 'v', 'wz', 'z', 't', 'tqc', 'hbas_con', 'htop_con']

msg_var = ['synmsg_bt_cl_wv6.2', 'synmsg_bt_cl_ir3.9', 'synmsg_bt_cl_ir10.8',]

ground_var = ['2t', 'pmsl', '2r', 'vmax_10m']

integrated_var = ['tp', 'tqc', 'tqv']

other_var = ['tp']

# Get available variables
variables =  other_var #list(ds.data_vars)

#open dataset with all timestep
ds_all = xr.open_mfdataset(all_nc_files[:-1], combine='nested', concat_dim='time', parallel=True)
print(ds_all)

#get max min values in each channel
vmins = []
vmaxs = []
for ch in variables:
    min, max = get_max_min(ds_all,ch)
    vmins.append(min)
    vmaxs.append(max)
print(vmins,vmaxs)

#TODO customized colormaps for each variable!

# Define custom colors in RGBA format
colors = [
    [1, 1, 1, 1],    # White (index 0)
    [255/255, 255/255, 204/255, 1],  # Pale yellow
    [204/255, 255/255, 204/255, 1],  # Light green
    [153/255, 255/255, 204/255, 1],  # Lighter green
    [102/255, 255/255, 204/255, 1],  # Lightest green
    [51/255, 255/255, 204/255, 1],   # Green
    [0/255, 204/255, 204/255, 1],    # Cyan
    [0/255, 153/255, 204/255, 1],    # Light blue
    [0/255, 102/255, 204/255, 1],    # Blue
    [0/255, 51/255, 204/255, 1],     # Dark blue
    [0/255, 0/255, 204/255, 1],      # Darker blue
    [204/255, 204/255, 255/255, 1],  # Pale blue
    [204/255, 204/255, 153/255, 1],  # Pale green
    [204/255, 204/255, 102/255, 1],  # Pale yellow-green
    [255/255, 204/255, 102/255, 1],  # Light orange
    [255/255, 153/255, 51/255, 1],   # Orange
    [255/255, 102/255, 51/255, 1],   # Light red-orange
    [255/255, 51/255, 51/255, 1],    # Red
    [204/255, 51/255, 51/255, 1],    # Dark red
    [153/255, 51/255, 51/255, 1],    # Darker red
]

# Create a ListedColormap with the defined colors
cmap = mcolors.ListedColormap(colors)


#loop over the files
for nc_file in all_nc_files[:-1]: #exclude the last one becasue is the const variables
    #extract time
    time = nc_file.split('/')[-1].split('_')[1]
    hour = nc_file.split('/')[-1].split('_')[2]
    timestamp = f"{time}{hour}"
    print(timestamp)   

    # Open the NetCDF file
    ds = xr.open_dataset(nc_file)

    for i,variable in enumerate(variables):
        print(variable)
        # Extract data for the selected variable
        data = ds[variable].squeeze()

        # Create output directory for the variable if it doesn't exist
        var_out_path = os.path.join(out_path, variable)
        os.makedirs(var_out_path, exist_ok=True)

        # Plotting the data
        fig = plt.figure(figsize=(10, 6))
        ax = plt.axes(projection=ccrs.PlateCarree())

        # Plot the variable data on the map
        data.plot(ax=ax, transform=ccrs.PlateCarree(), cmap=cmap, 
              vmin=vmins[i], vmax=vmaxs[i], 
              cbar_kwargs={'label': data.attrs.get('units', '')})
        
        # Add coastlines 
        ax.coastlines()
        # Customize gridlines to show labels only on the bottom and left axes
        gl = ax.gridlines(draw_labels=True, dms=True, x_inline=False, y_inline=False)
        gl.top_labels = False
        gl.right_labels = False
        gl.bottom_labels = True
        gl.left_labels = True

        # Set title
        plt.title(f"{variable} on {str(ds.time.values[0])}")

        # Save the plot
        fig.savefig(os.path.join(var_out_path, f"{variable}_{timestamp}.png"), bbox_inches='tight')
        plt.close(fig)  # Close the figure to free memory
        #exit()


#make the gif
for variable in variables:
    var_out_path = os.path.join(out_path, variable)
    create_gif_from_folder(var_out_path,out_path+f'{variable}.gif')

