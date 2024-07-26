"""
Open and Plot the example ICON model output
"""

import xarray as xr
import matplotlib.pyplot as plt
from glob import glob
import os
import sys
import matplotlib.colors as mcolors
import numpy as np

sys.path.append('/home/dcorradi/Documents/Codes/NIMROD/')
from figures.quality_check_functions import create_gif_from_folder
from compare.comparison_function import get_max_min

folder_path = '/data/sat/msg/icon_output/marche_flood_15-09-22/netcdf/'
nc_filename = 'icon_*.nc'
out_path = '/home/dcorradi/Documents/Data/ICON-GLORI/Marche_Flood_22/Fig/'

#constants
g = 9.8  # m/s^2, approximate value of gravitational acceleration
earth_radius = 6371e3 #m

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

vertical_var = ['rwmr'] #['t',  'clwmr', 'rwmr',  'qv_2'] #(time, height_3, lat, lon)
height_name = 'height_3'




#Cross section
specified_lat = 43.3 #Should be Cantiano latitude, where precipitation extremes were recorded

# Get available variables
variables =  vertical_var #list(ds.data_vars)


#open dataset with all timestep
ds_all = xr.open_mfdataset(all_nc_files[:-1], combine='nested', concat_dim='time', parallel=True)
print(ds_all)

ds_all_lat = ds_all.sel(lat=specified_lat, method='nearest')

#get max min values in each channel
vmins = []
vmaxs = []
for ch in variables:
    min, max = get_max_min(ds_all_lat,ch)
    vmins.append(min)
    vmaxs.append(max)
print(vmins,vmaxs)
vmins = [0.0] #[198.85406, 0.0, 0.0, 2.2300594e-07]
vmaxs = [0.018323898] #[315.33566, 0.0063489676, 0.018323898, 0.022355773]

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

# Create a colorbar with the custom colormap
#bounds = np.arange(len(colors) + 1)  # Boundaries for each color
#norm = mcolors.BoundaryNorm(bounds, cmap.N)  # Normalize the boundaries


#loop over the files
for nc_file in all_nc_files[:-1]: #exclude the last one becasue is the const variables
    #extract time
    time = nc_file.split('/')[-1].split('_')[1]
    hour = nc_file.split('/')[-1].split('_')[2]
    timestamp = f"{time}{hour}"
    print(timestamp)   

    # Open the NetCDF file
    ds = xr.open_dataset(nc_file)

    #Convert geopotential to geometric heght
    ds_geopotential_height = ds['z']/g #z(time, height_3, lat, lon)
    ds_geom_height = (ds_geopotential_height*earth_radius)/(earth_radius-ds_geopotential_height)
    #print(ds_geom_height) #variable still called z
    geom_height = ds_geom_height.sel(lat=specified_lat, method='nearest').squeeze()

    # Create a meshgrid for lon and height indices
    height_index_grid , lon_grid = np.meshgrid(ds[height_name].values,ds.lon.values, indexing='ij')

    # #TODO add convective cloud base and tops
    # ds_cloud_base = ds['hbas_con']
    # ds_cloud_top = ds['htop_con']
    # cloud_base = ds_cloud_base.sel(lat=specified_lat, method='nearest').squeeze().values
    # cloud_top = ds_cloud_top.sel(lat=specified_lat, method='nearest').squeeze().values
    # print(cloud_base,cloud_top)

    for i, variable in enumerate(vertical_var):
        print(variable)
        # Extract data for the selected variable along the specified latitude
        data = ds[variable].sel(lat=specified_lat, method='nearest').squeeze()
        #print(data.values.shape,data.values)

        # Create output directory for the variable if it doesn't exist
        var_out_path = os.path.join(out_path, variable, f"lat_{specified_lat}")
        os.makedirs(var_out_path, exist_ok=True)

        # Plotting the data
        fig, ax = plt.subplots(figsize=(18, 5))
        ax.set_facecolor('black')

        # Plot the vertical profile
        im = ax.contourf(lon_grid, geom_height.values, data, cmap=cmap)#, vmin=vmins[i], vmax=vmaxs[i])
        cbar = fig.colorbar(im, ax=ax)
        cbar.set_label(data.attrs.get('units', ''))

        # Plot the cloud base and top
        #ax.plot(data.lon, cloud_base, 'r--', label='Cloud Base')
        #ax.plot(data.lon, cloud_top, 'b--', label='Cloud Top')

        # Add labels and title
        ax.set_xlabel('Longitude')
        ax.set_ylabel('Geometric Height (m)')
        ax.set_yscale('asinh')
        ax.set_title(f"{variable} Vertical Profile at {specified_lat}° Latitude on {timestamp}")
        #ax.legend()

        # Save the plot
        plot_filename = f"{variable}_{specified_lat}lat_{timestamp}.png"
        fig.savefig(os.path.join(var_out_path, plot_filename), bbox_inches='tight')
        plt.close(fig)  # Close the figure to free memory
        #exit()


# #TODO did't manange to fix the colorbar so the gif won't be produced
# #make the gif
# for variable in variables:
#     var_out_path = os.path.join(out_path, variable, f"lat_{specified_lat}")
#     create_gif_from_folder(var_out_path,out_path+f"{variable}_{specified_lat}lat.gif")

