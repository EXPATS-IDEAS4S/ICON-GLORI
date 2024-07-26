"""
Open and check content of ICON model outputs
"""

import xarray as xr
from glob import glob
import netCDF4 as nc

def open_nc(nc_file):
    # Open the NetCDF file
    dataset = nc.Dataset(nc_file, 'r')  # 'r' is for read mode

    # Accessing global attributes
    print("Global attributes:")
    for attr in dataset.ncattrs():
        print(f"{attr}: {getattr(dataset, attr)}")

    # Accessing dimensions
    print("\nDimensions:")
    for dim in dataset.dimensions.values():
        print(dim)

    # Accessing variables
    print("\nVariables:")
    for var in dataset.variables:
        print(f"{var}: {dataset.variables[var]}")



folder_path = '/data/sat/msg/icon_output/marche_flood_15-09-22/netcdf/'
nc_filename = 'icon_*.nc'

#get all filenames in folder
all_nc_files = sorted(glob(folder_path+nc_filename)) #const file is the last one
print(all_nc_files) 

#open constant variable
ds = xr.open_dataset(all_nc_files[-1])
print(ds)

open_nc(all_nc_files[-1])


#loop over the files
for nc_file in all_nc_files[:-1]: #exclude the last one becasue is the const variables
    #extract time
    time = nc_file.split('/')[-1].split('_')[1]
    hour = nc_file.split('/')[-1].split('_')[2]
    timestamp = f"{time}{hour}"
    print(timestamp)

    open_nc(nc_file)   

    # Open the NetCDF file
    ds = xr.open_dataset(nc_file)
    print(ds.lat.values)
    print(len(ds.lat.values))
    print(ds.lon.values)
    print(len(ds.lon.values))
    