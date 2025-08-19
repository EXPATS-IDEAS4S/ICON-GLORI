#!/usr/bin/env python3
import sys
import numpy as np
import netCDF4 as nc
import xarray as xr

grid_file = "/work/dcorradi/icon_output/teamx/acinn-data.uibk.ac.at/domain/domain2_DOM02.nc"
output_file = "/work/dcorradi/icon_output/teamx/acinn-data.uibk.ac.at/domain/output_grid.txt"

#read domain file using xarray
ds = xr.open_dataset(grid_file)
print(ds.attrs)
exit()

# Load ICON grid
ds = nc.Dataset(grid_file)
clon = np.degrees(ds.variables["clon"][:])  # radians → degrees
clat = np.degrees(ds.variables["clat"][:])  # radians → degrees
ds.close()

# Sort for distance calc
order = np.argsort(clon)
clon = clon[order]
clat = clat[order]

# Approximate resolution in lon/lat
dlon = np.median(np.abs(np.diff(clon)))
dlat = np.median(np.abs(np.diff(np.sort(clat))))

# Round to reasonable decimal
dlon = round(dlon, 5)
dlat = round(dlat, 5)

print(f"Detected resolution: Δlon ≈ {dlon}°, Δlat ≈ {dlat}°")

# Build CDO grid description
lonmin, lonmax = -180.0, 180.0
latmin, latmax = -90.0, 90.0
nlon = int((lonmax - lonmin) / dlon)
nlat = int((latmax - latmin) / dlat)

grid_desc = f"""gridtype = lonlat
xsize     = {nlon}
ysize     = {nlat}
xfirst    = {lonmin + dlon/2}
xinc      = {dlon}
yfirst    = {latmin + dlat/2}
yinc      = {dlat}
"""

# Save
with open(output_file, "w") as f:
    f.write(grid_desc)

print(grid_desc)
print(f"Grid description saved to {output_file}")
