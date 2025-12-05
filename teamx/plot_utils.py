import numpy as np
import numpy as np
import matplotlib.pyplot as plt
import os
import PIL.Image


def regrid_coarsen_safe(ds, target_xsize, target_ysize,
                        dim_lon='lon', dim_lat='lat',
                        agg='mean', crop=True):
    """
    Regrid `ds` to (target_ysize, target_xsize) by block-aggregating.
    - If original sizes are not divisible, crop symmetrically to the nearest multiple (default).
    - agg: 'mean' or 'sum' (other reductions could be added).
    Returns: regridded dataset and new (lon, lat) 1D center coordinates.
    """
    if dim_lat not in ds.dims or dim_lon not in ds.dims:
        raise ValueError(f"Dataset must have dims '{dim_lat}' and '{dim_lon}'")

    nlat = ds.dims[dim_lat]
    nlon = ds.dims[dim_lon]

    if nlat < target_ysize or nlon < target_xsize:
        raise ValueError("Target size larger than source size — use interpolation/xesmf instead")

    # compute target-compatible sizes (floor multiple)
    factor_lat = nlat // target_ysize
    factor_lon = nlon // target_xsize

    if factor_lat < 1 or factor_lon < 1:
        raise ValueError("Computed coarsen factor < 1 — cannot coarsen")

    # if not exactly divisible, crop symmetrically to nearest multiple
    new_nlat = factor_lat * target_ysize
    new_nlon = factor_lon * target_xsize

    ds_work = ds
    if (new_nlat != nlat) or (new_nlon != nlon):
        if not crop:
            raise ValueError("Source dimensions not divisible by target and crop=False")
        # symmetric crop
        drop_before_lat = (nlat - new_nlat) // 2
        drop_after_lat  = nlat - new_nlat - drop_before_lat
        drop_before_lon = (nlon - new_nlon) // 2
        drop_after_lon  = nlon - new_nlon - drop_before_lon

        lat_slice = slice(drop_before_lat, nlat - drop_after_lat)
        lon_slice = slice(drop_before_lon, nlon - drop_after_lon)
        ds_work = ds.isel({dim_lat: lat_slice, dim_lon: lon_slice})

    # coarsen and aggregate
    if agg == 'mean':
        ds_re = ds_work.coarsen({dim_lat: factor_lat, dim_lon: factor_lon}, boundary='trim').mean()
    elif agg == 'sum':
        ds_re = ds_work.coarsen({dim_lat: factor_lat, dim_lon: factor_lon}, boundary='trim').sum()
    else:
        raise ValueError("Unsupported agg, choose 'mean' or 'sum'")

    # recompute new lon/lat center coords (average within each block)
    # Note: if coords are 1D arrays
    if dim_lat in ds.coords:
        new_lat = ds_work[dim_lat].coarsen({dim_lat: factor_lat}, boundary='trim').mean().values
        ds_re = ds_re.assign_coords({dim_lat: new_lat})
    if dim_lon in ds.coords:
        new_lon = ds_work[dim_lon].coarsen({dim_lon: factor_lon}, boundary='trim').mean().values
        ds_re = ds_re.assign_coords({dim_lon: new_lon})

    # final sanity-check
    assert ds_re.dims[dim_lat] == target_ysize, f"lat size {ds_re.dims[dim_lat]} != {target_ysize}"
    assert ds_re.dims[dim_lon] == target_xsize, f"lon size {ds_re.dims[dim_lon]} != {target_xsize}"

    return ds_re


def create_fig(image, pixel_size, cmap, vmin=None, vmax=None, flip=True):
    if flip:
        image = np.flipud(image)
    fig, ax = plt.subplots(figsize=pixel_size, dpi=1)
    fig.subplots_adjust(left=0, right=1, bottom=0, top=1)
    ax.imshow(image, cmap=cmap, vmin=vmin, vmax=vmax)
    ax.axis(False)
    plt.close(fig)
    return fig

def convert_crops_to_images(ds_image, x_pixel, y_pixel, filename, format, out_path,
                            cmap, vmin, vmax, color_mode, apply_cma=False):
    fig = create_fig(ds_image.values.squeeze(), [x_pixel, y_pixel], cmap, vmin, vmax)
    out_dir = f'{out_path}/{format}_{color_mode}'
    os.makedirs(out_dir, exist_ok=True)

    crop_filepath = f'{out_dir}/{filename}_{color_mode}.tiff'
    if apply_cma:
        crop_filepath = f'{out_dir}/{filename}_{color_mode}_CMA.tiff'

    fig.savefig(crop_filepath, dpi=1)
    
    image = PIL.Image.open(crop_filepath)
    if color_mode == 'RGB':
        converted_image = image.convert('RGB')
    elif color_mode == 'greyscale':
        converted_image = image.convert('L')
    else:
        raise ValueError("Color mode must be 'RGB' or 'greyscale'")

    #check dimension of img
    print(f'Image size: {converted_image.size}, expected: ({x_pixel}, {y_pixel})')
    #save also png images in a subfolder
    png_out_dir = f'{out_path}/png_{color_mode}'
    os.makedirs(png_out_dir, exist_ok=True)
    png_crop_filepath = f'{png_out_dir}/{filename}_{color_mode}.png'
    converted_image.save(png_crop_filepath)

    
    converted_image.save(crop_filepath)
    converted_image.close()
    image.close()
    print(f'{crop_filepath} saved')