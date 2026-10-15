"""Create aligned ICON/MSG crop samples on a common 100x100 reference grid.

This script reads daily NetCDF files from the configured S3 buckets, filters each
source to the requested geographic domain and time range, applies threshold-based
cloud masking, and then regrids the ICON data onto the MSG grid before exporting
fixed-size crop samples for downstream model training or evaluation.

The workflow is intentionally structured as:
1. crop to the target extent,
2. mask cloudy pixels for both sources,
3. interpolate ICON linearly onto the MSG grid,
4. save both datasets as 100x100 crops for consistent comparison.

A second ICON initialization time (12Z) is processed alongside the standard 00Z
run so each reference MSG crop can be paired with two ICON realizations.
"""

import io
import logging
import os
import sys

import boto3
import numpy as np
import xarray as xr
from botocore.config import Config
from botocore.exceptions import ClientError
from scipy.ndimage import binary_closing

sys.path.append('/home/Daniele/codes/ICON-GLORI/')
from credentials_buckets_old import S3_ACCESS_KEY, S3_BUCKET_ICON, S3_BUCKET_MSG, S3_ENDPOINT_URL, S3_SECRET_ACCESS_KEY

sys.path.append('/home/Daniele/codes/ML_data_generator/')
from cropping_functions import filter_by_domain, filter_by_time

# -----------------------------------------------------------------------------
# Configuration
# -----------------------------------------------------------------------------
years = [2025]
months = range(4, 10)
days = range(1, 32)

icon_basename = 'merged_SYNMSG_BT_CL_IR10.8_CLCT'
icon_initialization_hours = ['00', '12']

msg_path_dir = '/data/sat/msg/ml_train_crops/IR_108-WV_062-CMA_FULL_EXPATS_DOMAIN'
msg_basename = 'merged_MSG_CMSAF'

crop_extents = [
    (11.0, 15.0, 45.0, 49.0),
    (7.0, 11.0, 45.0, 49.0),
    (9.0, 13.0, 45.0, 49.0),
]
domain_names = ['east', 'west', 'central']
x_pixel = 100
y_pixel = 100

hour_start = '01'
hour_end = '24'

value_min = [180.0]
value_max = [320.0]

cloud_prm_icon = ['SYNMSG_BT_CL_IR10.8']
cloud_prm_msg = ['IR_108']
plot_var_name = 'IR_108'

apply_cma = True
cma_icon = 'CLCT'
cma_msg = 'cma'
cloud_threshold_icon = 50.0
cloud_threshold_msg = 0
clear_sky_fill_value = 320.0

file_extension = 'nc'
save_sanity_plot = True
sanity_plot_outpath = '/data1/crops/teamx_Apr-Sep_2025_icon_msg_regrid/sanity_plots'

outpath = f'/data1/crops/teamx_Apr-Sep_2025_icon_msg_regrid/{file_extension}/1'
os.makedirs(outpath, exist_ok=True)
os.makedirs(sanity_plot_outpath, exist_ok=True)

# -----------------------------------------------------------------------------
# Helper functions
# -----------------------------------------------------------------------------
def select_timestamps(timestamps, file_date, hour_start, hour_end, include_next_day_midnight=False):
    next_date = str(np.datetime64(file_date) + np.timedelta64(1, 'D'))
    selected = []
    for t in timestamps:
        t_date = str(t).split('T')[0]
        t_hour = str(t).split('T')[1][0:2]

        in_day_hour_range = t_date == file_date and hour_start <= t_hour < hour_end
        next_day_midnight = include_next_day_midnight and t_date == next_date and t_hour == '00'

        if in_day_hour_range or next_day_midnight:
            selected.append(t)
    return selected


def resample_by_extent(ds_crop, extent, x_pixel, y_pixel):
    """Resample a dataset to a target grid over the requested geographic extent."""
    lonmin, lonmax, latmin, latmax = extent

    if ds_crop.sizes.get('lon', 0) == 0 or ds_crop.sizes.get('lat', 0) == 0:
        raise ValueError(f'Empty crop for extent: {extent}')

    def _prepare_unique_coord(ds_in, dim_name):
        coord = ds_in[dim_name].values
        finite_idx = np.where(np.isfinite(coord))[0]
        if finite_idx.size == 0:
            raise ValueError(f"No finite values found on coordinate '{dim_name}'")

        ds_tmp = ds_in.isel({dim_name: finite_idx})
        coord = ds_tmp[dim_name].values

        order = np.argsort(coord)
        ds_tmp = ds_tmp.isel({dim_name: order})
        coord_sorted = ds_tmp[dim_name].values

        _, unique_idx = np.unique(coord_sorted, return_index=True)
        if unique_idx.size < coord_sorted.size:
            ds_tmp = ds_tmp.isel({dim_name: np.sort(unique_idx)})

        return ds_tmp

    ds_crop = _prepare_unique_coord(ds_crop, 'lon')
    ds_crop = _prepare_unique_coord(ds_crop, 'lat')

    lon_vals = ds_crop.lon.values
    lat_vals = ds_crop.lat.values
    src_lon_min = float(np.nanmin(lon_vals))
    src_lon_max = float(np.nanmax(lon_vals))
    src_lat_min = float(np.nanmin(lat_vals))
    src_lat_max = float(np.nanmax(lat_vals))

    lon_lo = max(min(lonmin, lonmax), src_lon_min)
    lon_hi = min(max(lonmin, lonmax), src_lon_max)
    lat_lo = max(min(latmin, latmax), src_lat_min)
    lat_hi = min(max(latmin, latmax), src_lat_max)

    if lon_lo >= lon_hi or lat_lo >= lat_hi:
        raise ValueError(
            f'No overlap between requested extent {extent} and source bounds '
            f'lon[{src_lon_min}, {src_lon_max}] lat[{src_lat_min}, {src_lat_max}]'
        )

    target_lon = np.linspace(lon_lo, lon_hi, x_pixel)
    target_lat = np.linspace(lat_lo, lat_hi, y_pixel)

    ds_out = ds_crop.interp(
        lon=target_lon,
        lat=target_lat,
        method='nearest',
        kwargs={'fill_value': 'extrapolate'},
    )

    if all(xr.DataArray.isnull(ds_out[v]).all() for v in ds_out.data_vars):
        raise ValueError(
            'Resampling produced all-NaN output. '
            f'Requested extent: {extent}; source bounds: '
            f'lon[{src_lon_min}, {src_lon_max}] lat[{src_lat_min}, {src_lat_max}]'
        )

    return ds_out


def apply_cloud_mask_threshold(ds_var, ds_full, cloud_mask_var='CLCT', cloud_threshold=50.0, clear_sky_fill_value=320.0):
    """Apply threshold-based cloud masking on a dataset using the provided mask variable."""
    if cloud_mask_var not in ds_full:
        raise ValueError(f"Cloud mask variable '{cloud_mask_var}' not found in dataset")

    mask_rs = ds_full[cloud_mask_var]
    cloudy_mask = mask_rs > cloud_threshold

    for var in ds_var.data_vars:
        ds_var[var] = ds_var[var].where(cloudy_mask, clear_sky_fill_value)

    return ds_var.fillna(clear_sky_fill_value)


def read_file(s3, file_name, bucket):
    """Read a file from an S3 bucket and return its raw bytes."""
    try:
        obj = s3.get_object(Bucket=bucket, Key=file_name)
        return obj['Body'].read()
    except ClientError as exc:
        logging.error(exc)
        return None


def plot_sanity_triptych_cartopy(
    da_before_mask,
    da_after_mask,
    da_after_resample,
    var_name,
    out_dir,
    plot_tag,
    da_before_mask_2=None,
    da_after_mask_2=None,
    da_after_resample_2=None,
    var_name_2=None,
    row_label_1='ICON',
    row_label_2='MSG',
):
    """Create a 3-panel Cartopy sanity plot for a before/after/resampled comparison."""
    try:
        import matplotlib.pyplot as plt
        import cartopy.crs as ccrs
        import cartopy.feature as cfeature
        from cartopy.mpl.ticker import LongitudeFormatter, LatitudeFormatter
    except ImportError as exc:
        print(f'Skipping sanity plot (missing plotting dependency): {exc}')
        return

    os.makedirs(out_dir, exist_ok=True)

    def _to_2d(da):
        da2d = da.squeeze(drop=True)
        if 'time' in da2d.dims:
            da2d = da2d.isel(time=0)
        return da2d

    da_before = _to_2d(da_before_mask)
    da_after_m1 = _to_2d(da_after_mask)
    da_after_res1 = _to_2d(da_after_resample)

    has_row2 = da_before_mask_2 is not None
    nrows = 2 if has_row2 else 1

    row1_das = [da_before, da_after_m1, da_after_res1]
    all_das = row1_das[:]
    if has_row2:
        row2_das = [_to_2d(da_before_mask_2), _to_2d(da_after_mask_2), _to_2d(da_after_resample_2)]
        all_das += row2_das

    vals = np.concatenate([da.values.ravel() for da in all_das])
    vals = vals[np.isfinite(vals)]
    if vals.size == 0:
        print(f'Skipping sanity plot for {plot_tag}: no finite values')
        return

    vmin, vmax = float(np.nanmin(vals)), float(np.nanmax(vals))
    proj = ccrs.PlateCarree()
    fig, axes = plt.subplots(
        nrows, 3,
        figsize=(7, 4 * nrows),
        subplot_kw={'projection': proj},
        constrained_layout=True,
    )
    if nrows == 1:
        axes = axes[np.newaxis, :]

    col_titles = ['Before Cloud Mask', 'After Cloud Mask', 'After Resampling']
    rows_meta = [(row_label_1, var_name, row1_das)]
    if has_row2:
        rows_meta.append((row_label_2, var_name_2 or var_name, row2_das))

    mappable = None
    for r, (row_label, rv_name, das) in enumerate(rows_meta):
        axes[r, 1].text(
            0.5,
            1.18,
            row_label,
            transform=axes[r, 1].transAxes,
            ha='center',
            va='bottom',
            fontsize=10,
            fontweight='bold',
        )
        for c, da in enumerate(das):
            ax = axes[r, c]
            mappable = ax.pcolormesh(
                da['lon'].values,
                da['lat'].values,
                da.values,
                cmap='gray_r',
                vmin=vmin,
                vmax=vmax,
                shading='auto',
                transform=proj,
            )
            ax.add_feature(cfeature.COASTLINE.with_scale('50m'), linewidth=0.7, color='yellow')
            ax.add_feature(cfeature.BORDERS.with_scale('50m'), linewidth=0.5, color='yellow')
            ax.xaxis.set_major_formatter(LongitudeFormatter(3))
            ax.yaxis.set_major_formatter(LatitudeFormatter(4))
            ax.tick_params(labelsize=7)
            grid = ax.gridlines(draw_labels=True, linewidth=0.3, alpha=0.5)
            grid.top_labels = False
            grid.right_labels = False
            grid.bottom_labels = (r == nrows - 1)
            grid.left_labels = (c == 0)
            if r == 0:
                ax.set_title(col_titles[c])
            if c == 0:
                ax.set_ylabel(f'{row_label}\n({rv_name})', fontsize=8)

    cbar = fig.colorbar(mappable, ax=axes, orientation='horizontal', fraction=0.05, pad=0.08)
    cbar.set_label(var_name)
    fig.suptitle(plot_tag, y=1.00)
    fig_path = f'{out_dir}/sanity_{plot_tag}.png'
    fig.savefig(fig_path, dpi=150, bbox_inches='tight')
    plt.close(fig)
    print(f'Sanity plot saved: {fig_path}')


# -----------------------------------------------------------------------------
# S3 client
# -----------------------------------------------------------------------------
s3 = boto3.client(
    's3',
    endpoint_url=S3_ENDPOINT_URL,
    aws_access_key_id=S3_ACCESS_KEY,
    aws_secret_access_key=S3_SECRET_ACCESS_KEY,
    config=Config(
        connect_timeout=10,
        read_timeout=120,
        retries={'max_attempts': 3, 'mode': 'standard'},
    ),
)


# -----------------------------------------------------------------------------
# Main processing loop
# -----------------------------------------------------------------------------
outpath = f'/data1/crops/teamx_Apr-Sep_2025_icon_msg_regrid/{file_extension}/1'
os.makedirs(outpath, exist_ok=True)

for crop_extent, domain_name in zip(crop_extents, domain_names):
    for year in years:
        for month in months:
            month = f'{month:02d}'
            for day in days:
                day_str = f'{year:04d}-{month}-{day:02d}'

                msg_file = f'{msg_path_dir}/{year:04d}/{month}/{msg_basename}_{year:04d}-{month}-{day:02d}.nc'
                print(f'Day {day_str} | Domain {domain_name} | MSG: {msg_file}')

                msg_obj = read_file(s3, msg_file, S3_BUCKET_MSG)
                if msg_obj is None:
                    print(f'[MSG] missing: {msg_file}')
                    continue

                with xr.open_dataset(io.BytesIO(msg_obj)) as ds_msg:
                    required_msg = cloud_prm_msg + [cma_msg]
                    missing_msg = [v for v in required_msg if v not in ds_msg]
                    if missing_msg:
                        print(f'[MSG] skipping {msg_file}: missing variables {missing_msg}')
                        continue

                    ds_msg_var = ds_msg[cloud_prm_msg]
                    ds_msg_mask = ds_msg[[cma_msg]]

                    timestamps = select_timestamps(
                        ds_msg_var.time.values,
                        file_date=day_str,
                        hour_start=hour_start,
                        hour_end=hour_end,
                        include_next_day_midnight=False,
                    )

                    for timestamp in timestamps:
                        t_str = str(timestamp)
                        t_date = t_str.split('T')[0]
                        t_hour = t_str.split('T')[1][0:2]

                        if t_str.split('T')[1][3:5] != '00':
                            continue

                        ds_time_var = filter_by_time(ds_msg_var, timestamp)
                        ds_time_mask = filter_by_time(ds_msg_mask, timestamp)

                        msg_before_mask = ds_time_var[cloud_prm_msg[0]].copy(deep=True)

                        is_all_nan = all(xr.DataArray.isnull(ds_time_var[var]).all() for var in ds_time_var.data_vars)
                        is_outside_range = any(
                            ((ds_time_var[var] < value_min[i]) | (ds_time_var[var] > value_max[i])).any()
                            for i, var in enumerate(ds_time_var.data_vars)
                        )
                        if is_all_nan or is_outside_range:
                            continue

                        if apply_cma:
                            if cma_msg == 'cma':
                                cma_da = ds_time_mask[cma_msg]
                                if 'time' in cma_da.dims:
                                    cma_slice = cma_da.isel(time=0).values
                                else:
                                    cma_slice = cma_da.values

                                structure = np.ones((3, 3), dtype=np.uint8)
                                closed_cma_2d = binary_closing(cma_slice, structure=structure)

                                if 'time' in cma_da.dims:
                                    closed_cma = np.expand_dims(closed_cma_2d, axis=0)
                                    ds_time_mask = ds_time_mask.copy()
                                    ds_time_mask[cma_msg] = xr.DataArray(
                                        closed_cma,
                                        dims=('time', 'lat', 'lon'),
                                        coords={
                                            'time': ds_time_mask[cma_msg].time.values[:1],
                                            'lat': ds_time_mask[cma_msg].lat.values,
                                            'lon': ds_time_mask[cma_msg].lon.values,
                                        },
                                    )
                                else:
                                    ds_time_mask = ds_time_mask.copy()
                                    ds_time_mask[cma_msg] = xr.DataArray(
                                        closed_cma_2d,
                                        dims=cma_da.dims,
                                        coords=cma_da.coords,
                                    )

                            ds_time_var = apply_cloud_mask_threshold(
                                ds_var=ds_time_var,
                                ds_full=ds_time_mask,
                                cloud_mask_var=cma_msg,
                                cloud_threshold=cloud_threshold_msg,
                                clear_sky_fill_value=clear_sky_fill_value,
                            )

                        msg_crop = resample_by_extent(
                            ds_crop=filter_by_domain(ds_time_var, crop_extent),
                            extent=crop_extent,
                            x_pixel=x_pixel,
                            y_pixel=y_pixel,
                        )

                        msg_target_lon = msg_crop.lon.values
                        msg_target_lat = msg_crop.lat.values

                        for init_hour in icon_initialization_hours:
                            file_icon = f'{icon_basename}_{year:04d}{month}{day:02d}_{init_hour}.nc'
                            icon_obj = read_file(s3, file_icon, S3_BUCKET_ICON)
                            if icon_obj is None:
                                print(f'[ICON {init_hour}Z] missing: {file_icon}')
                                continue

                            with xr.open_dataset(io.BytesIO(icon_obj)) as ds_icon:
                                required_icon = cloud_prm_icon + [cma_icon]
                                missing_icon = [v for v in required_icon if v not in ds_icon]
                                if missing_icon:
                                    print(f'[ICON {init_hour}Z] skipping {file_icon}: missing variables {missing_icon}')
                                    continue

                                ds_icon_var = ds_icon[cloud_prm_icon]
                                ds_icon_mask = ds_icon[[cma_icon]]

                                icon_timestamps = select_timestamps(
                                    ds_icon_var.time.values,
                                    file_date=day_str,
                                    hour_start=hour_start,
                                    hour_end=hour_end,
                                    include_next_day_midnight=(init_hour == '00'),
                                )

                                selected_icon_timestamp = None
                                for candidate in icon_timestamps:
                                    c_date = str(candidate).split('T')[0]
                                    c_hour = str(candidate).split('T')[1][0:2]
                                    if c_date == t_date and c_hour == t_hour:
                                        selected_icon_timestamp = candidate
                                        break

                                if selected_icon_timestamp is None:
                                    continue

                                ds_icon_time_var = filter_by_time(ds_icon_var, selected_icon_timestamp)
                                ds_icon_time_mask = filter_by_time(ds_icon_mask, selected_icon_timestamp)

                                icon_before_mask = ds_icon_time_var[cloud_prm_icon[0]].copy(deep=True)

                                is_all_nan_icon = all(
                                    xr.DataArray.isnull(ds_icon_time_var[var]).all() for var in ds_icon_time_var.data_vars
                                )
                                is_outside_icon_range = any(
                                    ((ds_icon_time_var[var] < value_min[i]) | (ds_icon_time_var[var] > value_max[i])).any()
                                    for i, var in enumerate(ds_icon_time_var.data_vars)
                                )
                                if is_all_nan_icon or is_outside_icon_range:
                                    continue

                                if apply_cma:
                                    ds_icon_time_var = apply_cloud_mask_threshold(
                                        ds_var=ds_icon_time_var,
                                        ds_full=ds_icon_time_mask,
                                        cloud_mask_var=cma_icon,
                                        cloud_threshold=cloud_threshold_icon,
                                        clear_sky_fill_value=clear_sky_fill_value,
                                    )

                                icon_on_msg_grid = ds_icon_time_var.interp(
                                    lon=msg_target_lon,
                                    lat=msg_target_lat,
                                    method='linear',
                                    kwargs={'fill_value': 'extrapolate'},
                                )

                                icon_crop = resample_by_extent(
                                    ds_crop=filter_by_domain(icon_on_msg_grid, crop_extent),
                                    extent=crop_extent,
                                    x_pixel=x_pixel,
                                    y_pixel=y_pixel,
                                )

                                if any(xr.DataArray.isnull(msg_crop[var]).any() for var in msg_crop.data_vars):
                                    print(f'[MSG] NaN values detected at {timestamp}; skipping')
                                    continue
                                if any(xr.DataArray.isnull(icon_crop[var]).any() for var in icon_crop.data_vars):
                                    print(f'[ICON {init_hour}Z] NaN values detected at {selected_icon_timestamp}; skipping')
                                    continue

                                if save_sanity_plot:
                                    plot_sanity_triptych_cartopy(
                                        da_before_mask=icon_before_mask,
                                        da_after_mask=ds_icon_time_var[cloud_prm_icon[0]].copy(deep=True),
                                        da_after_resample=icon_crop[cloud_prm_icon[0]].copy(deep=True),
                                        var_name=plot_var_name,
                                        out_dir=sanity_plot_outpath,
                                        plot_tag=f'{domain_name}_{day_str}_{t_hour}_{init_hour}_ICON',
                                        da_before_mask_2=msg_before_mask,
                                        da_after_mask_2=ds_time_var[cloud_prm_msg[0]].copy(deep=True),
                                        da_after_resample_2=msg_crop[cloud_prm_msg[0]].copy(deep=True),
                                        var_name_2=plot_var_name,
                                        row_label_1='ICON',
                                        row_label_2='MSG',
                                    )

                                icon_clean = icon_crop.copy(deep=True)
                                if 'SYNMSG_BT_CL_IR10.8' in icon_clean.data_vars:
                                    icon_clean = icon_clean.rename({'SYNMSG_BT_CL_IR10.8': 'IR_108'})

                                icon_clean.attrs = {}
                                for var_name in list(icon_clean.variables):
                                    icon_clean[var_name].attrs = {}

                                msg_filepath = (
                                    f'{outpath}/MSG_{cloud_prm_msg[0].split("_")[-1]}_{cma_msg}_'
                                    f'{t_date}_{t_hour}_{domain_name}.{file_extension}'
                                )
                                icon_filepath = (
                                    f'{outpath}/ICON500m_{cloud_prm_icon[0].split("_")[-1]}_{cma_icon}_'
                                    f'{day_str.replace("-", "")}_{init_hour}_{t_date}_{t_hour}_{domain_name}.{file_extension}'
                                )

                                msg_encoding = {
                                    var: {
                                        'zlib': True,
                                        'complevel': 4,
                                        'dtype': msg_crop[var].dtype.name,
                                    }
                                    for var in msg_crop.data_vars
                                }
                                icon_encoding = {
                                    var: {
                                        'zlib': True,
                                        'complevel': 9,
                                        'dtype': icon_clean[var].dtype.name,
                                    }
                                    for var in icon_clean.data_vars
                                }

                                msg_crop.to_netcdf(msg_filepath, encoding=msg_encoding, engine='h5netcdf')
                                icon_clean.to_netcdf(icon_filepath, encoding=icon_encoding, engine='h5netcdf')

                                print(f'[MSG] saved: {msg_filepath}')
                                print(f'[ICON {init_hour}Z] saved: {icon_filepath}')
