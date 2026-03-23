import os
import io
import boto3
import logging
from botocore.exceptions import ClientError
from botocore.config import Config
import xarray as xr
import numpy as np
import sys
from scipy.ndimage import binary_closing

sys.path.append('/home/Daniele/codes/ICON-GLORI/teamx/')
from credentials_buckets import S3_BUCKET_ICON, S3_BUCKET_MSG, S3_ACCESS_KEY, S3_SECRET_ACCESS_KEY, S3_ENDPOINT_URL

sys.path.append('/home/Daniele/codes/ML_data_generator/')
from cropping_functions import filter_by_domain, filter_by_time


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
    """Create a 3-panel Cartopy plot (1 row) or 2x3-panel plot when MSG data is provided."""
    try:
        import matplotlib.pyplot as plt
        import cartopy.crs as ccrs
        import cartopy.feature as cfeature
        from cartopy.mpl.ticker import LongitudeFormatter, LatitudeFormatter
    except ImportError as exc:
        print(f"Skipping sanity plot (missing plotting dependency): {exc}")
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
        print(f"Skipping sanity plot for {plot_tag}: no finite values")
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
                ax.set_ylabel(f"{row_label}\n({rv_name})", fontsize=8)

    cbar = fig.colorbar(mappable, ax=axes, orientation='horizontal', fraction=0.05, pad=0.08)
    cbar.set_label(var_name)
    fig.suptitle(plot_tag, y=1.00)

    fig_path = f"{out_dir}/sanity_{plot_tag}.png"
    fig.savefig(fig_path, dpi=150, bbox_inches='tight')
    plt.close(fig)
    print(f"Sanity plot saved: {fig_path}")


def resample_by_extent(
    ds_crop,
    extent,
    x_pixel,
    y_pixel,
):
    """
    Resample a pre-filtered dataset to target pixel size over the provided extent.
    """
    lonmin, lonmax, latmin, latmax = extent

    if ds_crop.sizes.get('lon', 0) == 0 or ds_crop.sizes.get('lat', 0) == 0:
        raise ValueError(f"Empty crop for extent: {extent}")

    def _prepare_unique_coord(ds_in, dim_name):
        """Keep finite coordinates, sort them, and drop duplicates for safe interpolation."""
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

    # xarray interp requires unique indexes on interpolation coordinates.
    ds_crop = _prepare_unique_coord(ds_crop, 'lon')
    ds_crop = _prepare_unique_coord(ds_crop, 'lat')

    # Use overlap between requested extent and actual available coordinate range
    # to avoid out-of-bounds NaNs during interpolation.
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
            f"No overlap between requested extent {extent} and source bounds "
            f"lon[{src_lon_min}, {src_lon_max}] lat[{src_lat_min}, {src_lat_max}]"
        )

    # Coordinates are sorted above, so target grid is built in ascending order.
    target_lon = np.linspace(lon_lo, lon_hi, x_pixel)
    target_lat = np.linspace(lat_lo, lat_hi, y_pixel)

    ds_out = ds_crop.interp(
        lon=target_lon,
        lat=target_lat,
        method='nearest',
        kwargs={'fill_value': 'extrapolate'},
    )

    # Fail fast with useful context if output still collapses to NaN.
    if all(xr.DataArray.isnull(ds_out[v]).all() for v in ds_out.data_vars):
        raise ValueError(
            "Resampling produced all-NaN output. "
            f"Requested extent: {extent}; source bounds: "
            f"lon[{src_lon_min}, {src_lon_max}] lat[{src_lat_min}, {src_lat_max}]"
        )

    #print(ds_out)

    return ds_out


def apply_cloud_mask_threshold(
    ds_var,
    ds_full,
    cloud_mask_var='CLCT',
    cloud_threshold=50.0,
    clear_sky_fill_value=320.0,
):
    """
    Apply threshold-based cloud mask on a resampled dataset.

    Masking rule:
    - cloud_mask > cloud_threshold: keep original value (cloudy)
    - cloud_mask <= cloud_threshold: set to clear_sky_fill_value (clear sky)
    """
    if cloud_mask_var not in ds_full:
        raise ValueError(f"Cloud mask variable '{cloud_mask_var}' not found in dataset")

    # Nearest-neighbor mask interpolation keeps mask semantics.
    mask_rs = ds_full[cloud_mask_var]
    cloudy_mask = mask_rs > cloud_threshold

    for var in ds_var.data_vars:
        ds_var[var] = ds_var[var].where(cloudy_mask, clear_sky_fill_value)

    return ds_var.fillna(clear_sky_fill_value)



def read_file(s3, file_name, bucket):
    """Upload a file to an S3 bucket
    :param s3: Initialized S3 client object
    :param file_name: File to upload
    :param bucket: Bucket to upload to
    :return: object if file was uploaded, else False
    """
    try:
        #with open(file_name, "rb") as f:
        obj = s3.get_object(Bucket=bucket, Key=file_name)
        #print(obj)
        myObject = obj['Body'].read()
    except ClientError as e:
        logging.error(e)
        return None
    return myObject


# Initialize the S3 client
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

# List the objects in our bucket
debug_list_bucket = False
if debug_list_bucket:
    print(f"Bucket contents before upload:")
    response = s3.list_objects(Bucket=S3_BUCKET_ICON)
    for item in response['Contents']:
        print(item['Key'])


# Directory and source settings
years = [2025]
months = range(4, 5)
days = range(1, 32)

icon_basename = "merged_SYNMSG_BT_CL_IR10.8_CLCT"
icon_initialization_hour = '00'

msg_path_dir = "/data/sat/msg/ml_train_crops/IR_108-WV_062-CMA_FULL_EXPATS_DOMAIN"
msg_basename = "merged_MSG_CMSAF"

# Extent-based cropping settings (lonmin, lonmax, latmin, latmax)
crop_extent = (11.0, 15.0, 45.0, 49.0) #(7.0, 11.0, 45.0, 49.0) #(11.0, 15.0, 45.0, 49.0) #(9.0, 13.0, 45.0, 49.0)
domain_name =  'east' #'west' #'east' #'central'
x_pixel = 100
y_pixel = 100

# Hour range [hour_start, hour_end)
hour_start = '01'
hour_end = '24'

# Value range checks
value_min = [180.0]
value_max = [320.0]

# Cloud mask settings


cloud_prm_icon = ['SYNMSG_BT_CL_IR10.8']
cloud_prm_msg = ['IR_108']

apply_cma = True
cma_icon = 'CLCT'
cma_msg = 'cma'
cloud_threshold_icon = 50.0
cloud_threshold_msg = 0
clear_sky_fill_value = 320.0

file_extension = 'nc'
save_sanity_plot = False
verbose = False


def vprint(*args, **kwargs):
    if verbose:
        print(*args, **kwargs)


def sanitize_timestamp(ts):
    return str(ts).split('.')[0].replace('T', '_').replace(':', '')


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


def process_day_dataset(ds_day, source_tag, cloud_prm, cma_var, cma_th, file_date, init_label):
    panels_by_hour = {}
    ds_day_var = None
    ds_day_mask = None
    ds_time_var = None
    ds_time_mask = None
    ds_extent_crop = None

    try:
        ds_day_var = ds_day[cloud_prm]
        ds_day_mask = ds_day[[cma_var]]

        ds_day_var = filter_by_domain(ds_day_var, crop_extent)
        ds_day_mask = filter_by_domain(ds_day_mask, crop_extent)

        timestamps = ds_day_var.time.values
        # ICON keeps previous behavior (include next-day 00), MSG stays on day/hour range.
        timestamps = select_timestamps(
            timestamps=timestamps,
            file_date=file_date,
            hour_start=hour_start,
            hour_end=hour_end,
            include_next_day_midnight=(source_tag == 'ICON'),
        )

        for timestamp in timestamps:
            vprint(f"[{source_tag}] Processing timestamp: {timestamp}")
            t_str = str(timestamp)
            t_date = t_str.split('T')[0]
            t_hour = t_str.split('T')[1][0:2]
            # MSG: only process whole-hour timestamps
            if source_tag == 'MSG' and t_str.split('T')[1][3:5] != '00':
                continue
            ds_extent_crop = None
            ds_time_var = None
            ds_time_mask = None

            try:
                ds_time_var = filter_by_time(ds_day_var, timestamp)
                ds_time_mask = filter_by_time(ds_day_mask, timestamp)

                is_all_nan_ds = all(xr.DataArray.isnull(ds_time_var[var]).all() for var in ds_time_var.data_vars)
                is_outside_range = any(
                    ((ds_time_var[var] < value_min[i]) | (ds_time_var[var] > value_max[i])).any()
                    for i, var in enumerate(ds_time_var.data_vars)
                )

                if is_all_nan_ds or is_outside_range:
                    continue

                da_before_mask = None
                if save_sanity_plot:
                    da_before_mask = ds_time_var[cloud_prm[0]].copy(deep=True)

                if apply_cma:
                    ds_time_var = apply_cloud_mask_threshold(
                        ds_var=ds_time_var,
                        ds_full=ds_time_mask,
                        cloud_mask_var=cma_var,
                        cloud_threshold=cma_th,
                        clear_sky_fill_value=clear_sky_fill_value,
                    )

                da_after_mask = ds_time_var[cloud_prm[0]]
                ds_extent_crop = resample_by_extent(
                    ds_crop=ds_time_var,
                    extent=crop_extent,
                    x_pixel=x_pixel,
                    y_pixel=y_pixel,
                )

                if save_sanity_plot:
                    panels_by_hour[f"{t_date}_{t_hour}"] = (
                        da_before_mask,
                        ds_time_var[cloud_prm[0]].copy(deep=True),
                        ds_extent_crop[cloud_prm[0]].copy(deep=True),
                        cloud_prm[0],
                    )

                has_nan = any(xr.DataArray.isnull(ds_extent_crop[var]).any() for var in ds_extent_crop.data_vars)
                if has_nan:
                    print(f"[{source_tag}] NaN values detected at {timestamp}; skipping")
                    continue

                if source_tag == 'ICON':
                    filepath = (
                        f"{outpath}/ICON500m_{cloud_prm[0].split('_')[-1]}_{cma_var}_"
                        f"{file_date.replace('-', '')}_{init_label}_{t_date}_{t_hour}_{domain_name}.{file_extension}"
                    )
                else:
                    filepath = (
                        f"{outpath}/MSG_{cloud_prm[0].split('_')[-1]}_{cma_var}_"
                        f"{t_date}_{t_hour}_{domain_name}.{file_extension}"
                    )
                encoding = {
                    var: {
                        'zlib': True,
                        'complevel': 4,
                        'dtype': ds_extent_crop[var].dtype.name,
                    }
                    for var in ds_extent_crop.data_vars
                }
                ds_extent_crop.to_netcdf(filepath, encoding=encoding, engine='h5netcdf')
                print(f"[{source_tag}] saved: {filepath}")

            finally:
                if ds_extent_crop is not None:
                    ds_extent_crop.close()
                if ds_time_mask is not None:
                    ds_time_mask.close()
                if ds_time_var is not None:
                    ds_time_var.close()

        return panels_by_hour

    finally:
        if ds_day_mask is not None:
            ds_day_mask.close()
        if ds_day_var is not None:
            ds_day_var.close()


outpath = f'/data1/crops/teamx_Apr-Sep_2025_icon_msg/{file_extension}/1'
os.makedirs(outpath, exist_ok=True)
sanity_plot_outpath = "/data1/crops/teamx_Apr-Sep_2025_icon_msg/sanity_plots"

for year in years:
    for month in months:
        month = f"{month:02d}"
        for day in days:
            day_str = f"{year:04d}-{month}-{day:02d}"

            file_icon = f"{icon_basename}_{year:04d}{month}{day:02d}_{icon_initialization_hour}.nc"
            file_msg = f"{msg_path_dir}/{year:04d}/{month}/{msg_basename}_{year:04d}-{month}-{day:02d}.nc"

            print(f"Day {day_str} | ICON: {file_icon} | MSG: {file_msg}")

            my_obj_icon = read_file(s3, file_icon, S3_BUCKET_ICON)
            my_obj_msg = read_file(s3, file_msg, S3_BUCKET_MSG)

            if my_obj_icon is not None:
                icon_panels = {}
                ds_icon = None
                try:
                    ds_icon = xr.open_dataset(io.BytesIO(my_obj_icon))
                    required_icon = cloud_prm_icon + [cma_icon]
                    missing_icon = [v for v in required_icon if v not in ds_icon]
                    if missing_icon:
                        print(f"[ICON] skipping {file_icon}: missing variables {missing_icon}")
                    else:
                        icon_panels = process_day_dataset(
                            ds_day=ds_icon,
                            source_tag='ICON',
                            cloud_prm=cloud_prm_icon,
                            cma_var=cma_icon,
                            cma_th=cloud_threshold_icon,
                            file_date=day_str,
                            init_label=icon_initialization_hour,
                        ) or {}
                finally:
                    if ds_icon is not None:
                        ds_icon.close()
                    del my_obj_icon
            else:
                print(f"[ICON] missing: {file_icon}")

            if my_obj_msg is not None:
                msg_panels = {}
                ds_msg = None
                try:
                    ds_msg = xr.open_dataset(io.BytesIO(my_obj_msg))
                    required_msg = cloud_prm_msg + [cma_msg]
                    missing_msg = [v for v in required_msg if v not in ds_msg]
                    if missing_msg:
                        print(f"[MSG] skipping {file_msg}: missing variables {missing_msg}")
                    else:
                        #apply closing algorithm (structure 3x3 to cma variable only)
                        cma_values = ds_msg['cma'].values
                        closed_cma = binary_closing(cma_values, structure=np.ones((1, 3, 3), dtype=np.uint8))
                        ds_msg['cma'] = (('time', 'lat', 'lon'), closed_cma)
                        msg_panels = process_day_dataset(
                            ds_day=ds_msg,
                            source_tag='MSG',
                            cloud_prm=cloud_prm_msg,
                            cma_var=cma_msg,
                            cma_th=cloud_threshold_msg,
                            file_date=day_str,
                            init_label='hourly',
                        ) or {}
                finally:
                    if ds_msg is not None:
                        ds_msg.close()
                    del my_obj_msg
            else:
                print(f"[MSG] missing: {file_msg}")

            if save_sanity_plot and (icon_panels or msg_panels):
                for hour_key in sorted(set(icon_panels) | set(msg_panels)):
                    ip = icon_panels.get(hour_key)
                    mp = msg_panels.get(hour_key)
                    if ip and mp:
                        plot_sanity_triptych_cartopy(
                            da_before_mask=ip[0], da_after_mask=ip[1], da_after_resample=ip[2],
                            var_name=ip[3],
                            da_before_mask_2=mp[0], da_after_mask_2=mp[1], da_after_resample_2=mp[2],
                            var_name_2=mp[3],
                            out_dir=sanity_plot_outpath,
                            plot_tag=f"{day_str}_{hour_key}",
                        )
                    elif ip:
                        plot_sanity_triptych_cartopy(
                            da_before_mask=ip[0], da_after_mask=ip[1], da_after_resample=ip[2],
                            var_name=ip[3],
                            out_dir=sanity_plot_outpath,
                            plot_tag=f"ICON_{day_str}_{hour_key}",
                        )
                    elif mp:
                        plot_sanity_triptych_cartopy(
                            da_before_mask=mp[0], da_after_mask=mp[1], da_after_resample=mp[2],
                            var_name=mp[3],
                            out_dir=sanity_plot_outpath,
                            plot_tag=f"MSG_{day_str}_{hour_key}",
                        )

#nohup 2050506

#east 2052445

#west 2052974