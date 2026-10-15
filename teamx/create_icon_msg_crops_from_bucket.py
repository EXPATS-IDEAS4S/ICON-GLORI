"""Generate hourly cropped ICON/MSG datasets for ML-ready training samples.

Workflow summary:
1. Connect to the configured S3 buckets and load daily MSG and ICON NetCDF files.
2. Select target variables, apply temporal selection, and keep the configured crop domains.
3. Run quality checks to reject problematic data before writing outputs.
4. Apply cloud-mask logic and spatial resampling on accepted timestamps.
5. Save per-timestamp crops to NetCDF and optionally produce sanity plots.
6. Write a text report with sample-level checks and skip reasons.

Quality-control logic:
- Missing-in-bucket and unreadable/corrupted file detection.
- Required-variable checks for MSG and ICON.
- ICON file-level NaN fraction screening (skip file when threshold is exceeded).
- ICON per-timestamp stripe-artifact screening using spectral/autocorrelation and
    north-vs-south consistency heuristics.
- Per-timestamp range checks and post-resample NaN checks.
- MSG round-hour filtering to align with hourly output expectations.

Outputs:
- Cropped NetCDF files for MSG and ICON in the configured output directory.
- Optional sanity plots (before mask, after mask, after resampling).
- A plain-text sample-check report listing missing timestamps and reasons for
    skipped samples.

Notes:
- The script is configuration-driven; edit the parameter block near the top of
    the file to change dates, domains, thresholds, and paths.
- The processing loops are domain/day/init based and are designed to continue on
    failures while logging the reason for each skipped sample.
"""

import os
import io
from datetime import date
from collections import Counter
import boto3
import logging
from botocore.exceptions import ClientError
from botocore.config import Config
import xarray as xr
import numpy as np
import sys
from scipy.ndimage import binary_closing

sys.path.append('/home/Daniele/codes/ICON-GLORI/')
from credentials_buckets_old import S3_BUCKET_ICON, S3_BUCKET_MSG, S3_ACCESS_KEY, S3_SECRET_ACCESS_KEY, S3_ENDPOINT_URL

sys.path.append('/home/Daniele/codes/ML_data_generator/')
from cropping_functions import filter_by_domain, filter_by_time


# ==========================
# User configuration
# ==========================

# S3 client behavior
s3_connect_timeout = 10
s3_read_timeout = 120
s3_retries_max_attempts = 3
s3_retries_mode = 'standard'
debug_list_bucket = False

# Directory and source settings
years = [2025]
months = range(4, 10)
days = range(1, 32)

icon_basename = "merged_SYNMSG_BT_CL_IR10.8_CLCT"
icon_initialization_hours = ['00', '12']

msg_path_dir = "/data/sat/msg/ml_train_crops/IR_108-WV_062-CMA_FULL_EXPATS_DOMAIN"
msg_basename = "merged_MSG_CMSAF"

# Source toggles
process_msg_data = True
process_icon_data = False
skip_existing_crops = True

# ICON forecast handling
icon_process_all_timestamps = True
icon_expected_forecast_steps = 48

# Extent-based cropping settings (lonmin, lonmax, latmin, latmax)
crop_domains = [
    ((11.0, 15.0, 45.0, 49.0), 'east'),
    ((7.0, 11.0, 45.0, 49.0), 'west'),
    ((9.0, 13.0, 45.0, 49.0), 'center'),
]
x_pixel = 100
y_pixel = 100

# Hour range [hour_start, hour_end)
hour_start = '00'
hour_end = '01'

# Value range checks
value_min = [180.0]
value_max = [320.0]

# Cloud mask settings
cloud_prm_icon = ['SYNMSG_BT_CL_IR10.8']
cloud_prm_msg = ['IR_108']
plot_var_name = 'IR_108'

apply_cma = True
cma_icon = 'CLCT'
cma_msg = 'cma'
cloud_threshold_icon = 50.0
cloud_threshold_msg = 0
clear_sky_fill_value = 320.0

# ICON stripe-artifact quality gate (pre-processing, per timestamp)
enable_icon_stripe_check = True
stripe_peak_ratio_threshold = 10.0
stripe_jump_score_threshold = 2.8
stripe_autocorr_threshold = 0.35
stripe_north_lat_threshold = 47.0
stripe_north_vs_south_jump_ratio_threshold = 1.4
stripe_half_frequency_diff_threshold = 0.08
stripe_half_frequency_min_peak_ratio = 4.0

# Output / logging
file_extension = 'nc'
save_sanity_plot = False
verbose = False
sample_check_log_path = './logs/sample_checks_report.txt'
outpath = f'/data1/crops/teamx_Apr-Sep_2025_icon_msg_new/{file_extension}/1'
msg_outpath = f'/data1/crops/teamx_Apr-Sep_2025_icon_msg_new/{file_extension}/MSG/1'
icon_outpath_by_init = {
    init_hour: f'/data1/crops/teamx_Apr-Sep_2025_icon_msg_new/{file_extension}/ICON_{init_hour}/1'
    for init_hour in icon_initialization_hours
}
sanity_plot_outpath = "/data1/crops/teamx_Apr-Sep_2025_icon_msg_new/sanity_plots"


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
        connect_timeout=s3_connect_timeout,
        read_timeout=s3_read_timeout,
        retries={'max_attempts': s3_retries_max_attempts, 'mode': s3_retries_mode},
    ),
)

# List the objects in our bucket
if debug_list_bucket:
    print(f"Bucket contents before upload:")
    response = s3.list_objects(Bucket=S3_BUCKET_ICON)
    for item in response['Contents']:
        print(item['Key'])


def vprint(*args, **kwargs):
    if verbose:
        print(*args, **kwargs)


def sanitize_timestamp(ts):
    return str(ts).split('.')[0].replace('T', '_').replace(':', '')


def build_expected_hour_keys(file_date, hour_start, hour_end, include_next_day_midnight=False):
    out = [f"{file_date}_{hour:02d}" for hour in range(int(hour_start), int(hour_end))]
    if include_next_day_midnight:
        next_date = str(np.datetime64(file_date) + np.timedelta64(1, 'D'))
        out.append(f"{next_date}_00")
    return out


def build_icon_forecast_hour_keys(file_date, init_hour, forecast_steps=48):
    """Build expected ICON forecast keys (YYYY-MM-DD_HH) for one init.

    Lead-time convention is 01..N (no +00 forecast step).
    """
    init_ts = np.datetime64(f"{file_date}T{init_hour}:00")
    out = []
    for step in range(1, int(forecast_steps) + 1):
        ts = init_ts + np.timedelta64(step, 'h')
        ts_str = np.datetime_as_string(ts, unit='h')
        date_part, hour_part = ts_str.split('T')
        out.append(f"{date_part}_{hour_part}")
    return out


def compute_icon_lead_hour(file_date, init_hour, valid_date, valid_hour):
    """Compute ICON lead time in hours from init timestamp to valid timestamp."""
    init_ts = np.datetime64(f"{file_date}T{init_hour}:00")
    valid_ts = np.datetime64(f"{valid_date}T{valid_hour}:00")
    return int((valid_ts - init_ts) / np.timedelta64(1, 'h'))


def record_event(event_log, day, domain, source, reason, timestamp=None, init_hour=None, details=None):
    event_log.append(
        {
            'day': day,
            'domain': domain,
            'source': source,
            'init_hour': init_hour,
            'timestamp': timestamp,
            'reason': reason,
            'details': details or '',
        }
    )


def set_skip_reason(skip_reason_by_key, source, domain, day, hour_key, reason):
    key = (source, domain, day, hour_key)
    if key not in skip_reason_by_key:
        skip_reason_by_key[key] = reason


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


def apply_msg_cma_closing_3x3(ds_msg, cma_var='cma'):
    """Apply 2D 3x3 binary closing on MSG CMA, one time slice at a time."""
    if cma_var not in ds_msg:
        return ds_msg

    cma_da = ds_msg[cma_var]
    structure = np.ones((3, 3), dtype=np.uint8)

    if 'time' in cma_da.dims:
        closed_slices = []
        for i in range(cma_da.sizes['time']):
            cma_2d = cma_da.isel(time=i).values
            closed_slices.append(binary_closing(cma_2d, structure=structure))
        closed_cma = np.stack(closed_slices, axis=0)
        ds_msg[cma_var] = xr.DataArray(
            closed_cma,
            dims=cma_da.dims,
            coords=cma_da.coords,
        )
    else:
        closed_2d = binary_closing(cma_da.values, structure=structure)
        ds_msg[cma_var] = xr.DataArray(
            closed_2d,
            dims=cma_da.dims,
            coords=cma_da.coords,
        )

    return ds_msg


def has_any_nan(ds_in):
    return any(xr.DataArray.isnull(ds_in[var]).any() for var in ds_in.data_vars)


def find_variable_with_high_nan_fraction(ds_in, min_nan_fraction=0.10):
    """Return (var_name, nan_fraction) if any data variable has at least the threshold of NaNs."""
    for var_name in ds_in.data_vars:
        da = ds_in[var_name]
        if da.size == 0:
            continue
        nan_fraction = float(da.isnull().mean().values)
        if nan_fraction >= min_nan_fraction:
            return var_name, nan_fraction
    return None


def detect_regular_stripe_artifact(
    da_in,
    peak_ratio_threshold=10.0,
    jump_score_threshold=2.8,
    autocorr_threshold=0.35,
    north_lat_threshold=47.0,
    north_vs_south_jump_ratio_threshold=1.4,
    half_frequency_diff_threshold=0.08,
    half_frequency_min_peak_ratio=4.0,
):
    """Heuristic detector for regular stripe artifacts on 2D fields.

    Returns (is_bad, reason). A frame is flagged when a strong periodic peak
    appears along rows or columns together with unusually strong line-to-line
    jumps in that same direction.
    """
    arr = np.asarray(da_in.values, dtype=np.float64)
    arr2d = np.squeeze(arr)
    if arr2d.ndim != 2:
        return False, "not_2d"

    finite = np.isfinite(arr2d)
    if not finite.any():
        return True, "all_non_finite"

    valid = arr2d[finite]
    med = float(np.nanmedian(valid))

    work = arr2d.copy()
    work[~finite] = med

    # Focus on north area where artifacts are known to happen.
    nrows = work.shape[0]
    north_rows = None
    lat_name = 'lat' if 'lat' in da_in.coords else None
    if lat_name is not None:
        lat_vals = np.asarray(da_in[lat_name].values)
        if lat_vals.ndim == 1 and lat_vals.size == nrows:
            north_rows = lat_vals >= north_lat_threshold
        elif lat_vals.ndim == 2 and lat_vals.shape[0] == nrows:
            north_rows = np.nanmean(lat_vals, axis=1) >= north_lat_threshold

    if north_rows is None or int(np.sum(north_rows)) < max(8, nrows // 5):
        north_rows = np.zeros(nrows, dtype=bool)
        north_rows[: max(8, nrows // 2)] = True

    south_rows = ~north_rows
    if int(np.sum(south_rows)) < 8:
        south_rows = np.zeros(nrows, dtype=bool)

    north_work = work[north_rows, :]

    north_valid = north_work[np.isfinite(north_work)]
    if north_valid.size == 0:
        return True, "north_region_non_finite"

    north_med = float(np.nanmedian(north_valid))
    north_mad = float(np.nanmedian(np.abs(north_valid - north_med)))
    robust_scale = north_mad * 1.4826 + 1e-6

    def _spectral_peak_ratio(signal_1d):
        sig = np.asarray(signal_1d, dtype=np.float64)
        if sig.size < 8:
            return 0.0
        sig = sig - float(np.mean(sig))
        spec = np.abs(np.fft.rfft(sig))[1:]
        if spec.size < 3:
            return 0.0
        return float(np.max(spec) / (np.median(spec) + 1e-6))

    def _dominant_frequency(signal_1d):
        """Return dominant normalized frequency in (0, 0.5], or None if unavailable."""
        sig = np.asarray(signal_1d, dtype=np.float64)
        n = sig.size
        if n < 8:
            return None
        sig = sig - float(np.mean(sig))
        spec = np.abs(np.fft.rfft(sig))[1:]
        if spec.size < 3:
            return None
        idx = int(np.argmax(spec)) + 1
        return idx / n

    def _autocorr_max(signal_1d):
        sig = np.asarray(signal_1d, dtype=np.float64)
        if sig.size < 16:
            return 0.0
        sig = sig - float(np.mean(sig))
        denom = float(np.dot(sig, sig))
        if denom <= 1e-12:
            return 0.0
        ac = np.correlate(sig, sig, mode='full')[sig.size - 1:] / denom
        if ac.size <= 4:
            return 0.0
        # Ignore tiny lags and read strongest periodic recurrence.
        return float(np.max(ac[3:]))

    row_signal = np.mean(north_work, axis=1)
    col_signal = np.mean(north_work, axis=0)

    if int(np.sum(south_rows)) >= 8:
        south_work = work[south_rows, :]
        south_row_signal = np.mean(south_work, axis=1)
        south_col_signal = np.mean(south_work, axis=0)
    else:
        south_work = None
        south_row_signal = None
        south_col_signal = None

    row_peak = _spectral_peak_ratio(row_signal)
    col_peak = _spectral_peak_ratio(col_signal)
    south_row_peak = _spectral_peak_ratio(south_row_signal) if south_row_signal is not None else 0.0
    south_col_peak = _spectral_peak_ratio(south_col_signal) if south_col_signal is not None else 0.0

    row_dom_freq_n = _dominant_frequency(row_signal)
    row_dom_freq_s = _dominant_frequency(south_row_signal) if south_row_signal is not None else None
    col_dom_freq_n = _dominant_frequency(col_signal)
    col_dom_freq_s = _dominant_frequency(south_col_signal) if south_col_signal is not None else None

    row_freq_diff = (
        abs(row_dom_freq_n - row_dom_freq_s)
        if row_dom_freq_n is not None and row_dom_freq_s is not None
        else 0.0
    )
    col_freq_diff = (
        abs(col_dom_freq_n - col_dom_freq_s)
        if col_dom_freq_n is not None and col_dom_freq_s is not None
        else 0.0
    )

    row_freq_mismatch = (
        row_dom_freq_n is not None
        and row_dom_freq_s is not None
        and row_peak >= half_frequency_min_peak_ratio
        and south_row_peak >= half_frequency_min_peak_ratio
        and row_freq_diff >= half_frequency_diff_threshold
    )
    col_freq_mismatch = (
        col_dom_freq_n is not None
        and col_dom_freq_s is not None
        and col_peak >= half_frequency_min_peak_ratio
        and south_col_peak >= half_frequency_min_peak_ratio
        and col_freq_diff >= half_frequency_diff_threshold
    )

    half_freq_mismatch = row_freq_mismatch or col_freq_mismatch
    row_ac = _autocorr_max(row_signal)
    col_ac = _autocorr_max(col_signal)

    row_jump = float(np.median(np.abs(np.diff(north_work, axis=0))) / robust_scale)
    col_jump = float(np.median(np.abs(np.diff(north_work, axis=1))) / robust_scale)

    if int(np.sum(south_rows)) >= 8:
        south_valid = south_work[np.isfinite(south_work)]
        south_med = float(np.nanmedian(south_valid)) if south_valid.size else north_med
        south_mad = float(np.nanmedian(np.abs(south_valid - south_med))) if south_valid.size else north_mad
        south_scale = south_mad * 1.4826 + 1e-6
        south_row_jump = float(np.median(np.abs(np.diff(south_work, axis=0))) / south_scale)
    else:
        south_row_jump = max(1.0, row_jump / north_vs_south_jump_ratio_threshold)

    periodic_row = row_peak >= peak_ratio_threshold or row_ac >= autocorr_threshold
    periodic_col = col_peak >= peak_ratio_threshold or col_ac >= autocorr_threshold

    bad_row = (
        periodic_row
        and row_jump >= jump_score_threshold
        and row_jump >= south_row_jump * north_vs_south_jump_ratio_threshold
    )
    bad_col = periodic_col and col_jump >= jump_score_threshold

    if bad_row or bad_col or half_freq_mismatch:
        reason = (
            f"row_peak={row_peak:.2f}, col_peak={col_peak:.2f}, "
            f"row_ac={row_ac:.2f}, col_ac={col_ac:.2f}, "
            f"row_jump={row_jump:.2f}, col_jump={col_jump:.2f}, "
            f"south_row_jump={south_row_jump:.2f}, "
            f"row_fdiff={row_freq_diff:.3f}, col_fdiff={col_freq_diff:.3f}"
        )
        return True, reason

    return False, "ok"


def is_round_hour_timestamp(timestamp) -> bool:
    """Return True only for exact HH:00:00 timestamps."""
    try:
        ts_str = np.datetime_as_string(np.datetime64(timestamp, 's'), unit='s')
    except Exception:
        ts_str = str(timestamp)

    if 'T' in ts_str:
        time_part = ts_str.split('T', 1)[1]
    elif ' ' in ts_str:
        time_part = ts_str.split(' ', 1)[1]
    else:
        return False

    hhmmss = time_part.split('+', 1)[0].split('Z', 1)[0]
    parts = hhmmss.split(':')
    if len(parts) < 3:
        return False

    minute = parts[1]
    second = parts[2].split('.', 1)[0]
    return minute == '00' and second == '00'


def process_day_dataset(
    ds_day,
    source_tag,
    cloud_prm,
    cma_var,
    cma_th,
    file_date,
    crop_extent,
    include_next_day_midnight=False,
    domain_name=None,
    init_hour=None,
    event_log=None,
    skip_reason_by_key=None,
    select_all_timestamps=False,
):
    panels_by_hour = {}
    crops_by_hour = {}
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
        if not select_all_timestamps:
            timestamps = select_timestamps(
                timestamps=timestamps,
                file_date=file_date,
                hour_start=hour_start,
                hour_end=hour_end,
                include_next_day_midnight=include_next_day_midnight,
            )

        for timestamp in timestamps:
            vprint(f"[{source_tag}] Processing timestamp: {timestamp}")
            t_str = str(timestamp)
            t_date = t_str.split('T')[0]
            t_hour = t_str.split('T')[1][0:2]
            # MSG: keep only exact whole-hour timestamps to match ICON hourly cadence.
            if source_tag == 'MSG' and not is_round_hour_timestamp(timestamp):
                print(f"[MSG] non-round timestamp {timestamp}; skipping")
                hour_key = f"{t_date}_{t_hour}"
                if event_log is not None and domain_name is not None:
                    record_event(
                        event_log,
                        day=file_date,
                        domain=domain_name,
                        source=source_tag,
                        init_hour=init_hour,
                        timestamp=hour_key,
                        reason='non_round_timestamp',
                    )
                if skip_reason_by_key is not None and domain_name is not None:
                    set_skip_reason(skip_reason_by_key, source_tag, domain_name, file_date, hour_key, 'non_round_timestamp')
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
                    hour_key = f"{t_date}_{t_hour}"
                    reason = 'all_nan_after_time_filter' if is_all_nan_ds else 'outside_value_range'
                    if event_log is not None and domain_name is not None:
                        record_event(
                            event_log,
                            day=file_date,
                            domain=domain_name,
                            source=source_tag,
                            init_hour=init_hour,
                            timestamp=hour_key,
                            reason=reason,
                        )
                    if skip_reason_by_key is not None and domain_name is not None:
                        set_skip_reason(skip_reason_by_key, source_tag, domain_name, file_date, hour_key, reason)
                    continue

                if source_tag == 'ICON' and enable_icon_stripe_check:
                    stripe_bad, stripe_reason = detect_regular_stripe_artifact(
                        ds_time_var[cloud_prm[0]],
                        peak_ratio_threshold=stripe_peak_ratio_threshold,
                        jump_score_threshold=stripe_jump_score_threshold,
                        autocorr_threshold=stripe_autocorr_threshold,
                        north_lat_threshold=stripe_north_lat_threshold,
                        north_vs_south_jump_ratio_threshold=stripe_north_vs_south_jump_ratio_threshold,
                        half_frequency_diff_threshold=stripe_half_frequency_diff_threshold,
                        half_frequency_min_peak_ratio=stripe_half_frequency_min_peak_ratio,
                    )
                    if stripe_bad:
                        print(
                            f"[ICON] stripe-like artifact at {timestamp}; skipping "
                            f"({stripe_reason})"
                        )
                        hour_key = f"{t_date}_{t_hour}"
                        if event_log is not None and domain_name is not None:
                            record_event(
                                event_log,
                                day=file_date,
                                domain=domain_name,
                                source=source_tag,
                                init_hour=init_hour,
                                timestamp=hour_key,
                                reason='stripe_artifact',
                                details=stripe_reason,
                            )
                        if skip_reason_by_key is not None and domain_name is not None:
                            set_skip_reason(skip_reason_by_key, source_tag, domain_name, file_date, hour_key, 'stripe_artifact')
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
                        plot_var_name,
                    )

                has_nan = has_any_nan(ds_extent_crop)
                if has_nan:
                    print(f"[{source_tag}] NaN values detected at {timestamp}; skipping")
                    hour_key = f"{t_date}_{t_hour}"
                    if event_log is not None and domain_name is not None:
                        record_event(
                            event_log,
                            day=file_date,
                            domain=domain_name,
                            source=source_tag,
                            init_hour=init_hour,
                            timestamp=hour_key,
                            reason='nan_after_resample',
                        )
                    if skip_reason_by_key is not None and domain_name is not None:
                        set_skip_reason(skip_reason_by_key, source_tag, domain_name, file_date, hour_key, 'nan_after_resample')
                    continue
                crops_by_hour[f"{t_date}_{t_hour}"] = ds_extent_crop.copy(deep=True)

            finally:
                if ds_extent_crop is not None:
                    ds_extent_crop.close()
                if ds_time_mask is not None:
                    ds_time_mask.close()
                if ds_time_var is not None:
                    ds_time_var.close()

        return panels_by_hour, crops_by_hour

    finally:
        if ds_day_mask is not None:
            ds_day_mask.close()
        if ds_day_var is not None:
            ds_day_var.close()


os.makedirs(outpath, exist_ok=True)
os.makedirs(msg_outpath, exist_ok=True)
for icon_output_dir in icon_outpath_by_init.values():
    os.makedirs(icon_output_dir, exist_ok=True)

sample_events = []
skip_reason_by_key = {}

for crop_extent, domain_name in crop_domains:
    for year in years:
        for month in months:
            month = f"{month:02d}"
            for day in days:
                try:
                    date(year, int(month), day)
                except ValueError:
                    vprint(f"Skipping invalid date: {year:04d}-{month}-{day:02d}")
                    continue

                day_str = f"{year:04d}-{month}-{day:02d}"
                expected_msg_hour_keys = build_expected_hour_keys(
                    file_date=day_str,
                    hour_start=hour_start,
                    hour_end=hour_end,
                    include_next_day_midnight=False,
                )
                msg_panels = {}
                msg_crops = {}
                if process_msg_data:
                    file_msg = f"{msg_path_dir}/{year:04d}/{month}/{msg_basename}_{year:04d}-{month}-{day:02d}.nc"
                    print(f"Day {day_str} | Domain {domain_name} | MSG: {file_msg}")

                    my_obj_msg = read_file(s3, file_msg, S3_BUCKET_MSG)
                    if my_obj_msg is None:
                        print(f"[MSG] missing: {file_msg}")
                        record_event(sample_events, day_str, domain_name, 'MSG', 'missing_in_bucket', details=file_msg)
                        for hour_key in expected_msg_hour_keys:
                            set_skip_reason(skip_reason_by_key, 'MSG', domain_name, day_str, hour_key, 'missing_in_bucket')
                    else:
                        ds_msg = None
                        try:
                            try:
                                ds_msg = xr.open_dataset(io.BytesIO(my_obj_msg))
                            except Exception as exc:
                                record_event(sample_events, day_str, domain_name, 'MSG', 'corrupted_or_unreadable', details=str(exc))
                                for hour_key in expected_msg_hour_keys:
                                    set_skip_reason(skip_reason_by_key, 'MSG', domain_name, day_str, hour_key, 'corrupted_or_unreadable')
                            else:
                                required_msg = cloud_prm_msg + [cma_msg]
                                missing_msg = [v for v in required_msg if v not in ds_msg]
                                if missing_msg:
                                    print(f"[MSG] skipping {file_msg}: missing variables {missing_msg}")
                                    record_event(
                                        sample_events,
                                        day_str,
                                        domain_name,
                                        'MSG',
                                        'missing_required_variables',
                                        details=','.join(missing_msg),
                                    )
                                    for hour_key in expected_msg_hour_keys:
                                        set_skip_reason(skip_reason_by_key, 'MSG', domain_name, day_str, hour_key, 'missing_required_variables')
                                else:
                                    ds_msg = apply_msg_cma_closing_3x3(ds_msg, cma_var=cma_msg)
                                    msg_panels, msg_crops = process_day_dataset(
                                        ds_day=ds_msg,
                                        source_tag='MSG',
                                        cloud_prm=cloud_prm_msg,
                                        cma_var=cma_msg,
                                        cma_th=cloud_threshold_msg,
                                        file_date=day_str,
                                        crop_extent=crop_extent,
                                        include_next_day_midnight=False,
                                        domain_name=domain_name,
                                        event_log=sample_events,
                                        skip_reason_by_key=skip_reason_by_key,
                                    )
                        finally:
                            if ds_msg is not None:
                                ds_msg.close()
                            del my_obj_msg
                else:
                    print(f"Day {day_str} | Domain {domain_name} | MSG processing disabled")

                icon_panels_by_init = {}
                icon_crops_by_init = {}
                saved_msg_hour_keys = set()
                saved_icon_hour_keys_by_init = {h: set() for h in icon_initialization_hours}
                if process_icon_data:
                    for init_hour in icon_initialization_hours:
                        expected_icon_hour_keys = build_expected_hour_keys(
                            file_date=day_str,
                            hour_start=hour_start,
                            hour_end=hour_end,
                            include_next_day_midnight=(init_hour == '00'),
                        )
                        if icon_process_all_timestamps:
                            expected_icon_hour_keys = build_icon_forecast_hour_keys(
                                file_date=day_str,
                                init_hour=init_hour,
                                forecast_steps=icon_expected_forecast_steps,
                            )
                        file_icon = f"{icon_basename}_{year:04d}{month}{day:02d}_{init_hour}.nc"
                        print(f"Day {day_str} | Domain {domain_name} | ICON {init_hour}Z: {file_icon}")

                        my_obj_icon = read_file(s3, file_icon, S3_BUCKET_ICON)
                        if my_obj_icon is None:
                            print(f"[ICON {init_hour}Z] missing: {file_icon}")
                            record_event(sample_events, day_str, domain_name, 'ICON', 'missing_in_bucket', init_hour=init_hour, details=file_icon)
                            for hour_key in expected_icon_hour_keys:
                                set_skip_reason(skip_reason_by_key, f'ICON{init_hour}', domain_name, day_str, hour_key, 'missing_in_bucket')
                            continue

                        icon_panels = {}
                        icon_crops = {}
                        ds_icon = None
                        try:
                            try:
                                ds_icon = xr.open_dataset(io.BytesIO(my_obj_icon))
                                #print(ds_icon)
                                #exit()
                            except Exception as exc:
                                record_event(sample_events, day_str, domain_name, 'ICON', 'corrupted_or_unreadable', init_hour=init_hour, details=str(exc))
                                for hour_key in expected_icon_hour_keys:
                                    set_skip_reason(skip_reason_by_key, f'ICON{init_hour}', domain_name, day_str, hour_key, 'corrupted_or_unreadable')
                                continue

                            required_icon = cloud_prm_icon + [cma_icon]
                            missing_icon = [v for v in required_icon if v not in ds_icon]
                            if missing_icon:
                                print(f"[ICON {init_hour}Z] skipping {file_icon}: missing variables {missing_icon}")
                                record_event(
                                    sample_events,
                                    day_str,
                                    domain_name,
                                    'ICON',
                                    'missing_required_variables',
                                    init_hour=init_hour,
                                    details=','.join(missing_icon),
                                )
                                for hour_key in expected_icon_hour_keys:
                                    set_skip_reason(skip_reason_by_key, f'ICON{init_hour}', domain_name, day_str, hour_key, 'missing_required_variables')
                                continue

                            high_nan = find_variable_with_high_nan_fraction(ds_icon, min_nan_fraction=0.10)
                            if high_nan is not None:
                                var_name, nan_fraction = high_nan
                                print(
                                    f"[ICON {init_hour}Z] skipping {file_icon}: "
                                    f"variable {var_name} has {nan_fraction:.2%} NaNs (threshold 10%)"
                                )
                                record_event(
                                    sample_events,
                                    day_str,
                                    domain_name,
                                    'ICON',
                                    'high_nan_fraction_in_file',
                                    init_hour=init_hour,
                                    details=f"{var_name}:{nan_fraction:.2%}",
                                )
                                for hour_key in expected_icon_hour_keys:
                                    set_skip_reason(skip_reason_by_key, f'ICON{init_hour}', domain_name, day_str, hour_key, 'high_nan_fraction_in_file')
                                continue

                            icon_panels, icon_crops = process_day_dataset(
                                ds_day=ds_icon,
                                source_tag='ICON',
                                cloud_prm=cloud_prm_icon,
                                cma_var=cma_icon,
                                cma_th=cloud_threshold_icon,
                                file_date=day_str,
                                crop_extent=crop_extent,
                                include_next_day_midnight=(init_hour == '00'),
                                domain_name=domain_name,
                                init_hour=init_hour,
                                event_log=sample_events,
                                skip_reason_by_key=skip_reason_by_key,
                                select_all_timestamps=icon_process_all_timestamps,
                            )
                            icon_panels_by_init[init_hour] = icon_panels
                            icon_crops_by_init[init_hour] = icon_crops
                        finally:
                            if ds_icon is not None:
                                ds_icon.close()
                            del my_obj_icon
                else:
                    print(f"Day {day_str} | Domain {domain_name} | ICON processing disabled")

                if process_msg_data:
                    for hour_key in sorted(msg_crops):
                        msg_crop = msg_crops[hour_key]
                        if has_any_nan(msg_crop):
                            print(f"[MSG] NaNs detected at {day_str} {hour_key} ({domain_name}); skipping")
                            record_event(sample_events, day_str, domain_name, 'MSG', 'nan_before_save', timestamp=hour_key)
                            set_skip_reason(skip_reason_by_key, 'MSG', domain_name, day_str, hour_key, 'nan_before_save')
                            continue

                        t_date, t_hour = hour_key.split('_')
                        msg_filepath = (
                            f"{msg_outpath}/MSG_{cloud_prm_msg[0].split('_')[-1]}_{cma_msg}_"
                            f"{t_date}_{t_hour}_{domain_name}.{file_extension}"
                        )
                        if skip_existing_crops and os.path.exists(msg_filepath):
                            print(f"[MSG] exists, skipping: {msg_filepath}")
                            saved_msg_hour_keys.add(hour_key)
                            continue
                        msg_encoding = {
                            var: {
                                'zlib': True,
                                'complevel': 4,
                                'dtype': msg_crop[var].dtype.name,
                            }
                            for var in msg_crop.data_vars
                        }
                        msg_crop.to_netcdf(msg_filepath, encoding=msg_encoding, engine='h5netcdf')
                        print(f"[MSG] saved: {msg_filepath}")
                        saved_msg_hour_keys.add(hour_key)

                if process_icon_data:
                    for init_hour in icon_initialization_hours:
                        icon_crops_for_init = icon_crops_by_init.get(init_hour, {})
                        for hour_key in sorted(icon_crops_for_init):
                            icon_crop = icon_crops_for_init[hour_key]
                            if has_any_nan(icon_crop):
                                print(f"[ICON {init_hour}Z] NaNs detected at {day_str} {hour_key} ({domain_name}); skipping")
                                record_event(sample_events, day_str, domain_name, 'ICON', 'nan_before_save', timestamp=hour_key, init_hour=init_hour)
                                set_skip_reason(skip_reason_by_key, f'ICON{init_hour}', domain_name, day_str, hour_key, 'nan_before_save')
                                continue

                            t_date, t_hour = hour_key.split('_')
                            lead_hour = compute_icon_lead_hour(day_str, init_hour, t_date, t_hour)
                            lead_hour_tag = f"{lead_hour:02d}"
                            icon_clean = icon_crop.copy(deep=True)
                            if 'SYNMSG_BT_CL_IR10.8' in icon_clean.data_vars:
                                icon_clean = icon_clean.rename({'SYNMSG_BT_CL_IR10.8': 'IR_108'})

                            icon_filepath = (
                                f"{icon_outpath_by_init[init_hour]}/ICON500m_IR_108_{cma_icon}_"
                                f"{day_str.replace('-', '')}_{init_hour}_{lead_hour_tag}_{t_date}_{t_hour}_{domain_name}.{file_extension}"
                            )
                            if skip_existing_crops and os.path.exists(icon_filepath):
                                print(f"[ICON {init_hour}Z] exists, skipping: {icon_filepath}")
                                saved_icon_hour_keys_by_init[init_hour].add(hour_key)
                                icon_clean.close()
                                continue
                            icon_encoding = {
                                var: {
                                    'zlib': True,
                                    'complevel': 4,
                                    'dtype': icon_clean[var].dtype.name,
                                }
                                for var in icon_clean.data_vars
                            }
                            icon_clean.to_netcdf(icon_filepath, encoding=icon_encoding, engine='h5netcdf')
                            print(f"[ICON {init_hour}Z] saved: {icon_filepath}")
                            saved_icon_hour_keys_by_init[init_hour].add(hour_key)

                            if save_sanity_plot:
                                ip = icon_panels_by_init.get(init_hour, {}).get(hour_key)
                                mp = msg_panels.get(hour_key)
                                if ip and mp:
                                    plot_sanity_triptych_cartopy(
                                        da_before_mask=ip[0], da_after_mask=ip[1], da_after_resample=ip[2],
                                        var_name=ip[3],
                                        da_before_mask_2=mp[0], da_after_mask_2=mp[1], da_after_resample_2=mp[2],
                                        var_name_2=mp[3],
                                        out_dir=sanity_plot_outpath,
                                        plot_tag=f"{domain_name}_{day_str}_{hour_key}_{init_hour}",
                                    )

                            icon_clean.close()

                if process_icon_data:
                    for icon_crops in icon_crops_by_init.values():
                        for ds_crop in icon_crops.values():
                            ds_crop.close()

                if process_msg_data:
                    for ds_crop in msg_crops.values():
                        ds_crop.close()

                # Timestamp-level missing checks after all save decisions.
                if process_msg_data:
                    for hour_key in expected_msg_hour_keys:
                        if hour_key not in saved_msg_hour_keys:
                            reason = skip_reason_by_key.get(
                                ('MSG', domain_name, day_str, hour_key),
                                'missing_timestamp_not_produced',
                            )
                            record_event(
                                sample_events,
                                day=day_str,
                                domain=domain_name,
                                source='MSG',
                                timestamp=hour_key,
                                reason='missing_timestamp',
                                details=reason,
                            )

                if process_icon_data:
                    for init_hour in icon_initialization_hours:
                        expected_icon_hour_keys = build_expected_hour_keys(
                            file_date=day_str,
                            hour_start=hour_start,
                            hour_end=hour_end,
                            include_next_day_midnight=(init_hour == '00'),
                        )
                        if icon_process_all_timestamps:
                            expected_icon_hour_keys = build_icon_forecast_hour_keys(
                                file_date=day_str,
                                init_hour=init_hour,
                                forecast_steps=icon_expected_forecast_steps,
                            )
                        for hour_key in expected_icon_hour_keys:
                            if hour_key not in saved_icon_hour_keys_by_init[init_hour]:
                                reason = skip_reason_by_key.get(
                                    (f'ICON{init_hour}', domain_name, day_str, hour_key),
                                    'missing_timestamp_not_produced',
                                )
                                record_event(
                                    sample_events,
                                    day=day_str,
                                    domain=domain_name,
                                    source='ICON',
                                    init_hour=init_hour,
                                    timestamp=hour_key,
                                    reason='missing_timestamp',
                                    details=reason,
                                )

# Persist sample checks log to text file.
log_dir = os.path.dirname(sample_check_log_path)
if log_dir:
    os.makedirs(log_dir, exist_ok=True)

reason_counts = Counter(evt['reason'] for evt in sample_events)

with open(sample_check_log_path, 'w', encoding='utf-8') as f:
    f.write('=== Sample Checks Log ===\n')
    f.write(f'total_events: {len(sample_events)}\n')
    f.write('reason_counts:\n')
    for reason, count in sorted(reason_counts.items()):
        f.write(f'  - {reason}: {count}\n')

    f.write('\n=== Missing MSG Timestamps ===\n')
    msg_missing = [e for e in sample_events if e['source'] == 'MSG' and e['reason'] == 'missing_timestamp']
    for e in msg_missing:
        f.write(
            f"day={e['day']} domain={e['domain']} ts={e['timestamp']} "
            f"reason={e['details']}\n"
        )

    f.write('\n=== Missing ICON Timestamps ===\n')
    icon_missing = [e for e in sample_events if e['source'] == 'ICON' and e['reason'] == 'missing_timestamp']
    for e in icon_missing:
        f.write(
            f"day={e['day']} domain={e['domain']} init={e['init_hour']} ts={e['timestamp']} "
            f"reason={e['details']}\n"
        )

    f.write('\n=== Skip/Failure Events ===\n')
    skip_events = [e for e in sample_events if e['reason'] != 'missing_timestamp']
    for e in skip_events:
        f.write(
            f"day={e['day']} domain={e['domain']} source={e['source']} "
            f"init={e['init_hour']} ts={e['timestamp']} reason={e['reason']} details={e['details']}\n"
        )

print(f"Sample checks log saved: {sample_check_log_path}")

#nohup  600104