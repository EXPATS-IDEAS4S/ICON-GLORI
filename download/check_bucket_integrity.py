#!/usr/bin/env python3
"""
6_check_bucket_integrity.py

Standalone checker for ICON D2/EU buckets.

What it does:
- lists all NetCDF objects in both buckets
- parses date/init from filename pattern merged_icon_<model>_initHH_YYYY-MM-DD.nc
- validates each file is downloadable and openable as NetCDF
- reports variable names, number of timestamps, and lat/lon grid size
- prints an overview and writes a TXT report under ./logs

Usage:
    python 6_check_bucket_integrity.py
    python 6_check_bucket_integrity.py --output ./logs/my_report.txt
"""

from __future__ import annotations

import argparse
import os
import re
import tempfile
from collections import Counter
from datetime import datetime
from typing import Dict, List, Optional, Tuple

import boto3
import xarray as xr
from botocore.exceptions import BotoCoreError, ClientError

try:
    import sys

    sys.path.append("/home/Daniele/codes/ICON-GLORI")
    from credentials_buckets import (
        S3_ACCESS_KEY,
        S3_BUCKET_ICON_D2,
        S3_BUCKET_ICON_EU,
        S3_ENDPOINT_URL,
        S3_SECRET_ACCESS_KEY,
    )
except ImportError:
    print("ERROR: credentials_buckets.py not found or incomplete")
    print("Please create credentials_buckets.py with the required S3 credentials")
    raise SystemExit(1)


FILENAME_RE = re.compile(
    r"^merged_(?P<icon_type>icon_d2|icon_eu)_init(?P<init>\d{2})_(?P<date>\d{4}-\d{2}-\d{2})\.nc$"
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Standalone integrity checker for ICON D2/EU NetCDF buckets"
    )
    parser.add_argument(
        "--output",
        default=None,
        help="Optional output report path. Defaults to ./logs/bucket_integrity_report_<timestamp>.txt",
    )
    parser.add_argument(
        "--max-files",
        type=int,
        default=None,
        help="Optional limit of files checked per bucket (for quick tests)",
    )
    return parser.parse_args()


def build_s3_client():
    return boto3.client(
        "s3",
        endpoint_url=S3_ENDPOINT_URL,
        aws_access_key_id=S3_ACCESS_KEY,
        aws_secret_access_key=S3_SECRET_ACCESS_KEY,
    )


def list_bucket_keys(s3_client, bucket_name: str) -> List[Dict[str, object]]:
    keys: List[Dict[str, object]] = []
    continuation_token: Optional[str] = None

    while True:
        kwargs = {"Bucket": bucket_name, "MaxKeys": 1000}
        if continuation_token:
            kwargs["ContinuationToken"] = continuation_token

        response = s3_client.list_objects_v2(**kwargs)

        for item in response.get("Contents", []):
            key = item.get("Key")
            if isinstance(key, str) and key.endswith(".nc"):
                keys.append(
                    {
                        "key": key,
                        "size": int(item.get("Size", 0)),
                        "last_modified": item.get("LastModified"),
                    }
                )

        if not response.get("IsTruncated", False):
            break

        continuation_token = response.get("NextContinuationToken")

    return sorted(keys, key=lambda x: x["key"])


def parse_filename_metadata(filename: str) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    match = FILENAME_RE.match(filename)
    if not match:
        return None, None, None
    return match.group("icon_type"), match.group("date"), match.group("init")


def detect_lat_lon_sizes(ds: xr.Dataset) -> Tuple[Optional[int], Optional[int]]:
    lat_dim = None
    lon_dim = None

    for candidate in ("lat", "latitude", "y"):
        if candidate in ds.sizes:
            lat_dim = candidate
            break

    for candidate in ("lon", "longitude", "x"):
        if candidate in ds.sizes:
            lon_dim = candidate
            break

    lat_size = int(ds.sizes[lat_dim]) if lat_dim else None
    lon_size = int(ds.sizes[lon_dim]) if lon_dim else None
    return lat_size, lon_size


def detect_time_count(ds: xr.Dataset) -> Optional[int]:
    for candidate in ("time", "valid_time", "step"):
        if candidate in ds.sizes:
            return int(ds.sizes[candidate])
    return None


def analyze_single_object(s3_client, bucket: str, key: str) -> Dict[str, object]:
    result: Dict[str, object] = {
        "key": key,
        "openable": False,
        "content_ok": False,
        "error": None,
        "variables": [],
        "time_count": None,
        "lat_size": None,
        "lon_size": None,
    }

    tmp_path = None
    try:
        with tempfile.NamedTemporaryFile(suffix=".nc", delete=False) as tmp:
            tmp_path = tmp.name
            s3_client.download_fileobj(bucket, key, tmp)

        with xr.open_dataset(tmp_path) as ds:
            result["openable"] = True
            result["variables"] = sorted(list(ds.data_vars))
            result["time_count"] = detect_time_count(ds)
            lat_size, lon_size = detect_lat_lon_sizes(ds)
            result["lat_size"] = lat_size
            result["lon_size"] = lon_size
            result["content_ok"] = (
                len(result["variables"]) > 0
                and result["time_count"] is not None
                and result["lat_size"] is not None
                and result["lon_size"] is not None
            )

    except (ClientError, BotoCoreError) as exc:
        result["error"] = f"S3 error: {exc}"
    except Exception as exc:
        result["error"] = f"Open/read error: {type(exc).__name__}: {exc}"
    finally:
        if tmp_path and os.path.exists(tmp_path):
            os.remove(tmp_path)

    return result


def render_bucket_summary(bucket_label: str, bucket_name: str, items: List[Dict[str, object]]) -> List[str]:
    lines: List[str] = []
    lines.append(f"Bucket: {bucket_name} ({bucket_label})")
    lines.append(f"  NetCDF files found: {len(items)}")

    dates = Counter()
    inits = Counter()
    malformed = 0

    for item in items:
        key = str(item["key"])
        filename = os.path.basename(key)
        _, date_value, init_value = parse_filename_metadata(filename)
        if date_value is None or init_value is None:
            malformed += 1
            continue
        dates[date_value] += 1
        inits[init_value] += 1

    if dates:
        lines.append("  Dates in bucket:")
        for d in sorted(dates.keys()):
            lines.append(f"    {d}: {dates[d]} file(s)")
    else:
        lines.append("  Dates in bucket: none")

    if inits:
        lines.append("  Init cycles in bucket:")
        for init in sorted(inits.keys()):
            lines.append(f"    init{init}: {inits[init]} file(s)")
    else:
        lines.append("  Init cycles in bucket: none")

    if malformed:
        lines.append(f"  Filenames not matching expected pattern: {malformed}")

    return lines


def render_file_check_lines(bucket_name: str, file_result: Dict[str, object]) -> List[str]:
    key = str(file_result["key"])
    openable = bool(file_result["openable"])

    lines = [f"  - {bucket_name}/{key}"]
    if not openable:
        lines.append(f"    status: FAIL ({file_result['error']})")
        return lines

    vars_list = file_result["variables"]
    vars_txt = ", ".join(vars_list) if vars_list else "none"
    if file_result["content_ok"]:
        lines.append("    status: OK")
    else:
        lines.append("    status: FAIL (opened but missing expected content metadata)")
    lines.append(f"    vars ({len(vars_list)}): {vars_txt}")
    lines.append(f"    timestamps: {file_result['time_count']}")
    lines.append(
        f"    grid size (lat x lon): {file_result['lat_size']} x {file_result['lon_size']}"
    )
    return lines


def main() -> int:
    args = parse_args()

    buckets = {
        "icon_d2": S3_BUCKET_ICON_D2,
        "icon_eu": S3_BUCKET_ICON_EU,
    }

    if args.output:
        output_path = args.output
    else:
        stamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        output_path = f"./logs/bucket_integrity_report_{stamp}.txt"

    output_dir = os.path.dirname(output_path)
    if output_dir:
        os.makedirs(output_dir, exist_ok=True)

    report_lines: List[str] = []
    report_lines.append("=" * 78)
    report_lines.append("ICON Bucket Integrity Report")
    report_lines.append(f"UTC generated at: {datetime.utcnow().isoformat(timespec='seconds')}Z")
    report_lines.append("=" * 78)
    report_lines.append("")

    s3_client = build_s3_client()

    global_total = 0
    global_ok = 0
    global_fail = 0

    for bucket_label, bucket_name in buckets.items():
        try:
            items = list_bucket_keys(s3_client, bucket_name)
        except (ClientError, BotoCoreError) as exc:
            report_lines.append("-" * 78)
            report_lines.append(f"ERROR listing bucket {bucket_name}: {exc}")
            report_lines.append("")
            continue

        if args.max_files is not None:
            items = items[: args.max_files]

        report_lines.append("-" * 78)
        report_lines.extend(render_bucket_summary(bucket_label, bucket_name, items))
        report_lines.append("  File checks:")

        for item in items:
            key = str(item["key"])
            result = analyze_single_object(s3_client, bucket_name, key)
            global_total += 1
            if result["openable"] and result["content_ok"]:
                global_ok += 1
            else:
                global_fail += 1
            report_lines.extend(render_file_check_lines(bucket_name, result))

        report_lines.append("")

    report_lines.append("=" * 78)
    report_lines.append("Global Summary")
    report_lines.append(f"  Files checked: {global_total}")
    report_lines.append(f"  Openable files: {global_ok}")
    report_lines.append(f"  Failed files: {global_fail}")
    report_lines.append("=" * 78)

    report_text = "\n".join(report_lines) + "\n"
    print(report_text)

    with open(output_path, "w", encoding="utf-8") as handle:
        handle.write(report_text)

    print(f"Report saved to: {output_path}")
    return 0 if global_fail == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
