import os
import io
import logging
import subprocess
from collections import defaultdict

import boto3
import xarray as xr

from credentials_buckets_old import (
    S3_ACCESS_KEY,
    S3_SECRET_ACCESS_KEY,
    S3_ENDPOINT_URL
)
S3_BUCKET_NAME='icon-glori-deterministic'  # replace with your bucket name
PREFIX = "/hpc/uwork/dlittman/experiments/ilam_062024_icon-d2_teamx/dwd"
SUFFIX = ".nc"
TIME = "00140000"
OUT_DIR = "/sat_data/icon/icon-glori/deterministic"
os.makedirs(OUT_DIR, exist_ok=True)

logging.basicConfig(level=logging.INFO)


# -----------------------
# S3 INIT
# -----------------------
def init_s3():
    return boto3.client(
        "s3",
        endpoint_url=S3_ENDPOINT_URL,
        aws_access_key_id=S3_ACCESS_KEY,
        aws_secret_access_key=S3_SECRET_ACCESS_KEY,
    )


# -----------------------
# LIST FILES
# -----------------------
def list_nc_files(s3):
    paginator = s3.get_paginator("list_objects_v2")

    keys = []
    for page in paginator.paginate(Bucket=S3_BUCKET_NAME, Prefix=PREFIX):
        for obj in page.get("Contents", []):
            k = obj["Key"]
            if k.endswith(SUFFIX) and "ICON-D2_DOM" in k:
                if TIME in k:
                    keys.append(k)

    return keys


# -----------------------
# PICK ONE PER DOM
# -----------------------
def pick_one_per_dom(keys):
    dom_map = {}

    for k in sorted(keys):
        if "DOM" not in k:
            continue

        try:
            dom = k.split("DOM")[1].split("_")[0]
        except Exception:
            continue

        if dom not in dom_map:
            dom_map[dom] = k

    return dom_map


# -----------------------
# DOWNLOAD FILE
# -----------------------
def download_file(s3, key, out_dir):
    filename = os.path.basename(key)   # <-- keeps original name
    local_path = os.path.join(out_dir, filename)

    logging.info(f"Downloading {key} -> {local_path}")

    with open(local_path, "wb") as f:
        s3.download_fileobj(S3_BUCKET_NAME, key, f)

    return local_path


# -----------------------
# SAFE XARRAY OPEN
# -----------------------
def open_dataset(path):
    for engine in ["netcdf4", "scipy", "h5netcdf"]:
        try:
            ds = xr.open_dataset(path, engine=engine)
            logging.info(f"Opened with engine: {engine}")
            return ds
        except Exception:
            pass

    raise RuntimeError(f"Cannot open {path} with xarray")


# -----------------------
# SYSTEM INSPECTION
# -----------------------
def inspect_file_on_disk(path):
    print("\n" + "=" * 80)
    print(f"FILE: {path}")
    print("=" * 80)

    # 1. system file type
    try:
        out = subprocess.check_output(["file", path]).decode()
        print("\n[FILE TYPE]")
        print(out.strip())
    except Exception as e:
        print("file command failed:", e)

    # 2. ncdump header (if available)
    try:
        out = subprocess.check_output(["ncdump", "-h", path]).decode()
        print("\n[NCDUMP HEADER]")
        print(out[:2000])  # truncate
    except Exception as e:
        print("ncdump failed (maybe not NetCDF):", e)


# -----------------------
# XARRAY INSPECTION
# -----------------------
def inspect_xarray(ds):
    print("\n[XARRAY STRUCTURE]")

    print("\nVariables:")
    for v in ds.data_vars:
        print(f" - {v}: dims={ds[v].dims}, shape={ds[v].shape}")

    print("\nCoordinates:")
    for c in ds.coords:
        print(f" - {c}: dims={ds[c].dims}, shape={ds[c].shape}")

    print("\nAttributes:")
    for k, v in ds.attrs.items():
        print(f" - {k}: {v}")


# -----------------------
# MAIN
# -----------------------
def main():
    s3 = init_s3()

    keys = list_nc_files(s3)
    dom_map = pick_one_per_dom(keys)

    logging.info(f"Found DOMs: {list(dom_map.keys())}")

    for dom, key in dom_map.items():
        local_file = os.path.join(OUT_DIR, os.path.basename(key))

        # download
        download_file(s3, key, OUT_DIR)

        # system inspection
        inspect_file_on_disk(local_file)

        # xarray inspection
        try:
            ds = open_dataset(local_file)
            inspect_xarray(ds)
        except Exception as e:
            print(f"\n[XARRAY ERROR] {e}")


if __name__ == "__main__":
    main()