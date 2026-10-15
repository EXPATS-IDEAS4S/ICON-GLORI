#!/usr/bin/env python3

import io
import logging

import boto3
import xarray as xr
import re

from credentials_buckets_old import (
    S3_ACCESS_KEY,
    S3_SECRET_ACCESS_KEY,
    S3_ENDPOINT_URL,
)

S3_BUCKET_NAME = 'icon-glori-deterministic'  # replace with your bucket name
PREFIX = "/hpc/uwork/dlittman/experiments/ilam_062024_icon-d2_teamx/dwd/"

# ICON pattern:
# ICON-D2_DOM01_20240601.nc
PATTERN = re.compile(r"ICON-D2_DOM(\d+)_.*\.nc$")


def init_s3():
    return boto3.client(
        "s3",
        endpoint_url=S3_ENDPOINT_URL,
        aws_access_key_id=S3_ACCESS_KEY,
        aws_secret_access_key=S3_SECRET_ACCESS_KEY,
    )


def list_nc_files(s3, bucket, prefix):
    paginator = s3.get_paginator("list_objects_v2")

    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if key.endswith(".nc"):
                yield key


def pick_one_per_domain(keys):
    """
    Returns {domain_id: key}
    """
    chosen = {}

    for key in keys:
        m = PATTERN.search(key)
        if not m:
            continue

        dom = m.group(1)

        # keep first occurrence per domain
        if dom not in chosen:
            chosen[dom] = key

    return chosen


def read_file(s3, bucket, key):
    obj = s3.get_object(Bucket=bucket, Key=key)
    return obj["Body"].read()


def inspect(ds, key):
    print("\n" + "=" * 100)
    print(key)
    print("=" * 100)

    print(ds)

    print("\nDimensions:", dict(ds.dims))
    print("Variables:", list(ds.data_vars))
    print("Coordinates:", list(ds.coords))


def main():

    logging.basicConfig(level=logging.INFO)

    s3 = init_s3()

    all_keys = list(list_nc_files(s3, S3_BUCKET_NAME, PREFIX))

    print(f"Total .nc files found: {len(all_keys)}")

    selected = pick_one_per_domain(all_keys)

    print(f"Domains found: {sorted(selected.keys())}")

    for dom, key in sorted(selected.items()):

        logging.info(f"Loading DOM{dom}: {key}")

        file_bytes = read_file(
            s3,
            S3_BUCKET_NAME,
            key,
        )

        try:
            ds = xr.open_dataset(io.BytesIO(file_bytes), engine="netcdf4")

            inspect(ds, key)

            ds.close()

        except Exception as e:
            print(f"ERROR opening {key}: {e}")


if __name__ == "__main__":
    main()