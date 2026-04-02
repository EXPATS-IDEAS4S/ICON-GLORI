#!/usr/bin/env python3
"""
4_upload_grib_case_study.py

Upload raw GRIB / compressed GRIB files to the case-study S3 buckets.

- Reads files from:
  /sat_data/icon/icon_d2_full
  /sat_data/icon/icon_eu_full
- Uploads raw files directly to the case-study buckets
- Uses credentials from credentials_buckets.py
"""

import os
import sys
import logging
from glob import glob

import boto3
from botocore.exceptions import ClientError

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
BASE_INPUT_DIRS = {
    "icon_d2_full": "/sat_data/icon/icon_d2_full",
    "icon_eu_full": "/sat_data/icon/icon_eu_full",
}

try:
    sys.path.append("/home/Daniele/codes/ICON-GLORI")
    from credentials_buckets import (
        S3_BUCKET_ICON_D2_CASE_STUDY,
        S3_BUCKET_ICON_EU_CASE_STUDY,
        S3_ACCESS_KEY,
        S3_SECRET_ACCESS_KEY,
        S3_ENDPOINT_URL,
    )
except ImportError:
    print("ERROR: credentials_buckets.py not found or incomplete")
    sys.exit(1)


def upload_file(s3_client, file_path, bucket, object_name=None):
    if object_name is None:
        object_name = os.path.basename(file_path)

    try:
        with open(file_path, "rb") as file_handle:
            s3_client.upload_fileobj(file_handle, bucket, object_name)
        return True
    except ClientError as error:
        logging.error(f"Upload error: {error}")
        return False
    except Exception as error:
        logging.error(f"Unexpected error: {error}")
        return False


def upload_raw_grib_files():
    print("=" * 70)
    print("Raw GRIB Upload to Case-Study Buckets")
    print("=" * 70)

    icon_source_dirs = {
        "icon_d2_full": os.path.abspath(BASE_INPUT_DIRS["icon_d2_full"]),
        "icon_eu_full": os.path.abspath(BASE_INPUT_DIRS["icon_eu_full"]),
    }

    for icon_type, path in icon_source_dirs.items():
        print(f"Source: {icon_type} -> {path}")
    print()

    s3_client = boto3.client(
        "s3",
        endpoint_url=S3_ENDPOINT_URL,
        aws_access_key_id=S3_ACCESS_KEY,
        aws_secret_access_key=S3_SECRET_ACCESS_KEY,
    )

    bucket_map = {
        "icon_d2_full": S3_BUCKET_ICON_D2_CASE_STUDY,
        "icon_eu_full": S3_BUCKET_ICON_EU_CASE_STUDY,
    }

    total_uploaded = 0
    total_failed = 0

    allowed_patterns = ["*.bz2", "*.grib", "*.grib2"]

    for icon_type, bucket_name in bucket_map.items():
        icon_dir = icon_source_dirs[icon_type]

        if not os.path.isdir(icon_dir):
            print(f"Directory not found: {icon_dir}")
            continue

        file_list = []
        for pattern in allowed_patterns:
            file_list.extend(glob(os.path.join(icon_dir, "**", pattern), recursive=True))
        file_list = sorted(set(file_list))

        if not file_list:
            print(f"No raw files found in {icon_dir}")
            continue

        print(f"Uploading {len(file_list)} files for {icon_type} to bucket '{bucket_name}'")

        for file_path in file_list:
            filename = os.path.basename(file_path)
            if upload_file(s3_client, file_path, bucket_name, filename):
                print(f"  ✓ {filename}")
                total_uploaded += 1
            else:
                print(f"  ✗ {filename} (FAILED)")
                total_failed += 1
        print()

    print("=" * 70)
    print("Upload Summary")
    print("=" * 70)
    print(f"Files uploaded: {total_uploaded}")
    print(f"Files failed: {total_failed}")
    print("=" * 70)

    return total_uploaded > 0 and total_failed == 0


if __name__ == "__main__":
    success = upload_raw_grib_files()
    sys.exit(0 if success else 1)
