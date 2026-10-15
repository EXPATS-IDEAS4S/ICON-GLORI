#!/usr/bin/env python3

import os
import logging
import boto3
from botocore.exceptions import ClientError

from credentials_buckets_old import (
    S3_ACCESS_KEY,
    S3_SECRET_ACCESS_KEY,
    S3_ENDPOINT_URL,
)

# ==========================================================
# SETTINGS
# ==========================================================
S3_BUCKET_NAME = 'icon-glori-ensemble'  # replace with your bucket name
PREFIX = "/hpc/uwork/zparsakh/experiments/exp1_2mom/data/20220621000000/main0000/"

DOWNLOAD_DIR = "/sat_data/icon/icon-glori/ensemble"

GRID = "fc_R19B08_N02_ll"

FORECAST = "20220621000000_00040000"

MEMBER = "003"     # None -> all members

# ==========================================================


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)


def init_s3():
    return boto3.client(
        "s3",
        endpoint_url=S3_ENDPOINT_URL,
        aws_access_key_id=S3_ACCESS_KEY,
        aws_secret_access_key=S3_SECRET_ACCESS_KEY,
    )


def parse_filename(key):
    """
    fc_R19B08_N02_ll.20220621000000_00040000.003
    """

    name = os.path.basename(key)

    parts = name.split(".")

    if len(parts) != 3:
        return None

    return {
        "grid": parts[0],
        "forecast": parts[1],
        "member": parts[2],
    }


def list_matching_files(
    s3,
    bucket,
    prefix,
    grid=None,
    forecast=None,
    member=None,
):
    paginator = s3.get_paginator("list_objects_v2")

    matches = []

    for page in paginator.paginate(
        Bucket=bucket,
        Prefix=prefix,
    ):
        if "Contents" not in page:
            continue

        for obj in page["Contents"]:

            key = obj["Key"]

            info = parse_filename(key)

            if info is None:
                continue

            if grid is not None:
                if info["grid"] != grid:
                    continue

            if forecast is not None:
                if info["forecast"] != forecast:
                    continue

            if member is not None:
                if info["member"] != member:
                    continue

            matches.append(key)

    return matches


def download_file(s3, bucket, key, outdir):

    os.makedirs(outdir, exist_ok=True)

    local_file = os.path.join(
        outdir,
        os.path.basename(key)
    )

    logging.info(f"Downloading {key}")
    logging.info(f" -> {local_file}")

    with open(local_file, "wb") as f:
        s3.download_fileobj(bucket, key, f)

    return local_file


def main():

    s3 = init_s3()

    files = list_matching_files(
        s3,
        S3_BUCKET_NAME,
        PREFIX,
        grid=GRID,
        forecast=FORECAST,
        member=MEMBER,
    )

    logging.info(f"Found {len(files)} matching files")

    for f in files:
        print(f)

    for f in files:
        try:
            download_file(
                s3,
                S3_BUCKET_NAME,
                f,
                DOWNLOAD_DIR,
            )

        except ClientError as e:
            logging.error(e)

    logging.info("Done")


if __name__ == "__main__":
    main()