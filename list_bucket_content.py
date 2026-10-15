#!/usr/bin/env python3

from pathlib import Path
from collections import Counter
import boto3

from credentials_buckets_old import (
    S3_ACCESS_KEY,
    S3_SECRET_ACCESS_KEY,
    S3_ENDPOINT_URL,
)

#S3_BUCKET_NAME = 'icon-glori-ensemble'  # replace with your bucket name
S3_BUCKET_NAME = 'icon-500m-teamx-ir108'  # replace with your bucket name


def init_s3():
    return boto3.client(
        "s3",
        endpoint_url=S3_ENDPOINT_URL,
        aws_access_key_id=S3_ACCESS_KEY,
        aws_secret_access_key=S3_SECRET_ACCESS_KEY,
    )


def get_extension(filename):
    """
    Handles multi-part extensions such as:
      file.grib.gz
      file.nc.gz
    """
    name = Path(filename).name

    if name.endswith(".grib.gz"):
        return ".grib.gz"
    if name.endswith(".grib2.gz"):
        return ".grib2.gz"
    if name.endswith(".nc.gz"):
        return ".nc.gz"

    return Path(name).suffix or "<no_extension>"


def list_bucket_contents(s3, bucket, prefix=""):
    paginator = s3.get_paginator("list_objects_v2")

    objects = []
    ext_counter = Counter()

    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        if "Contents" not in page:
            continue

        for obj in page["Contents"]:
            key = obj["Key"]
            size_mb = obj["Size"] / 1024**2

            ext = get_extension(key)
            ext_counter[ext] += 1

            objects.append(
                {
                    "key": key,
                    "size_mb": size_mb,
                    "extension": ext,
                }
            )

    return objects, ext_counter


if __name__ == "__main__":

    PREFIX = ""


    s3 = init_s3()

    objects, ext_counter = list_bucket_contents(
        s3=s3,
        bucket=S3_BUCKET_NAME,
        prefix=PREFIX,
    )

    output_file = Path(__file__).parent / f"bucket_inventory_{S3_BUCKET_NAME}.txt"

    with open(output_file, "w", encoding="utf-8") as f:

        f.write(f"Bucket: {S3_BUCKET_NAME}\n")
        f.write(f"Prefix: {PREFIX}\n")
        f.write("=" * 120 + "\n\n")

        for obj in objects:

            line = (
                f"{obj['extension']:12s} "
                f"{obj['size_mb']:12.2f} MB "
                f"{obj['key']}"
            )

            print(line)
            f.write(line + "\n")

        f.write("\n")
        f.write("=" * 120 + "\n")
        f.write("FORMAT SUMMARY\n")
        f.write("=" * 120 + "\n")

        print("\nFORMAT SUMMARY")
        print("-" * 40)

        for ext, count in sorted(
            ext_counter.items(),
            key=lambda x: x[1],
            reverse=True,
        ):
            summary = f"{ext:15s} {count:10d}"

            print(summary)
            f.write(summary + "\n")

        total = len(objects)

        print("-" * 40)
        print(f"TOTAL FILES: {total}")

        f.write("\n")
        f.write("-" * 40 + "\n")
        f.write(f"TOTAL FILES: {total}\n")

    print(f"\nInventory written to: {output_file}")