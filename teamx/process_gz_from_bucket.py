import os
import gzip
import shutil
import logging
import boto3
from botocore.exceptions import ClientError
from cdo import Cdo
import sys

sys.path.append("/home/dcorradi/Documents/Codes/ICON-GLORI")
from s3_bucket_credentials import (
    S3_BUCKET_NAME, S3_ACCESS_KEY, S3_SECRET_ACCESS_KEY, S3_ENDPOINT_URL
)

# === SETTINGS ===
date_str = "20250605_00"  # date folder to process
BUCKET_PREFIX = f"/sat_data/icon_teamx/{date_str}/"   # folder inside bucket
OUT_DIR = "/data/trade_pc/ICON/icon_teamx"          # where processed NC go
GRID_FOLDER = "/work/dcorradi/icon_output/teamx/acinn-data.uibk.ac.at/domain"
GRID_FILE = os.path.join(GRID_FOLDER, "grid_500m.txt")
WEIGHTS_FILE = os.path.join(GRID_FOLDER, "weights_500m.nc")
GRID_INFO_FILE = os.path.join(GRID_FOLDER, "domain2_DOM02.nc")
UNSTRUCTURED_GRID = os.path.join(GRID_FOLDER, "unstructured_grid.nc")

# Setup
os.makedirs(OUT_DIR, exist_ok=True)
cdo = Cdo()
print(cdo.version())
exit()
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

def init_s3():
    return boto3.client(
        "s3",
        endpoint_url=S3_ENDPOINT_URL,
        aws_access_key_id=S3_ACCESS_KEY,
        aws_secret_access_key=S3_SECRET_ACCESS_KEY,
    )

def list_gz_files(s3, bucket, prefix):
    """List all .gz files under the given prefix"""
    paginator = s3.get_paginator('list_objects_v2')
    pages = paginator.paginate(Bucket=bucket)

    all_objects = []
    for page in pages:
        if "Contents" in page:
            #print(page['Contents'])
            all_objects.extend(page["Contents"])

    #print(f"Total objects in bucket: {len(all_objects)}")
    return [obj['Key'] for obj in all_objects if obj['Key'].startswith(prefix) and obj['Key'].endswith('.gz')]


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

def download_and_decompress(s3, bucket, key, local_dir):
    """Download gzipped file from S3 and decompress to local .grib"""
    base = os.path.basename(key)[:-3]  # strip .gz

    gz_local = os.path.join(local_dir, os.path.basename(key))
   
    grib_local = os.path.join(local_dir, base)
   

    logging.info(f"Downloading {key} ...")
    with open(gz_local, "wb") as f:
        s3.download_fileobj(bucket, key, f)

    logging.info(f"Decompressing {gz_local} → {grib_local}")
    with gzip.open(gz_local, "rb") as f_in, open(grib_local, "wb") as f_out:
        shutil.copyfileobj(f_in, f_out)

    os.remove(gz_local)  # cleanup compressed
    return grib_local

# --- Precompute ICON grid + weights ---
if not os.path.exists(UNSTRUCTURED_GRID):
    logging.info(f"Extracting unstructured grid from {GRID_INFO_FILE} ...")
    cdo.selgrid("2", input=GRID_INFO_FILE, output=UNSTRUCTURED_GRID)

if not os.path.exists(WEIGHTS_FILE):
    logging.info("Generating remapping weights ...")
    cdo.gennn(f"{GRID_FILE},{UNSTRUCTURED_GRID}", output=WEIGHTS_FILE, options="-P 4")

# --- Main loop ---
s3 = init_s3()
gz_files = list_gz_files(s3, S3_BUCKET_NAME, BUCKET_PREFIX)

for key in gz_files:
    logging.info(f"Processing {key} ...")
    grib_file = download_and_decompress(s3, S3_BUCKET_NAME, key, OUT_DIR)
    print(grib_file)

    base = grib_file  # same as bash "$base"
    nc_path = f"{base}.nc"
    grid_nc_path = f"{base}_grid.nc"
    final_nc_path = os.path.join(OUT_DIR, os.path.basename(base) + ".nc")

    try:
        # 2. Convert GRIB → NetCDF
        cdo.run(f"-P 4 -f nc copy {base} {nc_path}")

        # 3. Attach correct ICON grid
        cdo.run(f"setgrid,{UNSTRUCTURED_GRID} {nc_path} {grid_nc_path}")

        # 4. Remap to target grid
        cdo.run(f"-P 4 remap,{GRID_FILE},{WEIGHTS_FILE} {grid_nc_path} {final_nc_path}")

        logging.info(f"✅ Created {final_nc_path}")

    except Exception as e:
        logging.error(f"❌ Error converting {grib_file}: {e}")
        # cleanup if remap fails
        for f in [base, nc_path, grid_nc_path]:
            if os.path.exists(f):
                os.remove(f)
        continue

    # 5. Cleanup intermediates
    for f in [base, nc_path, grid_nc_path]:
        if os.path.exists(f):
            os.remove(f)

logging.info("All files processed successfully.")

