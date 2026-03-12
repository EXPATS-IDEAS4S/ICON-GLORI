"""
Upload local files to an S3-compatible object storage bucket.

This script scans a given local directory for files with a specified extension
(e.g., ".nc"), and uploads them to a configured S3 bucket using the boto3 library.
It authenticates using credentials and endpoint information defined in the 
`credentials_buckets.py` file.

Steps performed:
1. Initialize an S3 client using the provided endpoint and credentials.
2. Recursively list all files matching the given extension in the target directory.
3. Upload each file to the configured S3 bucket.
4. Print a summary of uploaded files by listing all objects in the bucket.

Dependencies (tested versions in current environment):
- boto3==1.35.72
- botocore==1.35.99
- s3transfer==0.10.4
- jmespath==1.0.1
- python-dateutil==2.8.2
- urllib3==1.26.19

Configuration:
- The following constants must be defined in `credentials_buckets.py`:
  - S3_BUCKET_NAME
  - S3_ACCESS_KEY
  - S3_SECRET_ACCESS_KEY
  - S3_ENDPOINT_URL

Usage:
- Modify the `path_dir` and `extension` variables in the script to match the 
  local directory and file type you want to upload.
- Run the script in an environment where the dependencies above are installed.

Example:
    $ python upload_to_bucket.py

Author: Daniele Corradini
"""



import os
import io
import boto3
from glob import glob
import xarray as xr
import logging
from botocore.exceptions import ClientError

from credentials_buckets import S3_BUCKET_ICON, S3_ACCESS_KEY, S3_SECRET_ACCESS_KEY, S3_ENDPOINT_URL

def upload_file(s3_client, file_name, bucket, object_name=None):
    """Upload a file to an S3 bucket

    :param file_name: File to upload
    :param bucket: Bucket to upload to
    :param object_name: S3 object name. If not specified then file_name is used
    :return: True if file was uploaded, else False
    """

    # If S3 object_name was not specified, use file_name
    if object_name is None:
        object_name = os.path.basename(file_name)
    try:
        with open(file_name, "rb") as f:
            s3_client.upload_fileobj(f, bucket, object_name)
        #response = s3_client.upload_file(file_name, bucket, object_name)
    except ClientError as e:
        logging.error(e)
        return False
    return True
 


# Initialize the S3 client
s3 = boto3.client(
    's3',
    endpoint_url=S3_ENDPOINT_URL,
    aws_access_key_id=S3_ACCESS_KEY,
    aws_secret_access_key=S3_SECRET_ACCESS_KEY
)

# # List the objects in our bucket
# response = s3.list_objects(Bucket=S3_BUCKET_NAME)
# for item in response['Contents']:
#     print(item['Key'])

# Get date from command line
import sys
if len(sys.argv) < 2:
    print("Usage: python 4_upload_data_bucket.py <DATE>")
    print("Example: python 4_upload_data_bucket.py 20250401_00")
    sys.exit(1)

date = sys.argv[1]
print(f"Processing date: {date}")

#Directory with the data to upload
path_dir = f"/sat_data/icon_teamx/nc_tmp/{date}"
extension = ".nc"  # Change this to the desired file extension
data_filepattern = f"{path_dir}/*{extension}"   # match all files in all subfolders
file_list = sorted(glob(data_filepattern, recursive=True))

# List the objects in our bucket
print(f"Bucket contents before upload:")
response = s3.list_objects(Bucket=S3_BUCKET_ICON)
for item in response['Contents']:
    print(item['Key'])


print(f'uploading {len(file_list)} {extension} files')
for file in file_list:
    #print(file)
    #get the basename of the file (without path)
    basename = os.path.basename(file)
    print(f"Uploading file: {basename}")
    #ds = xr.open_dataset(file)
    #print(ds)
    #print(ds.lat.values)
    #print(ds.lon.values)
    #print(ds.time.values)
    #print(ds["SYNMSG_BT_CL_IR10.8"].values)

    #Uploading a file to the bucket (make sure you have write access)
    #file_size = os.path.getsize(file)  # Get file size in bytes
    # Open file in binary mode and upload
    upload_file(s3, file, S3_BUCKET_ICON, basename)
         

# List the objects in our bucket
response = s3.list_objects(Bucket=S3_BUCKET_ICON)
for item in response['Contents']:
    print(item['Key'])

