import os
import io
import boto3
from glob import glob
import logging
from botocore.exceptions import ClientError

from s3_bucket_credentials import S3_BUCKET_NAME, S3_ACCESS_KEY, S3_SECRET_ACCESS_KEY, S3_ENDPOINT_URL

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
# List the objects in our bucket
response = s3.list_objects(Bucket=S3_BUCKET_NAME)
count = 0
for item in response['Contents']:
    print(item['Key'])
    count += 1
print(f"Total objects in bucket: {count}")


# Pagination to get all objects
paginator = s3.get_paginator('list_objects_v2')
pages = paginator.paginate(Bucket=S3_BUCKET_NAME)

all_objects = []
for page in pages:
    if "Contents" in page:
        print(page['Contents'])
        all_objects.extend(page["Contents"])

print(f"Total objects in bucket: {len(all_objects)}")
exit()

#Directory with the data to uplad
path_dir = f"/work/dcorradi/icon_output/marche_flood_15-09-22/netcdf"


data_filepattern = f"{path_dir}/*.nc"
file_list = sorted(glob(data_filepattern))
#print(file_list)


for file in file_list:
    print(file)
    #Uploading a file to the bucket (make sure you have write access)
    #file_size = os.path.getsize(file)  # Get file size in bytes
    # Open file in binary mode and upload
    upload_file(s3, file, S3_BUCKET_NAME, os.path.basename(file))
         


# # List the objects in our bucket
# response = s3.list_objects(Bucket=S3_BUCKET_NAME)
# for item in response['Contents']:
#     print(item['Key'])