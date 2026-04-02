#!/usr/bin/env python3
"""
4_upload_data_bucket.py

Upload merged NetCDF files to S3-compatible object storage buckets.

- Reads merged NC files from output directory
- Uploads to corresponding S3 buckets (icon_d2, icon_eu)
- Uses credentials from credentials_buckets.py

Usage:
    python 4_upload_data_bucket.py [merged_nc_dir]
"""

import os
import sys
import boto3
from glob import glob
import logging
from botocore.exceptions import ClientError


# Default config: read merged files from fixed per-model source directories.
DEFAULT_ICON_MERGED_DIRS = {
    "icon_d2": "/sat_data/icon/icon_d2/merged_nc",
    "icon_eu": "/sat_data/icon/icon_eu/merged_nc",
}

# Try to import credentials; gracefully handle if file doesn't exist
try:
    sys.path.append("/home/Daniele/codes/ICON-GLORI")  # Ensure current directory is in path
    from credentials_buckets import (
        S3_BUCKET_ICON_D2, 
        S3_BUCKET_ICON_EU,
        S3_ACCESS_KEY, 
        S3_SECRET_ACCESS_KEY, 
        S3_ENDPOINT_URL
    )
except ImportError:
    print("ERROR: credentials_buckets.py not found or incomplete")
    print("Please create credentials_buckets.py with the required S3 credentials")
    sys.exit(1)


def upload_file(s3_client, file_path, bucket, object_name=None):
    """
    Upload a file to an S3 bucket.
    
    Parameters:
    -----------
    s3_client : boto3.client
        Initialized S3 client
    file_path : str
        Local file path to upload
    bucket : str
        Target S3 bucket name
    object_name : str
        S3 object name (defaults to basename)
    
    Returns:
    --------
    bool : True if upload successful, False otherwise
    """
    
    if object_name is None:
        object_name = os.path.basename(file_path)
    
    try:
        with open(file_path, "rb") as f:
            s3_client.upload_fileobj(f, bucket, object_name)
        return True
    except ClientError as e:
        logging.error(f"Upload error: {e}")
        return False
    except Exception as e:
        logging.error(f"Unexpected error: {e}")
        return False


def upload_merged_files(merged_nc_dir=None):
    """
    Upload merged NC files to corresponding S3 buckets.
    
    Parameters:
    -----------
    merged_nc_dir : str
        Directory containing merged NC files
    """
    
    print("=" * 70)
    print("NetCDF File Upload to S3 Buckets")
    print("=" * 70)
    if merged_nc_dir is None:
        icon_source_dirs = {
            icon_type: os.path.abspath(os.path.expanduser(path))
            for icon_type, path in DEFAULT_ICON_MERGED_DIRS.items()
        }
        print("Source directories (default config):")
        for icon_type, path in icon_source_dirs.items():
            print(f"  - {icon_type}: {path}")
    else:
        merged_nc_dir = os.path.abspath(os.path.expanduser(merged_nc_dir))
        icon_source_dirs = {
            "icon_d2": os.path.join(merged_nc_dir, "icon_d2"),
            "icon_eu": os.path.join(merged_nc_dir, "icon_eu"),
        }
        print(f"Source directory: {merged_nc_dir}")
    print()
    
    # Initialize S3 client
    s3_client = boto3.client(
        's3',
        endpoint_url=S3_ENDPOINT_URL,
        aws_access_key_id=S3_ACCESS_KEY,
        aws_secret_access_key=S3_SECRET_ACCESS_KEY
    )
    
    # Verify buckets exist
    bucket_map = {
        "icon_d2": S3_BUCKET_ICON_D2,
        "icon_eu": S3_BUCKET_ICON_EU,
    }
    
    total_uploaded = 0
    total_failed = 0
    
    for icon_type, bucket_name in bucket_map.items():
        icon_dir = icon_source_dirs[icon_type]
        
        if not os.path.isdir(icon_dir):
            print(f"Directory not found: {icon_dir}")
            continue
        
        # Find all NC files for this icon type
        nc_files = sorted(glob(os.path.join(icon_dir, "*.nc")))
        
        if not nc_files:
            print(f"No NC files found in {icon_dir}")
            continue
        
        print(f"Uploading {len(nc_files)} files for {icon_type} to bucket '{bucket_name}'")
        
        for nc_file in nc_files:
            filename = os.path.basename(nc_file)
            
            # Upload file
            if upload_file(s3_client, nc_file, bucket_name, filename):
                print(f"  ✓ {filename}")
                total_uploaded += 1
            else:
                print(f"  ✗ {filename} (FAILED)")
                total_failed += 1
        
        print()
    
    # List bucket contents for verification
    print("=" * 70)
    print("Bucket Contents After Upload:")
    print("=" * 70)
    
    for icon_type, bucket_name in bucket_map.items():
        print(f"\nBucket: {bucket_name} ({icon_type})")
        try:
            response = s3_client.list_objects(Bucket=bucket_name, MaxKeys=100)
            if "Contents" in response:
                for item in response["Contents"]:
                    size_mb = item["Size"] / (1024**2)
                    print(f"  {item['Key']} ({size_mb:.2f} MB)")
            else:
                print("  (empty)")
        except ClientError as e:
            print(f"  Error listing bucket: {e}")
    
    # Summary
    print()
    print("=" * 70)
    print(f"Upload Summary:")
    print(f"  Files uploaded: {total_uploaded}")
    print(f"  Files failed: {total_failed}")
    print("=" * 70)
    
    return total_failed == 0


if __name__ == "__main__":
    merged_dir = sys.argv[1] if len(sys.argv) > 1 else None
    success = upload_merged_files(merged_dir)
    sys.exit(0 if success else 1)
