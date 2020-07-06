# Cloudinary-to-S3

Utility script to copy files from Cloudinary to S3 bucket.

## Requirements

- python >= 3.6
- python-pip3

## Installation

To install necessary packages for script, run following command.

`pip3 install -r requirements.txt`

## Usage

This script requires following arguments (necessary to run):

- Cloudinary Cloud Name
- Cloudinary API key
- Cloudinary API Secret
- S3 Endpoint URL
- S3 Access key
- S3 secret key
- S3 Bucket name

For example,

```bash
python3 storage_migration.py \
    --cloudinary_cloud_name cloudinary_cloud name \
    --cloudinary_api_key cloudinary_api_key \
    --cloudinary_api_secret cloudinary_api_secret \
    --s3_endpoint_url s3_endpoint_url \
    --s3_access_key_id s3_access_key \
    --s3_secret_access_key s3_secret_key \
    --s3_bucket_name s3_bucket_name
```
For more info, run following commmand.

```bash
python3 storage_migration.py --help
```
