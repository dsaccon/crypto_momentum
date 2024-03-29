import os
import boto3
import logging

logger = logging.getLogger(__name__)


if 'AWS_S3_KEY' in os.environ:
    S3_KEY = os.environ["AWS_S3_KEY"]
else:
    S3_KEY = ''

if 'AWS_S3_SECRET' in os.environ:
    S3_SECRET = os.environ["AWS_S3_SECRET"]
else:
    S3_SECRET = ''


def get_s3_obj():
    return boto3.resource(
        's3',
        aws_access_key_id=S3_KEY,
        aws_secret_access_key=S3_SECRET)

def write_s3(src_file, bkt=None, s3_obj=None, s3_path=None):
    if not S3_KEY or not S3_SECRET:
        logger.info('S3 keys not available. Skipping S3 file upload')
        return False
    if not bkt:
        logger.info('S3 bucket name not provided. Skipping S3 file upload')
        return False
    if not s3_obj:
        s3_obj = boto3.resource(
            's3',
            aws_access_key_id=S3_KEY,
            aws_secret_access_key=S3_SECRET)
    s3_key = src_file
    if s3_path:
        s3_key = s3_path

    with open(src_file, 'rb') as f:
        try:
            logger.info(f'Uploading {src_file} ...')
            s3_obj.Object(bkt, s3_key).put(Body=f)
            logger.info(f'S3 upload successful: {src_file}, to {bkt}, {s3_key}')
            return True
        except Exception as e:
            logger.info(e)
            return False
