"""
aws_s3_utils
"""
import os
import sys
import threading
import boto3
from botocore.exceptions import ClientError
from boto3.s3.transfer import TransferConfig
from job_runner.common import LOGGER

# # Set the desired multipart threshold value (5GB)
# GB = 1024 ** 3
# upload_config = TransferConfig(multipart_threshold=5*GB)
# # To consume less downstream bandwidth, decrease the maximum concurrency
download_config = TransferConfig(max_concurrency=4)


# # Disable thread use/transfer concurrency
# config = TransferConfig(use_threads=False)


def create_bucket(bucket_name, region=None):
    """Create an S3 bucket in a specified region

    If a region is not specified, the bucket is created in the S3 default
    region (us-east-1).

    :param bucket_name: Bucket to create
    :param region: String region to create bucket in, e.g., 'us-west-2'
    :return: True if bucket created, else False
    """

    # Create bucket
    try:
        if region is None:
            s3_client = boto3.client('s3')
            s3_client.create_bucket(Bucket=bucket_name)
        else:
            s3_client = boto3.client('s3', region_name=region)
            location = {'LocationConstraint': region}
            s3_client.create_bucket(Bucket=bucket_name,
                                    CreateBucketConfiguration=location)
    except ClientError as error:
        LOGGER.error(error)
        return False
    return True


def list_buckets():
    """
    list_buckets
    :return: list of bucket names
    """
    # Retrieve the list of existing buckets
    s3_client = boto3.client('s3')
    response = s3_client.list_buckets()

    bucket_name_list = list()
    for bucket in response['Buckets']:
        bucket_name_list.append(bucket["Name"])
    return bucket_name_list


class ProgressPercentage(object):
    """
    ProgressPercentage
    """
    def __init__(self, filename):
        self._filename = filename
        self._size = float(os.path.getsize(filename))
        self._seen_so_far = 0
        self._lock = threading.Lock()

    def __call__(self, bytes_amount):
        # To simplify, assume this is hooked up to a single filename
        with self._lock:
            self._seen_so_far += bytes_amount
            percentage = (self._seen_so_far / self._size) * 100
            LOGGER.debug(f"{self._filename}  {self._seen_so_far} / {self._size} ({percentage:.2f})")

            # sys.stdout.write(
            #     "\r%s  %s / %s  (%.2f%%)" % (
            #         self._filename, self._seen_so_far, self._size,
            #         percentage))
            sys.stdout.flush()


def upload_file(bucket, file_path, s3_file_path, extra_args=None):
    """
    Upload a file to an S3 bucket
    :param file_path: File to upload path
    :param bucket: Bucket to upload to
    :param s3_file_path: S3 path
    :param extra_args: dictionary object name. Check available extra parameters allowed here -
    boto3.s3.transfer.S3Transfer.ALLOWED_UPLOAD_ARGS
    :return: True if file was uploaded, else False
    """

    # Upload the file
    LOGGER.debug("S3 Client creating")
    s3_client = boto3.client('s3')
    LOGGER.debug("S3 Client created")
    try:
        s3_file_path = s3_file_path.replace("//", "/")
        if extra_args:
            LOGGER.debug(
                f"File Uploading to s3 - from {file_path} to s3://{bucket}/{s3_file_path}")
            s3_client.upload_file(file_path,
                                  bucket,
                                  s3_file_path,
                                  ExtraArgs=extra_args,
                                  Callback=ProgressPercentage(file_path)
                                  )
            LOGGER.debug(
                f"File Uploaded to s3 - from {file_path} to s3://{bucket}/{s3_file_path}")
        else:
            LOGGER.debug(
                f"File Uploading to s3 - from {file_path} to s3://{bucket}/{s3_file_path}")
            s3_client.upload_file(file_path,
                                  bucket,
                                  s3_file_path,
                                  Callback=ProgressPercentage(file_path)
                                  )
            LOGGER.debug(
                f"File Uploaded to s3 - from {file_path} to s3://{bucket}/{s3_file_path}")
    except ClientError as error:
        LOGGER.error(error)
        return False
    return True


def download_file(bucket, s3_file_path, file_path):
    """
    download_file
    :param bucket: bucket name
    :param s3_file_path: s3_file_path
    :param file_path: local file path
    :return: True if file was downloaded, else False
    """
    try:
        s3_client = boto3.client('s3')
        s3_client.download_file(bucket, s3_file_path,
                                file_path, Config=download_config)
    except ClientError as error:
        LOGGER.error(error)
        return False
    return True


def list_objects(bucket, prefix=""):
    """
    list_objects
    :return: list of objects
    """
    # Retrieve the list of existing buckets
    s3_client = boto3.client('s3')
    s3_object_list = []
    response = s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix)
    if response["KeyCount"] > 0:
        s3_object_list.extend(response["Contents"])

    while response.get("IsTruncated", False):
        response = s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix,
                                             ContinuationToken=response["NextContinuationToken"])
        s3_object_list.extend(response["Contents"])

    return s3_object_list


def copy_objects(des_bucket, des_key, src_bucket, src_key, version_id=""):
    """
    copy_objects
    :param des_bucket:
    :param des_key:
    :param src_bucket:
    :param src_key:
    :param version_id:
    :return: status
    """
    s3_client = boto3.client('s3')
    source_dict = {"Bucket": src_bucket, "Key": src_key}
    if version_id:
        source_dict["VersionId"] = version_id
    response = s3_client.copy_object(Bucket=des_bucket, Key=des_key,
                              CopySource=source_dict)
    print(response)
    return True if response["ResponseMetadata"]["HTTPStatusCode"] == 200 else False


def delete_objects(bucket, key, version_id="", **kwargs):
    """
    delete_objects
    @param bucket:
    @param key:
    @param version_id:
    @param kwargs:
    @return:
    """
    s3_client = boto3.client('s3')
    param_dict = {"Bucket": bucket, "Key": key}
    if version_id:
        param_dict["VersionId"] = version_id

    response = s3_client.delete_object(**param_dict, **kwargs)
    print(response)
    return True if response["ResponseMetadata"]["HTTPStatusCode"] == 204 else False
