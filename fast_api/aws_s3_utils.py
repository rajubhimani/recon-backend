"""aws_s3_utils
"""
import os
import sys
import logging
from urllib.parse import urlparse
import threading
import boto3
from botocore.exceptions import ClientError
from boto3.s3.transfer import TransferConfig

# # Set the desired multipart threshold value (5GB)
# GB = 1024 ** 3
# upload_config = TransferConfig(multipart_threshold=5*GB)
# # To consume less downstream bandwidth, decrease the maximum concurrency
download_config = TransferConfig(max_concurrency=4)


# # Disable thread use/transfer concurrency
# config = TransferConfig(use_threads=False)

class ProgressPercentage(object):
    """
    ProgressPercentage
    """

    def __init__(self, filename, logger: logging = None) -> None:
        """__init__
        """
        self.logger = logger if logger else logging.getLogger(__name__)
        self._filename = filename
        self._size = float(os.path.getsize(filename))
        self._seen_so_far = 0
        self._lock = threading.Lock()

    def __call__(self, bytes_amount):
        # To simplify, assume this is hooked up to a single filename
        with self._lock:
            self._seen_so_far += bytes_amount
            percentage = (self._seen_so_far / self._size) * 100
            self.logger.debug(
                f"{self._filename}  {self._seen_so_far} / {self._size} ({percentage:.2f})")

            # sys.stdout.write(
            #     "\r%s  %s / %s  (%.2f%%)" % (
            #         self._filename, self._seen_so_far, self._size,
            #         percentage))
            sys.stdout.flush()


class S3Utils:
    """S3Utils
    """

    def __init__(self, logger: logging = None) -> None:
        """__init__
        """
        self.logger = logger if logger else logging.getLogger(__name__)
        self.logger.debug("S3 Client creating")
        self.s3_resource = boto3.resource(service_name="s3",)
        self.s3_client = self.s3_resource.meta.client
        self.logger.debug("S3 Client created")

    def _get_prefix(self, prefix: str, match_prefix=False) -> str:
        """_get_prefix
        Args:
            prefix (str): prefix
            match_prefix (bool, optional): If true all matched prefix will be deleted else only that
                                            folder is deleted. Defaults to False.

        Returns:
            str: Add '/' in prefix if self.match_prefix is false.
        """
        self.logger.debug("Getting prefix")
        if match_prefix:
            return prefix
        else:
            return prefix if prefix[-1] == "/" else prefix + "/"

    def list_files(self, bucket_name, prefix, match_prefix=False, raw_list=False) -> list:
        """Get list of s3 files

        Args:
            bucket_name (str): bucket_name
            prefix (str): prefix
            match_prefix (bool, optional): If true all matched prefix will be deleted else only that
                                            folder is deleted. Defaults to False.
            return_s3_resource (bool, optional): return s3_resource and folder with list of files or not.
                                                Defaults to False.

        Returns:
            list : list of files
        """
        prefix = self._get_prefix(prefix=prefix, match_prefix=match_prefix)

        self.logger.debug(bucket_name)
        self.logger.debug(prefix)
        self.logger.debug("listing files")
        try:
            if raw_list:
                bucket_file_list = self.s3_resource.Bucket(
                    bucket_name).objects.filter(Prefix=prefix)
            else:
                file_format = "s3://{bucket}/{key}"
                bucket_file_list = list({file_format.format(bucket=bucket_name, key=obj.key) for obj in
                                         self.s3_resource.Bucket(bucket_name).objects.filter(Prefix=prefix)})
        except ClientError as error:
            self.logger.error(error)
            raise ClientError from error
        return bucket_file_list

    def delete_all_files_from_folder(self, bucket_name: str, prefix: str, match_prefix: bool = False) -> bool:
        """delete all files from s3 specific path based on bucket name and folder

        Args:
            bucket_name (str): bucket_name
            prefix (str): prefix
            match_prefix (bool, optional): If true all matched prefix will be deleted else only that
                                            folder is deleted. Defaults to False.

        Returns:
            bool: deleted or failed
        """
        try:

            bucket_file_list = self.list_files(
                bucket_name, prefix, match_prefix, raw_list=True)
            bucket_file_list = [{"Key": obj.key} for obj in bucket_file_list]
            if bucket_file_list:
                self.logger.debug(f"{len(bucket_file_list)} files found")
                self.logger.debug(bucket_file_list)
                response = self.s3_client.delete_objects(
                    Bucket=bucket_name, Delete={'Objects': bucket_file_list})
                if response.get("Errors"):
                    self.logger.debug("Response %s", response)
                    return False
            else:
                self.logger.debug("No files found")
                return True
        except ClientError as error:
            self.logger.error(error)
            return False
        return True

    def create_presigned_url(self, bucket_name: str, object_name: str, expiration: int = 3600) -> str:
        """Generate a presigned URL to share an S3 object

        Args:
            bucket_name (str): bucket_name
            object_name (str): object_name
            expiration (int, optional): Time in seconds for the presigned URL to remain valid. Defaults to 3600.

        Returns:
            str: Presigned URL as string. If error, returns None.
        """
        # Generate a presigned URL for the S3 object
        try:
            response = self.s3_client.generate_presigned_url('get_object',
                                                             Params={'Bucket': bucket_name,
                                                                     'Key': object_name},
                                                             ExpiresIn=expiration)
        except ClientError as error:
            self.logger.error(error)
            return None

        # The response contains the presigned URL
        return response

    def get_bucket_and_path(self, s3_path: str) -> tuple:
        """Extract bucket and path

        Args:
            s3_path (str): s3_path

        Returns:
            tuple: bucket, path
        """
        parsed_url = urlparse(s3_path)
        bucket = parsed_url.netloc
        path = parsed_url.path[1:]
        return bucket, path

    def generate_presigned_url_for_s3path(self, s3_path: str, expiration: int = 3600) -> str:
        """generate_presigned_url_for_s3path

        Args:
            s3_path (str): s3_path
            expiration (int, optional): Time in seconds for the presigned URL to remain valid. Defaults to 3600.

        Returns:
            str: Presigned URL as string. If error, returns None.
        """
        bucket, path = self.get_bucket_and_path(s3_path)
        return self.create_presigned_url(bucket, path, expiration)

    def create_bucket(self, bucket_name: str, region: str = None) -> bool:
        """Create an S3 bucket in a specified region. 
        If a region is not specified, the bucket is created in the S3 default
        region (us-east-1)

        Args:
            bucket_name (str): Bucket to create
            region (str, optional): String region to create bucket in, e.g., 'us-west-2'. Defaults to None.

        Returns:
            bool: True if bucket created, else False
        """

        # Create bucket
        try:
            if region is None:
                self.s3_client.create_bucket(Bucket=bucket_name)
            else:
                location = {'LocationConstraint': region}
                self.s3_client.create_bucket(Bucket=bucket_name,
                                             CreateBucketConfiguration=location)
        except ClientError as error:
            self.logger.error(error)
            return False
        return True

    def list_buckets(self) -> list:
        """list_buckets

        Returns:
            list: list of bucket names
        """
        # Retrieve the list of existing buckets
        response = self.s3_client.list_buckets()

        bucket_name_list = list()
        for bucket in response['Buckets']:
            bucket_name_list.append(bucket["Name"])
        return bucket_name_list

    def upload_file(self, bucket: str, file_path: str, s3_file_path: str, extra_args: dict = None) -> bool:
        """Upload a file to an S3 bucket

        Args:
            bucket (str): Bucket to upload to
            file_path (str): File to upload path
            s3_file_path (str): S3 path
            extra_args (dict, optional): dictionary object name. Check available extra parameters allowed here -
        boto3.s3.transfer.S3Transfer.ALLOWED_UPLOAD_ARGS. Defaults to None.

        Returns:
            bool: True if file was uploaded, else False
        """

        # Upload the file
        try:
            s3_file_path = s3_file_path.replace("//", "/")
            if extra_args:
                self.logger.debug(
                    f"File Uploading to s3 - from {file_path} to s3://{bucket}/{s3_file_path}")
                self.s3_client.upload_file(file_path,
                                           bucket,
                                           s3_file_path,
                                           ExtraArgs=extra_args,
                                           Callback=ProgressPercentage(
                                               file_path)
                                           )
                self.logger.debug(
                    f"File Uploaded to s3 - from {file_path} to s3://{bucket}/{s3_file_path}")
            else:
                self.logger.debug(
                    f"File Uploading to s3 - from {file_path} to s3://{bucket}/{s3_file_path}")
                self.s3_client.upload_file(file_path,
                                           bucket,
                                           s3_file_path,
                                           Callback=ProgressPercentage(
                                               file_path)
                                           )
                self.logger.debug(
                    f"File Uploaded to s3 - from {file_path} to s3://{bucket}/{s3_file_path}")
        except ClientError as error:
            self.logger.error(error)
            return False
        return True

    def download_file(self, bucket: str, s3_file_path: str, file_path: str) -> bool:
        """download_file

        Args:
            bucket (str): bucket name
            s3_file_path (str): s3_file_path
            file_path (str): local file path

        Returns:
            bool: True if file was downloaded, else False
        """
        try:
            self.s3_client.download_file(bucket, s3_file_path,
                                         file_path, Config=download_config)
        except ClientError as error:
            self.logger.error(error)
            return False
        return True

    def copy_object(self, des_bucket: str, des_key: str, src_bucket: str, src_key: str, version_id: str = "") -> bool:
        """copy_object

        Args:
            des_bucket (str): destination bucket name
            des_key (str): destination s3 path
            src_bucket (str): source bucket name
            src_key (str): source s3 path
            version_id (str, optional): version id. Defaults to "".

        Returns:
            bool: status
        """
        source_dict = {"Bucket": src_bucket, "Key": src_key}
        if version_id:
            source_dict["VersionId"] = version_id
        response = self.s3_client.copy_object(Bucket=des_bucket, Key=des_key,
                                              CopySource=source_dict)

        return True if response["ResponseMetadata"]["HTTPStatusCode"] == 200 else False

    def delete_object(self, bucket: str, key: str, version_id: str = "", **kwargs) -> bool:
        """delete_object

        Args:
            bucket (str): bucket
            key (str): key
            version_id (str, optional): version_id. Defaults to "".

        Returns:
            bool: status
        """

        param_dict = {"Bucket": bucket, "Key": key}
        if version_id:
            param_dict["VersionId"] = version_id

        response = self.s3_client.delete_object(**param_dict, **kwargs)

        return True if response["ResponseMetadata"]["HTTPStatusCode"] == 200 else False
