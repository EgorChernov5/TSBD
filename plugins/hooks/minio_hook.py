from typing import Optional
from airflow.sdk.bases.hook import BaseHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook


class MinioHook(BaseHook):
    def __init__(
            self, 
            aws_conn_id: str = "minio_s3"
    ):
        super().__init__()
        self.aws_conn_id = aws_conn_id
        self.hook = S3Hook(aws_conn_id=self.aws_conn_id)

    def ensure_bucket_exists(
            self, 
            bucket_name: str
    ):
        if not self.hook.check_for_bucket(bucket_name):
            self.log.info(f"Bucket '{bucket_name}' does not exist. Creating it...")
            self.hook.create_bucket(bucket_name=bucket_name)

    def upload_bytes(
        self,
        bucket: str,
        object_name: str,
        data: bytes,
        put_kwargs: Optional[dict] = None,
    ):
        self.ensure_bucket_exists(bucket)
        client = self.hook.get_conn()
        kwargs = put_kwargs.copy() if put_kwargs else {}
        client.put_object(Bucket=bucket, Key=object_name, Body=data, **kwargs)

    def upload_file(
        self,
        bucket: str,
        object_name: str,
        file_path: str,
        replace: bool = True,
    ):
        self.ensure_bucket_exists(bucket)
        self.hook.load_file(filename=file_path, key=object_name, bucket_name=bucket, replace=replace)

    def download_to_bytes(
            self, 
            bucket: str,
            object_name: str
    )-> bytes:
        client = self.hook.get_conn()
        response = client.get_object(Bucket=bucket, Key=object_name)
        return response["Body"].read()

    def download_to_file(
            self, 
            bucket: str, 
            object_name: str, 
            file_path: str
    ):
        client = self.hook.get_conn()
        client.download_file(bucket, object_name, file_path)