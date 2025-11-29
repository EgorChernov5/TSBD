import os

from airflow.sdk.bases.hook import BaseHook
from minio import Minio

class MinioHook(BaseHook):
    def __init__(
            self, 
            host: str = None,
            access_key: str = None,
            secret_key: str = None,
            secure: bool = True
    ):
        super().__init__()

        if host is None:
            self.host = f"minio:9000"
            self.access_key = os.getenv("MINIO_ROOT_USER")
            self.secret_key = os.getenv("MINIO_ROOT_PASSWORD")
            self.secure = os.getenv("MINIO_SECURE", "True").lower() == "true"
        else:
            self.host = host
            self.access_key = access_key
            self.secret_key = secret_key
            self.secure = secure

        self.client = Minio(
            self.host,
            access_key=self.access_key,
            secret_key=self.secret_key,
            secure=self.secure
        )

    def ensure_bucket_exists(
            self, 
            bucket_name: str
    ):
        if not self.client.bucket_exists(bucket_name):
            self.log.info(f"Bucket '{bucket_name}' does not exist. Creating it...")
            self.client.make_bucket(bucket_name)

    def upload_bytes(
            self, 
            bucket: str, 
            object_name: str, 
            data: bytes
    ):
        self.ensure_bucket_exists(bucket)
        self.client.put_object(bucket, object_name, data, length=len(data))

    def upload_file(
            self,
            bucket: str, 
            object_name: str, 
            file_path: str
    ):
        self.ensure_bucket_exists(bucket)
        self.client.fput_object(bucket, object_name, file_path)

    def download_to_bytes(
            self, 
            bucket: str, 
            object_name: str
    )-> bytes:
        with self.client.get_object(bucket, object_name) as response:
            return response.read()

    def download_to_file(
            self, 
            bucket: str, 
            object_name: str, 
            file_path: str
    ):
        self.client.fget_object(bucket, object_name, file_path)
