import json

from airflow.utils.decorators import apply_defaults
from airflow.models import BaseOperator

from ..hooks.minio_hook import MinioHook

class MinioUploadOperator(BaseOperator):
    @apply_defaults
    def __init__(
        self,
        bucket: str,
        object_name: str,
        data = None,
        file_path: str = None,
        *args, 
        **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.bucket = bucket
        self.object_name = object_name
        self.data = data
        self.file_path = file_path

    def execute(self, context):
        hook = MinioHook()

        if self.file_path:
            self.log.info(f"Uploading file {self.file_path} to {self.bucket}/{self.object_name}")
            hook.upload_file(self.bucket, self.object_name, self.file_path)
            return f"{self.bucket}/{self.object_name}"

        if isinstance(self.data, dict):
            upload_bytes = json.dumps(self.data).encode("utf-8")
        elif isinstance(self.data, str):
            upload_bytes = self.data.encode("utf-8")
        elif isinstance(self.data, bytes):
            upload_bytes = self.data
        else:
            raise ValueError("Data must be dict, str, or bytes")

        self.log.info(f"Uploading raw data to {self.bucket}/{self.object_name}")
        hook.upload_bytes(self.bucket, self.object_name, upload_bytes)
        return f"{self.bucket}/{self.object_name}"
