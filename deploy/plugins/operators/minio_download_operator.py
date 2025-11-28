from airflow.utils.decorators import apply_defaults
from airflow.models import BaseOperator

from hooks.minio_hook import MinioHook


class MinioDownloadOperator(BaseOperator):
    @apply_defaults
    def __init__(
        self,
        bucket: str,
        object_name: str,
        file_path: str = None,
        *args, 
        **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.bucket = bucket
        self.object_name = object_name
        self.file_path = file_path

    def execute(self, context):
        hook = MinioHook()

        if self.file_path:
            self.log.info(f"Downloading {self.bucket}/{self.object_name} to {self.file_path}")
            hook.download_to_file(self.bucket, self.object_name, self.file_path)
            return self.file_path
        else:
            self.log.info(f"Get {self.bucket}/{self.object_name}")
            data = hook.download_to_bytes(self.bucket, self.object_name)
            return data
