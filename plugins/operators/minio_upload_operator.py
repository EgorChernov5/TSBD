from pathlib import Path
import json
import os

from airflow.models import BaseOperator

from plugins.hooks.minio_hook import MinioHook

class MinioUploadOperator(BaseOperator):
    def __init__(
        self,
        bucket: str,
        object_name: str,
        data = None,
        file_paths: list[str] = None,
        source_task_ids: str = None,
        **kwargs
    ):
        super().__init__(**kwargs)
        self.bucket = bucket
        self.object_name = object_name
        self.data = data
        self.file_paths = file_paths
        self.source_task_ids = source_task_ids
        self.hook = MinioHook()

    def pull_from_xcom(self, ti):
        if isinstance(self.source_task_ids, str):
            return ti.xcom_pull(task_ids=self.source_task_ids, key='data')
        return None

    def convert_to_bytes(self, payload):
        # path
        if isinstance(payload, (str, Path)) and os.path.exists(str(payload)):
            p = Path(payload).expanduser()
            with p.open("rb") as f:
                return f.read()
        # dict
        elif isinstance(payload, dict):
            return json.dumps(payload).encode("utf-8")
        # bytes
        elif isinstance(payload, bytes):
            return payload
        # str
        elif isinstance(payload, str):
            return payload.encode("utf-8")
        else: 
            raise ValueError("Data must be path, dict, str, or bytes!")

    def execute(self, **context):
        if self.file_paths is not None:
            minio_paths = []
            for file_path in self.file_paths:
                minio_path = f"{self.bucket}/{self.object_name}"
                self.log.info(f"Uploading file {file_path} to {minio_path}")
                self.hook.upload_file(self.bucket, self.object_name, file_path)
                minio_paths.append(minio_path)

            return minio_paths

        data = self.data
        if data is None:
            data = self.pull_from_xcom(context["ti"])

        if data is None:
            raise ValueError(
                "No data provided to MinioUploadOperator!"
            )

        if isinstance(data, str):
            try:
                parsed = json.loads(data)
                if isinstance(parsed, (dict, list)):
                    data = parsed
            except json.JSONDecodeError:
                pass

        upload_bytes = self.convert_to_bytes(data)
        self.log.info("Uploading raw data to %s/%s (size=%s bytes)", self.bucket, self.object_name, len(upload_bytes))
        self.hook.upload_bytes(self.bucket, self.object_name, upload_bytes)

        return f"{self.bucket}/{self.object_name}"
