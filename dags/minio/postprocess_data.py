from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import timezone
from airflow import DAG

from plugins.utils.minio_tasks import load_minio_raw_data, postprocess_minio_raw_data

with DAG(
    dag_id="coc_minio_postprocess_data",
    start_date=timezone.datetime(2025, 12, 9, 0, 0, 0),
    schedule=None,
    catchup=False,
) as dag:

    load_minio_raw_data_task = PythonOperator(
        task_id="load_minio_raw_data",
        python_callable=load_minio_raw_data
    )

    postprocess_minio_raw_data_task = PythonOperator(
        task_id="postprocess_minio_raw_data",
        python_callable=postprocess_minio_raw_data
    )

    load_minio_raw_data_task >> postprocess_minio_raw_data_task