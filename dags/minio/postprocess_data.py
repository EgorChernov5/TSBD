from datetime import timedelta

from airflow.providers.standard.operators.python import PythonOperator
from airflow.utils import timezone
from airflow import DAG

from plugins.utils import load_minio_raw_data, postprocess_minio_raw_data

with DAG(
    dag_id="coc_minio_postprocess_data",
    start_date=timezone.utcnow() - timedelta(days=1),
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