from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import timezone
from airflow import DAG

from plugins.utils.mongodb_tasks import load_mongodb_raw_data, postprocess_mongodb_raw_data

with DAG(
    dag_id="coc_mongodb_postprocess_data",
    start_date=timezone.datetime(2025, 12, 9, 0, 0, 0),
    schedule=None,
    catchup=False,
) as dag:

    load_mongodb_raw_data_task = PythonOperator(
        task_id="load_mongodb_raw_data",
        python_callable=load_mongodb_raw_data
    )

    postprocess_mongodb_raw_data_task = PythonOperator(
        task_id="postprocess_mongodb_raw_data",
        python_callable=postprocess_mongodb_raw_data
    )

    load_mongodb_raw_data_task >> postprocess_mongodb_raw_data_task