from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import timezone
from airflow import DAG

from plugins.utils.postgres_tasks import load_postgres_raw_data, postprocess_postgres_raw_data

with DAG(
    dag_id="coc_postgres_postprocess_data",
    start_date=timezone.datetime(2025, 12, 9, 0, 0, 0),
    schedule=None,
    catchup=False,
) as dag:

    load_postgres_raw_data_task = PythonOperator(
        task_id="load_postgres_raw_data",
        python_callable=load_postgres_raw_data
    )

    postprocess_postgres_raw_data_task = PythonOperator(
        task_id="postprocess_postgres_raw_data",
        python_callable=postprocess_postgres_raw_data
    )

    load_postgres_raw_data_task >> postprocess_postgres_raw_data_task