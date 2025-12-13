from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import timezone
from airflow import DAG

from plugins.utils import load_minio_raw_data, split_minio_raw_data, norm_minio_raw_data, load_minio_raw_clan_data
from plugins.utils import save_postgres_norm_data

with DAG(
    dag_id="dds_postgres_norm_data",
    start_date=timezone.datetime(2025, 12, 9, 0, 0, 0),
    schedule=None,
    catchup=False,
) as dag:
    
    load_minio_raw_data_task = PythonOperator(
        task_id="load_minio_raw_data",
        python_callable=load_minio_raw_data
    )

    load_minio_raw_clan_data_task = PythonOperator(
        task_id="load_minio_raw_clan_data",
        python_callable=load_minio_raw_clan_data
    )

    split_minio_raw_data_task = PythonOperator(
        task_id="split_minio_raw_data",
        python_callable=split_minio_raw_data
    )

    norm_minio_raw_data_task = PythonOperator(
        task_id="norm_minio_raw_data",
        python_callable=norm_minio_raw_data
    )

    save_postgres_norm_data_task = PythonOperator(
        task_id="save_postgres_norm_data",
        python_callable=save_postgres_norm_data
    )

    # Players
    load_minio_raw_data_task >> split_minio_raw_data_task >>\
    norm_minio_raw_data_task >> save_postgres_norm_data_task
    # Clans
    load_minio_raw_clan_data_task >> split_minio_raw_data_task >>\
    norm_minio_raw_data_task >> save_postgres_norm_data_task
