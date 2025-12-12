from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import timezone
from airflow import DAG

from plugins.utils import scrape_troop_images, upload_icons_to_minio

with DAG(
    dag_id="coc_scrape_images",
    start_date=timezone.datetime(2025, 12, 9, 0, 0, 0),
    schedule="@once",
    catchup=False,
) as dag:

    scrape_task = PythonOperator(
        task_id="scrape_task",
        python_callable=scrape_troop_images
    )

    upload_task = PythonOperator(
        task_id="upload_task",
        python_callable=upload_icons_to_minio
    )

    scrape_task >> upload_task