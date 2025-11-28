from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

from plugins.utils import scrape_troop_images
from plugins.operators import MinioUploadOperator

with DAG(
    dag_id="coc_scrape_images",
    start_date=datetime.now(),
    schedule="@once",
    catchup=False,
) as dag:

    scrape_task = PythonOperator(
        task_id="scrape_task",
        python_callable=scrape_troop_images,
        provide_context=True,
    )

    upload_task = MinioUploadOperator(
        task_id="upload_task",
        bucket="my-bucket",
        source_task_ids="scrape_task"
    )

    scrape_task >> upload_task