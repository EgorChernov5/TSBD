from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import timezone
from airflow import DAG

from plugins.utils import fetch_metrics

with DAG(
    dag_id="get_task_speed_metrics",
    start_date=timezone.datetime(2025, 12, 9, 0, 0, 0),
    schedule="@daily",
    catchup=False,
) as dag:

    scrape_task = PythonOperator(
        task_id="fetch_metrics",
        python_callable=fetch_metrics
    )