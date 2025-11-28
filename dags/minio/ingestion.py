from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
import datetime
import requests
import json
from minio import Minio
import io

def fetch_and_save_raw(**context):
    execution_ts = context["ds_nodash"]

    # --- API CALL ---
    resp = requests.get(
        "https://api.clashofclans.com/v1/clans?limit=10",
        headers={"Authorization": "Bearer YOUR_TOKEN"}
    )
    data = resp.json()

    # --- SAVE TO MINIO ---
    client = Minio(
        "minio:9000",
        access_key="minio_access",
        secret_key="minio_secret",
        secure=False
    )

    bucket = "coc"
    filename = f"raw/clans/ingested_at={execution_ts}/clans_{execution_ts}.json"
    json_bytes = json.dumps(data).encode("utf-8")

    client.put_object(
        bucket,
        filename,
        io.BytesIO(json_bytes),
        len(json_bytes)
    )

with DAG(
    dag_id="coc_ingestion",
    start_date=days_ago(1),
    schedule_interval="0 */6 * * *",   # every 6 hours
    catchup=False,
) as dag:

    ingest_task = PythonOperator(
        task_id="fetch_and_save_raw",
        python_callable=fetch_and_save_raw
    )

    trigger_processing = TriggerDagRunOperator(
        task_id="trigger_processing_dag",
        trigger_dag_id="coc_processing",
        conf={"date": "{{ ds_nodash }}"}
    )

    ingest_task >> trigger_processing
