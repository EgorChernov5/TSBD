from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from minio import Minio
import pandas as pd
import io
import json

def process_raw_to_parquet(**context):
    processing_date = context["dag_run"].conf["date"]

    client = Minio("minio:9000", "minio_access", "minio_secret", secure=False)

    # --- DOWNLOAD RAW ---
    raw_path = f"raw/clans/ingested_at={processing_date}/clans_{processing_date}.json"
    obj = client.get_object("coc", raw_path)
    raw_json = json.loads(obj.data)

    # --- TRANSFORM ---
    clans = raw_json.get("items", [])

    clans_df = pd.json_normalize(clans)
    # extract clan members if needed

    clans_bytes = clans_df.to_parquet(index=False)

    # --- SAVE TO PROCESSED ---
    processed_path = f"processed/clans/dt={processing_date}/clans.parquet"

    client.put_object(
        "coc",
        processed_path,
        io.BytesIO(clans_bytes),
        len(clans_bytes)
    )

with DAG(
    dag_id="coc_processing",
    start_date=days_ago(1),
    schedule_interval=None,  # only triggered
    catchup=False,
) as dag:

    process_task = PythonOperator(
        task_id="process_raw_data",
        python_callable=process_raw_to_parquet
    )

    trigger_metrics = TriggerDagRunOperator(
        task_id="trigger_metrics_dag",
        trigger_dag_id="coc_metrics",
        conf={"date": "{{ dag_run.conf['date'] }}"}
    )

    process_task >> trigger_metrics
