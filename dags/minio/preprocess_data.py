from datetime import datetime

from airflow.providers.standard.operators.python import PythonOperator
from airflow import DAG

from plugins.utils import get_raw_data, save_raw_data, process_raw_data

with DAG(
    dag_id="coc_minio_preprocess_data",
    start_date=datetime.now(),
    schedule="0 */6 * * *",
    catchup=False,
) as dag:

    get_raw_data_task = PythonOperator(
        task_id="get_raw_data",
        python_callable=get_raw_data
    )

    process_raw_data_task = PythonOperator(
        task_id="process_raw_data",
        python_callable=process_raw_data
    )

    save_raw_data_task = PythonOperator(
        task_id="save_raw_data",
        python_callable=save_raw_data
    )

    get_raw_data_task >> process_raw_data_task >> save_raw_data_task