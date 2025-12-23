from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import timezone
from airflow import DAG

from plugins.utils.clash_of_clans_api import get_raw_data, preprocess_raw_data
from plugins.utils.minio_tasks import save_minio_raw_data

with DAG(
    dag_id="coc_minio_preprocess_data",
    start_date=timezone.datetime(2025, 12, 9, 0, 0, 0),
    schedule="0 */6 * * *",
    catchup=False,
) as dag:

    get_raw_data_task = PythonOperator(
        task_id="get_raw_data",
        python_callable=get_raw_data
    )

    preprocess_raw_data_task = PythonOperator(
        task_id="preprocess_raw_data",
        python_callable=preprocess_raw_data
    )

    save_minio_raw_data_task = PythonOperator(
        task_id="save_minio_raw_data",
        python_callable=save_minio_raw_data
    )

    # Без всего
    # get_raw_data_task >> preprocess_raw_data_task >> \
    # save_minio_raw_data_task

    # Тригер на постобработку
    # trigger_postprocess_data_DAG_task = TriggerDagRunOperator(
    #    task_id='trigger_postprocess_data_DAG',
    #    trigger_dag_id='coc_minio_postprocess_data',
    #    wait_for_completion=False
    # )

    # get_raw_data_task >> preprocess_raw_data_task >> \
    # save_minio_raw_data_task >> trigger_postprocess_data_DAG_task

    # Тригер на нормализацию
    trigger_minio_norm_data_task = TriggerDagRunOperator(
       task_id='trigger_minio_norm_data',
       trigger_dag_id='minio_norm_data',
       wait_for_completion=False
    )

    get_raw_data_task >> preprocess_raw_data_task >> \
    save_minio_raw_data_task >> trigger_minio_norm_data_task