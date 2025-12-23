from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import timezone
from airflow import DAG
from airflow.sensors.external_task import ExternalTaskSensor

from plugins.utils.minio_tasks import (
    presettup,

    load_minio_raw_data,
    load_minio_raw_clan_data,

    split_minio_raw_data,
    norm_minio_raw_data,
    
    save_minio_norm_data
)

with DAG(
    dag_id="minio_norm_data",
    start_date=timezone.datetime(2025, 12, 9, 0, 0, 0),
    schedule=None,
    catchup=False,
) as dag:
    wait_for_coc_minio_data = ExternalTaskSensor(
        task_id="wait_for_coc_minio_data_dag",
        external_dag_id="coc_minio_preprocess_data",    # dag, который ждём
        external_task_id="save_minio_raw_data",         # ждём завершения последней задачи
        allowed_states=["success"],
        failed_states=["failed"],
        mode="reschedule",                              # важно, чтобы не жрал слот воркера
        poke_interval=60*5,                              # проверка каждые 5 мин
        timeout=60*60*6,                                # таймаут 6 часов
    )

    presettup_task = PythonOperator(
        task_id="presettup",
        python_callable=presettup
    )

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

    save_minio_norm_data_task = PythonOperator(
        task_id="save_minio_norm_data",
        python_callable=save_minio_norm_data
    )

    # wait_for_coc_minio_data >> presettup_task >> [load_minio_raw_clan_data_task, load_minio_raw_data_task] >>\
    # split_minio_raw_data_task >> norm_minio_raw_data_task >>\
    # save_minio_norm_data_task
    presettup_task >> [load_minio_raw_clan_data_task, load_minio_raw_data_task] >>\
    split_minio_raw_data_task >> norm_minio_raw_data_task >>\
    save_minio_norm_data_task
