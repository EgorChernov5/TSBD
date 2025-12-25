from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import timezone
from airflow import DAG
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.state import DagRunState

from plugins.utils.minio_tasks import (
    load_minio_raw_data,
    load_minio_raw_clan_data,

    split_minio_raw_data,
    norm_minio_raw_data,
    
    save_minio_norm_data
)
from plugins.utils.settup_task import presettup

with DAG(
    dag_id="minio_norm_data",
    start_date=timezone.datetime(2025, 12, 9, 0, 0, 0),
    schedule="0 * * * *",
    catchup=False,
) as dag:
    # wait_for_coc_minio_data = ExternalTaskSensor(
    #     task_id="wait_for_coc_minio_data_dag",
    #     external_dag_id="coc_minio_preprocess_data",    # dag, который ждём
    #     external_task_id="save_minio_raw_data",         # ждём завершения последней задачи
    #     allowed_states=[DagRunState.SUCCESS],
    #     failed_states=[DagRunState.FAILED],
    #     mode="reschedule",                              # важно, чтобы не жрал слот воркера
    #     poke_interval=20,                               # проверка каждые 20 сек
    #     timeout=60*60*6,                                # таймаут 6 часов
    #     execution_date_fn=lambda dt: dt,
    # )

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

    # Без всего
    # presettup_task >> [load_minio_raw_clan_data_task, load_minio_raw_data_task] >>\
    # split_minio_raw_data_task >> norm_minio_raw_data_task >>\
    # save_minio_norm_data_task

    # Сенсор на обновление STG слоя
    # wait_for_coc_minio_data >> presettup_task >> [load_minio_raw_clan_data_task, load_minio_raw_data_task] >>\
    # split_minio_raw_data_task >> norm_minio_raw_data_task >>\
    # save_minio_norm_data_task

    # Тригер на DDS
    trigger_postgres_scd_data_task = TriggerDagRunOperator(
       task_id='trigger_postgres_scd_data',
       trigger_dag_id='postgres_scd_data',
       wait_for_completion=False
    )

    presettup_task >> [load_minio_raw_clan_data_task, load_minio_raw_data_task] >>\
    split_minio_raw_data_task >> norm_minio_raw_data_task >>\
    save_minio_norm_data_task >> trigger_postgres_scd_data_task
