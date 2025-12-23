from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import timezone
from airflow import DAG
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.utils.state import DagRunState

from plugins.utils.minio_tasks import load_minio_norm_data
from plugins.utils.postgres_tasks import load_postgres_sqd_data, compare_scd_data, save_postgres_scd_data
from plugins.utils.settup_task import presettup

with DAG(
    dag_id="postgres_scd_data",
    start_date=timezone.datetime(2025, 12, 9, 0, 0, 0),
    schedule="0 */6 * * *",
    catchup=False,
) as dag:
    wait_for_minio_norm_data = ExternalTaskSensor(
        task_id="wait_for_minio_norm_data_dag",
        external_dag_id="minio_norm_data",              # dag, который ждём
        external_task_id='save_minio_norm_data',        # ждём завершения последней задачи
        allowed_states=[DagRunState.SUCCESS],
        failed_states=[DagRunState.FAILED],
        mode="reschedule",                              # важно, чтобы не жрал слот воркера
        poke_interval=20,                               # проверка каждые 20 сек
        timeout=60*60*6,                                # таймаут 6 часов
        execution_date_fn=lambda dt: dt,
    )

    presettup_task = PythonOperator(
        task_id="presettup",
        python_callable=presettup
    )

    load_minio_norm_data_task = PythonOperator(
        task_id='load_minio_norm_data',
        python_callable=load_minio_norm_data
    )

    load_postgres_sqd_data_task = PythonOperator(
        task_id='load_postgres_sqd_data',
        python_callable=load_postgres_sqd_data
    )

    compare_scd_data_task = PythonOperator(
        task_id='compare_scd_data',
        python_callable=compare_scd_data
    )

    save_postgres_scd_data_task = PythonOperator(
        task_id='save_postgres_scd_data',
        python_callable=save_postgres_scd_data
    )

    wait_for_minio_norm_data >> presettup_task >>\
    [load_minio_norm_data_task, load_postgres_sqd_data_task] >>\
    compare_scd_data_task >> save_postgres_scd_data_task
    # presettup_task >>\
    # [load_minio_norm_data_task, load_postgres_sqd_data_task] >>\
    # compare_scd_data_task >> save_postgres_scd_data_task
