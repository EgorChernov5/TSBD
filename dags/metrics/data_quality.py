from airflow.providers.standard.operators.python import PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.utils.state import DagRunState
from airflow.sdk import timezone
from airflow import DAG

from plugins.utils.data_quality_tasks import compare_data_size, check_anomalies, scd_validation

with DAG(
    dag_id="get_data_quality_metrics",
    start_date=timezone.datetime(2025, 12, 9, 0, 0, 0),
    schedule=None,
    catchup=False,
    max_active_tasks=3
) as dag:

    start_task = ExternalTaskSensor(
        task_id="start_get_data_quality_metrics_DAG_task",
        external_dag_id="coc_minio_postprocess_data",
        external_task_id=None,
        allowed_states=[DagRunState.SUCCESS],
        failed_states=[DagRunState.FAILED],
        mode="reschedule",
        poke_interval=60,
        timeout=60 * 60 * 6,
    )

    compare_data_size_task = PythonOperator(
        task_id="compare_data_size",
        python_callable=compare_data_size
    )

    check_anomalies_task = PythonOperator(
        task_id="check_anomalies",
        python_callable=check_anomalies
    )

    scd_validation_task = PythonOperator(
        task_id="scd_validation",
        python_callable=scd_validation
    )

    start_task >> [
        compare_data_size_task,
        check_anomalies_task,
        scd_validation_task,
    ]