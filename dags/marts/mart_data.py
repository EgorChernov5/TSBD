from airflow import DAG
from airflow.sdk import timezone
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator


with DAG(
    dag_id="mart_data",
    start_date=timezone.datetime(2025, 12, 9, 0, 0, 0),
    schedule="0 * * * *",
    catchup=False,
    template_searchpath=["/opt/airflow/sql"]
) as dag:
    clan_activity = SQLExecuteQueryOperator(
        task_id="build_mart_clan_activity",
        conn_id="postgres_default",
        sql="mart_clan_activity.sql"
    )

    clan_statistic = SQLExecuteQueryOperator(
        task_id="build_mart_clan_statistic",
        conn_id="postgres_default",
        sql="mart_clan_statistic.sql"
    )

    player_activity = SQLExecuteQueryOperator(
        task_id="build_mart_player_activity",
        conn_id="postgres_default",
        sql="mart_player_activity.sql"
    )

    [clan_activity, clan_statistic, player_activity]
