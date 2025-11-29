from airflow import DAG
from datetime import datetime

from airflow.operators.python import PythonOperator

from operators.mongo_insert_operator import MongoInsertOperator
from utils.clash_of_clans_api import get_clan_rankings


with DAG(
    dag_id="clan_rankings_to_mongo",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["clash_of_clans", "mongo", "api", "clan_rankings"]
):
    fetch_task = PythonOperator(
        task_id="get_clan_rankings",
        python_callable=get_clan_rankings,
        provide_context=True
    )
    
    insert_task = MongoInsertOperator(
        task_id="insert_data",
        collection="clan_rankings",
        documents="{{ ti.xcom_pull(task_ids='get_clan_rankings') }}"
    )

    fetch_task >> insert_task