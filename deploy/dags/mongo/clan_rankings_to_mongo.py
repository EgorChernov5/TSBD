from airflow import DAG
from datetime import datetime

from airflow.operators.python import PythonOperator

# from operators.mongo_insert_operator import MongoInsertOperator
from hooks.mongo_hook import MongoHook
from utils.clash_of_clans_api import get_clan_rankings


def insert_clan_rankings_to_mongo(**context):
    # Получаем данные из XCom
    data = context['ti'].xcom_pull(task_ids='get_clan_rankings')
    
    if not data:
        raise ValueError("No data received from get_clan_rankings")
    
    hook = MongoHook()
    inserted_ids = hook.insert("clan_rankings", data)
    print(f"Inserted documents: {inserted_ids}")
    return inserted_ids


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

    insert_task = PythonOperator(
        task_id="insert_clan_rankings",
        python_callable=insert_clan_rankings_to_mongo,
        provide_context=True
    )

    fetch_task >> insert_task