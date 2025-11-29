import os
from airflow import DAG
from datetime import datetime

from airflow.operators.python import PythonOperator

# from operators.mongo_insert_operator import MongoInsertOperator
from hooks.mongo_hook import MongoHook
from utils.clash_of_clans_api import get_api_data


def get_clans_from_mongo():
    hook = MongoHook()
    finded_ids = hook.find('clan_rankings')
    if not finded_ids:
        raise ValueError("No data received from clan_rankings")
    
    clans = [clan['tag'] for clan in finded_ids]
    return clans


def get_clans(**context):
    # Получаем данные из XCom
    clan_rankings = context['ti'].xcom_pull(task_ids='get_clans_from_mongo')
    if not clan_rankings:
        raise ValueError('Empty clan rankings')
    
    TOKEN_API = os.getenv('TOKEN_API')
    data = []
    for tag in clan_rankings:
        tag_clan = tag.replace('#', '%23')
        base_url = f'https://api.clashofclans.com/v1/clans/{tag_clan}'
        headers = {
            'Authorization': f'Bearer {TOKEN_API}',
            'Content-Type': 'application/json'
        }
        params = {}

        clan_data = get_api_data(base_url, headers, params)

        if not clan_data:
            continue
        
        data.append(clan_data)

    if not data:
        raise ValueError("API returned unexpected structure")

    return data


def insert_clans_to_mongo(**context):
    # Получаем данные из XCom
    data = context['ti'].xcom_pull(task_ids='get_clans')
    
    if not data:
        raise ValueError("No data received from get_clans")
    
    hook = MongoHook()
    inserted_ids = hook.insert("clans", data)
    print(f"Inserted documents: {inserted_ids}")
    return inserted_ids


with DAG(
    dag_id="clans_to_mongo",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["clash_of_clans", "mongo", "api", "clans"]
):
    get_task = PythonOperator(
        task_id="get_clans_from_mongo",
        python_callable=get_clans_from_mongo
    )

    fetch_task = PythonOperator(
        task_id="get_clans",
        python_callable=get_clans,
        provide_context=True
    )

    insert_task = PythonOperator(
        task_id="insert_clans",
        python_callable=insert_clans_to_mongo,
        provide_context=True
    )

    get_task >> fetch_task
    fetch_task >> insert_task