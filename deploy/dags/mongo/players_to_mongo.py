import os
from airflow import DAG
from datetime import datetime

from airflow.operators.python import PythonOperator

# from operators.mongo_insert_operator import MongoInsertOperator
from hooks.mongo_hook import MongoHook
from utils.clash_of_clans_api import get_api_data


def get_clans_from_mongo():
    hook = MongoHook()
    finded_ids = hook.find('clans')
    if not finded_ids:
        raise ValueError("No data received from clans")
    
    clans = [clan['tag'] for clan in finded_ids]
    return clans


def get_players(**context):
    # Получаем данные из XCom
    clans = context['ti'].xcom_pull(task_ids='get_clans_from_mongo')
    if not clans:
        raise ValueError('Empty clans')
    
    TOKEN_API = os.getenv('TOKEN_API')
    data = []
    for tag in clans:
        tag_clan = tag.replace('#', '%23')
        base_url = f"https://api.clashofclans.com/v1/clans/{tag_clan}/members"
        headers = {
            "Authorization": f"Bearer {TOKEN_API}",
            "Content-Type": "application/json"
        }
        params = {}

        members_data = get_api_data(base_url, headers, params)
        if members_data is None or 'items' not in members_data:
            raise ValueError("API returned unexpected structure")

        data.extend(members_data["items"])

    if not data:
        raise ValueError("API returned unexpected structure")

    return data


def insert_players_to_mongo(**context):
    # Получаем данные из XCom
    data = context['ti'].xcom_pull(task_ids='get_players')
    
    if not data:
        raise ValueError("No data received from get_players")
    
    hook = MongoHook()
    inserted_ids = hook.insert("players", data)
    print(f"Inserted documents: {inserted_ids}")
    return inserted_ids


with DAG(
    dag_id="players_to_mongo",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["clash_of_clans", "mongo", "api", "players"]
):
    get_task = PythonOperator(
        task_id="get_clans_from_mongo",
        python_callable=get_clans_from_mongo
    )

    fetch_task = PythonOperator(
        task_id="get_players",
        python_callable=get_players,
        provide_context=True
    )

    insert_task = PythonOperator(
        task_id="insert_players",
        python_callable=insert_players_to_mongo,
        provide_context=True
    )

    get_task >> fetch_task
    fetch_task >> insert_task