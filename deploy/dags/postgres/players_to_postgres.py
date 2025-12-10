import os
from airflow import DAG
from datetime import datetime
import pandas as pd
import json

from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from utils.clash_of_clans_api import get_api_data


def get_clans_from_postgres():
    """Получаем список тегов кланов из Postgres."""
    hook = PostgresHook(postgres_conn_id="postgres_default")
    sql = "SELECT tag FROM clans"
    records = hook.get_pandas_df(sql)
    
    if records.empty:
        raise ValueError("No data received from clans table")
    
    clans = records['tag'].tolist()
    return clans


def get_players(**context):
    """Получаем игроков для каждого клана через API."""
    clans = context['ti'].xcom_pull(task_ids='get_clans_from_postgres')
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
        raise ValueError("API returned no players")

    return data


def insert_players_to_postgres(**context):
    """Вставляем игроков в Postgres."""
    data = context['ti'].xcom_pull(task_ids='get_players')
    if not data:
        raise ValueError("No data received from get_players")

    df = pd.DataFrame(data)

    # Преобразуем все dict- и list-колонки в JSON строки
    for col in df.columns:
        if df[col].apply(lambda x: isinstance(x, (dict, list))).any():
            df[col] = df[col].apply(json.dumps)

    hook = PostgresHook(postgres_conn_id="postgres_default")
    engine = hook.get_sqlalchemy_engine()

    df.to_sql('players', engine, schema='public', if_exists='append', index=False)
    print(f"Inserted {len(df)} players into Postgres")
    return len(df)


with DAG(
    dag_id="players_to_postgres",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["clash_of_clans", "postgres", "api", "players"]
):
    get_task = PythonOperator(
        task_id="get_clans_from_postgres",
        python_callable=get_clans_from_postgres
    )

    fetch_task = PythonOperator(
        task_id="get_players",
        python_callable=get_players,
        provide_context=True
    )

    insert_task = PythonOperator(
        task_id="insert_players",
        python_callable=insert_players_to_postgres,
        provide_context=True
    )

    get_task >> fetch_task >> insert_task
