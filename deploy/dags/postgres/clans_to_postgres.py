import os
from airflow import DAG
from datetime import datetime
import pandas as pd
import json

from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from utils.clash_of_clans_api import get_api_data


def get_clan_rankings_from_postgres():
    """Получить теги кланов из таблицы clan_rankings в Postgres."""
    hook = PostgresHook(postgres_conn_id="postgres_default")
    engine = hook.get_sqlalchemy_engine()
    
    query = "SELECT tag FROM public.clan_rankings"
    df = pd.read_sql(query, engine)
    
    if df.empty:
        raise ValueError("No data received from clan_rankings")
    
    clans = df['tag'].tolist()
    return clans[:10]


def get_clans(**context):
    """Вызов API Clash of Clans для каждого клана."""
    clan_rankings = context['ti'].xcom_pull(task_ids='get_clan_rankings_from_postgres')
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


def insert_clans_to_postgres(**context):
    """Загрузка данных о кланах в Postgres."""
    data = context['ti'].xcom_pull(task_ids='get_clans')
    
    if not data:
        raise ValueError("No data received from get_clans")
    
    df = pd.DataFrame(data)
    
    # Сериализация всех колонок с dict/list в JSON
    for col in df.columns:
        if df[col].apply(lambda x: isinstance(x, (dict, list))).any():
            df[col] = df[col].apply(json.dumps)
    
    hook = PostgresHook(postgres_conn_id="postgres_default")
    engine = hook.get_sqlalchemy_engine()
    
    df.to_sql(
        'clans',
        engine,
        schema='public',
        if_exists='append',
        index=False
    )
    print(f"Inserted {len(df)} players into Postgres")


with DAG(
    dag_id="clans_to_postgres",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["clash_of_clans", "postgres", "api", "clans"]
):
    get_task = PythonOperator(
        task_id="get_clan_rankings_from_postgres",
        python_callable=get_clan_rankings_from_postgres
    )

    fetch_task = PythonOperator(
        task_id="get_clans",
        python_callable=get_clans,
        provide_context=True
    )

    insert_task = PythonOperator(
        task_id="insert_clans",
        python_callable=insert_clans_to_postgres,
        provide_context=True
    )

    get_task >> fetch_task >> insert_task
