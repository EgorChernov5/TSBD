from datetime import datetime, timedelta
import requests
import pandas as pd
import json
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

from utils.clash_of_clans_api import get_clan_rankings


def load_to_postgres(**context):
    """Загрузить данные в Postgres."""
    data = context['ti'].xcom_pull(task_ids='get_clan_rankings')

    if not data:
        raise ValueError("No data received from get_clan_rankings")

    df = pd.DataFrame(data)

    # Преобразуем dict-колонки в JSON
    df['location'] = df['location'].apply(json.dumps)
    df['badgeUrls'] = df['badgeUrls'].apply(json.dumps)

    # Подключение
    hook = PostgresHook(postgres_conn_id="postgres_default")
    engine = hook.get_sqlalchemy_engine()

    # Загрузка
    df.to_sql('clan_rankings',
              engine,
              schema='public',
              if_exists='append',
              index=False)


with DAG(
    dag_id="clan_rankings_to_postgres",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["clash_of_clans", "postgres", "api", "clan_rankings"]
):

    fetch_task = PythonOperator(
        task_id="get_clan_rankings",
        python_callable=get_clan_rankings
    )

    load_data = PythonOperator(
        task_id="load_to_postgres",
        python_callable=load_to_postgres,
    )

    fetch_task >> load_data
