import logging
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

from hooks.mongo_hook import MongoHook  # путь к твоему хуку

def test_mongo_hook():
    hook = MongoHook()
    
    # Получаем список коллекций в базе
    collections = hook.db.list_collection_names()
    logging.info(f"Collections in DB {hook.database}: {collections}")
    
    # Пробуем вставить документ и прочитать его
    doc_id = hook.insert("test_collection", {"name": "AirflowTest"})
    logging.info(f"Inserted document ID: {doc_id}")

    docs = hook.find("test_collection", {"name": "AirflowTest"})
    logging.info(f"Found documents: {docs}")

    # Удаляем тестовый документ
    deleted_count = hook.delete("test_collection", {"name": "AirflowTest"})
    logging.info(f"Deleted documents count: {deleted_count}")

# DAG
with DAG(
    dag_id="test_mongo_hook",
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["test"],
) as dag:

    test_hook_task = PythonOperator(
        task_id="test_mongo_hook_task",
        python_callable=test_mongo_hook,
    )

    test_hook_task
