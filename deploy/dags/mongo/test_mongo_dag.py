from airflow import DAG
from datetime import datetime

from operators.mongo_insert_operator import MongoInsertOperator
from operators.mongo_find_operator import MongoFindOperator


with DAG(
    dag_id="mongo_example",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
):

    insert_task = MongoInsertOperator(
        task_id="insert_data",
        collection="users",
        documents={"name": "Alice", "age": 25}
    )

    read_task = MongoFindOperator(
        task_id="read_data",
        collection="users",
        query={"age": {"$gt": 20}}
    )

    insert_task >> read_task