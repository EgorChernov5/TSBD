import os
from airflow.hooks.base import BaseHook
from pymongo import MongoClient


class MongoHook(BaseHook):
    """
    Hook для подключения к MongoDB 
    и выполнения операций.
    """

    def __init__(
        self,
        host: str = None,
        port: int = None,
        username: str = None,
        password: str = None,
        database: str = None,
        **kwargs
    ):
        super().__init__(**kwargs)

        # Если параметры не переданы — читаем из ENV
        self.host = host or os.getenv("MONGO_HOST", "localhost")
        self.port = port or int(os.getenv("MONGO_PORT", 27017))
        self.username = username or os.getenv("MONGO_INITDB_ROOT_USERNAME")
        self.password = password or os.getenv("MONGO_INITDB_ROOT_PASSWORD")
        self.database = database or os.getenv("MONGO_DB", "default")

        # Формируем URI
        if self.username and self.password:
            uri = f"mongodb://{self.username}:{self.password}@{self.host}:{self.port}"
        else:
            uri = f"mongodb://{self.host}:{self.port}"

        self.client = MongoClient(uri)
        self.db = self.client[self.database]

    # ------------ БАЗОВЫЕ МЕТОДЫ ---------------

    def insert(self, collection: str, documents):
        """Вставка одного или нескольких документов"""
        col = self.db[collection]

        if isinstance(documents, list):
            return col.insert_many(documents).inserted_ids
        return col.insert_one(documents).inserted_id

    def find(self, collection: str, query=None, projection=None):
        """Чтение документов"""
        col = self.db[collection]
        return list(col.find(query or {}, projection))

    def delete(self, collection: str, query):
        """Удаление документов"""
        col = self.db[collection]
        return col.delete_many(query)
