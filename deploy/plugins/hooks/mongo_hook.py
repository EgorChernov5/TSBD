import os
from airflow.hooks.base import BaseHook
from pymongo import MongoClient
from bson import ObjectId


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

    def insert(self, collection: str, documents):
        """Вставка одного или нескольких документов"""
        col = self.db[collection]

        if isinstance(documents, list):
            ids = col.insert_many(documents).inserted_ids
            return [str(_id) for _id in ids]
        
        _id = col.insert_one(documents).inserted_id
        return str(_id)
    
    def upsert_if_changed(self, collection: str, documents, key="tag"):
        col = self.db[collection]
        updated_count = 0
        inserted_count = 0
        skipped_count = 0
        
        for doc in documents:
            # Находим существующий документ по ключу
            existing = col.find_one({key: doc[key]})
            
            if existing is None:
                # Документ новый — вставляем
                col.insert_one(doc)
                inserted_count += 1
                continue
            
            # Сравниваем документы, удаляя поле _id
            existing_clean = {k: v for k, v in existing.items() if k != "_id"}
            
            # Если данные полностью совпадают — пропускаем
            if existing_clean == doc:
                skipped_count += 1
                continue
            
            # Если данные отличаются — обновляем
            col.insert_one(doc)
            updated_count += 1

        return {
            "inserted": inserted_count,
            "updated": updated_count,
            "skipped": skipped_count
        }

    def find(self, collection: str, query=None, projection=None):
        """Чтение документов"""
        col = self.db[collection]

        docs = list(col.find(query or {}, projection))

        for doc in docs:
            if "_id" in doc and isinstance(doc["_id"], ObjectId):
                doc["_id"] = str(doc["_id"])

        return docs

    def delete(self, collection: str, query):
        """Удаление документов"""
        col = self.db[collection]
        result = col.delete_many(query)

        return {
            "deleted_count": result.deleted_count
        }
