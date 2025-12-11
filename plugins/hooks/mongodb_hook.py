from typing import Optional, Union, List, Any, Dict
import re

from airflow.sdk.bases.hook import BaseHook
from airflow.providers.mongo.hooks.mongo import MongoHook
from bson import ObjectId


class MongoDBHook(BaseHook):
    KEY_FIELD = "path"
    
    def __init__(
            self, 
            mongo_conn_id: str = "mongo_default"
    ):
        super().__init__()
        self.mongo_conn_id = mongo_conn_id
        self.hook = MongoHook(conn_id=self.mongo_conn_id)

    def list_prefixes(
        self,
        prefix: str = "",
        collection: Optional[str] = None
    ) -> List[str]:
        """
        Returns unique "folders" directly under the prefix.
        """
        col = self.hook.get_collection(collection)
        regex = re.compile(f"^{re.escape(prefix)}[^/]+/?")
        cursor = col.find({self.KEY_FIELD: {"$regex": f"^{re.escape(prefix)}"}})

        prefixes = set()
        for doc in cursor:
            path = doc.get(self.KEY_FIELD, "")
            m = regex.match(path)
            if m:
                prefixes.add(m.group(0).rstrip("/"))

        return sorted(prefixes)

    def list_objects(
        self,
        prefix: str,
        collection: Optional[str] = None
    ) -> List[str]:
        """
        Returns full file paths that start with prefix.
        """
        col = self.hook.get_collection(collection)
        cursor = col.find({self.KEY_FIELD: {"$regex": f"^{re.escape(prefix)}"}})

        return sorted(doc.get(self.KEY_FIELD, "") for doc in cursor)

    def insert(
            self,
            collection: str, 
            documents: Union[dict, List[dict]]
    ) -> Union[str, List[str]]:
        """Вставка одного или нескольких документов"""
        col = self.hook.get_collection(collection)

        if isinstance(documents, list):
            ids = col.insert_many(documents).inserted_ids
            return [str(_id) for _id in ids]

        _id = col.insert_one(documents).inserted_id

        return str(_id)

    def find(self, 
             collection: str, 
             query: Optional[Dict] = None, 
             projection: Optional[Dict] = None
    ) -> List[Dict[str, Any]]:
        """Чтение документов"""
        col = self.hook.get_collection(collection)

        docs = list(col.find(query or {}, projection))
        for doc in docs:
            if "_id" in doc and isinstance(doc["_id"], ObjectId):
                doc["_id"] = str(doc["_id"])

        return docs

    def delete(
            self, 
            collection: str, 
            query: Dict
    ) -> Dict[str, int]:
        """Удаление документов"""
        col = self.hook.get_collection(collection)
        result = col.delete_many(query)

        return {"deleted_count": result.deleted_count}