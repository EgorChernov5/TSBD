from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

from ..hooks.mongo_hook import MongoHook


class MongoFindOperator(BaseOperator):

    @apply_defaults
    def __init__(
        self,
        collection: str,
        query: dict = None,
        projection: dict = None,
        database: str = None,
        *args,
        **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.collection = collection
        self.query = query or {}
        self.projection = projection
        self.database = database

    def execute(self, context):
        hook = MongoHook(database=self.database)
        self.log.info(
            f"Fetching docs from '{self.collection}' "
            f"with query={self.query}"
        )

        result = hook.find(self.collection, self.query, self.projection)

        self.log.info(f"Found {len(result)} documents")
        return result
