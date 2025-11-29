from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

from hooks.mongo_hook import MongoHook


class MongoInsertOperator(BaseOperator):
    @apply_defaults
    def __init__(
        self,
        collection: str,
        documents,
        database: str = None,
        *args,
        **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.collection = collection
        self.documents = documents
        self.database = database

    def execute(self, context):
        hook = MongoHook(database=self.database)
        self.log.info(f"Inserting into collection '{self.collection}'")

        result = hook.insert(self.collection, self.documents)

        self.log.info(f"Inserted documents: {result}")
        return result
