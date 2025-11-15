from pymongo import MongoClient #, AsyncMongoClient
from pymongo.database import Database
from sqlalchemy.orm import DeclarativeBase
from utils.execution import *

class MongoExtractorFactory:

    class _extractor:
        def __init__(self, collection_name: str, wrapper_model: DeclarativeBase, database: Database, batch_size: int):
            self.col = collection_name
            self.wrapper = wrapper_model
            self.cursor = database[collection_name].find({}, batch_size = batch_size)

        def __iter__(self):
            return self._extractor_gen()
        
        # Generator to extract objects from database wrapped in "wrapper" ORM
        @safe_generate(fail_return=None)
        def _extractor_gen(self):
            for doc in self.cursor:
                yield self.wrapper(doc)

    def __call__(self, collection_name: str, wrapper_model: DeclarativeBase, batch_size : int = 100):
        if batch_size <= 0:
            raise Exception(f"Batch size must be > 0 (current: {batch_size})")
        return self._extractor(collection_name, wrapper_model, self.db, batch_size)

    def __init__(self, mongo_conn_url: str, r_database: str):
        # Set up MongoDB connection
        self.client = MongoClient(host = mongo_conn_url) #or AsyncMongoClient for async operations
        self.db = self.client[r_database]
    
    
    
    
