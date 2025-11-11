from pymongo import MongoClient #, AsyncMongoClient
from urllib.parse import quote_plus
from utils.execution import *
import os

class TfWrangler:

    @staticmethod
    def with_transaction(operation):
        def wrapper(self, *args,**kwargs) -> ExitCode:
            with self.client.start_session() as session:
                with session.start_transaction(): # auto abort/commit
                    return operation(self,*args, session=session,**kwargs)
        return wrapper
    
    def __init__(self, mongo_conn_url:str, database:str):
        # Set up MongoDB connection
        self.client = MongoClient(host = mongo_conn_url) #or AsyncMongoClient for async operations
        self.db = self.client[database]

    @safe_execute
    @with_transaction
    def clean_nulls(self, collection_name) -> ExitCode: 
        coll = self.db[collection_name]
        #coll.find({"$or": [{"field1": None}, {"field2": None}]})
        return SUCCESS