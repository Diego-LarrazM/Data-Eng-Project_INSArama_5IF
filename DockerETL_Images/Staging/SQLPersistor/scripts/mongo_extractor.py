from pymongo import MongoClient #, AsyncMongoClient
from utils.execution import *

class MongoExtractor:

    def __init__(self, mongo_conn_url: str, r_database: str):
        # Set up MongoDB connection
        self.client = MongoClient(host = mongo_conn_url) #or AsyncMongoClient for async operations
        self.db = self.client[r_database]
    
    @safe_execute(fail_return=[])
    def get_collection(self, collection_name: str) -> list[dict]:
        return self.db[collection_name].find({})
    
    