from pymongo import MongoClient #, AsyncMongoClient
from urllib.parse import quote_plus
import os

class TfWrangler:
    
    def __init__(self, mongo_conn_url:str, database:str):
        # Set up MongoDB connection
        try:
            self.client = MongoClient(host = mongo_conn_url) #or AsyncMongoClient for async operations
            self.db = self.client[database]
        except Exception as e:
            raise Exception(f"Failed to connect to MongoDB: {e}")

    def clean_nulls(self, collection_name): 
        coll = self.db[collection_name]
        #coll.find({"$or": [{"field1": None}, {"field2": None}]})