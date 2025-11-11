from pymongo import MongoClient #, AsyncMongoClient
from urllib.parse import quote_plus
import os

class TfWrangler:

    HOST = os.environ.get("MONGO_HOST_NAME")
    PORT = int(os.environ.get("MONGO_PORT"))
    USERNAME = os.environ.get("MONGO_USERNAME")
    PASSWORD = os.environ.get("MONGO_PASSWORD")
    MONGO_DB = os.environ.get("MONGO_DB")
    DATA_DIR = os.environ.get("DATA_FILE_DIRECTORY")
    
    def __init__(self, host=HOST, port=PORT, username=USERNAME, password=PASSWORD):
        credentials = ""
        if username and password:
            credentials = f"{quote_plus(username)}:{quote_plus(password)}@"
        self.client = MongoClient(host = f"mongodb://{credentials}{host}:{port}/") #or AsyncMongoClient for async operations
        self.db = self.client[self.MONGO_DB]
        if not os.path.exists(self.DATA_DIR):
            raise Exception(f"Data directory {self.DATA_DIR} does not exist.")

    def clean_nulls(self, collection_name): 
        coll = self.db[collection_name]
        #coll.find({"$or": [{"field1": None}, {"field2": None}]})