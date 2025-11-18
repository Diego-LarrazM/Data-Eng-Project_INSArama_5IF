from pymongo import MongoClient #, AsyncMongoClient
from pymongo.client_session import ClientSession
from execution import *
from decimal import Decimal
import ijson.backends.python as ijson
import ast
import os

class MongoLoader:

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
    def load_from_text(self, file_path: str, collection_name: str, session: ClientSession=None) -> ExitCode:
        if not os.path.exists(file_path):
            raise Exception(f"File {file_path} does not exist.")
        
        collection = self.db[collection_name]
        with open(file_path, "r", encoding="utf-8") as file:
            data = file.readlines()
            documents = [ast.literal_eval(line) for line in data]
            collection.insert_many(documents, session=session)
        return SUCCESS
    
    @safe_execute
    @with_transaction
    def load_from_json(self, file_path: str, session: ClientSession=None) -> ExitCode: # Does not accept decimals, must be stringyfied
        if not os.path.exists(file_path):
            raise Exception(f"File {file_path} does not exist.")
        
        with open(file_path, "r", encoding="utf-8") as file:
            # Iterative load for large files
            for collection_name, documents in ijson.kvitems(file,""): # "" is root
                collection = self.db[collection_name]
                for document in documents:
                    collection.insert_one(document, session=session)

        return SUCCESS
    
    @safe_execute
    @with_transaction
    def load_single(self, data: dict, collection_name: str, session: ClientSession=None) -> ExitCode:
        if not data:
            raise Exception(f"Data to load not provided.")
        self.db[collection_name].insert_one(data, session=session)
        return SUCCESS
    
    @safe_execute
    @with_transaction
    def load_multiple(self, data: list[dict], collection_name: str, session: ClientSession=None) -> ExitCode:
        if not data:
            raise Exception(f"Data to load not provided.")
        self.db[collection_name].insert_many(data, session=session)
        return SUCCESS