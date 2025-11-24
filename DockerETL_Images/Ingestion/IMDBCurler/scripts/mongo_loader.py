from pymongo import MongoClient #, AsyncMongoClient
from pymongo.client_session import ClientSession
from utils.execution import *
from utils.batch_generator import BatchGenerator
import ijson.backends.python as ijson
import csv
import os



class MongoLoader:

    @staticmethod
    def _make_filter_func(filter_columns, filter_values_list):
        def filter_func(row):
            
            # 1. Remove forbidden columns (tu veux les supprimer mais pas skip)
            if filter_columns is not None:
                for col in filter_columns:
                    if col in row:
                        del row[col]

            # 2. Filter by values
            if filter_values_list is not None:
                match_found = False

                for filter_values in filter_values_list:
                    match = True

                    for col, vals in filter_values.items():
                        if col not in row:
                            match = False
                            break

                        vals_to_include = vals if isinstance(vals, list) else [vals]

                        if row[col] not in vals_to_include:
                            match = False
                            break

                    if match:
                        match_found = True
                        break

                if not match_found:
                    return True  # skip this row

            return False  # keep the row

        return filter_func
    
    @staticmethod
    def with_session(operation):
        def wrapper(self, *args,**kwargs) -> ExitCode:
            with self.client.start_session() as session:

                    return operation(self,*args, session=session,**kwargs)
        return wrapper
                
    
    def __init__(self, mongo_conn_url:str, database:str):
        # Set up MongoDB connection
        self.client = MongoClient(host = mongo_conn_url) #or AsyncMongoClient for async operations
        self.db = self.client[database]

    @safe_execute
    @with_session
    def load_from_csv(
        self,
        file_path: str,
        collection_name: str,
        session: ClientSession=None,
        filter_columns: list[str]=None,
        filter_values_list: list[dict] = None,
        batch_size: int = 1000
    ) -> ExitCode:

        if not os.path.exists(file_path):
            raise Exception(f"File {file_path} does not exist.")
        
        collection = self.db[collection_name]

        filter_func = MongoLoader._make_filter_func(filter_columns, filter_values_list)

        with open(file_path, "r", encoding="utf-8") as csv_file:
            row_gen = csv.DictReader(csv_file, delimiter= ";")
        
            batch_gen = BatchGenerator(
                generator=row_gen,
                batch_size=batch_size,
                filter_func=filter_func
            )

        
            for batch in batch_gen:
                collection.insert_many(batch, session=session)

        return SUCCESS
        

    @safe_execute
    @with_session
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
    @with_session
    def load_single(self, data: dict, collection_name: str, session: ClientSession=None) -> ExitCode:
        if not data:
            raise Exception(f"Data to load not provided.")
        self.db[collection_name].insert_one(data, session=session)
        return SUCCESS
    
    @safe_execute
    @with_session
    def load_multiple(self, data: list[dict], collection_name: str, session: ClientSession=None) -> ExitCode:
        if not data:
            raise Exception(f"Data to load not provided.")
        self.db[collection_name].insert_many(data, session=session)
        return SUCCESS
        