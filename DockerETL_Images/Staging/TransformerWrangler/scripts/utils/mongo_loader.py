from pymongo import MongoClient  # , AsyncMongoClient
from pymongo.client_session import ClientSession
from typing import Callable, Any

from .execution import *
from .batch_generator import BatchGenerator


import ijson.backends.python as ijson
import csv
import os


class MongoLoader:

    @staticmethod
    def _make_filter_func(
        blacklist_columns: list[str], whitelist_values: list[dict[str, set]]
    ) -> Callable[[dict[str, Any]], bool]:
        """
        Blacklist columns are removed from the row.
        Whitelist values is a list of dicts, each dict maps column names to sets of allowed values.
        A row is kept if it matches at least one of the dicts in whitelist_values or w
        """

        def filter_func(row: dict[str, Any]) -> bool:
            # 1. Remove forbidden columns (tu veux les supprimer mais pas skip)
            if blacklist_columns is not None:
                for col in blacklist_columns:
                    row.pop(col, None)

            # 2. Filter by values
            if whitelist_values is None:
                return True  # keep the row if no filters

            for filter_group in whitelist_values:
                match = True
                for col, allowed_set in filter_group.items():
                    # Check if the row's value for the column is in the allowed set
                    actual_value = row.get(col, None)
                    if actual_value is None or actual_value not in allowed_set:
                        match = False
                        break
                if match:
                    return True  # Found a matching group, keep the row
            return False  # Not found, row skipped

        return filter_func

    # def with_transaction(operation):
    #     def wrapper(self, *args,**kwargs) -> ExitCode:
    #         with self.client.start_session() as session:
    #             with session.start_transaction(): # auto abort/commit
    #                 return operation(self,*args, session=session,**kwargs)
    #     return wrapper

    def __init__(self, mongo_conn_url: str, database: str):
        # Set up MongoDB connection
        self.client = MongoClient(
            host=mongo_conn_url
        )  # or AsyncMongoClient for async operations
        self.db = self.client[database]

    @safe_execute
    def load_from_csv(
        self,
        file_path: str,
        collection_name: str,
        session: ClientSession = None,
        filter_columns: list[str] = None,
        filter_values_list: list[dict] = None,
        batch_size: int = 1000,
    ) -> ExitCode:

        if not os.path.exists(file_path):
            raise Exception(f"File {file_path} does not exist.")

        collection = self.db[collection_name]

        filter_func = MongoLoader._make_filter_func(filter_columns, filter_values_list)

        with open(file_path, "r", encoding="utf-8") as csv_file:
            row_gen = csv.DictReader(csv_file, delimiter=";")

            batch_gen = BatchGenerator(
                generator=row_gen, batch_size=batch_size, filter_func=filter_func
            )

            for batch in batch_gen:
                collection.insert_many(batch, session=session)

        return SUCCESS

    @safe_execute
    def load_from_json(
        self, file_path: str, session: ClientSession = None
    ) -> ExitCode:  # Does not accept decimals, must be stringyfied
        if not os.path.exists(file_path):
            raise Exception(f"File {file_path} does not exist.")

        with open(file_path, "r", encoding="utf-8") as file:
            # Iterative load for large files
            for collection_name, documents in ijson.kvitems(file, ""):  # "" is root
                collection = self.db[collection_name]
                for document in documents:
                    collection.insert_one(document, session=session)

        return SUCCESS

    @safe_execute
    def load_single(
        self, data: dict, collection_name: str, session: ClientSession = None
    ) -> ExitCode:
        if not data:
            raise Exception(f"Data to load not provided.")
        self.db[collection_name].insert_one(data, session=session)
        return SUCCESS

    @safe_execute
    def load_multiple(
        self, data: list[dict], collection_name: str, session: ClientSession = None
    ) -> ExitCode:
        if not data:
            raise Exception(f"Data to load not provided.")
        self.db[collection_name].insert_many(data, session=session)
        return SUCCESS
