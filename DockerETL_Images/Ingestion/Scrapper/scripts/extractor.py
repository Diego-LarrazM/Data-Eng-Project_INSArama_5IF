from pymongo import MongoClient, AsyncMongoClient
import ast
import os

class Extractor:

    DATA_DIR = os.environ.get("DATA_FILE_DIRECTORY")
    
    def __init__(self, mongo_conn_url:str, database:str):

        if not os.path.exists(Extractor.DATA_DIR):
            raise Exception(f"Data directory {Extractor.DATA_DIR} does not exist.")
        
        # Set up MongoDB connection
        try:
            self.client = MongoClient(host = mongo_conn_url) #or AsyncMongoClient for async operations
            self.db = self.client[database]
        except Exception as e:
            raise Exception(f"Failed to connect to MongoDB: {e}")
        
    # def extract_IMDB_data(self, filename = "imdb_data.txt"): or whatever extension
    # def extract_games_metacritic_data(self, filename = "games_metac_data.txt"):
    # def extract_filmTvReviews_data(self, filename = "ftv_reviews_data.txt"):

    def extract_from_to(self, filename: str, collection_name: str):
        collection = self.db[collection_name]
        with open(f"{self.DATA_DIR}{filename}", "r", encoding="utf-8") as file:
            data = file.readlines()
            documents = [ast.literal_eval(line) for line in data]
            collection.insert_many(documents)