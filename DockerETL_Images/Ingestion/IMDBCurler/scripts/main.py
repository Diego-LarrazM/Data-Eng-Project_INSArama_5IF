from urllib.parse import quote_plus
import os

from mongo_loader import MongoLoader

HOST = os.environ.get("MONGO_HOST_NAME")
PORT = int(os.environ.get("MONGO_PORT"))
USERNAME = os.environ.get("MONGO_USERNAME")
PASSWORD = os.environ.get("MONGO_PASSWORD")
MONGO_DB = os.environ.get("MONGO_DB")

DATA_DIR = os.environ.get("DATA_FILE_DIRECTORY")

if __name__ == "__main__":

    credentials = ""
    if USERNAME and PASSWORD:
        credentials = f"{quote_plus(USERNAME)}:{quote_plus(PASSWORD)}@"
    mongo_url = f"mongodb://{credentials}{HOST}:{PORT}/"

    loader = MongoLoader(
                    mongo_conn_url=mongo_url, 
                    database=MONGO_DB
                )
    
   
    loader.load_from_csv(f"{DATA_DIR}/title.principals.csv", "imdb_title_principals")
    loader.load_from_csv(f"{DATA_DIR}/title.crew.csv", "imdb_title_crew")
    loader.load_from_csv(f"{DATA_DIR}/name.basics.csv", "imdb_name_basics")
    loader.load_from_csv(f"{DATA_DIR}/title.basics.csv", "imdb_title_basics")
    