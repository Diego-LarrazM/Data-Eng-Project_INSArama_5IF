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

    extractor = MongoLoader(
                    mongo_conn_url=mongo_url, 
                    database=MONGO_DB
                )
    
    # Image Testing
    extractor.extract_from_to(f"{DATA_DIR}/test.txt", "test_collection")