from urllib.parse import quote_plus
import os

from mongo_loader import MongoLoader

HOST = os.environ.get("MONGO_HOST_NAME")
PORT = int(os.environ.get("MONGO_PORT"))
USERNAME = os.environ.get("MONGO_USERNAME")
PASSWORD = os.environ.get("MONGO_PASSWORD")
MONGO_DB = os.environ.get("MONGO_DB")

USER_AGENT = {"User-agent": "Mozilla/5.0"}

if __name__ == "__main__":

    credentials = ""
    if USERNAME and PASSWORD:
        credentials = f"{quote_plus(USERNAME)}:{quote_plus(PASSWORD)}@"
    mongo_url = f"mongodb://{credentials}{HOST}:{PORT}/"

    extractor = MongoLoader(
                    mongo_conn_url=mongo_url, 
                    database=MONGO_DB
                )