import os
from urllib.parse import quote_plus

from tf_wrangler import TfWrangler

HOST = os.environ.get("MONGO_HOST_NAME")
PORT = int(os.environ.get("MONGO_PORT"))
USERNAME = os.environ.get("MONGO_USERNAME")
PASSWORD = os.environ.get("MONGO_PASSWORD")
MONGO_DB = os.environ.get("MONGO_DB")

if __name__ == "__main__":
    credentials = ""
    if USERNAME and PASSWORD:
        credentials = f"{quote_plus(USERNAME)}:{quote_plus(PASSWORD)}@"
    mongo_url = f"mongodb://{credentials}{HOST}:{PORT}/"

    TransformerWrangler = TfWrangler(mongo_conn_url=mongo_url, database=MONGO_DB)
