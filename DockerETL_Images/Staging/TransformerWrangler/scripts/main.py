import os
from urllib.parse import quote_plus

from utils.mongo_loader import MongoLoader

HOST = os.environ.get("MONGO_HOST_NAME")
PORT = int(os.environ.get("MONGO_PORT"))
USERNAME = os.environ.get("MONGO_USERNAME")
PASSWORD = os.environ.get("MONGO_PASSWORD")
MONGO_DB = os.environ.get("MONGO_DB")

credentials = ""
if USERNAME and PASSWORD:
    credentials = f"{quote_plus(USERNAME)}:{quote_plus(PASSWORD)}@"
mongo_url = f"mongodb://{credentials}{HOST}:{PORT}/"

SOURCE_DATA_DIR = os.path.join(os.environ.get("DATA_FILE_DIRECTORY"), "/source_data")
OUTPUT_DIR = os.path.join(os.environ.get("DATA_FILE_DIRECTORY"), "/processed_data")

COLLECTIONS = [  # ORDER MATTERS WITH RELATIONSHIPS !
    # Bridged Entities
    "COMPANIES",
    "GENRES",
    "ROLES",
    # Dimensions
    "DIM_FRANCHISE",
    "DIM_MEDIA_INFO",
    "DIM_PLATFORM",
    "DIM_REVIEWER",
    "DIM_TIME",
    # Bridges
    "BRIDGE_MEDIA_ROLE",
    "BRIDGE_MEDIA_GENRE",
    "BRIDGE_MEDIA_COMPANY",
    # Fact
    "FACT_REVIEWS",
]

if __name__ == "__main__":

    # Transform

    # Load to transient database (MongoDB)
    print(f"[ Loading transformed data to transient MongoDB at: <{mongo_url}>... ]")
    loader = MongoLoader(mongo_conn_url=mongo_url, database=MONGO_DB)

    for collection in COLLECTIONS:
        print(f"Loading collection: {collection}...")
        if not loader.load_from_csv(f"{SOURCE_DATA_DIR}/{collection}.csv", collection):
            raise Exception(f"Failed to load collection: {collection}!")

    print("All collections loaded successfully!")