from urllib.parse import quote_plus
import subprocess
import sys
from pathlib import Path
import os

from pymongo import MongoClient
from dotenv import load_dotenv

dotenv_path = os.path.join(os.path.dirname(__file__), ".env")
load_dotenv(dotenv_path)

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
sys.path.insert(
    1, os.path.join(str(ROOT), "DockerETL_Images/Staging/SQLPersistor/scripts/")
)

from DockerETL_Images.Staging.SQLPersistor.scripts.models import *
from DockerETL_Images.Staging.SQLPersistor.scripts.persistor import Persistor
from DockerETL_Images.Staging.SQLPersistor.scripts.extractor_factory import (
    ExtractorFactory,
)
from DockerETL_Images.Staging.TransformerWrangler.scripts.utils.mongo_loader import (
    MongoLoader,
)

# ------------- < Constants > -------------
# Read
R_HOST = "localhost"  # os.environ.get("MONGO_HOST_NAME")
R_PORT = int(os.environ.get("MONGO_PORT"))
R_USERNAME = ""  # os.environ.get("MONGO_USERNAME")
R_PASSWORD = ""  # os.environ.get("MONGO_PASSWORD")
MONGO_DB = os.environ.get("MONGO_DB")
REPLICA_SET_NAME = os.environ.get("MONGO_RSET_NAME")
WITH_ID = False  # Whether to include _id field from MongoDB documents

credentials = ""
if R_USERNAME and R_PASSWORD:
    credentials = f"{quote_plus(R_USERNAME)}:{quote_plus(R_PASSWORD)}@"
MONGO_URL = f"mongodb://{credentials}{R_HOST}:{R_PORT}/"

# Write
POSTGRES_HOST = "localhost"  # os.environ.get('POSTGRES_HOST')
DW_POSTGRES_DB_URL = f"postgresql+psycopg2://{os.environ.get('DW_POSTGRES_USER')}:{os.environ.get('DW_POSTGRES_PASSWORD')}@{POSTGRES_HOST}:{os.environ.get('DW_POSTGRES_COM_PORT')}/{os.environ.get("DW_POSTGRES_DB")}"
DW_POSTGRES_LOAD_BATCH_SIZE = int(os.environ.get("DW_POSTGRES_LOAD_BATCH_SIZE", 100))

COLLECTIONS = [  # ORDER MATTERS WITH RELATIONSHIPS !
    # Bridged Entities
    ("COMPANIES", CompanyORM),
    ("GENRES", GenreORM),
    ("ROLES", RoleORM),
    # Dimensions
    ("DIM_FRANCHISE", FranchiseDimORM),
    ("DIM_MEDIA_INFO", MediaInfoDimORM),
    ("DIM_PLATFORM", PlatformDimORM),
    ("DIM_REVIEWER", ReviewerDimORM),
    ("DIM_TIME", TimeDimORM),
    # Bridges
    ("BRIDGE_MEDIA_ROLE", MediaRoleBridgeORM),
    ("BRIDGE_MEDIA_GENRE", MediaGenreBridgeORM),
    ("BRIDGE_MEDIA_COMPANY", MediaCompanyBridgeORM),
    # Fact
    ("FACT_REVIEWS", ReviewsFactORM),
]

# ------------- < main > -------------
if __name__ == "__main__":

    print("\n[pytest] Starting Docker stack...")
    # subprocess.run(["sh", "setup.sh"], cwd=os.path.dirname(__file__), check=True)

    print(f"Connecting(Mongo) to: <{MONGO_URL}>...")

    # Transient load
    loader = MongoLoader(mongo_conn_url=MONGO_URL, database=MONGO_DB)

    if loader.load_from_json(os.path.join(os.path.dirname(__file__), "example.json")):
        raise Exception("Stopped...")
    print("Loaded examples.")

    # Extracting and Persisting
    print(f"[ Connecting to (Mongo): <{MONGO_URL}>... ]")
    client = MongoClient(host=MONGO_URL)  # or AsyncMongoClient for async operations
    transient_db = client[MONGO_DB]

    print("<-- Connected to MongoDB -->\n")

    print(f"[ Connecting to (Postgres): <{DW_POSTGRES_DB_URL}>... ]")

    persistor = Persistor(sqlw_conn_url=DW_POSTGRES_DB_URL)
    if persistor.create_tables(ModelsBase):
        raise Exception("<@@ Pipeline Stopped...(FAIL) @@>")

    print("<-- Tables created -->\n")

    print(f"[ Loading to DataWarehouse ]")
    print(f"src   : <{MONGO_URL}>")
    print(f"target: <{DW_POSTGRES_DB_URL}>\n")

    with persistor.session_scope() as session:
        for collection_name, orm in COLLECTIONS:
            for batch in ExtractorFactory().build_extractor(
                iter=transient_db[collection_name].find(
                    {}, {"_id": int(WITH_ID)}, batch_size=DW_POSTGRES_LOAD_BATCH_SIZE
                ),
                batch_size=DW_POSTGRES_LOAD_BATCH_SIZE,
                wrapper=persistor.orm_wrapper(orm),
            ):
                persistor.persist_all(batch, session=session)

            print(f"<-- Loaded {collection_name} Data -->\n")

    print(f"\n<-- Transaction status: {persistor.last_execution_status} -->")
