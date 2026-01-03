import os
from urllib.parse import quote_plus
from pymongo import MongoClient
from extractor_factory import ExtractorFactory
from persistor import Persistor
from models import *  # Importe TOUS les ORM, NECESSAIRE POUR 'create_tables'

# ------------- < Constants > -------------
# Read
R_HOST = os.environ.get("MONGO_HOST_NAME")
R_PORT = int(os.environ.get("MONGO_PORT"))
R_USERNAME = os.environ.get("MONGO_USERNAME")
R_PASSWORD = os.environ.get("MONGO_PASSWORD")
MONGO_DB = os.environ.get("MONGO_DB")
WITH_ID = False  # Whether to include _id field from MongoDB documents

credentials = ""
if R_USERNAME and R_PASSWORD:
    credentials = f"{quote_plus(R_USERNAME)}:{quote_plus(R_PASSWORD)}@"
    auth_source = f"/?authSource=admin"
MONGO_URL = f"mongodb://{credentials}{R_HOST}:{R_PORT}{auth_source}"

# Write
DW_POSTGRES_HOST = os.environ.get("DW_POSTGRES_HOST")
DW_POSTGRES_PORT = 5432  # Hardcoded
DW_POSTGRES_DB_URL = f"postgresql+psycopg2://{os.environ.get('DW_POSTGRES_USER')}:{os.environ.get('DW_POSTGRES_PASSWORD')}@{DW_POSTGRES_HOST}:{DW_POSTGRES_PORT}/{os.environ.get("DW_POSTGRES_DB")}"

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

# ------------- < Pipeline Task > -------------
if __name__ == "__main__":

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
