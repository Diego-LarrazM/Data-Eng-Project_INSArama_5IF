import os
from urllib.parse import quote_plus

from mongo_extractor_factory import MongoExtractorFactory
from persistor import Persistor
from models import * # Importe TOUS les ORM, NECESSAIRE POUR 'create_tables'

# ------------- < Constants > -------------
# Read
R_HOST = os.environ.get("MONGO_HOST_NAME")
R_PORT = int(os.environ.get("MONGO_PORT"))
R_USERNAME = ""#os.environ.get("MONGO_USERNAME")
R_PASSWORD = ""#os.environ.get("MONGO_PASSWORD")
MONGO_DB = os.environ.get("MONGO_DB")
REPLICA_SET_NAME= os.environ.get("MONGO_RSET_NAME")

credentials = ""
if R_USERNAME and R_PASSWORD:
    credentials = f"{quote_plus(R_USERNAME)}:{quote_plus(R_PASSWORD)}@"
MONGO_URL = f"mongodb://{credentials}{R_HOST}:{R_PORT}/"

# Write
DW_POSTGRES_HOST = os.environ.get('DW_POSTGRES_HOST')
DW_POSTGRES_PORT = 5432 # Hardcoded
DW_POSTGRES_DB_URL = f"postgresql+psycopg2://{os.environ.get('DW_POSTGRES_USER')}:{os.environ.get('DW_POSTGRES_PASSWORD')}@{DW_POSTGRES_HOST}:{DW_POSTGRES_PORT}/{os.environ.get("DW_POSTGRES_DB")}"

DW_POSTGRES_LOAD_BATCH_SIZE= int(os.environ.get("DW_POSTGRES_LOAD_BATCH_SIZE", 100))

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
    ("FACT_REVIEWS", ReviewsFactORM)
]

# ------------- < Pipeline Task > -------------
if __name__ == "__main__":

    
    print(f"[ Connecting to (Mongo): <{MONGO_URL}>... ]")


    ex_factory = MongoExtractorFactory(
        mongo_conn_url=MONGO_URL,
        r_database=MONGO_DB
    )
    
    
    print("<-- MongoExtractorFactory connected -->\n")
    
    print(f"[ Connecting to (Postgres): <{DW_POSTGRES_DB_URL}>... ]")
    
    
    persistor = Persistor(sqlw_conn_url=DW_POSTGRES_DB_URL)
    if persistor.create_tables(ModelsBase):
        raise Exception("<@@ Pipeline Stopped...(FAIL) @@>")
    
    
    print("<-- Tables created -->\n")

    print(f"[ Loading to DataWarehouse ]")
    print(f"src   : <{MONGO_URL}>")
    print(f"target: <{DW_POSTGRES_DB_URL}>\n")
    
    fail_counter = 0
    counter = 0
    with persistor.session_scope() as session:
      for collection_name, model in COLLECTIONS:
        for object in ex_factory(collection_name, model, batch_size=DW_POSTGRES_LOAD_BATCH_SIZE):
            fail_counter+=persistor.persist(object)
            counter+=1
            
    print(f"\n<-- Persisted successfully {counter-fail_counter}/{counter} rows | Transaction status: {persistor.last_execution_status} -->")