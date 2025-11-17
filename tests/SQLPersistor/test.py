from urllib.parse import quote_plus

from persistor import Persistor
from mongo_extractor_factory import MongoExtractorFactory
from mongo_loader import MongoLoader
from models import *

# ------------- < Constants > -------------
# Read 
R_HOST = "localhost"
R_PORT = 27017
R_USERNAME = "test"
R_PASSWORD = "test"
MONGO_DB = "testdb"
REPLICA_SET_NAME = "TestReplicaSet"

# Write
POSTGRES_DB_URL = "postgresql+psycopg2://test:test@localhost:5432/TestQl"

# ------------- < main > -------------
if __name__ == "__main__":
    print("Starting...")

    credentials = ""
    if R_USERNAME and R_PASSWORD:
        credentials = f"{quote_plus(R_USERNAME)}:{quote_plus(R_PASSWORD)}@"
    mongo_url = f"mongodb://{credentials}{R_HOST}:{R_PORT}/?replicaSet={REPLICA_SET_NAME}"

    # Transient load
    loader = MongoLoader(
        mongo_conn_url=mongo_url,
        database=MONGO_DB
    )

    if loader.load_from_json("example.json"):
        raise Exception("Stopped...")
    print("Loaded examples.")

    # Extracting and Persisting
    collections = [  # ORDER MATTERS WITH RELATIONSHIPS !
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

    ex_factory = MongoExtractorFactory(
        mongo_conn_url=mongo_url,
        r_database=MONGO_DB
    )
    persistor = Persistor(sqlw_conn_url=POSTGRES_DB_URL)
    if persistor.create_tables(ModelsBase):
        raise Exception("Stopped...")

    print("Tables created.")

    fail_counter = 0
    counter = 0
    for collection_name, model in collections:
      for object in ex_factory(collection_name, model, batch_size=2):
          fail_counter+=persistor.persist(object)
          counter+=1
    
    print(f"<-- Persisted successfully {counter-fail_counter}/{counter} rows -->")
    