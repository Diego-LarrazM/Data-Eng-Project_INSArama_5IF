from urllib.parse import quote_plus

from persistor import Persistor
from mongo_extractor_factory import MongoExtractorFactory
from mongo_loader import MongoLoader
from models import *

import os
from dotenv import load_dotenv

dotenv_path = os.path.join(os.path.dirname(__file__), '.env')
load_dotenv(dotenv_path)

# ------------- < Constants > -------------
# Read
R_HOST = "localhost"#os.environ.get("MONGO_HOST_NAME")
R_PORT = int(os.environ.get("MONGO_PORT"))
R_USERNAME = ""#os.environ.get("MONGO_USERNAME")
R_PASSWORD = ""#os.environ.get("MONGO_PASSWORD")
MONGO_DB = os.environ.get("MONGO_DB")
REPLICA_SET_NAME= os.environ.get("MONGO_RSET_NAME")

# Write
POSTGRES_HOST = "localhost"#os.environ.get('POSTGRES_HOST')
POSTGRES_DB_URL = f"postgresql+psycopg2://{os.environ.get('POSTGRES_USER')}:{os.environ.get('POSTGRES_PASSWORD')}@{POSTGRES_HOST}:{os.environ.get('POSTGRES_PORT')}/{os.environ.get("POSTGRES_DB")}"

# ------------- < main > -------------
if __name__ == "__main__":
    print("Starting...")

    credentials = ""
    if R_USERNAME and R_PASSWORD:
        credentials = f"{quote_plus(R_USERNAME)}:{quote_plus(R_PASSWORD)}@"
    MONGO_URL = f"mongodb://{credentials}{R_HOST}:{R_PORT}/?replicaSet={REPLICA_SET_NAME}"
    
    print(f"Connecting(Mongo) to: <{MONGO_URL}>...")

    # Transient load
    loader = MongoLoader(
        mongo_conn_url=MONGO_URL,
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
        mongo_conn_url=MONGO_URL,
        r_database=MONGO_DB
    )
    
    print(f"Connecting(Postgres) to: <{POSTGRES_DB_URL}>...")
    
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
    