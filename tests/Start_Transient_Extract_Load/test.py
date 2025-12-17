from urllib.parse import quote_plus
import subprocess
import sys
from pathlib import Path
import os
from dotenv import load_dotenv

dotenv_path = os.path.join(os.path.dirname(__file__), '.env')
load_dotenv(dotenv_path)

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
sys.path.insert(1, os.path.join(str(ROOT),"DockerETL_Images/Staging/SQLPersistor/scripts/"))

from DockerETL_Images.Staging.SQLPersistor.scripts.models import *
from DockerETL_Images.Staging.SQLPersistor.scripts.persistor import Persistor
from DockerETL_Images.Staging.SQLPersistor.scripts.mongo_extractor_factory import MongoExtractorFactory
from DockerETL_Images.Ingestion.IMDBCurler.scripts.mongo_loader import MongoLoader

# ------------- < Constants > -------------
# Read
R_HOST = "localhost"#os.environ.get("MONGO_HOST_NAME")
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
POSTGRES_HOST = "localhost"#os.environ.get('POSTGRES_HOST')
POSTGRES_DB_URL = f"postgresql+psycopg2://{os.environ.get('DW_POSTGRES_USER')}:{os.environ.get('DW_POSTGRES_PASSWORD')}@{POSTGRES_HOST}:{os.environ.get('DW_POSTGRES_COM_PORT')}/{os.environ.get("DW_POSTGRES_DB")}"

POSTGRES_LOAD_BATCH_SIZE= int(os.environ.get("DW_POSTGRES_LOAD_BATCH_SIZE", 100))

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

# ------------- < main > -------------
if __name__ == "__main__":
    
    print("\n[pytest] Starting Docker stack...")
    subprocess.run(
        ["sh", "setup.sh"],
        cwd=os.path.dirname(__file__),
        check=True
    )
    
    print(f"Connecting(Mongo) to: <{MONGO_URL}>...")

    # Transient load
    loader = MongoLoader(
        mongo_conn_url=MONGO_URL,
        database=MONGO_DB
    )

    if loader.load_from_json(os.path.join(os.path.dirname(__file__),"example.json")):
        raise Exception("Stopped...")
    print("Loaded examples.")

    # Extracting and Persisting
    print(f"[ Connecting to (Mongo): <{MONGO_URL}>... ]")


    ex_factory = MongoExtractorFactory(
        mongo_conn_url=MONGO_URL,
        r_database=MONGO_DB
    )
    
    
    print("<-- MongoExtractorFactory connected -->\n")
    
    print(f"[ Connecting to (Postgres): <{POSTGRES_DB_URL}>... ]")
    
    
    persistor = Persistor(sqlw_conn_url=POSTGRES_DB_URL)
    if persistor.create_tables(ModelsBase):
        raise Exception("<@@ Pipeline Stopped...(FAIL) @@>")
    
    
    print("<-- Tables created -->\n")

    print(f"[ Loading to DataWarehouse ]")
    print(f"src   : <{MONGO_URL}>")
    print(f"target: <{POSTGRES_DB_URL}>\n")
    
    with persistor.session_scope() as session:
      for collection_name, model in COLLECTIONS:
        extractor = ex_factory(collection_name, model, batch_size=POSTGRES_LOAD_BATCH_SIZE)
        persistor.persist_from(extractor, batch_size=POSTGRES_LOAD_BATCH_SIZE, session=session)
            
    print(f"\n<-- Transaction status: {persistor.last_execution_status} -->")
    