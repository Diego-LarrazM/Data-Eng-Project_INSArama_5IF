import os
from urllib.parse import quote_plus
from pymongo import MongoClient
from extractor_factory import ExtractorFactory
from persistor import Persistor

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
DW_NEO_DB = "neo4j"
DW_NEO_HOST = os.environ.get("DW_NEO_HOST")
DW_NEO_PORT = 7687  # Hardcoded
DW_NEO_DB_URL = f"bolt://{DW_NEO_HOST}:{DW_NEO_PORT}"
DW_NEO_AUTH = None

DW_NEO_LOAD_BATCH_SIZE = int(os.environ.get("DW_NEO_LOAD_BATCH_SIZE", 100))


if __name__ == "__main__":

    print(f"[ Connecting to (Mongo): <{MONGO_URL}>... ]")
    client = MongoClient(host=MONGO_URL)  # or AsyncMongoClient for async operations
    transient_db = client[MONGO_DB]

    print("<-- Connected to MongoDB -->\n")

    print(f"[ Connecting to (NEO4J): <{DW_NEO_DB_URL}>... ]")

    persistor = Persistor(uri=DW_NEO_DB_URL, auth=DW_NEO_AUTH, database=DW_NEO_DB)

    print(f"[ Loading to DataWarehouse ]")
    print(f"src   : <{MONGO_URL}>")
    print(f"target: <{DW_NEO_DB_URL}>\n")

    # Extractors
    extractorFactory = ExtractorFactory()

    entity_extractor = extractorFactory.build_extractor(
        iter=transient_db["GRAPH_ENTITIES"].find(
            {}, {"_id": int(WITH_ID)}, batch_size=DW_NEO_LOAD_BATCH_SIZE
        ),
        batch_size=DW_NEO_LOAD_BATCH_SIZE,
    )

    link_extractor = extractorFactory.build_extractor(
        iter=transient_db["GRAPH_LINKS"].find(
            {}, {"_id": int(WITH_ID)}, batch_size=DW_NEO_LOAD_BATCH_SIZE
        ),
        batch_size=DW_NEO_LOAD_BATCH_SIZE,
    )

    # Persistance
    for batch in entity_extractor:
        persistor.persist_nodes(batch)
    print(
        f"\n<-- Node persistance status: {"FAILURE" if persistor.last_execution_status else "SUCCESS"} -->"
    )

    for batch in link_extractor:
        persistor.persist_links(batch)
    print(
        f"\n<-- Link persistance status: {"FAILURE" if persistor.last_execution_status else "SUCCESS"} -->"
    )

    print("<-- Neo4J Persistance finished -->")
