from persistor import Persistor
from models.base import ModelsBase
# from models.class import ORMClass for each
import os

# Read
R_HOST = os.environ.get("MONGO_HOST_NAME")
R_PORT = int(os.environ.get("MONGO_PORT"))
R_USERNAME = os.environ.get("MONGO_USERNAME")
R_PASSWORD = os.environ.get("MONGO_PASSWORD")
R_MONGO_DB = os.environ.get("MONGO_DB")

# Write
DB_URL = f"postgresql+psycopg2://{os.environ.get('POSTGRES_USER')}:{os.environ.get('POSTGRES_PASSWORD')}@{os.environ.get('POSTGRES_HOST')}:{os.environ.get('POSTGRES_PORT')}/{os.environ.get("POSTGRES_DB")}"

if __name__ == "__main__":
    read_info = {
        "host":R_HOST, 
        "port":R_PORT, 
        "username":R_USERNAME, 
        "password":R_PASSWORD,
        "mongo_db":R_MONGO_DB,
    }

    PersistorObj = Persistor(read_info, DB_URL)
    PersistorObj.create_tables(ModelsBase)