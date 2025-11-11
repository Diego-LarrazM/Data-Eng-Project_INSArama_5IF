import os
from urllib.parse import quote_plus

from persistor import Persistor
from models.base import ModelsBase
# from models.class import ORMClass for each



# Read
R_HOST = os.environ.get("MONGO_HOST_NAME")
R_PORT = int(os.environ.get("MONGO_PORT"))
R_USERNAME = os.environ.get("MONGO_USERNAME")
R_PASSWORD = os.environ.get("MONGO_PASSWORD")
R_MONGO_DB = os.environ.get("MONGO_DB")

# Write
DB_URL = f"postgresql+psycopg2://{os.environ.get('POSTGRES_USER')}:{os.environ.get('POSTGRES_PASSWORD')}@{os.environ.get('POSTGRES_HOST')}:{os.environ.get('POSTGRES_PORT')}/{os.environ.get("POSTGRES_DB")}"

if __name__ == "__main__":

    credentials = ""
    if R_USERNAME and R_PASSWORD:
        credentials = f"{quote_plus(R_USERNAME)}:{quote_plus(R_PASSWORD)}@"
    mongo_url = f"mongodb://{credentials}{R_HOST}:{R_PORT}/"

    persistor = Persistor(
                        mongo_conn_url=mongo_url, 
                        r_database=R_MONGO_DB, 
                        sqlw_conn_url=DB_URL
                    )
    persistor.create_tables(ModelsBase)