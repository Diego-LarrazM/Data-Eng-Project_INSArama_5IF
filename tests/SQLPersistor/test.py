from urllib.parse import quote_plus

from persistor import Persistor
from models import *

import os
from dotenv import load_dotenv

dotenv_path = os.path.join(os.path.dirname(__file__), '.env')
load_dotenv(dotenv_path)

# ------------- < Constants > -------------
POSTGRES_HOST = "localhost"#os.environ.get('POSTGRES_HOST')
POSTGRES_DB_URL = f"postgresql+psycopg2://{os.environ.get('DW_POSTGRES_USER')}:{os.environ.get('DW_POSTGRES_PASSWORD')}@{POSTGRES_HOST}:{os.environ.get('DW_POSTGRES_COM_PORT')}/{os.environ.get("DW_POSTGRES_DB")}"
POSTGRES_LOAD_BATCH_SIZE= int(os.environ.get("DW_POSTGRES_LOAD_BATCH_SIZE", 100))

example_data_all = [
    GenreORM(name="all1"),
    GenreORM(name="all2"),
    GenreORM(name="all3"),
]

example_data = [
    GenreORM(name="one1"),
    GenreORM(name="one2"),
]

error_data = [
    GenreORM(name="shouldNotAppear1"),
    GenreORM(name=None),  # This will cause an integrity error (name cannot be null)
]

# ------------- < main > -------------
if __name__ == "__main__":

    print(f"[ Connecting to (Postgres): <{POSTGRES_DB_URL}>... ]")
    

    persistor = Persistor(sqlw_conn_url=POSTGRES_DB_URL)
    if persistor.create_tables(ModelsBase):
        raise Exception("<@@ Pipeline Stopped...(FAIL) @@>")
    

    print("<-- Tables created -->\n")

    print(f"[ Loading to DataWarehouse ]")
    print(f"target: <{POSTGRES_DB_URL}>\n")
    print(f"(Errors raised are normal, it's part of the test)\n")

    
    with persistor.session_scope() as s:
        persistor.persist_all(example_data_all, session=s)
        persistor.persist_all(error_data, session=s) # This will raise an integrity error and abort the transaction
    
    persistor.persist(example_data[0])

    with persistor.session_scope() as s:
        persistor.persist(example_data[1], session=s)
        persistor.persist_all(error_data, session=s) # This will raise an integrity error and abort the transaction
        persistor.persist_all(example_data) # this isn't in the same session as above thus not aborted
    
    persistor.persist(error_data[1])  # This will raise an integrity error

    # Verify results:
    # There should be only 5 records in the GenreORM table:
    # - 2 from example_data
    # - 3 from example_data_all
    # The records from error_data should not be present due to the integrity error.
    
    print(f"\n---------- < Test completed > ----------")
    