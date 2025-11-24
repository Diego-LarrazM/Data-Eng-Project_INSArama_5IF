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
    GenreORM(GenreTitle="all1"),
    GenreORM(GenreTitle="all2"),
    GenreORM(GenreTitle="all3"),
]

example_data_all2 = [
    GenreORM(GenreTitle="all4"),
    GenreORM(GenreTitle="all5"),
    GenreORM(GenreTitle="all6"),
]

example_data = [
    GenreORM(GenreTitle="one1"),
    GenreORM(GenreTitle="one2"),
]

error_data = [
    GenreORM(GenreTitle="shouldNotAppear1"),
    GenreORM(GenreTitle=None),  # This will cause an integrity error (name cannot be null)
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
        code = persistor.persist_all(example_data_all, session=s)
        print(f"Insert example_data_all: {persistor.last_execution_status}=={code} ---------------------------------")
        persistor.persist_all(error_data, session=s) # This will raise an integrity error and abort the transaction
    
    print(f"First_transaction: {persistor.last_execution_status} ---------------------------------\n")
    
    code = persistor.persist(example_data[0])

    print(f"Second_transaction: {persistor.last_execution_status} == {code} ---------------------------------\n")

    with persistor.session_scope() as s:
        code = persistor.persist(example_data[1], session=s)
        print(f"Insert example_data[1]: {persistor.last_execution_status}=={code} ---------------------------------")
        persistor.persist_all(error_data, session=s) # This will raise an integrity error and abort the transaction
        print("------------------------ not entered here due to error above -----------------------------")
        persistor.persist_all(example_data) # this isn't in the same session as above BUUUUT it won't be executed due to the error above (Exception raised)
    
    code = persistor.persist(error_data[1])  # This will raise an integrity error
    print(f"Third_transaction: {persistor.last_execution_status} == {code} ---------------------------------\n")

    code = persistor.persist_all(example_data_all)
    print(f"Fourth_transaction: {persistor.last_execution_status} == {code} ---------------------------------\n")

    with persistor.session_scope() as s:
        persistor.persist(example_data[1], session=s)
        print(f"Insert example_data[1]: {persistor.last_execution_status}=={code}---------------------------------")
        persistor.persist_all(example_data_all2, session=s)
        print(f"Insert example_data_all2: {persistor.last_execution_status}=={code}---------------------------------")
    
    print(f"Fifth_transaction: {persistor.last_execution_status} ---------------------------------\n")
    

    # Verify results:
    # There should be only 5 records in the GenreORM table:
    # - 2 from example_data
    # - 3 from example_data_all
    # The records from error_data should not be present due to the integrity error.
    
    print(f"\n---------- < Test completed > ----------")
    