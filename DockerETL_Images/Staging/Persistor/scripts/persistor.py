from pymongo import MongoClient #, AsyncMongoClient
from urllib.parse import quote_plus
import os

from contextlib import contextmanager
from sqlalchemy import create_engine, sessionmaker
from models.base import DeclarativeMeta, ModelType

class Persistor:

    DATA_DIR = os.environ.get("DATA_FILE_DIRECTORY")

    @contextmanager # to be used with WITH statement easily
    def session_scope(self):
        session = self.session_factory()
        try:
            yield session
            session.commit()
        except:
            session.rollback()
            raise
        finally:
            session.close()

    def __init__(self, read_config: dict[str:str], write_db_url: str):

        if not ("password" in read_config and "username" in read_config and "host" in read_config and "port" in read_config and "mongo_db" in read_config):
            raise Exception("Read configuration must include host, port, username, and password.")
        
        if not os.path.exists(Persistor.DATA_DIR):
            raise Exception(f"Data directory {Persistor.DATA_DIR} does not exist.")
        
        # Set up MongoDB connection
        credentials = ""
        if read_config["username"] and read_config["password"]:
            credentials = f"{quote_plus(read_config["username"])}:{quote_plus(read_config["password"])}@"
        try:
            self.client = MongoClient(host = f"mongodb://{credentials}{read_config["host"]}:{read_config["port"]}/") #or AsyncMongoClient for async operations
        except Exception as e:
            raise Exception(f"Failed to connect to MongoDB: {e}")
        
        self.db = self.client[read_config["mongo_db"]]

        # Set up PostgreSQL connection
        self.r_url = write_db_url
        self.engine = create_engine(write_db_url, echo=False)
        self.session_factory = sessionmaker(bind=self.engine)
    
    def create_tables(self, base: DeclarativeMeta):
        base.metadata.create_all(self.engine)

    def persist(self, obj: ModelType):
        with self.session_scope() as session:
            session.add(obj)
    
    def persist_all(self, obj_list: list[ModelType]):
        with self.session_scope() as session:
            session.add_all(obj_list)
    
    