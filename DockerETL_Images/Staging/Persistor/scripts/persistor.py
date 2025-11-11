from pymongo import MongoClient #, AsyncMongoClient

from contextlib import contextmanager
from sqlalchemy import create_engine, sessionmaker
from models.base import DeclarativeMeta, ModelType
from utils.execution import *

class Persistor:

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

    def __init__(self, mongo_conn_url: str, r_database: str, sqlw_conn_url: str):
        # Set up MongoDB connection
        self.client = MongoClient(host = mongo_conn_url) #or AsyncMongoClient for async operations
        self.db = self.client[r_database]

        # Set up PostgreSQL connection
        self.r_url = sqlw_conn_url
        self.engine = create_engine(sqlw_conn_url, echo=False)
        self.session_factory = sessionmaker(bind=self.engine)
    
    @safe_execute
    def create_tables(self, base: DeclarativeMeta):
        base.metadata.create_all(self.engine)

    @safe_execute
    def persist(self, obj: ModelType):
        with self.session_scope() as session:
            session.add(obj)
    
    @safe_execute
    def persist_all(self, obj_list: list[ModelType]):
        with self.session_scope() as session:
            session.add_all(obj_list)
    
    