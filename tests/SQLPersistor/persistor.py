from contextlib import contextmanager
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from models.base import DeclarativeMeta, ModelType
from execution import *

class Persistor:

    @contextmanager # to be used with WITH statement easily
    def session_scope(self):
        session = self.session_factory()
        try:
            yield session
            session.commit()
        except Exception as e:
            session.rollback()
            raise Exception(f"Session failure - {e}") from e # to be catched by safe execution
        finally:
            session.close()

    def __init__(self, sqlw_conn_url: str):
        # Set up PostgreSQL connection
        self.r_url = sqlw_conn_url
        self.engine = create_engine(sqlw_conn_url, echo=False)
        self.session_factory = sessionmaker(bind=self.engine)
    
    @safe_execute
    def create_tables(self, base: DeclarativeMeta) -> ExitCode:
        base.metadata.create_all(self.engine)
        return SUCCESS

    @safe_execute
    def persist(self, obj: ModelType) -> ExitCode:
        with self.session_scope() as session:
            session.add(obj)
        return SUCCESS
    
    @safe_execute
    def persist_all(self, obj_list: list[ModelType]) -> ExitCode:
        with self.session_scope() as session:
            session.add_all(obj_list)
        return SUCCESS