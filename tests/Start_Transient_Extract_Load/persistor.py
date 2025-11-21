from contextlib import contextmanager
from sqlalchemy import create_engine
from sqlalchemy.orm import Session
from models.base import DeclarativeMeta, ModelType
from execution import *

class Persistor:

    @contextmanager # to be used with WITH statement easily
    def session_scope(self, session: Session = None) -> Session:
        session = session or Session(self.engine)
        session.begin()
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
    
    @safe_execute
    def create_tables(self, base: DeclarativeMeta) -> ExitCode:
        base.metadata.create_all(self.engine)
        return SUCCESS

    @safe_execute
    def persist(self, obj: ModelType, session: Session = None) -> ExitCode:
        with self.session_scope(session) as s:
            s.add(obj)
        return SUCCESS
    
    @safe_execute
    def persist_all(self, obj_list: list[ModelType], session: Session = None) -> ExitCode:
        with self.session_scope(session) as s:
            s.add_all(obj_list)
        return SUCCESS