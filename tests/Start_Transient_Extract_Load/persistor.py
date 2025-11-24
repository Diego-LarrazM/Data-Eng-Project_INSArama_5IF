from contextlib import contextmanager
from sqlalchemy import create_engine
from sqlalchemy.orm import Session
from models.base import DeclarativeMeta, ModelType
from execution import *

class Persistor:

    @contextmanager # to be used with WITH statement easily
    def session_scope(self):
        session = Session(self.engine)
        session.begin()
        try:
            yield session
            session.commit()
            self.last_execution_status = SUCCESS
        except Exception as e:
            session.rollback()
            self.last_execution_status = FAILURE
            print(f"Session failure - {e}")
        finally:
            session.close()

    def __init__(self, sqlw_conn_url: str):
        # Set up PostgreSQL connection
        self.r_url = sqlw_conn_url
        self.engine = create_engine(sqlw_conn_url, echo=False)
        self.last_execution_status = None
    
    @safe_execute
    def create_tables(self, base: DeclarativeMeta) -> ExitCode:
        base.metadata.create_all(self.engine)
        return SUCCESS

    def persist(self, obj: ModelType, session: Session = None) -> ExitCode:
        self.last_execution_status = FAILURE
        if session is None:
            with self.session_scope() as s:
                s.add(obj)
        else:
            session.add(obj)
            self.last_execution_status = SUCCESS
        return self.last_execution_status
    
    def persist_all(self, obj_list: list[ModelType], session: Session = None) -> ExitCode:
        self.last_execution_status = FAILURE
        if session is None:
            with self.session_scope() as s:
                s.add_all(obj_list)
        else:
            session.add_all(obj_list)
            self.last_execution_status = SUCCESS
        return self.last_execution_status