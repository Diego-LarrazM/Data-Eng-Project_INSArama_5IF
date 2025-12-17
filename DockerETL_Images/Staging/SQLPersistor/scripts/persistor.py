from contextlib import contextmanager
from sqlalchemy import create_engine, text
from sqlalchemy.orm import Session
from models.base import DeclarativeMeta, ModelType
from utils.execution import *
from utils.batch_generator import *
from typing import Iterable
import time

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

    def __init__(self, sqlw_conn_url: str, timeouts=5, retries=3):
        # Set up PostgreSQL connection
        self.r_url = sqlw_conn_url
        self.engine = create_engine(sqlw_conn_url, echo=False)
        self.last_execution_status = None
        self.wait_for_postgres(timeouts=timeouts, retries=retries)
    
    def wait_for_postgres(self, timeouts=5, retries=3):
        print(f"Testing connexion to <{self.r_url}>...")
        while retries:
            try:
                with self.engine.connect() as conn:
                    conn.execute(text("SELECT 1"))
                return  # Postgres is healthy and running
            except Exception:
                print(f"FAILED - Retrying after {timeouts}s...")
                retries -= 1
                time.sleep(timeouts)  # wait timeouts seconds and retry
        raise TimeoutError(f"Couldn't connect to Postgres after {retries} retries.")
    
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

    def persist_from(self, iterator: Iterable[ModelType], batch_size: int = 1, session: Session = None):
        batches = BatchGenerator(
            generator=iterator,
            batch_size=batch_size
        )
        for batch in batches:
            self.persist_all(batch, session)
            print(self.last_execution_status)
        return self.last_execution_status
        