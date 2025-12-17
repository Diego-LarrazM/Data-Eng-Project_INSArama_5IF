import unittest
import sys
from pathlib import Path
import os
from dotenv import load_dotenv
import subprocess
import pytest

from models import *

import logging

logging.basicConfig(level=logging.INFO)

dotenv_path = os.path.join(os.path.dirname(__file__), ".env")
load_dotenv(dotenv_path)

# ============================================================
# -------- < Constants > ------------------------------------
# ============================================================
POSTGRES_HOST = "localhost"  # os.environ.get('POSTGRES_HOST')
POSTGRES_DB_URL = f"postgresql+psycopg2://{os.environ.get('DW_POSTGRES_USER')}:{os.environ.get('DW_POSTGRES_PASSWORD')}@{POSTGRES_HOST}:{os.environ.get('DW_POSTGRES_COM_PORT')}/{os.environ.get("DW_POSTGRES_DB")}"
POSTGRES_LOAD_BATCH_SIZE = int(os.environ.get("DW_POSTGRES_LOAD_BATCH_SIZE", 100))


# ============================================================
# -------- < AJOUT DU CHEMIN RACINE DU PROJET > --------------
# ============================================================
# Ce script est dans /test/SQLPersistor
# Donc on remonte de deux crans pour aller à la racine du projet
ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
sys.path.insert(
    1, os.path.join(str(ROOT), "DockerETL_Images/Staging/SQLPersistor/scripts/")
)


# ============================================================
# -------- < IMPORT ABSOLU PROPRE > --------------------------
# ============================================================
from DockerETL_Images.Staging.SQLPersistor.scripts.persistor import Persistor


# ============================================================
# -------- < TESTS > -----------------------------------------
# ============================================================
class SQLPersistor_BasicTests(unittest.TestCase):

    def setUp(self):
        self.persistor = Persistor(sqlw_conn_url=POSTGRES_DB_URL)

    def test_persist_one_noses(self):
        data = GenreORM(GenreTitle="BasicTest-1-One")
        code = self.persistor.persist(data)
        self.assertFalse(code)

    def test_persist_all_noses(self):
        data = [
            GenreORM(GenreTitle="BasicTest-2-All"),
            GenreORM(GenreTitle="BasicTest-3-All"),
            GenreORM(GenreTitle="BasicTest-4-All"),
        ]
        code = self.persistor.persist_all(data)
        self.assertFalse(code)

    def test_persist_one_wses(self):
        data = GenreORM(GenreTitle="BasicTest-5-One")
        with self.persistor.session_scope() as session:
            code = self.persistor.persist(data, session=session)
        self.assertFalse(code)

    def test_persist_all_wses(self):
        data = [
            GenreORM(GenreTitle="BasicTest-6-All"),
            GenreORM(GenreTitle="BasicTest-7-All"),
            GenreORM(GenreTitle="BasicTest-8-All"),
        ]
        with self.persistor.session_scope() as session:
            code = self.persistor.persist_all(data, session=session)
        self.assertFalse(code)

    def test_persist_one_all_one(self):
        dataOnes = [
            GenreORM(GenreTitle="BasicTest-9-One"),
            GenreORM(GenreTitle="BasicTest-11-One"),
        ]
        dataAll = [GenreORM(GenreTitle="BasicTest-10-All")]
        code = self.persistor.persist(dataOnes[0])
        self.assertFalse(code)
        code = self.persistor.persist_all(dataAll)
        self.assertFalse(code)
        code = self.persistor.persist(dataOnes[1])
        self.assertFalse(code)

    def test_persist_all_one_all(self):
        dataOne = GenreORM(GenreTitle="BasicTest-12-One")
        dataAlls = [
            [GenreORM(GenreTitle="BasicTest-11-All")],
            [GenreORM(GenreTitle="BasicTest-13-All")],
        ]
        code = self.persistor.persist_all(dataAlls[0])
        self.assertFalse(code)
        code = self.persistor.persist(dataOne)
        self.assertFalse(code)
        code = self.persistor.persist_all(dataAlls[1])
        self.assertFalse(code)


class SQLPersistor_ExceptionTests(unittest.TestCase):

    def setUp(self):
        self.persistor = Persistor(sqlw_conn_url=POSTGRES_DB_URL)

    def test_persist_many_ex_noses(self):
        dataError = GenreORM(GenreTitle=None)
        dataOne = GenreORM(GenreTitle="ExceptionTest-1-One")
        dataErrorAll = [
            GenreORM(GenreTitle="shouldNotAppear"),
            GenreORM(GenreTitle=None),
            GenreORM(GenreTitle="shouldNotAppear"),
        ]
        code = self.persistor.persist(dataError)
        self.assertTrue(code)
        code = self.persistor.persist_all(dataErrorAll)
        self.assertTrue(code)
        code = self.persistor.persist(dataOne)
        self.assertFalse(code)

    def test_persist_one_ex(self):
        dataError = GenreORM(GenreTitle=None)
        with self.persistor.session_scope() as session:
            self.persistor.persist(dataError, session=session)
        self.assertTrue(self.persistor.last_execution_status)

    def test_persist_all_ex(self):
        dataErrorAll = [
            GenreORM(GenreTitle="shouldNotAppear1"),
            GenreORM(GenreTitle=None),
            GenreORM(GenreTitle="shouldNotAppear2"),
        ]
        with self.persistor.session_scope() as session:
            self.persistor.persist_all(dataErrorAll, session=session)
        self.assertTrue(self.persistor.last_execution_status)

    def test_persist_many_but_ex(self):
        dataError = GenreORM(GenreTitle=None)
        dataOne = GenreORM(GenreTitle="shouldNotAppear1")
        dataAll = [
            GenreORM(GenreTitle="shouldNotAppear2"),
            GenreORM(GenreTitle="shouldNotAppear3"),
            GenreORM(GenreTitle="shouldNotAppear4"),
        ]
        with self.persistor.session_scope() as session:
            code = self.persistor.persist_all(dataAll, session=session)
            self.assertFalse(code)
            code = self.persistor.persist(dataOne, session=session)
            self.assertFalse(code)
            self.persistor.persist(dataError, session=session)

        self.assertTrue(self.persistor.last_execution_status)

    def test_persist_many_ex_but_weird_sessioning(self):
        dataError = GenreORM(GenreTitle=None)
        dataOne = GenreORM(GenreTitle="shouldNotAppear1")
        dataTwo = GenreORM(GenreTitle="ExceptionTest-2-One")
        dataAll = [
            GenreORM(GenreTitle="ExceptionTest-4-All"),
            GenreORM(GenreTitle="ExceptionTest-5-All"),
        ]
        dataAllNotAppear = [
            GenreORM(GenreTitle="shouldNotAppear2"),
            GenreORM(GenreTitle="shouldNotAppear3"),
            GenreORM(GenreTitle="shouldNotAppear4"),
        ]
        dataThree = GenreORM(GenreTitle="ExceptionTest-3-One")
        with self.persistor.session_scope() as session:
            code = self.persistor.persist_all(dataAllNotAppear, session=session)
            self.assertFalse(code)
            code = self.persistor.persist(dataTwo)  # Other session so persisted
            self.assertFalse(code)
            code = self.persistor.persist(dataOne, session=session)
            self.assertFalse(code)
            code = self.persistor.persist(dataThree)  # Other session so persisted
            self.assertFalse(code)
            code = self.persistor.persist(dataError)  # Other session so ignored
            self.assertTrue(code)
            code = self.persistor.persist_all(dataAll)  # Other session so persisted
            self.assertFalse(code)
            self.persistor.persist(dataError, session=session)
            logging.info("Doesn't Show")
            self.persistor.persist_all(
                dataAllNotAppear
            )  # this isn't in the same session as above BUUUUT it won't be executed due to the error above (Exception raised)
        self.assertTrue(self.persistor.last_execution_status)


@pytest.fixture(scope="module", autouse=True)
def docker_up_down():
    logging.info("\n[pytest] Starting Docker stack...")
    subprocess.run(["docker", "compose", "up", "-d"], cwd=".", check=True)
    persistor = Persistor(sqlw_conn_url=POSTGRES_DB_URL)
    logging.info(f"target: <{POSTGRES_DB_URL}>\n")
    if persistor.create_tables(ModelsBase):
        raise Exception("<@@ Pipeline Stopped...(FAIL) @@>")
    logging.info("<-- Tables created -->\n")

    yield  # <-- tests in THIS FILE run here
    """
    logging.info("\n[pytest] Stopping Docker stack...")
    subprocess.run(
        ["docker", "compose", "down", "-v"],
        cwd=".",
        check=True
    )"""
