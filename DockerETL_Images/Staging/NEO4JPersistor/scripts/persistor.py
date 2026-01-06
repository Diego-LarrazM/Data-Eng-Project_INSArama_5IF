from contextlib import contextmanager
from neo4j import GraphDatabase, Session, Transaction
from utils.execution import *  # Assurez-vous que SUCCESS/FAILURE sont importés
from typing import Any
import time


class Persistor:
    # GRAPH_LINKS: [{"_GRAPH_SRC_NODE_ID", "_GRAPH_LINK_LABEL", "_GRAPH_TRGT_NODE_ID", ...}]
    # GRAPH_ENTITIES: [{"_GRAPH_NODE_ID", "_GRAPH_NODE_LABEL", attributes...}]

    def __init__(self, uri: str, auth: tuple = None, database: str = "neo4j"):
        self.driver = GraphDatabase.driver(uri, auth=auth)
        self.default_db = database
        self.last_execution_status = None
        self.wait_for_neo4j()

    @contextmanager
    def session_scope(self, db_name: str = None):
        target_db = db_name if db_name else self.default_db
        session = self.driver.session(database=target_db)
        try:
            yield session
        finally:
            session.close()

    def wait_for_neo4j(self, retries=3):
        while retries:
            try:
                self.driver.verify_connectivity()
                return
            except Exception:
                time.sleep(2)
                retries -= 1
        raise TimeoutError("Neo4j inaccessible")

    def close(self):
        self.driver.close()

    def _clean_props(self, row: dict) -> dict:
        """
        Nettoie les données avant insertion :
        1. Retire les clés internes (_GRAPH...).
        2. Convertit les chaînes vides "" en None (NULL) pour Neo4j.
        """
        props = {}
        for k, v in row.items():
            if k.startswith("_GRAPH"):
                continue
            # C'est ici qu'on règle définitivement le problème des champs vides
            props[k] = None if v == "" else v
        return props

    @safe_execute
    def persist_nodes(self, graph_entities: list[dict[str, Any]]) -> int:
        """
        Insère des nœuds de manière transactionnelle.
        """
        self.last_execution_status = FAILURE
        if not graph_entities:
            self.last_execution_status = SUCCESS
            return self.last_execution_status

        # 1. Grouper par Label
        grouped = {}
        for entity in graph_entities:
            label = entity.get("_GRAPH_NODE_TYPE", "Entity")
            grouped.setdefault(label, []).append(entity)

        # 2. Exécution Transactionnelle
        with self.session_scope() as session:
            # On démarre une transaction explicite
            with session.begin_transaction() as tx:
                for label, batch in grouped.items():
                    self._execute_node_batch(tx, label, batch)

                # Si tout se passe bien, on commit (valide) les changements
                tx.commit()
            self.last_execution_status = SUCCESS
        return self.last_execution_status

    def _execute_node_batch(self, tx: Transaction, label: str, batch: list):
        # Préparation des données
        cleaned_batch = []
        for row in batch:
            props = self._clean_props(row)
            # On s'assure que l'ID est bien présent dans les propriétés
            props["id"] = row["_GRAPH_NODE_ID"]
            cleaned_batch.append(props)

        query = f"""
        UNWIND $batch AS row
        MERGE (n:`{label}` {{ id: row.id }})
        SET n += row
        """
        tx.run(query, batch=cleaned_batch)

    @safe_execute
    def persist_links(self, graph_links: list[dict[str, Any]]) -> int:
        """
        Insère des relations de manière transactionnelle.
        """
        self.last_execution_status = FAILURE
        if not graph_links:
            self.transaction_status = SUCCESS
            return self.transaction_status

        # 1. Grouper par (SourceLabel, TargetLabel, RelationshipType)
        grouped = {}
        for link in graph_links:
            key = (
                link.get("_GRAPH_SRC_LABEL", "Entity"),
                link.get("_GRAPH_TRGT_LABEL", "Entity"),
                link.get("_GRAPH_LINK_LABEL", "RELATED_TO"),
            )
            grouped.setdefault(key, []).append(link)

        # 2. Exécution Transactionnelle
        with self.session_scope() as session:
            with session.begin_transaction() as tx:
                for (s_lbl, t_lbl, rel_type), batch in grouped.items():
                    self._execute_link_batch(tx, s_lbl, t_lbl, rel_type, batch)

                tx.commit()
            self.transaction_status = SUCCESS
        return self.transaction_status

    def _execute_link_batch(
        self, tx: Transaction, s_lbl: str, t_lbl: str, rel_type: str, batch: list
    ):
        cleaned_batch = []
        for link in batch:
            item = {
                "src_id": link["_GRAPH_SRC_NODE_ID"],
                "tgt_id": link["_GRAPH_TRGT_NODE_ID"],
                "props": self._clean_props(link.get("_GRAPH_LINK_ATTRIBUTES", {})),
            }
            cleaned_batch.append(item)

        query = f"""
        UNWIND $batch AS row
        MATCH (source:`{s_lbl}` {{ id: row.src_id }})
        MATCH (target:`{t_lbl}` {{ id: row.tgt_id }})
        MERGE (source)-[r:`{rel_type}`]->(target)
        SET r += row.props
        """
        tx.run(query, batch=cleaned_batch)
