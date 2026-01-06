from contextlib import contextmanager
from neo4j import GraphDatabase, Session, Transaction
from utils.execution import *  # Assurez-vous que SUCCESS/FAILURE sont importés
import time


class Persistor:
    # GRAPH_LINKS: [{"_GRAPH_SRC_NODE_ID", "_GRAPH_LINK_LABEL", "_GRAPH_TRGT_NODE_ID", ...}]
    # GRAPH_ENTITIES: [{"_GRAPH_NODE_ID", "_GRAPH_NODE_LABEL", attributes...}]

    def __init__(self, uri: str, auth: tuple = None, database: str = "neo4j"):
        self.driver = GraphDatabase.driver(uri, auth=auth)
        self.database = database
        self.last_execution_status = None
        self.wait_for_neo4j()

    @contextmanager
    def transaction_scope(self):
        with self.driver.session(database=self.database) as session:
            tx = session.begin_transaction()
            try:
                yield tx
                tx.commit()
                self.last_execution_status = SUCCESS
            except Exception as e:
                self.last_execution_status = FAILURE
                tx.rollback()
                raise e

    def wait_for_neo4j(self, retries=3):
        while retries:
            try:
                self.driver.verify_connectivity()
                return
            except Exception:
                time.sleep(2)
                retries -= 1
        raise TimeoutError("Neo4j inaccessible")

    def persist_nodes(self, nodes: list):
        """
        Persist a batch of nodes.
        Each node should be a dict like:
        {"_GRAPH_NODE_ID": id, "_GRAPH_NODE_LABEL": label, ...attributes}
        """
        if not nodes:
            return

        with self.transaction_scope() as tx:
            for node in nodes:
                node_id = node.pop("_GRAPH_NODE_ID")
                label = node.pop("_GRAPH_NODE_LABEL")
                props = ", ".join(f"{k}: ${k}" for k in node.keys())
                cypher = f"""
                MERGE (n:{label} {{_GRAPH_NODE_ID: $node_id}})
                SET n += {{{props}}}
                """
                tx.run(cypher, node_id=node_id, **node)

    def persist_links(self, links: list):
        """
        Persist a batch of relationships/links.
        Each link should be a dict like:
        {
            "_GRAPH_SRC_NODE_ID": src_id,
            "_GRAPH_SRC_LABEL": src_label,
            "_GRAPH_LINK_LABEL": rel_label,
            "_GRAPH_TRGT_NODE_ID": tgt_id,
            "_GRAPH_TRGT_LABEL": tgt_label,
            "_GRAPH_LINK_ATTRIBUTES": { ... }
        }
        """
        if not links:
            return

        with self.transaction_scope() as tx:
            for link in links:
                src_id = link["_GRAPH_SRC_NODE_ID"]
                src_label = link["_GRAPH_SRC_LABEL"]
                tgt_id = link["_GRAPH_TRGT_NODE_ID"]
                tgt_label = link["_GRAPH_TRGT_LABEL"]
                rel_label = link["_GRAPH_LINK_LABEL"]
                attrs = link.get("_GRAPH_LINK_ATTRIBUTES", {})

                props = ", ".join(f"{k}: ${k}" for k in attrs.keys())

                cypher = f"""
                MATCH (a:{src_label} {{_GRAPH_NODE_ID: $src_id}})
                MATCH (b:{tgt_label} {{_GRAPH_NODE_ID: $tgt_id}})
                MERGE (a)-[r:{rel_label}]->(b)
                """
                if props:
                    cypher += f"\nSET r += {{{props}}}"

                tx.run(cypher, src_id=src_id, tgt_id=tgt_id, **attrs)
