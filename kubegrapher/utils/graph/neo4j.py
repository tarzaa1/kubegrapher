from .base import GraphDB
from neo4j import GraphDatabase


class Neo4j(GraphDB):
    def __init__(self, URI="bolt://localhost:7687", AUTH=("neo4j", "password")) -> None:
        self.driver = self.create_connection(URI, AUTH)
        self.driver.verify_connectivity()

    def create_connection(self, URI, AUTH):
        return GraphDatabase.driver(URI, auth=AUTH)

    def get_session(self):
        return self.driver.session()

    def close_session(self, session):
        session.close()

    def execute_read(self, tx: callable, *args, **kwargs):
        session = self.get_session()
        try:
            result = session.execute_read(tx, *args, **kwargs)
            return result
        finally:
            self.close_session(session)

    def execute_write(self, tx: callable, *args, **kwargs):
        session = self.get_session()
        try:
            session.execute_write(tx, *args, **kwargs)
        finally:
            self.close_session(session)

    def close(self):
        self.driver.close()

    def __del__(self):
        self.close()

    def delete_all(self):
        query = """MATCH (n)
                   DETACH DELETE n
                """
        summary = self.driver.execute_query(
            query,
            database_='neo4j',
        ).summary

        print("\n\nDeleted {nodes_deleted} nodes and {edges_deleted} relationships in {time} ms.".format(
            nodes_deleted=summary.counters.nodes_deleted,
            edges_deleted=summary.counters.relationships_deleted,
            time=summary.result_available_after))
