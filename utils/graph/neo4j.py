from .base import GraphDB
from .utils import merge_node

from neo4j import GraphDatabase


class Neo4j(GraphDB):
    def __init__(self, URI="bolt://localhost:7687", AUTH=("neo4j", "password")) -> None:
        self.URI = URI
        self.AUTH = AUTH
        self.driver = GraphDatabase.driver(URI, auth=AUTH)
        self.driver.verify_connectivity()

    def add_node(self, type: str, properties: dict[str, any] = None):
        query = merge_node(type, properties)
        records, summary, keys = self.driver.execute_query(
            query,
            parameters_=properties,
            database_='neo4j',
        )
        return (records, summary, keys)

    def match_node(self, type: str, properties: dict):
        records, summary, keys = self.driver.execute_query(
            f"MATCH (n:{type}) RETURN n",
            database_="neo4j",
        )
        return records

    def add_edge(self, from_node, to_node) -> None:
        return super().add_edge(from_node, to_node)

    def set_node_property(self, node) -> None:
        return super().set_node_property(node)

    def set_edge_property(self, edge) -> None:
        return super().set_edge_property(edge)

    def delete_all(self):
        query = """MATCH (n)
                   DETACH DELETE n
                """
        summary = self.driver.execute_query(
            query,
            database_='neo4j',
        ).summary

        print("Deleted {nodes_deleted} nodes and {edges_deleted} relationships in {time} ms.".format(
            nodes_deleted=summary.counters.nodes_deleted,
            edges_deleted=summary.counters.relationships_deleted,
            time=summary.result_available_after))

    def close(self):
        self.driver.close()
