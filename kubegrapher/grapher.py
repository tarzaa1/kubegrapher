from kubegrapher.utils.graph.neo4j import Neo4j
from kubegrapher.model import Node, K8sNode, Pod

class Grapher(object):
    def __init__(self, graphdb: Neo4j) -> None:
        self.db = graphdb

    def merge(self, resource: Node):
        self.db.execute_write(resource.merge)
    
    def set_k8s_node(self, cluster_id: str, node_name: str, metrics: dict[str: any]):
        self.db.execute_write(K8sNode.set, cluster_id=cluster_id, hostname=node_name, metrics=metrics)

    def delete_k8s_node(self, name: str):
        self.db.execute_write(K8sNode.delete, name=name)

    def delete_pod(self, name: str):
        self.db.execute_write(Pod.delete, name=name)

    def link(self, resource: Node, target: Node, type: str):
        def show_result(tx, resource, target, type):
            result = resource.link(tx, type, target)
            print(result)
        self.db.execute_write(show_result, resource, target, type)

    def get_counts(self):
        self.db.execute_read(self.stats)

    def clear(self):
        self.db.delete_all()

    def count(self, tx, type):
        query = f"""
            MATCH (nodes:{type})
            RETURN count(nodes)
            """
        result = tx.run(query)
        print(f"{type}s: {result.single().data()['count(nodes)']}")
    
    def stats(self, tx):
        print('\n')
        self.count(tx, 'Cluster')
        self.count(tx, 'K8sNode')
        self.count(tx, 'Pod')
        self.count(tx, 'Deployment')
        self.count(tx, 'ReplicaSet')
        self.count(tx, 'Label')
        self.count(tx, 'Annotation')
        self.count(tx, 'Image')
        self.count(tx, 'Container')
        self.count(tx, 'Taint')
        self.count(tx, 'Service')
        self.count(tx, 'ConfigMap')
        self.count(tx, 'Ingress')

if __name__ == '__main__':

    URI = "bolt://localhost:7687"
    AUTH = ("neo4j", "password")
    
    graphdb = Neo4j(URI, AUTH)
    graphdb.delete_all()
