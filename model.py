from kubegrapher.utils.utils import merge_node, merge_relationship

class Node():
    def __init__(self, type: str, properties: dict[str: any] = None) -> None:
        self.type = type
        self.properties = properties
        self.id = None

    def merge(self, tx: callable):
        query = merge_node(type=self.type, properties=self.properties)
        result = tx.run(query, self.properties)
        return result.single().data()['node']
    
    def link(self, tx: callable, type: str, node: 'Node', properties: dict[str: any] = None, directed = True):
        query = merge_relationship(type=type, from_type=self.type, to_type=node.type, properties=properties, directed=directed)
        result = tx.run(query, from_id=self.id, to_id=node.id)
        return result.single().data()