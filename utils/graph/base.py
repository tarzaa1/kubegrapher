class GraphDB():
    def __init__(self) -> None:
        pass

    def add_node(self, type: str, properties: dict):
        raise NotImplementedError

    def add_edge(self, from_node, to_node) -> None:
        raise NotImplementedError

    def set_node_property(self, node) -> None:
        raise NotImplementedError

    def set_edge_property(self, edge) -> None:
        raise NotImplementedError