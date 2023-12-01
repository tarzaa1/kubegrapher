from kubegrapher.utils.graph.neo4j import Neo4j
from kubegrapher.utils.utils import merge_node, merge_relationship
from .model import Node
from pprint import pprint


class Grapher(object):
    def __init__(self, graphdb: Neo4j) -> None:
        self.db = graphdb

    def add_cluster(self) -> None:
        pass

    def add_k8snode(self, tx, properties: dict[str: any] = {}, labels: dict[str: str] = {}, annotations: dict[str: str] = {}, taints: list[dict[str: str]] = [], images: list[dict[str: any]] = []) -> None:
        merge_node_query = merge_node(type='K8sNode', properties=properties)
        result = tx.run(merge_node_query, properties)
        node = result.single().data()['node']
        print(node)
        for label in labels.items():
            result = self.add_label(tx, *label)
            l = result.single().data()['node']
            print(l)
            connect_label = merge_relationship(type='HAS_LABEL', from_type='K8sNode', from_properties={
                                               'name': node['name']}, to_type='Label', to_properties=l)
            print(connect_label)
            connect_label = f"""MATCH (node:K8sNode {{name: $name}}), (label:Label {{key: $key, value: $value}})
                                MERGE p = (node)-[relation:HAS_LABEL]->(label)
                                RETURN p
                            """
            result = tx.run(
                connect_label, name=node['name'], key=l['key'], value=l['value'])
            print(result.single().data())

        for annotation in annotations.items():
            result = self.add_annotation(tx, *annotation)
            annot = result.single().data()['node']
            print(annot)
            connect_annotation = f"""MATCH (node:K8sNode {{name: $name}}), (annot:Annotation {{key: $key, value: $value}})
                                MERGE p = (node)-[relation:HAS_ANNOTATION]->(annot)
                                RETURN p
                                """
            result = tx.run(
                connect_annotation, name=node['name'], key=annot['key'], value=annot['value'])
            print(result.single().data())

        for taint in taints:
            pass
            # result = self.add_taint(tx, **taint)
            # t = result.single().data()['node']
            # print(t)
            # connect_taint = f''
            # result = tx.run()
            # print(result.single().data())

        # for image in images:
        #     merge_image = f"""MERGE (node:Label {{{placeholders(image)}}})
        #                  RETURN node
        #                  """
        #     result = tx.run(merge_image, image)
        #     connect_image = ''
        #     result = tx.run(connect_image)

    def add_label(self, tx, key, value) -> None:
        query = merge_node(type='Label', key=key, value=value)
        result = tx.run(query, key=key, value=value)
        return result

    def add_annotation(self, tx, key, value) -> None:
        query = merge_node(type='Annotation', key=key, value=value)
        result = tx.run(query, key=key, value=value)
        return result

    def add_taint(self, tx, key, effect) -> None:
        query = merge_node(type='Taint', key=key, effect=effect)
        result = tx.run(query, key=key, effect=effect)
        return result

    def add_image(self, tx, name, sizeBytes) -> None:
        query = merge_node(type='Image', name=name, sizeBytes=sizeBytes)
        result = tx.run(query, name=name, sizeBytes=sizeBytes)
        return result

    def add_deployment(self) -> None:
        pass

    def add_replicaset(self) -> None:
        pass

    def add_pod(self) -> None:
        pass

    def add_service(self) -> None:
        pass

    def add_container(self) -> None:
        pass

    def add_configmap(self) -> None:
        pass

    def add_secret(self) -> None:
        pass


if __name__ == '__main__':

    # URI examples: "neo4j://localhost", "neo4j+s://xxx.databases.neo4j.io"
    URI = "bolt://localhost:7687"
    AUTH = ("neo4j", "password")

    graphdb = Neo4j(URI, AUTH)

    grapher = Grapher(graphdb)

    type = 'K8sNode'
    properties = {
        'name': 'hasib',
        'architecture': 'amd64',
        'os': 'linux',
        'kernel': '5.15.0-87-generic',
        'memory': 45763298
    }

    labels = {
        'role': 'control plane',
        'architecture': 'amd64'
    }

    annoations = {
        'network': 'vxlan'
    }

    session = graphdb.driver.session()
    session.execute_write(grapher.add_k8snode, properties, labels, annoations)

    session.close()

    # (records, summary, keys) = graphdb.add_node(type)

    # for record in records:
    #     print(record.data())

    # print("Created {nodes_created} nodes in {time} ms.".format(
    #     nodes_created=summary.counters.nodes_created,
    #     time=summary.result_available_after
    # ))

    # records = graphdb.match_node(type, None)
    # for record in records:
    #     print(record.data())

    graphdb.delete_all()
