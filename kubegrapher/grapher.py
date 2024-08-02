from kubegrapher.utils.graph.neo4j import Neo4j
from kubegrapher.model import *
import json

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

    def get_subgraph(self):
        self.db.execute_read(self.subgraph)

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

    def subgraph(self, tx):

        nodes = []
        edges = []
        ids = []

        result = self.get_nodes(tx, 'K8sNode')
        for record in result:
            node = record['node']
            if node.element_id not in ids:
                nodes.append({'id': node.element_id, 'name': node['name'], 'type': 'K8sNode'})
                ids.append(node.element_id)

        result = self.get_nodes(tx, 'Pod')
        for record in result:
            node = record['node']
            if node.element_id not in ids:
                nodes.append({'id': node.element_id, 'name': node['name'], 'type': 'Pod'})
                ids.append(node.element_id)

        result = self.get_nodes(tx, 'Container')
        for record in result:
            node = record['node']
            if node.element_id not in ids:
                nodes.append({'id': node.element_id, 'name': node['name'], 'type': 'Container'})
                ids.append(node.element_id)

        result = self.get_images(tx)
        for record in result:
            node = record['node']
            if node.element_id not in ids:
                nodes.append({'id': node.element_id, 'name': node['name'], 'type': 'Image'})
                ids.append(node.element_id)
            r = record['r']
            if r.element_id not in ids:
                edges.append({'id': r.element_id, 'start': r.start_node.element_id, 'end':r.end_node.element_id, 'label': r.type})
                ids.append(r.element_id)
        
        result = self.get_edges(tx, "K8sNode", "Pod")
        for record in result:
            r = record['r']
            if r.element_id not in ids:
                edges.append({'id': r.element_id, 'start': r.start_node.element_id, 'end':r.end_node.element_id, 'label': r.type})
                ids.append(r.element_id)

        result = self.get_edges(tx, "Container", "Pod")
        for record in result:
            r = record['r']
            if r.element_id not in ids:
                edges.append({'id': r.element_id, 'start': r.start_node.element_id, 'end':r.end_node.element_id, 'label': r.type})
                ids.append(r.element_id)

        result = self.get_edges(tx, "Container", "Image")
        for record in result:
            r = record['r']
            if r.element_id not in ids:
                edges.append({'id': r.element_id, 'start': r.start_node.element_id, 'end':r.end_node.element_id, 'label': r.type})
                ids.append(r.element_id)

        subgraph = {'nodes': nodes, 'edges': edges}
        with open('/home/ubuntu/dev/app/graphviz/subgraph.json', 'w') as fp:
            json.dump(subgraph, fp, indent=4)
    
    def get_nodes(self, tx, type):
        query = f"""
            MATCH (node:{type})
            RETURN node
            """
        return tx.run(query)
    
    def get_images(self, tx):
        query = f"""
            MATCH (node:Image)--(:Container)
            MATCH (node)-[r]-()
            RETURN node, r
            """
        return tx.run(query)
    
    def get_edges(self, tx, start, end):
        query = f"""
            MATCH (:{start})-[r]-(:{end})
            RETURN r
            """
        return tx.run(query)

if __name__ == '__main__':

    URI = "bolt://localhost:7687"
    AUTH = ("neo4j", "password")
    
    graphdb = Neo4j(URI, AUTH)
    grapher = Grapher(graphdb)

    # grapher.get_subgraph()

    # type = 'K8sNode'
    # properties = {
    #     'name': 'hasib',
    #     'architecture': 'amd64',
    #     'os': 'linux',
    #     'kernel': '5.15.0-87-generic',
    #     'memory': 45763298
    # }
    # uid = 'whatever'

    # label_1 = Label(key='role', value='control_plane')
    # label_2 = Label(key= 'architecture', value='amd64')

    # labels = [label_1, label_2]

    # annotation = Annotation(key='network', value='vxlan')
    # annotations = [annotation]

    # taint = Taint(key='NoSchedule', effect='node-role.kubernetes.io/control-plane')
    # taints = [taint]

    # image = Image('registry.k8s.io/etcd:3.5.6-0', 102542580)
    # images = [image]

    # k8snode = K8sNode(uid, properties, labels, annotations, taints, images)
    # deployment = Deployment('some_uid')
    # replicaset = ReplicaSet('some_other_uid', 'some_uid')
    # configmap = ConfigMap('config_uid', {'name': 'my_configmap'})

    # container = Container('registry.k8s.io/etcd:3.5.6-0', properties, 'my_configmap')
    # container1 = Container('registry.k8s.io/etcd:3.5.6-0', properties, 'my_configmap')

    # pod_label_1 = Label(key='app', value='myapp')
    # pod_label_2 = Label(key= 'lang', value='python3')

    # pod_labels = [pod_label_1, pod_label_2]

    # pod_annotation = Annotation(key='service', value='micro')
    # pod_annotations = [pod_annotation]

    # pod = Pod(uid='pod_uid', k8snode_name='hasib', labels=pod_labels, annotations=pod_annotations, containers=[container, container1], replicaset_uid='some_other_uid')

    # grapher.merge(k8snode)
    # grapher.merge(deployment)
    # grapher.merge(replicaset)
    # grapher.merge(configmap)
    # grapher.merge(container)
    # grapher.merge(pod)
    # grapher.get_subgraph()

    graphdb.delete_all()
