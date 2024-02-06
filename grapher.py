from kubegrapher.utils.graph.neo4j import Neo4j
from kubegrapher.model import Image, Container, Label, Taint, Annotation, Pod, Deployment, ReplicaSet, K8sNode, ConfigMap, Service
from kubegrapher.utils.utils import delete_node as delete


class Grapher(object):
    def __init__(self, graphdb: Neo4j) -> None:
        self.db = graphdb

    def add_cluster(self) -> None:
        pass

    def add_k8snode(self, tx, k8snode: K8sNode):
        k8snode.merge(tx)
        # link node to cluster

    def add_deployment(self, tx, deployment: Deployment):
        deployment.merge(tx)

    def add_replicaset(self, tx, replicaset: ReplicaSet):
        replicaset.merge(tx)

    def add_pod(self, tx, pod: Pod):
        pod.merge(tx)

    def add_container(self, tx, container: Container):
        container.merge(tx)
    
    def add_service(self, tx, service: Service):
        service.merge(tx)

    def add_configmap(self, tx, configmap: ConfigMap):
        configmap.merge(tx)

    def add_label(self, tx, label: Label):
        label.merge(tx)

    def add_annotation(self, tx, annotation: Annotation):
        annotation.merge(tx)
    
    def add_taint(self, tx, taint: Taint):
        taint.merge(tx)

    def add_image(self, tx, image: Image):
        image.merge(tx)

    def delete_pod(self, tx, id=None, **kwargs):
        self.delete_node(tx, type='Pod,' id=id, **kwargs)
    
    def delete_node(self, tx, type, id=None, **kwargs):
        query = delete(type=type, id=id, **kwargs)
        print('\n' + query + '\n')
        result = tx.run(query, id=id, **kwargs)
        print(result.consume().counters.nodes_deleted)


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
    uid = 'whatever'

    label_1 = Label(key='role', value='control_plane')
    label_2 = Label(key= 'architecture', value='amd64')

    labels = [label_1, label_2]

    annotation = Annotation(key='network', value='vxlan')
    annotations = [annotation]

    taint = Taint(key='NoSchedule', effect='node-role.kubernetes.io/control-plane')
    taints = [taint]

    image = Image('someimageid', 'registry.k8s.io/etcd:3.5.6-0', 102542580)
    images = [image]

    k8snode = K8sNode(uid, properties, labels, annotations, taints, images)
    deployment = Deployment('some_uid')
    replicaset = ReplicaSet('some_other_uid', 'some_uid')
    configmap = ConfigMap('config_uid', {'name': 'my_configmap'})

    container = Container('someimageid', properties, 'my_configmap')

    session = graphdb.driver.session()
    session.execute_write(grapher.add_k8snode, k8snode)
    session.execute_write(grapher.add_deployment, deployment)
    session.execute_write(grapher.add_replicaset, replicaset)
    session.execute_write(grapher.add_configmap, configmap)
    session.execute_write(grapher.add_container, container)
    # session.execute_write(grapher.delete_node, type='ConfigMap', name='my_configmap')

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
