from kubegrapher.cypher import merge_node, merge_relationship, to_properties, \
    merge_relationship_generic, delete_pod_query, delete_node_query, set_k8snode_metrics, \
    merge_relationship_pod_to_service, merge_relationship_service_to_pod
from kubegrapher.transactions import delete_orphans
from kubegrapher.relations import Relations
import uuid
import json

def toString(properties=None, kwargs=None):
    properties_str = None
    kwargs_str = None
    if properties is not None:
        properties_str = ', '.join(
            [f'{key}: {value}' for key, value in properties.items()])
    if kwargs is not None:
        kwargs_str = ', '.join(
            [f'{key}: {value}' for key, value in kwargs.items()])

    if properties_str and kwargs_str:
        return f'{properties_str}, {kwargs_str}'
    elif properties_str:
        return properties_str
    elif kwargs_str:
        return kwargs_str
    else:
        return ''

class Node():
    def __init__(self, type: str, uid: str = None, properties: dict[str: any] = None, **kwargs) -> None:

        if uid is None:
            self.id = str(uuid.uuid4())
        else:
            self.id = uid
        self.type = type
        self.properties = properties
        self.kwproperties = kwargs

    def merge(self, tx: callable):
        query = merge_node(type=self.type, properties=self.properties, id=self.id, **self.kwproperties)
        print('\n' + query + '\n')
        result = tx.run(query, self.properties, id=self.id, **self.kwproperties)
        return result.single()
    
    def link(self, tx: callable, type: str, target: 'Node', properties: dict[str: any] = None, directed = True, reverse = False):
        query = merge_relationship(type=type, from_type=self.type, to_type=target.type, properties=properties, directed=directed)
        print('\n' + query + '\n')
        result = tx.run(query, from_id=self.id, to_id=target.id)
        return result.single()

    def link_generic(self, tx: callable, type: str, target: 'Node', properties: dict[str: any] = None, directed = True, reverse = False):
        query = merge_relationship_generic(type=type, from_type=self.type, to_type=target.type, to_properties=target.kwproperties, properties=properties, directed=directed)
        print('\n' + query + '\n')
        result = tx.run(query, from_id=self.id, **to_properties(target.kwproperties))
        return result.single()
    
    @classmethod
    def delete(cls, tx, type, **kwargs):
        query = delete_node_query(type, **kwargs)
        print('\n' + query + '\n')
        result = tx.run(query, **kwargs)
        return result.consume()

    def __str__(self):
        properties_str = toString(self.properties, self.kwproperties)
        return f"UID: {self.id}, Type: {self.type}, Properties: {{{properties_str}}}"
    
    __repr__ = __str__
    

class Image(Node):
    def __init__(self, name: str, sizeInBytes: int) -> None:
        uid = name + str(sizeInBytes)
        super().__init__(type=self.__class__.__name__, uid=uid, name=name, sizeInBytes=sizeInBytes)

    def merge(self, tx: callable):
        print(super().merge(tx))
        

class Container(Node):
    def __init__(self, container_id: str, image_name: str, properties: dict[str: any], configmap_name: str = None) -> None:
        super().__init__(type=self.__class__.__name__, uid=container_id, properties=properties)

        self.imageName = image_name
        self.configMapName = configmap_name
    
    def merge(self, tx: callable):
        print(super().merge(tx))
        result = self.link_generic(tx, type=Relations.INSTANTIATES_IMAGE, target=Node(type='Image', name=self.imageName))
        print(result)
        if self.configMapName is not None:
            result = self.link_generic(tx, type=Relations.CONFIGMAP_REF, target=Node(type='ConfigMap', name=self.configMapName))
            print(result)

    def __str__(self):
        cont = f"{super().__str__()}, Image: {self.imageName}"
        if self.configMapName is not None:
            cont = f"{cont}, ConfigMap: {self.configMapName}"
        return cont
    
class Label(Node):
    def __init__(self, key: str, value: str) -> None:
        uid = key + value
        super().__init__(type=self.__class__.__name__, uid=uid, key=key, value=value)
    
    def merge(self, tx: callable):
        print(super().merge(tx))

class Annotation(Node):
    def __init__(self, key: str, value: str) -> None:
        uid = key + value
        super().__init__(type=self.__class__.__name__, uid=uid, key=key, value=value)
    
    def merge(self, tx: callable):
        print(super().merge(tx))

class Taint(Node):
    def __init__(self, key: str, effect: str, **kwargs) -> None:
        uid = key + effect
        super().__init__(type=self.__class__.__name__, uid=uid, key=key, effect=effect)
    
    def merge(self, tx: callable):
        print(super().merge(tx))

class ConfigMap(Node):
    def __init__(self, uid: str, properties: dict = None) -> None:
        super().__init__(type=self.__class__.__name__, uid=uid, properties=properties)

    def merge(self, tx: callable):
        print(super().merge(tx))

class Service(Node):
    def __init__(self, uid: str, properties: dict = None, labels: list[Label] = {}, **kwargs) -> None:
        super().__init__(type=self.__class__.__name__, uid=uid, properties=properties)
        self.labels = labels
    
    def link_pod(self, tx: callable):
        query = merge_relationship_service_to_pod()
        print('\n' + query + '\n')
        result = tx.run(query, service_id = self.id)
        return result.single()

    def merge(self, tx: callable):
        print(super().merge(tx))
        # merge and link all labels
        for label in self.labels:
            label.merge(tx)
            result = self.link(tx, type=Relations.HAS_LABEL, target=label)
            print(result)
        # selectors handling
        selector_label_dict = json.loads(self.properties["selector"])
        if not selector_label_dict:
            pass
        else:
            for k, v in selector_label_dict.items():
                label = Label(k, v)
                label.merge(tx)
                result = self.link(tx, type=Relations.HAS_SELECTOR, target=label)
                print(result)
        # link service and pod
        self.link_pod(tx)

class Pod(Node):
    def __init__(self, uid: str, k8snode_name: str, properties: dict[str: any] = {}, labels: list[Label] = {}, annotations: list[Annotation] = {}, containers: list[Container] = [], replicaset_uid: str = None) -> None:
        super().__init__(type=self.__class__.__name__, uid=uid, properties=properties)
        
        self.k8sNodeName = k8snode_name
        self.labels = labels
        self.annotations = annotations
        self.containers = containers
        self.replicaSetUID = replicaset_uid
    
    def merge(self, tx: callable):
        print(super().merge(tx))

        for label in self.labels:
            label.merge(tx)
            result = self.link(tx, type=Relations.HAS_LABEL, target=label)
            print(result)

        for annotation in self.annotations:
            annotation.merge(tx)
            result = self.link(tx, type=Relations.HAS_ANNOTATION, target=annotation)
            print(result)

        # link to replicaset
        if self.replicaSetUID is not None:
            result = self.link(tx, type=Relations.MANAGED_BY, target=Node(type='ReplicaSet', uid=self.replicaSetUID))
            print(result)
        
        # link to node
        result = self.link_generic(tx, type=Relations.SCHEDULED_ON, target=Node(type='K8sNode', name=self.k8sNodeName))
        print(result)

        for container in self.containers:
            container.merge(tx)
            result = self.link(tx, type=Relations.RUNS_CONTAINER, target=container)
            print(result)

        # link service and pod
        self.link_service(tx)


    @classmethod
    def delete(cls, tx: callable, **kwargs):
        query = delete_pod_query(**kwargs)
        print('\n' + query + '\n')
        result = tx.run(query, **kwargs)
        summary = result.consume()
        relationships_deleted = summary.counters.relationships_deleted
        labels_deleted = delete_orphans(tx, "Label")
        annots_deleted = delete_orphans(tx, "Annotation")
        nodes_deleted = summary.counters.nodes_deleted + labels_deleted + annots_deleted
        print(f"\nDeleted {nodes_deleted} graph nodes and {relationships_deleted} relationships")

    def link_service(self, tx: callable):
        query = merge_relationship_pod_to_service()
        print('\n' + query + '\n')
        result = tx.run(query, pod_id = self.id)
        return result.single()

    def __str__(self):
        representations = []
        pod = f"{super().__str__()}, K8sNode: {self.k8sNodeName}"
        if self.replicaSetUID is not None:
            pod = f"{pod}, ReplicaSetUID: {self.replicaSetUID}"
        representations.append(pod)
        if len(self.labels) > 0:
            representations.append('\n'.join([label.__str__() for label in self.labels]))
        if len(self.annotations) > 0:
            representations.append('\n'.join([annotation.__str__() for annotation in self.annotations]))
        representations.append('\n'.join([container.__str__() for container in self.containers]))
        return '\n'.join(representation for representation in representations)
    
class Deployment(Node):
    def __init__(self, uid: str, properties: dict[str: any] = {}, labels: list[Label] = {}, annotations: list[Annotation] = {}) -> None:
        super().__init__(type=self.__class__.__name__, uid=uid, properties=properties)

        self.labels = labels
        self.annotations = annotations
    
    def merge(self, tx: callable):
        print(super().merge(tx))

        for label in self.labels:
            label.merge(tx)
            result = self.link(tx, type=Relations.HAS_LABEL, target=label)
            print(result)

        for annotation in self.annotations:
            annotation.merge(tx)
            result = self.link(tx, type=Relations.HAS_ANNOTATION, target=annotation)
            print(result)

    def __str__(self):
        representations = []
        representations.append(super().__str__())
        if len(self.labels) > 0:
            representations.append('\n'.join([label.__str__() for label in self.labels]))
        if len(self.annotations) > 0:
            representations.append('\n'.join([annotation.__str__() for annotation in self.annotations]))
        return '\n'.join(representation for representation in representations)

class ReplicaSet(Node):
    def __init__(self, uid: str, deployment_uid: str, properties: dict[str: any] = {}, labels: list[Label] = {}, annotations: list[Annotation] = {}) -> None:
        super().__init__(type=self.__class__.__name__, uid=uid, properties=properties)

        self.deploymentUID = deployment_uid
        self.labels = labels
        self.annotations = annotations
    
    def merge(self, tx: callable):
        print(super().merge(tx))

        for label in self.labels:
            label.merge(tx)
            result = self.link(tx, type=Relations.HAS_LABEL, target=label)
            print(result)

        for annotation in self.annotations:
            annotation.merge(tx)
            result = self.link(tx, type=Relations.HAS_ANNOTATION, target=annotation)
            print(result)

        result = self.link(tx, type=Relations.MANAGED_BY, target=Node(type='Deployment', uid=self.deploymentUID))
        print(result)
    
    def __str__(self):
        representations = []
        representations.append(f"{super().__str__()}, DeploymentUID: {self.deploymentUID}")
        if len(self.labels) > 0:
            representations.append('\n'.join([label.__str__() for label in self.labels]))
        if len(self.annotations) > 0:
            representations.append('\n'.join([annotation.__str__() for annotation in self.annotations]))
        return '\n'.join(representation for representation in representations)

class Cluster(Node):
    """
    This class stands for clusters in our graph.
    """
    def __init__(self, uid: str, properties: dict[str: any] = {}) -> None:
        super().__init__(type=self.__class__.__name__, uid=uid, properties=properties)

    def merge(self, tx: callable):
        print(super().merge(tx))

class K8sNode(Node):
    def __init__(self, uid: str, properties: dict[str: any] = {}, labels: list[Label] = {}, annotations: list[Annotation] = {}, taints: list[Taint] = [],
                  images: list[Image] = [], cluster_uid: str = None) -> None:
        super().__init__(type=self.__class__.__name__, uid=uid, properties=properties)

        self.labels = labels
        self.annotations = annotations
        self.taints = taints
        self.images = images
        self.cluster_uid = cluster_uid
    
    def merge(self, tx: callable):
        print(super().merge(tx))

        result = self.link(tx, type=Relations.BELONGS_TO, target=Node("Cluster", uid=self.cluster_uid))

        for label in self.labels:
            label.merge(tx)
            result = self.link(tx, type=Relations.HAS_LABEL, target=label)
            print(result)

        for annotation in self.annotations:
            annotation.merge(tx)
            result = self.link(tx, type=Relations.HAS_ANNOTATION, target=annotation)
            print(result)

        for taint in self.taints:
            taint.merge(tx)
            result = self.link(tx, type=Relations.HAS_TAINT, target=taint)
            print(result)

        for image in self.images:
            image.merge(tx)
            result = self.link(tx, type=Relations.STORES_IMAGE, target=image)
            print(result)
    
    @classmethod
    def set(cls, tx: callable, cluster_id: str, hostname: str, metrics: dict[str: any]):
        query = set_k8snode_metrics(metrics=metrics)
        print('\n' + query + '\n')
        result = tx.run(query, metrics, hostname=hostname, cluster_id=cluster_id)
        print(result.single())
    
    @classmethod
    def delete(cls, tx, **kwargs):
        summary = Node.delete(tx, "K8sNode", **kwargs)
        relationships_deleted = summary.counters.relationships_deleted
        labels_deleted = delete_orphans(tx, "Label")
        annotations_deleted = delete_orphans(tx, "Annotation")
        taints_deleted = delete_orphans(tx, "Taint")
        images_deleted = delete_orphans(tx, "Image")
        nodes_deleted = summary.counters.nodes_deleted + labels_deleted + annotations_deleted + taints_deleted + images_deleted
        print(f"\nDeleted {nodes_deleted} graph nodes and {relationships_deleted} relationships")

    def __str__(self):
        representations = []
        representations.append(super().__str__())
        if len(self.labels) > 0:
            representations.append('\n'.join([label.__str__() for label in self.labels]))
        if len(self.annotations) > 0:
            representations.append('\n'.join([annotation.__str__() for annotation in self.annotations]))
        if len(self.taints) > 0:
            representations.append('\n'.join(taint.__str__() for taint in self.taints))
        if len(self.images) > 0:
            representations.append('\n'.join(image.__str__() for image in self.images))
        return '\n'.join(representation for representation in representations)