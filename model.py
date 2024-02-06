from kubegrapher.utils.utils import merge_node, merge_relationship, to_properties, merge_relationship_generic
import uuid

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

    def __str__(self):
        properties_str = toString(self.properties, self.kwproperties)
        return f"UID: {self.id}, Type: {self.type}, Properties: {{{properties_str}}}"
    
    __repr__ = __str__
    

class Image(Node):
    def __init__(self, imageID: str, name: str, sizeInBytes: int) -> None:
        super().__init__(type=self.__class__.__name__, uid=imageID, name=name, sizeInBytes=sizeInBytes)

    def merge(self, tx: callable):
        print(super().merge(tx))
        

class Container(Node):
    def __init__(self, image_id: str, properties: dict[str: any], configmap_name: str = None) -> None:
        super().__init__(type=self.__class__.__name__, properties=properties)

        self.imageID = image_id
        self.configMapName = configmap_name
    
    def merge(self, tx: callable):
        print(super().merge(tx))
        result = self.link(tx, type='INSTANTIATES_IMAGE', target=Node(type='Image', uid=self.imageID))
        print(result)
        if self.configMapName is not None:
            result = self.link_generic(tx, type='CONFIGMAP_REF', target=Node(type='ConfigMap', name=self.configMapName))
            print(result)

    def __str__(self):
        cont = f"{super().__str__()}, Image: {self.imageID}"
        if self.configMapName is not None:
            cont = f"{cont}, ConfigMap: {self.configMapName}"
        return cont
    
class Label(Node):
    def __init__(self, key: str, value: str) -> None:
        super().__init__(type=self.__class__.__name__, key=key, value=value)
    
    def merge(self, tx: callable):
        print(super().merge(tx))

class Annotation(Node):
    def __init__(self, key: str, value: str) -> None:
        super().__init__(type=self.__class__.__name__, key=key, value=value)
    
    def merge(self, tx: callable):
        print(super().merge(tx))

class Taint(Node):
    def __init__(self, key: str, effect: str) -> None:
        super().__init__(type=self.__class__.__name__, key=key, effect=effect)
    
    def merge(self, tx: callable):
        print(super().merge(tx))

class ConfigMap(Node):
    def __init__(self, uid: str, properties: dict = None) -> None:
        super().__init__(type=self.__class__.__name__, uid=uid, properties=properties)

    def merge(self, tx: callable):
        print(super().merge(tx))

class Service(Node):
    def __init__(self, uid: str, properties: dict = None, **kwargs) -> None:
        super().__init__(type=self.__class__.__name__, uid=uid, properties=properties)

    def merge(self, tx: callable):
        print(super().merge(tx))

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
            result = self.link(tx, type='HAS_LABEL', target=label)
            print(result)

        for annotation in self.annotations:
            annotation.merge(tx)
            result = self.link(tx, type='HAS_ANNOTATION', target=annotation)
            print(result)

        # link to replicaset
        if self.replicaSetUID is not None:
            result = self.link(tx, type='MANAGED_BY', target=Node(type='ReplicaSet', uid=self.replicaSetUID))
            print(result)
        
        # link to node
        result = self.link_generic(tx, type='SCHEDULED_ON', target=Node(type='K8sNode', name=self.k8sNodeName))
        print(result)

        for container in self.containers:
            container.merge(tx)
            result = self.link(tx, type='RUNS_CONTAINER', target=container)
            print(result)

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
            result = self.link(tx, type='HAS_LABEL', target=label)
            print(result)

        for annotation in self.annotations:
            annotation.merge(tx)
            result = self.link(tx, type='HAS_ANNOTATION', target=annotation)
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
            result = self.link(tx, type='HAS_LABEL', target=label)
            print(result)

        for annotation in self.annotations:
            annotation.merge(tx)
            result = self.link(tx, type='HAS_ANNOTATION', target=annotation)
            print(result)

        result = self.link(tx, type='MANAGED_BY', target=Node(type='Deployment', uid=self.deploymentUID))
        print(result)
    
    def __str__(self):
        representations = []
        representations.append(f"{super().__str__()}, DeploymentUID: {self.deploymentUID}")
        if len(self.labels) > 0:
            representations.append('\n'.join([label.__str__() for label in self.labels]))
        if len(self.annotations) > 0:
            representations.append('\n'.join([annotation.__str__() for annotation in self.annotations]))
        return '\n'.join(representation for representation in representations)

class K8sNode(Node):
    def __init__(self, uid: str, properties: dict[str: any] = {}, labels: list[Label] = {}, annotations: list[Annotation] = {}, taints: list[Taint] = [], images: list[Image] = []) -> None:
        super().__init__(type=self.__class__.__name__, uid=uid, properties=properties)

        self.labels = labels
        self.annotations = annotations
        self.taints = taints
        self.images = images
    
    def merge(self, tx: callable):
        print(super().merge(tx))

        for label in self.labels:
            label.merge(tx)
            result = self.link(tx, type='HAS_LABEL', target=label)
            print(result)

        for annotation in self.annotations:
            annotation.merge(tx)
            result = self.link(tx, type='HAS_ANNOTATION', target=annotation)
            print(result)

        for taint in self.taints:
            taint.merge(tx)
            result = self.link(tx, type='HAS_TAINT', target=taint)
            print(result)

        for image in self.images:
            image.merge(tx)
            result = self.link(tx, type='STORES_IMAGE', target=image)
            print(result)

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


