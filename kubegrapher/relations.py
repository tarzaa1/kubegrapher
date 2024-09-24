from enum import Enum

class Relations(Enum):
    """
    Enum class representing different types of relations in our data model 
    that can be appear in the graph.

    Attributes:
        INSTANTIATES_IMAGE: a Container instantiates an Image
        CONFIGMAP_REF: a Container has referenced ConfigMap
        HAS_SELECTOR: a resource* has a selector(Label).
        HAS_LABEL:  a resource* has a Label.
        HAS_ANNOTATION: a Pod has an Annotation.
        MANAGED_BY: a Pod is managed by a ReplicaSet
        SCHEDULED_ON: a Pod is scheduled on a K8sNode
        RUNS_CONTAINER: a Pod runs Container
        BELONGS_TO: a K8sNode belongs to a Cluster
        HAS_TAINT: a K8sNode has Taint
        STORES_IMAGE: a K8sNode stores Image
        EXPOSES: a Service exposes a Pod
        ROUTES_TO: an Ingress reoutes to a Service

    Note that the passed value of the range() should match the number of the Enums
    """
    INSTANTIATES_IMAGE, CONFIGMAP_REF, HAS_SELECTOR, \
    HAS_LABEL, HAS_ANNOTATION, MANAGED_BY, \
    SCHEDULED_ON, RUNS_CONTAINER, BELONGS_TO, \
    HAS_TAINT, STORES_IMAGE, EXPOSES, ROUTES_TO = range(13)

    def __get__(self, instance, owner):
            return self.name