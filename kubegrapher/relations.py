from enum import Enum

class Relations(Enum):
    INSTANTIATES_IMAGE, CONFIGMAP_REF, HAS_SELECTOR, \
    HAS_LABEL, HAS_ANNOTATION, MANAGED_BY, \
    SCHEDULED_ON, RUNS_CONTAINER, BELONGS_TO, \
    HAS_TAINT, STORES_IMAGE, EXPOSES = range(11)

    def __get__(self, instance, owner):
            return self.name