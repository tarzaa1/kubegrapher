from kubegrapher.model import (
    Label, 
    Annotation, 
    Taint, 
    Image, 
    K8sNode, 
    ConfigMap, 
    Service, 
    Deployment, 
    ReplicaSet, 
    Container, 
    Pod
)
import logging
import json

logging.basicConfig(level=logging.INFO)

def parse_k8s_node(k8snode: dict[str: any], topic_name: str) -> K8sNode:
    try:
        metadata = k8snode.get('metadata', {})
        spec = k8snode.get('spec', {})
        status = k8snode.get('status', {})

        uid = metadata.get('uid')
        if not uid:
            raise ValueError("UID is missing in the node metadata")
        
        properties = {
            'name': metadata.get('name', ''),
            'creationTimestamp': metadata.get('creationTimestamp', ''),
            **status.get('nodeInfo', {}),
            # **status.get('allocatable', {})
        }

        addresses = status.get('addresses', [])
        if addresses:
            properties['internalIP'] = addresses[0].get('address', '')
            if len(addresses) > 1:
                properties['hostname'] = addresses[1].get('address', '')

        labels = [Label(key, value) for key, value in metadata.get('labels', {}).items()]
        annotations = [Annotation(key, value) for key, value in metadata.get('annotations', {}).items()]
        taints = [Taint(**taint) for taint in spec.get('taints', [])]
        images = [Image(image['names'][-1], image['sizeBytes']) for image in status.get('images', []) if isinstance(image['names'], list)]
        return K8sNode(uid, properties, labels, annotations, taints, images, topic_name )
    except Exception as e:
        logging.error(f"Error parsing K8sNode: {e}")
        return None

def parse_configmap(configmap: dict[str, any]) -> ConfigMap:
    try:
        metadata = configmap.get('metadata', {})
        data = configmap.get('data', {})

        uid = metadata.get('uid')
        if not uid:
            raise ValueError("UID is missing in the configmap metadata")

        properties = {
            'name': metadata.get('name', ''),
            'creationTimestamp': metadata.get('creationTimestamp', ''),
            'data': json.dumps(data)
        }

        labels = [Label(key, value) for key, value in metadata.get('labels', {}).items()]
        annotations = [Annotation(key, value) for key, value in metadata.get('annotations', {}).items()]

        return ConfigMap(uid, properties)
    except Exception as e:
        logging.error(f"Error parsing ConfigMap: {e}")
        return None
    
def parse_deployment(deployment: dict[str, any]) -> Deployment:
    try:
        metadata = deployment.get('metadata', {})
        spec = deployment.get('spec', {})
        status = deployment.get('status', {})

        uid = metadata.get('uid')
        if not uid:
            raise ValueError("UID is missing in the deployment metadata")

        properties = {
            'name': metadata.get('name', ''),
            'creationTimestamp': metadata.get('creationTimestamp', ''),
            'replicas': spec.get('replicas'),
            'selector': json.dumps(spec.get('selector', {})),
            'strategy': json.dumps(spec.get('strategy', {})),
            'revisionHistoryLimit': spec.get('revisionHistoryLimit'),
            'progressDeadlineSeconds': spec.get('progressDeadlineSeconds'),
            'observedGeneration': status.get('observedGeneration'),
            # 'replicas': status.get('replicas'),
            'updatedReplicas': status.get('updatedReplicas'),
            'readyReplicas': status.get('readyReplicas'),
            'availableReplicas': status.get('availableReplicas')
        }

        labels = [Label(key, value) for key, value in metadata.get('labels', {}).items()]
        annotations = [Annotation(key, value) for key, value in metadata.get('annotations', {}).items()]

        return Deployment(uid, properties, labels, annotations)
    except Exception as e:
        logging.error(f"Error parsing Deployment: {e}")
        return None

def parse_replicaset(replicaset: dict[str, any]) -> ReplicaSet:
    try:
        metadata = replicaset.get('metadata', {})
        spec = replicaset.get('spec', {})
        status = replicaset.get('status', {})

        uid = metadata.get('uid')
        if not uid:
            raise ValueError("UID is missing in the replicaset metadata")

        deploymentUID = metadata.get('ownerReferences', [{}])[0].get('uid')
        if not deploymentUID:
            raise ValueError("Deployment UID is missing in the replicaset ownerReferences")

        properties = {
            'name': metadata.get('name', ''),
            'creationTimestamp': metadata.get('creationTimestamp', ''),
            'replicas': spec.get('replicas'),
            'selector': json.dumps(spec.get('selector', {})),
            'observedGeneration': status.get('observedGeneration'),
            # 'replicas': status.get('replicas'),
            'fullyLabeledReplicas': status.get('fullyLabeledReplicas'),
            'readyReplicas': status.get('readyReplicas'),
            'availableReplicas': status.get('availableReplicas')
        }

        labels = [Label(key, value) for key, value in metadata.get('labels', {}).items()]
        annotations = [Annotation(key, value) for key, value in metadata.get('annotations', {}).items()]

        return ReplicaSet(uid, deploymentUID, properties, labels, annotations)
    except Exception as e:
        logging.error(f"Error parsing ReplicaSet: {e}")
        return None
    
def parse_pod(pod: dict[str, any]) -> Pod:
    try:
        metadata = pod.get('metadata', {})
        spec = pod.get('spec', {})
        status = pod.get('status', {})

        uid = metadata.get('uid')
        if not uid:
            raise ValueError("UID is missing in the pod metadata")

        properties = {
            'name': metadata.get('name', ''),
            'creationTimestamp': metadata.get('creationTimestamp', ''),
            'restartPolicy': spec.get('restartPolicy', ''),
            'terminationGracePeriodSeconds': spec.get('terminationGracePeriodSeconds', ''),
            'dnsPolicy': spec.get('dnsPolicy', ''),
            'serviceAccountName': spec.get('serviceAccountName', ''),
            'serviceAccount': spec.get('serviceAccount', ''),
            # 'securityContext': spec.get('securityContext', ''),
            'schedulerName': spec.get('schedulerName', ''),
            'priority': spec.get('priority', ''),
            'enableServiceLinks': spec.get('enableServiceLinks', ''),
            'preemptionPolicy': spec.get('preemptionPolicy', ''),
            'phase': status.get('phase', ''),
            'hostIP': status.get('hostIP', ''),
            'podIP': status.get('podIP', ''),
            'startTime': status.get('startTime', ''),
            'qosClass': status.get('qosClass', '')
        }

        nodeName = spec.get('nodeName', '')
        # tolerations = spec.get('tolerations', [])
        containerStatuses = {containerStatus['name']: containerStatus for containerStatus in status.get('containerStatuses', [])}
        containers = [parse_container(container, containerStatuses.get(container['name'], {})) for container in spec.get('containers', [])]

        replicasetUID = metadata.get('ownerReferences', [{}])[0].get('uid', None)

        labels = [Label(key, value) for key, value in metadata.get('labels', {}).items()]
        annotations = [Annotation(key, value) for key, value in metadata.get('annotations', {}).items()]

        return Pod(uid, nodeName, properties, labels, annotations, containers, replicasetUID)
    except Exception as e:
        logging.error(f"Error parsing Pod: {e}")
        return None

def parse_container(container: dict[str, any], containerStatus: dict[str, any]) -> Container:
    try:
        properties = {
            'name': container.get('name', ''),
            'image': container.get('image', ''),
            'imagePullPolicy': container.get('imagePullPolicy', ''),
            'terminationMessagePath': container.get('terminationMessagePath', ''),
            'terminationMessagePolicy': container.get('terminationMessagePolicy', ''),
        }

        container_id = containerStatus.get('containerID', '')
        image_name = containerStatus.get('image', '')

        configmap_name = None
        if 'envFrom' in container.keys() and 'configMapRef' in container['envFrom'][0]:
            configmap_name = container['envFrom'][0]['configMapRef'].get('name', None)

        return Container(container_id, image_name, properties, configmap_name)
    except Exception as e:
        logging.error(f"Error parsing Container: {e}")
        return None

def parse_image(image: dict[str, any]) -> Image:
    try:
        names = image.get('names', [])
        sizeBytes = image.get('sizeBytes', 0)

        return Image(names[-1], sizeBytes)
    except Exception as e:
        logging.error(f"Error parsing Image: {e}")
        return None
    
def parse_service(service: dict[str, any]) -> Service:
    try:
        metadata = service.get('metadata', {})
        spec = service.get('spec', {})
        status = service.get('status', {})

        uid = metadata.get('uid')
        if not uid:
            raise ValueError("UID is missing in the service metadata")

        properties = {
            'name': metadata.get('name', ''),
            'creationTimestamp': metadata.get('creationTimestamp', ''),
            'ports': json.dumps(spec.get('ports', [])),
            'selector': json.dumps(spec.get('selector', {})),
            'clusterIP': spec.get('clusterIP', ''),
            'type': spec.get('type', ''),
            'sessionAffinity': spec.get('sessionAffinity', ''),
            'externalTrafficPolicy': spec.get('externalTrafficPolicy', ''),
            'ipFamilyPolicy': spec.get('ipFamilyPolicy', ''),
            'internalTrafficPolicy': spec.get('internalTrafficPolicy', '')
        }

        clusterIPs = spec.get('clusterIPs')
        ipFamilies = spec.get('ipFamilies')
        loadBalancer = status.get('loadBalancer')

        labels = [Label(key, value) for key, value in metadata.get('labels', {}).items()]
        annotations = [Annotation(key, value) for key, value in metadata.get('annotations', {}).items()]

        return Service(uid, properties)
    except Exception as e:
        logging.error(f"Error parsing Service: {e}")
        return None