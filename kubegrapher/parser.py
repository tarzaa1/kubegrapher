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
    Pod,
    Ingress
)
import logging
import json
import re

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
            'allocatable_cpu' : status.get('allocatable').get('cpu'),
            'allocatable_ephemeral_storage' : status.get('allocatable').get('ephemeral-storage'),
            'allocatable_memory' : status.get('allocatable').get('memory'),
            **status.get('nodeInfo', {}),
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
            "request_cpu": 0,
            "request_memory": 0,
            "limit_cpu": 0,
            "limit_memory": 0,
        }

        resources = container.get('resources', {})
        if resources:
            cpu_request = resources.get('requests', {}).get('cpu') 
            memory_request = resources.get('requests', {}).get('memory')
            cpu_limit = resources.get('limits', {}).get('cpu')
            memory_limit = resources.get('limits', {}).get('memory')

            properties["request_cpu"] = extract_number(cpu_request) or 0
            properties["request_memory"] = extract_number(memory_request) or 0
            properties["limit_cpu"] = extract_number(cpu_limit) or 0
            properties["limit_memory"] = extract_number(memory_limit) or 0

        container_id = containerStatus.get('containerID', '')
        image_name = containerStatus.get('image', '')

        configmap_name = None
        env_from = container.get('envFrom') or []
        if isinstance(env_from, list):
            for item in env_from:
                if isinstance(item, dict) and 'configMapRef' in item:
                    cmref = item.get('configMapRef') or {}
                    configmap_name = cmref.get('name')
                    break


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
    
def parse_service(service: dict[str, any], topic_name: str) -> Service:
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

        return Service(uid, properties, labels=labels, cluster_id=topic_name)
    except Exception as e:
        logging.error(f"Error parsing Service: {e}")
        return None
    
def parse_k8snode_metrics(cluster_id: str, metrics: dict[str, any]) -> list[dict[str, any]]:
    node_usage_list = []
    try:
        for node in metrics["items"]:
            usage = node["usage"]
            node_usage_list.append({
                "cluster_id": cluster_id,
                "hostname": node["metadata"]["name"],
                "usage_cpu": extract_number(usage["cpu"]),
                "usage_memory": extract_number(usage["memory"])
            })
        return node_usage_list
    except Exception as e:
        logging.error(f"Error parsing Metrics: {e}")
        return []
    
def parse_pod_metrics(metrics: dict[str, any]) -> list[dict[str, any]]:
    container_usage_list = []
    try:
        for pod in metrics["items"]:
            pod_name = pod["metadata"]["name"]
            for container in pod["containers"]:
                usage = container["usage"]
                container_usage_list.append({
                    "podName": pod_name,
                    "containerName": container["name"],
                    "usage_cpu": extract_number(usage["cpu"]),
                    "usage_memory": extract_number(usage["memory"])
                })
        return container_usage_list
    except Exception as e:
        logging.error(f"Error parsing Metrics: {e}")
        return []
    
def parse_ingress(ingress: dict[str, any], topic_name: str) -> Ingress:
    try:
        metadata = ingress.get('metadata', {})
        spec = ingress.get('spec', {})
        status = ingress.get('status', {})
        cluster_id = topic_name

        uid = metadata.get('uid')
        if not uid:
            raise ValueError("UID is missing in the ingress metadata")

        properties = {
            'name': metadata.get('name', ''),
            'creationTimestamp': metadata.get('creationTimestamp', ''),
            'ingressClassName': spec.get('ingressClassName', ''),
            'rules': json.dumps(spec.get('rules', [])),
        }
        annotations = [Annotation(key, value) for key, value in metadata.get('annotations', {}).items()]

        return Ingress(uid, properties, annotations, cluster_id)
    except Exception as e:
        logging.error(f"Error parsing Ingress: {e}")
        return None

def extract_number(x):
    if x is None:
        return None
    if isinstance(x, (int, float)):
        return int(x)
    x = str(x)
    m = re.match(r"(\d+)", x)
    return int(m.group(1)) if m else None
