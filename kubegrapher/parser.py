from kubegrapher.model import Label, Annotation, Taint, Image, K8sNode, ConfigMap, Service, Deployment, ReplicaSet, Container, Pod
import json

def parseK8sNode(k8snode: dict[str: any]):

    metadata = k8snode['metadata']
    spec = k8snode['spec']
    status = k8snode['status']

    uid = metadata['uid']

    properties = {}
    properties['name'] = metadata['name']
    properties['creationTimestamp'] = metadata['creationTimestamp']
    properties.update(status['nodeInfo'])
    # properties.update(status['allocatable'])
    properties['internalIP'] = status['addresses'][0]['address']
    properties['hostname'] = status['addresses'][1]['address']

    # pprint(properties)

    if 'labels' in metadata.keys():
        labels = [Label(key, value) for key, value in metadata['labels'].items()]
    else:
        labels = []
    if 'annotations' in metadata.keys():
        annotations = [Annotation(key, value) for key, value in metadata['annotations'].items()]
    else:
        annotations = []
    if 'taints' in spec.keys():
        taints = [Taint(**taint) for taint in spec['taints']]
    else:
        taints = []

    images = []
    for image in status['images']:
        if isinstance(image['names'], list):
            images.append(Image(image['names'][-1], image['sizeBytes']))

    k8snode = K8sNode(uid, properties, labels, annotations, taints, images)

    return k8snode

def parseConfigMap(configmap: dict[str: any]):

    metadata = configmap['metadata']
    data = configmap['data']

    uid = metadata['uid']

    properties = {}
    properties['name'] = metadata['name']
    properties['creationTimestamp'] = metadata['creationTimestamp']
    properties['data'] = json.dumps(data)

    # pprint(properties)
    
    if 'labels' in metadata.keys():
        labels = [Label(key, value) for key, value in metadata['labels'].items()]
    else:
        labels = []
    if 'annotations' in metadata.keys():
        annotations = [Annotation(key, value) for key, value in metadata['annotations'].items()]
    else:
        annotations = []

    configmap = ConfigMap(uid, properties)

    return configmap

def parseDeployment(deployment: dict[str: any]):

    metadata = deployment['metadata']
    spec = deployment['spec']
    status = deployment['status']

    uid = metadata['uid']

    properties = {}
    properties['name'] = metadata['name']
    properties['creationTimestamp'] = metadata['creationTimestamp']
    
    properties['replicas'] = spec['replicas']
    properties['selector'] = json.dumps(spec['selector'])
    properties['strategy'] = json.dumps(spec['strategy'])
    properties['revisionHistoryLimit'] = spec['revisionHistoryLimit']
    properties['progressDeadlineSeconds'] = spec['progressDeadlineSeconds']

    properties['observedGeneration'] = status['observedGeneration']
    # replicas = status['replicas']
    properties['updatedReplicas'] = status['updatedReplicas']
    properties['readyReplicas'] = status['readyReplicas']
    properties['availableReplicas'] = status['availableReplicas']

    # pprint(properties)

    if 'labels' in metadata.keys():
        labels = [Label(key, value) for key, value in metadata['labels'].items()]
    else:
        labels = []
    if 'annotations' in metadata.keys():
        annotations = [Annotation(key, value) for key, value in metadata['annotations'].items()]
    else:
        annotations = []

    deployment = Deployment(uid, properties, labels, annotations)

    return deployment

def parseReplicaSet(replicaset: dict[str: any]):

    metadata = replicaset['metadata']
    spec = replicaset['spec']
    status = replicaset['status']

    uid = metadata['uid']

    properties = {}
    properties['name'] = metadata['name']
    properties['creationTimestamp'] = metadata['creationTimestamp']
    deploymentUID = metadata['ownerReferences'][0]['uid']
    
    properties['replicas'] = spec['replicas']
    properties['selector'] = json.dumps(spec['selector'])

    properties['observedGeneration'] = status['observedGeneration']
    # replicas = status['replicas']
    properties['fullyLabeledReplicas'] = status['fullyLabeledReplicas']
    properties['readyReplicas'] = status['readyReplicas']
    properties['availableReplicas'] = status['availableReplicas']

    # pprint(properties)

    if 'labels' in metadata.keys():
        labels = [Label(key, value) for key, value in metadata['labels'].items()]
    else:
        labels = []
    if 'annotations' in metadata.keys():
        annotations = [Annotation(key, value) for key, value in metadata['annotations'].items()]
    else:
        annotations = []

    replicaset = ReplicaSet(uid, deploymentUID, properties, labels, annotations)

    return replicaset

def parsePod(pod: dict[str: any]):

    metadata = pod['metadata']
    spec = pod['spec']
    status = pod['status']

    uid = metadata['uid']

    properties = {}
    properties['name'] = metadata['name']
    properties['creationTimestamp'] = metadata['creationTimestamp']
    
    containers = spec['containers']
    properties['restartPolicy'] = spec['restartPolicy'] 
    properties['terminationGracePeriodSeconds'] = spec['terminationGracePeriodSeconds'] 
    properties['dnsPolicy'] = spec['dnsPolicy']
    properties['serviceAccountName'] = spec['serviceAccountName']
    properties['serviceAccount'] = spec['serviceAccount']
    nodeName = spec['nodeName']
    securityContext = spec['securityContext'] 
    properties['schedulerName'] = spec['schedulerName']
    tolerations = spec['tolerations']
    properties['priority'] = spec['priority']
    properties['enableServiceLinks'] = spec['enableServiceLinks'] 
    properties['preemptionPolicy'] = spec['preemptionPolicy']

    properties['phase'] = status['phase']
    properties['hostIP'] = status['hostIP']
    if 'podIP' in status:
        properties['podIP'] = status['podIP']
    # podIPs = status['podIPs']
    properties['startTime'] = status['startTime']
    containerStatuses = status['containerStatuses']
    properties['qosClass'] = status['qosClass']

    # pprint(properties)

    if 'ownerReferences' in metadata.keys():
        replicasetUID = metadata['ownerReferences'][0]['uid']
    else:
        replicasetUID = None
    if 'labels' in metadata.keys():
        labels = [Label(key, value) for key, value in metadata['labels'].items()]
    else:
        labels = []
    if 'annotations' in metadata.keys():
        annotations = [Annotation(key, value) for key, value in metadata['annotations'].items()]
    else:
        annotations = []

    containerStatuses = {containerStatus['name']: containerStatus for containerStatus in containerStatuses}

    containers = [parseContainer(container, containerStatuses[container['name']]) for container in containers]

    pod = Pod(uid, nodeName, properties, labels, annotations, containers, replicasetUID)

    return pod

def parseContainer(container: dict[str: any], containerStatus: dict[str: any]):
    
    properties = {}
    properties['name'] = container['name']
    properties['image'] = container['image']
    properties['terminationMessagePath'] = container['terminationMessagePath']
    properties['terminationMessagePolicy'] = container['terminationMessagePolicy']
    properties['imagePullPolicy'] = container['imagePullPolicy']

    # pprint(properties)

    imageName = containerStatus['image']

    if 'envFrom' in container.keys():
        configmap_name = container['envFrom'][0]['configMapRef']['name']
    else:
        configmap_name = None

    container = Container(imageName, properties, configmap_name)

    return container

def parseImage(image: dict[str: any]):
    names = image['names']
    sizeBytes = image['sizeBytes']

    return Image(names[-1], sizeBytes)

def parseService(service: dict[str: any]):

    metadata = service['metadata']
    spec = service['spec']
    status = service['status']

    uid = metadata['uid']

    properties = {}
    
    properties['name'] = metadata['name']
    properties['creationTimestamp'] = metadata['creationTimestamp']

    properties['ports'] = json.dumps(spec['ports'])
    if 'selector' in spec.keys():
        properties['selector'] = json.dumps(spec['selector'])
    properties['clusterIP'] = spec['clusterIP']
    clusterIPs = spec['clusterIPs']
    properties['type'] = spec['type']
    properties['sessionAffinity'] = spec['sessionAffinity']
    if 'externalTrafficPolicy' in spec.keys():
        properties['externalTrafficPolicy'] = spec['externalTrafficPolicy']
    ipFamilies = spec['ipFamilies']
    properties['ipFamilyPolicy'] = spec['ipFamilyPolicy']
    properties['internalTrafficPolicy'] = spec['internalTrafficPolicy']

    loadBalancer = status['loadBalancer']

    # pprint(properties)

    if 'labels' in metadata.keys():
        labels = [Label(key, value) for key, value in metadata['labels'].items()]
    else:
        labels = []
    if 'annotations' in metadata.keys():
        annotations = [Annotation(key, value) for key, value in metadata['annotations'].items()]
    else:
        annotations = []

    service = Service(uid, properties)

    return service