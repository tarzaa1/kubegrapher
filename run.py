from kubegrapher.utils.hedera.utils import get_client, subscribe
from kubegrapher.utils.graph.neo4j import Neo4j
from kubegrapher.grapher import Grapher
from kubegrapher.model import Label, Annotation, Taint, Image, K8sNode, ConfigMap, Service, Deployment, ReplicaSet, Container, Pod
from pprint import pprint

import json

def processMessage(graphdb, grapher: Grapher, session, *args):
    # print("time: {} received: {}".format(args[0], args[2]))

    message = json.loads(args[2])

    kind = message['kind']
    body = message['body']
    if kind == 'Node':
        k8snode = parseK8sNode(body)
        session.execute_write(grapher.add_k8snode, k8snode)
    elif kind == 'ConfigMap':
        configmap = parseConfigMap(body)
        session.execute_write(grapher.add_configmap, configmap)
    elif kind == 'Pod':
        pod = parsePod(body)
        session.execute_write(grapher.add_pod, pod)
    elif kind == 'Service':
        service = parseService(body)
        session.execute_write(grapher.add_service, service)
    elif kind == 'Deployment':
        deployment = parseDeployment(body)
        session.execute_write(grapher.add_deployment, deployment)
    elif kind == 'ReplicaSet':
        replicaset = parseReplicaSet(body)
        session.execute_write(grapher.add_replicaset, replicaset)
    elif kind == 'Done':
        session.execute_write(grapher.delete_pod, name="detectify-deployment-549d769bbd-6dbtv")
        session.close()
        graphdb.delete_all()
    else:
        pass

    # print(message)

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
    images = [Image(image['names'][0], image['names'][1], image['sizeBytes']) for image in status['images']]

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
    properties['podIP'] = status['podIP']
    podIPs = status['podIPs']
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

    imageID = containerStatus['imageID']

    if 'envFrom' in container.keys():
        configmap_name = container['envFrom'][0]['configMapRef']['name']
    else:
        configmap_name = None

    container = Container(imageID, properties, configmap_name)

    return container

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


if __name__ == '__main__':

    URI = "bolt://localhost:7687"
    AUTH = ("neo4j", "password")
    graphdb = Neo4j(URI, AUTH)
    grapher = Grapher(graphdb)

    session = graphdb.driver.session()

    client = get_client("config.json")

    subscribe(client, "0.0.1003", lambda *args: processMessage(graphdb, grapher, session, *args))

    # item = {'metadata': {'name': 'clever-tarek-m1', 'uid': 'b1a9a19a-7ac1-4d41-a6eb-c53cc10a6338', 'resourceVersion': '17582714', 'creationTimestamp': '2023-09-20T10:06:47Z', 'labels': {'beta.kubernetes.io/arch': 'amd64', 'beta.kubernetes.io/os': 'linux', 'foo': 'bar', 'kubernetes.io/arch': 'amd64', 'kubernetes.io/hostname': 'clever-tarek-m1', 'kubernetes.io/os': 'linux', 'node-role.kubernetes.io/control-plane': '', 'node.kubernetes.io/exclude-from-external-load-balancers': ''}, 'annotations': {'flannel.alpha.coreos.com/backend-data': '{"VNI":1,"VtepMAC":"d6:90:b9:0f:b1:36"}', 'flannel.alpha.coreos.com/backend-type': 'vxlan', 'flannel.alpha.coreos.com/kube-subnet-manager': 'true', 'flannel.alpha.coreos.com/public-ip': '10.18.1.40', 'kubeadm.alpha.kubernetes.io/cri-socket': 'unix:///var/run/containerd/containerd.sock', 'node.alpha.kubernetes.io/ttl': '0', 'volumes.kubernetes.io/controller-managed-attach-detach': 'true'}, 'managedFields': [{'manager': 'kubelet', 'operation': 'Update', 'apiVersion': 'v1', 'time': '2023-09-20T10:06:47Z', 'fieldsType': 'FieldsV1', 'fieldsV1': {'f:metadata': {'f:annotations': {'.': {}, 'f:volumes.kubernetes.io/controller-managed-attach-detach': {}}, 'f:labels': {'.': {}, 'f:beta.kubernetes.io/arch': {}, 'f:beta.kubernetes.io/os': {}, 'f:kubernetes.io/arch': {}, 'f:kubernetes.io/hostname': {}, 'f:kubernetes.io/os': {}}}}}, {'manager': 'kubeadm', 'operation': 'Update', 'apiVersion': 'v1', 'time': '2023-09-20T10:06:50Z', 'fieldsType': 'FieldsV1', 'fieldsV1': {'f:metadata': {'f:annotations': {'f:kubeadm.alpha.kubernetes.io/cri-socket': {}}, 'f:labels': {'f:node-role.kubernetes.io/control-plane': {}, 'f:node.kubernetes.io/exclude-from-external-load-balancers': {}}}}}, {'manager': 'OpenAPI-Generator', 'operation': 'Update', 'apiVersion': 'v1', 'time': '2023-10-12T14:32:29Z', 'fieldsType': 'FieldsV1', 'fieldsV1': {'f:metadata': {'f:labels': {'f:foo': {}}}}}, {'manager': 'kube-controller-manager', 'operation': 'Update', 'apiVersion': 'v1', 'time': '2024-01-18T10:48:23Z', 'fieldsType': 'FieldsV1', 'fieldsV1': {'f:metadata': {'f:annotations': {'f:node.alpha.kubernetes.io/ttl': {}}}, 'f:spec': {'f:podCIDR': {}, 'f:podCIDRs': {'.': {}, 'v:"10.244.0.0/24"': {}}, 'f:taints': {}}}}, {'manager': 'flanneld', 'operation': 'Update', 'apiVersion': 'v1', 'time': '2024-01-22T09:36:35Z', 'fieldsType': 'FieldsV1', 'fieldsV1': {'f:metadata': {'f:annotations': {'f:flannel.alpha.coreos.com/backend-data': {}, 'f:flannel.alpha.coreos.com/backend-type': {}, 'f:flannel.alpha.coreos.com/kube-subnet-manager': {}, 'f:flannel.alpha.coreos.com/public-ip': {}}}, 'f:status': {'f:conditions': {'k:{"type":"NetworkUnavailable"}': {'.': {}, 'f:lastHeartbeatTime': {}, 'f:lastTransitionTime': {}, 'f:message': {}, 'f:reason': {}, 'f:status': {}, 'f:type': {}}}}}, 'subresource': 'status'}, {'manager': 'kubelet', 'operation': 'Update', 'apiVersion': 'v1', 'time': '2024-01-28T11:56:26Z', 'fieldsType': 'FieldsV1', 'fieldsV1': {'f:status': {'f:allocatable': {'f:cpu': {}, 'f:ephemeral-storage': {}, 'f:memory': {}}, 'f:capacity': {'f:cpu': {}, 'f:ephemeral-storage': {}, 'f:memory': {}}, 'f:conditions': {'k:{"type":"DiskPressure"}': {'f:lastHeartbeatTime': {}, 'f:lastTransitionTime': {}, 'f:message': {}, 'f:reason': {}, 'f:status': {}}, 'k:{"type":"MemoryPressure"}': {'f:lastHeartbeatTime': {}, 'f:lastTransitionTime': {}, 'f:message': {}, 'f:reason': {}, 'f:status': {}}, 'k:{"type":"PIDPressure"}': {'f:lastHeartbeatTime': {}, 'f:lastTransitionTime': {}, 'f:message': {}, 'f:reason': {}, 'f:status': {}}, 'k:{"type":"Ready"}': {'f:lastHeartbeatTime': {}, 'f:lastTransitionTime': {}, 'f:message': {}, 'f:reason': {}, 'f:status': {}}}, 'f:images': {}, 'f:nodeInfo': {'f:bootID': {}, 'f:containerRuntimeVersion': {}, 'f:kernelVersion': {}}}}, 'subresource': 'status'}]}, 'spec': {'podCIDR': '10.244.0.0/24', 'podCIDRs': ['10.244.0.0/24'], 'taints': [{'key': 'node-role.kubernetes.io/control-plane', 'effect': 'NoSchedule'}]}, 'status': {'capacity': {'cpu': '16', 'ephemeral-storage': '257705868Ki', 'hugepages-1Gi': '0', 'hugepages-2Mi': '0', 'memory': '32860752Ki', 'pods': '110'}, 'allocatable': {'cpu': '16', 'ephemeral-storage': '237501727556', 'hugepages-1Gi': '0', 'hugepages-2Mi': '0', 'memory': '32758352Ki', 'pods': '110'}, 'conditions': [{'type': 'NetworkUnavailable', 'status': 'False', 'lastHeartbeatTime': '2024-01-22T09:36:35Z', 'lastTransitionTime': '2024-01-22T09:36:35Z', 'reason': 'FlannelIsUp', 'message': 'Flannel is running on this node'}, {'type': 'MemoryPressure', 'status': 'False', 'lastHeartbeatTime': '2024-01-28T11:56:26Z', 'lastTransitionTime': '2024-01-13T19:18:30Z', 'reason': 'KubeletHasSufficientMemory', 'message': 'kubelet has sufficient memory available'}, {'type': 'DiskPressure', 'status': 'False', 'lastHeartbeatTime': '2024-01-28T11:56:26Z', 'lastTransitionTime': '2024-01-13T19:18:30Z', 'reason': 'KubeletHasNoDiskPressure', 'message': 'kubelet has no disk pressure'}, {'type': 'PIDPressure', 'status': 'False', 'lastHeartbeatTime': '2024-01-28T11:56:26Z', 'lastTransitionTime': '2024-01-13T19:18:30Z', 'reason': 'KubeletHasSufficientPID', 'message': 'kubelet has sufficient PID available'}, {'type': 'Ready', 'status': 'True', 'lastHeartbeatTime': '2024-01-28T11:56:26Z', 'lastTransitionTime': '2024-01-18T10:48:23Z', 'reason': 'KubeletReady', 'message': 'kubelet is posting ready status. AppArmor enabled'}], 'addresses': [{'type': 'InternalIP', 'address': '10.18.1.40'}, {'type': 'Hostname', 'address': 'clever-tarek-m1'}], 'daemonEndpoints': {'kubeletEndpoint': {'Port': 10250}}, 'nodeInfo': {'machineID': 'b057678a09844d90b1596c053836ab36', 'systemUUID': 'd8970942-74e6-c889-5469-3c85e39cb499', 'bootID': '88f4c0d9-d2c8-41da-b9f4-c54141dfdd09', 'kernelVersion': '5.15.0-91-generic', 'osImage': 'Ubuntu 22.04.3 LTS', 'containerRuntimeVersion': 'containerd://1.6.26', 'kubeletVersion': 'v1.26.3', 'kubeProxyVersion': 'v1.26.3', 'operatingSystem': 'linux', 'architecture': 'amd64'}, 'images': [{'names': ['registry.k8s.io/etcd@sha256:dd75ec974b0a2a6f6bb47001ba09207976e625db898d1b16735528c009cb171c', 'registry.k8s.io/etcd:3.5.6-0'], 'sizeBytes': 102542580}, {'names': ['registry.k8s.io/kube-apiserver@sha256:5f354253b464a8802ce01ffdd1bb8d0d17785daca4b6c8b84df06e9949e21449', 'registry.k8s.io/kube-apiserver:v1.26.9'], 'sizeBytes': 36208951}, {'names': ['registry.k8s.io/kube-controller-manager@sha256:27bf12cdfcccc8893cd66e4573044ec46fb05964a02f67b3e1a59f66db7ee101', 'registry.k8s.io/kube-controller-manager:v1.26.9'], 'sizeBytes': 32914237}, {'names': ['docker.io/flannel/flannel@sha256:34585231b69718efc4f926ebca734659f01221554f37a925d9a1190bb16e5b91', 'docker.io/flannel/flannel:v0.22.3'], 'sizeBytes': 27017673}, {'names': ['registry.k8s.io/kube-proxy@sha256:d8c8e3e8fe630c3f2d84a22722d4891343196483ac4cc02c1ba9345b1bfc8a3d', 'registry.k8s.io/kube-proxy:v1.26.9'], 'sizeBytes': 21769909}, {'names': ['registry.k8s.io/kube-scheduler@sha256:cf0312a97893a0ff42b2863cdb43243afc6397c77f2f2036a73708069e9af315', 'registry.k8s.io/kube-scheduler:v1.26.9'], 'sizeBytes': 17977113}, {'names': ['docker.io/flannel/flannel-cni-plugin@sha256:ca6779c6ad63b77af8a00151cefc08578241197b9a6fe144b0e55484bc52b852', 'docker.io/flannel/flannel-cni-plugin:v1.2.0'], 'sizeBytes': 3879095}, {'names': ['registry.k8s.io/pause@sha256:7031c1b283388d2c2e09b57badb803c05ebed362dc88d84b480cc47f72a21097', 'registry.k8s.io/pause:3.9'], 'sizeBytes': 321520}, {'names': ['registry.k8s.io/pause@sha256:3d380ca8864549e74af4b29c10f9cb0956236dfb01c40ca076fb6c37253234db', 'registry.k8s.io/pause:3.6'], 'sizeBytes': 301773}]}}
    # pprint(item)
    # k8snode = parseK8sNode(item)

    # session = graphdb.driver.session()
    # session.execute_write(grapher.add_k8snode, k8snode)
    # session.close()

    # graphdb.delete_all()