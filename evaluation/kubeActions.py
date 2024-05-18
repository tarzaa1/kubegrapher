from kubernetes import client, config
from kubernetes.client.rest import ApiException
import time

config.load_kube_config()

v1 = client.CoreV1Api()

def add_pod(pod_name, zone, namespace='default'):
    pod_manifest = {
        'apiVersion': 'v1',
        'kind': 'Pod',
        'metadata': {'name': pod_name, 'labels': {'zone': zone}},
        'spec': {
            'containers': [
                {
                    'name': 'busybox',
                    'image': 'busybox',
                    'command': ['sleep', '3600']
                }
            ],
            'affinity': {
                'nodeAffinity': {
                    'requiredDuringSchedulingIgnoredDuringExecution': {
                        'nodeSelectorTerms': [
                            {
                                'matchExpressions': [
                                    {'key': 'zone', 'operator': 'In',
                                        'values': [zone]}
                                ]
                            }
                        ]
                    }
                }
            }
        }
    }

    try:
        v1.create_namespaced_pod(namespace=namespace, body=pod_manifest)
        print(f"Pod {pod_name} creation initiated in zone {zone}")
    except ApiException as e:
        print(e)

    while True:
        try:
            pod_status = v1.read_namespaced_pod(name=pod_name, namespace=namespace).status
            if pod_status.phase == 'Running':
                print(f"Pod {pod_name} is now running in zone {zone}")
                break
            elif pod_status.phase in ['Failed', 'Unknown']:
                print(f"Pod {pod_name} is in {pod_status.phase} state with reason: {pod_status.reason}")
                break
            else:
                print(f"Waiting for pod {pod_name} to be running... Current state: {pod_status.phase}")
                time.sleep(1)
        except ApiException as e:
            print(e)
            break

def delete_pod(pod_name, namespace='default'):
    try:
        v1.delete_namespaced_pod(name=pod_name, namespace=namespace,
                                 body=client.V1DeleteOptions(grace_period_seconds=0))
        print(f"Pod {pod_name} deletion initiated")
    except ApiException as e:
        print(e)

    while True:
        try:
            v1.read_namespaced_pod(name=pod_name, namespace=namespace)
            print(f"Waiting for pod {pod_name} to terminate...")
            time.sleep(1)
        except ApiException as e:
            if e.status == 404:
                print(f"Pod {pod_name} has been successfully terminated")
                break
            else:
                print(e)
                break

def move_pod(pod_name, new_zone, namespace='default'):
    delete_pod(pod_name, namespace)
    add_pod(pod_name, new_zone, namespace)
    print(f"Pod {pod_name} moved to zone {new_zone}")

def force_delete_all_pods(namespace='default'):
    try:
        v1.delete_collection_namespaced_pod(namespace=namespace, body=client.V1DeleteOptions(grace_period_seconds=0))
        print(f"All pods in namespace {namespace} deletion initiated")
        
        while True:
            pods = v1.list_namespaced_pod(namespace)
            if not pods.items:
                print(f"All pods in namespace {namespace} have been successfully terminated")
                break
            else:
                print("Waiting for all pods to terminate...")
                time.sleep(1)
    except ApiException as e:
        print(e)
