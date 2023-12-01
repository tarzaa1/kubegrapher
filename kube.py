from kubernetes import client, config
from grapher import Grapher
import json


def main():

    grapher = Grapher()
    config.load_kube_config()

    api_instance = client.CoreV1Api()

    node_list = api_instance.list_node()

    for node in node_list.items:
        # print(node)
        # Get node as json object and pass it to a method that adds it to the graph
        node_as_dict = node.to_dict()
        grapher.add_node(node_as_dict['metadata'], node_as_dict['spec'], node_as_dict['status'])
        break

        annoations = node.metadata.annotations
        creation_timestamp = node.metadata.creation_timestamp
        deletion_grace_period_seconds = node.metadata.deletion_grace_period_seconds
        deletion_timestamp = node.metadata.deletion_timestamp
        finalizers = node.metadata.finalizers
        generate_name = node.metadata.generate_name
        generation = node.metadata.generation
        labels = node.metadata.labels
        managed_fields = node.metadata.managed_fields
        name = node.metadata.name
        namespace = node.metadata.namespace
        owner_references = node.metadata.owner_references
        resource_version = node.metadata.resource_version
        self_link = node.metadata.self_link
        uid = node.metadata.uid

        config_source = node.spec.config_source
        external_id = node.spec.external_id
        pod_cid_rs = node.spec.pod_cid_rs
        pod_cidr = node.spec.pod_cidr
        provider_id = node.spec.provider_id
        unschedulable = node.spec.unschedulable

        addresses = node.status.addresses
        allocatable = node.status.allocatable
        capacity = node.status.capacity
        conditions = node.status.conditions
        status_config = node.status.config
        daemon_endpoints = node.status.daemon_endpoints
        images = node.status.images
        node_info = node.status.node_info
        phase = node.status.phase
        volumes_attached = node.status.volumes_attached
        volumes_in_use = node.status.volumes_in_use

        # print('name: ', name)
        # print('creation_timestamp:' , creation_timestamp)
        # print('addresses: ', addresses)
        # print('allocatable resources: ', allocatable)
        # print('capacity: ', capacity)
        # for condition in conditions:
        #     print(condition.message)

        # print(node_info)

    # print("Listing pods with their IPs:")
    # ret = api_instance.list_pod_for_all_namespaces(watch=False)
    # for i in ret.items:
    #     print("%s\t%s\t%s" % (i.status.pod_ip, i.metadata.namespace, i.metadata.name))

    # custom_api_instance = client.CustomObjectsApi()
    # info = custom_api_instance.list_cluster_custom_object('metrics.k8s.io', 'v1beta1', 'nodes', _preload_content=False)
    # print(json.loads(info.data)['items'][1]['usage'])

if __name__ == '__main__':
    main()