from kubegrapher.utils.graph import Neo4j
from kubegrapher.utils.source import Hedera, Kafka
from kubegrapher.grapher import Grapher
from kubegrapher.model import K8sNode, Cluster
from kubegrapher.relations import Relations
import kubegrapher.parser as parser

import os, sys
import json

from kubegrapher.conf import (
    DATA_SOURCE,
    KAFKA_BROKER_URL,
    KAFKA_GROUP_ID,
    KAFKA_TOPIC,
    HEDERA_TOPIC,
    URI,
    AUTH,
)

def processMessage(grapher: Grapher, *args):
    message = json.loads(args[2])
    if len(args) > 3:
        topic_name = args[3]
    else:
        topic_name = "0.0.1003"

    action = message['action']
    kind = message['kind']
    body = message['body']
    if action == 'Add':
        if kind == 'Cluster':
            cluster = Cluster(topic_name)
            grapher.merge(cluster)
        elif kind == 'Node':
            k8snode = parser.parse_k8s_node(body, topic_name)
            grapher.merge(k8snode)
        elif kind == 'ConfigMap':
            configmap = parser.parse_configmap(body)
            grapher.merge(configmap)
        elif kind == 'Pod':
            pod = parser.parse_pod(body)
            grapher.merge(pod)
        elif kind == 'Service':
            service = parser.parse_service(body)
            grapher.merge(service)
        elif kind == 'Deployment':
            deployment = parser.parse_deployment(body)
            grapher.merge(deployment)
        elif kind == 'ReplicaSet':
            replicaset = parser.parse_replicaset(body)
            grapher.merge(replicaset)
        elif kind == 'Image':
            image = parser.parse_image(body['data'])
            node = K8sNode(body['nodeUID'])
            grapher.merge(image)
            grapher.link(node, image, Relations.STORES_IMAGE)
        elif kind == 'Ingress':
            ingress = parser.parse_ingress(body, topic_name)
            grapher.merge(ingress)
            pass

        elif kind == 'Done':
            grapher.clear()                
        else:
            pass

    if action == 'Delete':
        if kind == 'Pod':
            grapher.delete_pod(name=body)
        elif kind == 'Node':
            grapher.delete_k8s_node(name=body)

    if action == 'Update':
        if kind == 'Metrics':
            metrics_lst = parser.parse_metrics(topic_name, body)
            for metrics in metrics_lst:
                grapher.set_k8s_node(*metrics)
    
    grapher.get_counts()
    # grapher.get_subgraph()

def main():
    graphdb = Neo4j(URI, AUTH)
    grapher = Grapher(graphdb)
    if DATA_SOURCE == "kafka":
        conf = {'bootstrap.servers': KAFKA_BROKER_URL, 
            'group.id': KAFKA_GROUP_ID,
            'auto.offset.reset': 'smallest'}
        topics = [KAFKA_TOPIC]
        consumer = Kafka(conf)
        consumer.subscribe(topics, grapher, processMessage)
    else:
        conf = "config.json"
        client = Hedera(conf)
        client.subscribe(HEDERA_TOPIC, lambda *args: processMessage(grapher, *args))

if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print('Interrupted')
        try:
            sys.exit(130)
        except SystemExit:
            os._exit(130)