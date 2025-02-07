from kubegrapher.utils.graph import Neo4j
from kubegrapher.utils.source import Hedera, Kafka
from kubegrapher.grapher import Grapher
from kubegrapher.model import K8sNode, Cluster
from kubegrapher.relations import Relations
import kubegrapher.parser as parser

import threading

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

def processMessage(grapher: Grapher, timestamp: str, offset: int, msg: str, topic_name: str):
    message = json.loads(msg)
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
            service = parser.parse_service(body, topic_name)
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
        if kind == 'NodeMetrics':
            metrics_lst = parser.parse_metrics(topic_name, body)
            for metrics in metrics_lst:
                grapher.set_k8s_node(*metrics)
    
    grapher.get_counts()

def discoverTopics(client: any, grapher: Grapher, timestamp: str, offset: int, msg: str, topic_name: str):
    print(msg)
    message = json.loads(msg)
    hedera_topic = message['topic']
    cluster = message['cluster']
    if cluster != KAFKA_TOPIC[0]:
        hedera_thread = threading.Thread(target=client.subscribe, args=(hedera_topic, lambda *args: processMessage(grapher, *args)))
        hedera_thread.start()

def main():
    graphdb = Neo4j(URI, AUTH)
    grapher = Grapher(graphdb)
    if DATA_SOURCE == "kafka":
        conf = {'bootstrap.servers': KAFKA_BROKER_URL, 
            'group.id': KAFKA_GROUP_ID,
            'auto.offset.reset': 'smallest'}
        topics = KAFKA_TOPIC
        consumer = Kafka(conf)
        consumer.subscribe(topics, grapher, processMessage)
    elif DATA_SOURCE == "hedera":
        conf = "config.json"
        client = Hedera(conf)
        client.subscribe(HEDERA_TOPIC, lambda *args: processMessage(grapher, *args))
    else:
        kafka_conf = {'bootstrap.servers': KAFKA_BROKER_URL, 
            'group.id': KAFKA_GROUP_ID,
            'auto.offset.reset': 'smallest'}
        topics = KAFKA_TOPIC
        consumer = Kafka(kafka_conf)
        kafka_thread = threading.Thread(target=consumer.subscribe, args=(topics, grapher, processMessage))
        kafka_thread.start()

        hedera_conf = "config.json"
        client = Hedera(hedera_conf)
        hedera_thread = threading.Thread(target=client.subscribe, args=(HEDERA_TOPIC, lambda *args: discoverTopics(client, grapher, *args)))
        hedera_thread.start()

        kafka_thread.join()
        hedera_thread.join()

if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print('Interrupted')
        try:
            sys.exit(130)
        except SystemExit:
            os._exit(130)