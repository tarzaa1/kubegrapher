from kubegrapher.utils.graph import Neo4j
from kubegrapher.utils.source import Hedera, Kafka
from kubegrapher.grapher import Grapher
from kubegrapher.model import K8sNode
import kubegrapher.parser as parser

import os, sys

from datetime import datetime
import csv
import json
import pytz

from kubegrapher.conf import (
    DATA_SOURCE,
    TIMEZONE,
    KAFKA_BROKER_URL,
    KAFKA_GROUP_ID,
    KAFKA_TOPIC,
    HEDERA_TOPIC,
    URI,
    AUTH,
)

timezone = pytz.timezone(TIMEZONE)

timestamps = []

def processMessage(grapher: Grapher, *args):

    received = datetime.now(tz=timezone)
    message = json.loads(args[2])
    id = message['id']
    send_timestamp = message['timestamp']
    consensus_timestamp = args[0]

    write_to_db(grapher, message)

    written_to_db = datetime.now(tz=timezone)
    timestamps.append((id, send_timestamp, received, written_to_db, consensus_timestamp))

def write_to_db(grapher: Grapher, message):
    action = message['action']
    kind = message['kind']
    body = message['body']
    if action == 'Add':
        if kind == 'Node':
            k8snode = parser.parse_k8s_node(body)
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
            grapher.link(node, image, 'STORES_IMAGE')
        elif kind == 'Done':
            with open('evaluation/results/records.csv', "w", newline='\n') as csvFile:
                writer = csv.writer(csvFile)
                writer.writerows(timestamps) 
            grapher.clear()                
        else:
            pass

    if action == 'Delete':
        if kind == 'Pod':
            grapher.deletePod(name=body)
        elif kind == 'Node':
            grapher.deleteNode(name=body)
    
    grapher.get_counts()
    grapher.get_subgraph()

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