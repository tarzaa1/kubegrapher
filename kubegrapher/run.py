from kubegrapher.utils.graph.neo4j import Neo4j
from kubegrapher.grapher import Grapher
from kubegrapher.model import K8sNode
import kubegrapher.parser as parser

from datetime import datetime
import csv
import json
import pytz

timezone = pytz.timezone("Europe/London")

source = "hedera"

if source == "kafka":
    from kubegrapher.utils.kafka.utils import get_client, subscribe
else: 
    from kubegrapher.utils.hedera.utils import get_client, subscribe

timestamps = []

def processMessage(grapher: Grapher, *args):
    # print("time: {} received: {}".format(args[0], args[2]))

    received = datetime.now(tz=timezone)
    
    message = json.loads(args[2])

    id = message['id']
    send_timestamp = message['timestamp']
    consensus_timestamp = args[0]

    write_to_db(grapher, message)

    written_to_db = datetime.now(tz=timezone)

    timestamps.append((id, send_timestamp, received, written_to_db, consensus_timestamp))

    # print(message)

def write_to_db(grapher: Grapher, message):
    action = message['action']
    kind = message['kind']
    body = message['body']
    if action == 'Add':
        if kind == 'Node':
            k8snode = parser.parseK8sNode(body)
            grapher.merge(k8snode)
        elif kind == 'ConfigMap':
            configmap = parser.parseConfigMap(body)
            grapher.merge(configmap)
        elif kind == 'Pod':
            pod = parser.parsePod(body)
            grapher.merge(pod)
        elif kind == 'Service':
            service = parser.parseService(body)
            grapher.merge(service)
        elif kind == 'Deployment':
            deployment = parser.parseDeployment(body)
            grapher.merge(deployment)
        elif kind == 'ReplicaSet':
            replicaset = parser.parseReplicaSet(body)
            grapher.merge(replicaset)
        elif kind == 'Image':
            image = parser.parseImage(body['data'])
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
    
    # grapher.get_counts()
    # grapher.get_subgraph()



if __name__ == '__main__':

    URI = "bolt://localhost:7687"
    AUTH = ("neo4j", "password")
    graphdb = Neo4j(URI, AUTH)
    grapher = Grapher(graphdb)

    if source == "kafka":

        conf = {'bootstrap.servers': '10.18.1.35:29092,', 
            'group.id': 'foo',
            'auto.offset.reset': 'smallest'}

        topics = ["myTopic"]

        consumer = get_client(conf)

        subscribe(consumer, topics, grapher, processMessage)

    else:

        client = get_client("config.json")

        subscribe(client, "0.0.1003", lambda *args: processMessage(grapher, *args))