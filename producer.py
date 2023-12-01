import json
from time import sleep, time
from datetime import datetime
from confluent_kafka import Producer
from kubernetes import client, config
import warnings
from threading import Thread
warnings.filterwarnings("ignore")


def get_configs():
    conf = dict()
    user_configs = dict()
    with open('app.config', 'r') as f:
        for l in f.read().splitlines():
            try: user_configs[l.split('=')[0]] = l.split('=')[1]
            except: pass
    with open('broker.config', 'r') as f:
        for l in f.read().splitlines():
            try: user_configs[l.split('=')[0]] = l.split('=')[1]
            except: pass

    conf['producer_interval'] = int(user_configs['producer_interval'])
    conf['metadata_interval'] = int(user_configs['metadata_interval']) if 'metadata_interval' in user_configs.keys() else 24 * 60 * 60
    conf['bootstrap.servers'] = user_configs['bootstrap.servers']
    return conf


def ack(err, msg):
    if err is not None:
        print("Failed to deliver message for {0}: {1}: {2}".format(msg.topic(),msg.value().decode(), err.str()))
    else:
        val = msg.value().decode()
        val = val[:50]+'...' if len(val)>50 else val
        print("Message produced for {}: {}".format(msg.topic(),val))


def produce_metadata(v1,producer,interval):
    topic = 'metadata'
    while True:
        try:
            message = v1.list_node(_preload_content=False).data
            producer.produce(topic, value=message, callback=ack)
            producer.flush(10)
            sleep(interval)
        except Exception as e:
            print(e)


def produce_stats(v1custom,producer,interval):
    while True:
        try:
            start = time()
            info = v1custom.list_cluster_custom_object('metrics.k8s.io', 'v1beta1', 'nodes', _preload_content=False)
            for node in json.loads(info.data)['items']:
                topic, message = node['metadata']['name'], node['usage']
                producer.produce(topic, value=json.dumps(message).encode('utf-8'), callback=ack)
            print('-------------------------------------------')
            print('Time:',datetime.now().strftime("%Y-%b-%d %H:%M:%S"))
            producer.flush(10)
            sleep(interval - (time()-start))
        except Exception as e:
            print(e)


if __name__ == "__main__":
    config.load_incluster_config()
    v1 = client.CoreV1Api()
    v1custom = client.CustomObjectsApi()
    conf = get_configs()
    p = Producer({'bootstrap.servers': conf['bootstrap.servers']})

    meta_thread = Thread(target=produce_metadata,args=(v1,p,conf['metadata_interval']))
    stats_thread = Thread(target=produce_stats,args=(v1custom,p,conf['producer_interval']))
    meta_thread.start()
    stats_thread.start()
    meta_thread.join()
    stats_thread.join()