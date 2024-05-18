from confluent_kafka import Consumer, KafkaError, KafkaException
import sys
import json

def get_client(conf):
    return Consumer(conf)

running = True

def subscribe(consumer, topics, grapher, msg_process):
    try:
        consumer.subscribe(topics)

        while running:
            msg = consumer.poll(timeout=1.0)
            if msg is None: continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    # End of partition event
                    sys.stderr.write('%% %s [%d] reached end at offset %d\n' %
                                     (msg.topic(), msg.partition(), msg.offset()))
                elif msg.error():
                    raise KafkaException(msg.error())
            else:
                msg_process(grapher, None, None, msg.value())
    finally:
        # Close down consumer to commit final offsets.
        consumer.close()

def shutdown():
    running = False

def msg_process(grapher, *args):
    print(json.loads(args[2]))

if __name__ == "__main__":

    conf = {'bootstrap.servers': '10.18.1.35:29092,', 
            'group.id': 'foo',
            'auto.offset.reset': 'smallest'}
    
    topics = ["myTopic"]

    consumer = get_client(conf)

    grapher = ''

    subscribe(consumer, topics, grapher, msg_process)

