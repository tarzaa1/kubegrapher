from .base import Source
from confluent_kafka import Consumer, KafkaError, KafkaException
import sys

class Kafka(Source):
    def __init__(self, conf):
        self.client = self.get_client(conf)

    def get_client(self, conf):
        return Consumer(conf)

    def subscribe(self, topics, grapher, msg_process):
        try:
            self.client.subscribe(topics)
            print(f"Subscribed to topic {topics}")

            while True:
                msg = self.client.poll(timeout=1.0)
                if msg is None: continue

                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        # End of partition event
                        sys.stderr.write('%% %s [%d] reached end at offset %d\n' %
                                        (msg.topic(), msg.partition(), msg.offset()))
                    elif msg.error():
                        raise KafkaException(msg.error())
                else:
                    msg_process(grapher, None, None, msg.value(), msg.topic())
        finally:
            # Close down consumer to commit final offsets.
            self.client.close()

