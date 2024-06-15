from decouple import config

DATA_SOURCE = config('DATA_SOURCE', default='hedera')

TIMEZONE = config('TIMEZONE', default='Europe/London')

#Neo4j
URI = config('URI', default='bolt://localhost:7687')
auth_string = config('AUTH')
AUTH = tuple(auth_string.split('/'))

#Kafka
KAFKA_BROKER_URL = config('KAFKA_BROKER_URL', default='localhost:9092,')
KAFKA_TOPIC = config('KAFKA_TOPIC')
KAFKA_GROUP_ID = config('KAFKA_GROUP_ID')

#Hedera
HEDERA_NODE_URL = config('HEDERA_NODE_URL', default='10.18.1.35:50211')
NETWORK_ACCOUNT_ID = config('NETWORK_ACCOUNT_ID', default='0.0.3')
HEDERA_MIRROR_NODE_URL = config('HEDERA_MIRROR_NODE_URL', default='10.18.1.35:5600')
OPERATOR_ACCOUNT_ID = config('OPERATOR_ACCOUNT_ID', default='0.0.2')
OPERATOR_PRIVATE_KEY = config('OPERATOR_PRIVATE_KEY', default='302e020100300506032b65700422042091132178e72057a1d7528025956fe39b0b847f200ab59b2fdd367017f3087137')
HEDERA_TOPIC = config('HEDERA_TOPIC')