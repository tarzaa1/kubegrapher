import os
import time
from itertools import count
from hedera import (
    Client,
    Hbar,
    PrivateKey,
    AccountCreateTransaction,
    TopicId,
    TopicMessageQuery,
    PyConsumer
    )

def get_client(fileName):
    current_dir = os.path.dirname(__file__)
    root_dir = os.path.abspath(os.path.join(current_dir, os.pardir, os.pardir, os.pardir))
    config = os.path.join(root_dir, fileName)
    client = Client.fromConfigFile(config)
    return client

def create_account(client):
    newKey = PrivateKey.generate()
    newPublicKey = newKey.getPublicKey()

    print(f"Private key: {newKey.toString()}")
    print(f"Public key: {newPublicKey.toString()}")

    resp = (AccountCreateTransaction()
            .setKey(newPublicKey)
            .setInitialBalance(Hbar(1000))
            .execute(client))
    receipt = resp.getReceipt(client)
    print("account = ",  receipt.accountId.toString())
    return receipt.accountId.toString(), newKey.toString()

def subscribe(client, topicID, processMessage):
    topicId = TopicId.fromString(topicID)
    print("Subscribed to TopicID: ",  topicId.toString())
    time.sleep(2)

    # please see https://github.com/wensheng/hcs-grpc-api-py-client for using grpc client
    query = TopicMessageQuery().setTopicId(topicId)
    query.subscribe(client, PyConsumer(processMessage))

    for i in count():
        time.sleep(2.5)