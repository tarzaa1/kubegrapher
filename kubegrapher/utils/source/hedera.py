from .base import Source
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

class Hedera(Source):
    def __init__(self, conf) -> None:
        self.client = self.get_client(conf)

    def get_client(self, fileName):
        current_dir = os.path.dirname(__file__)
        root_dir = os.path.abspath(os.path.join(current_dir, os.pardir, os.pardir, os.pardir))
        config = os.path.join(root_dir, fileName)
        client = Client.fromConfigFile(config)
        return client

    def subscribe(self, topicID, processMessage):
        topicId = TopicId.fromString(topicID)
        # please see https://github.com/wensheng/hcs-grpc-api-py-client for using grpc client
        query = TopicMessageQuery().setTopicId(topicId)
        query.subscribe(self.client, PyConsumer(processMessage))
        print("Subscribed to TopicID: ",  topicId.toString())

        for i in count():
            time.sleep(2.5)

    def create_account(self):
        newKey = PrivateKey.generate()
        newPublicKey = newKey.getPublicKey()

        print(f"Private key: {newKey.toString()}")
        print(f"Public key: {newPublicKey.toString()}")

        resp = (AccountCreateTransaction()
                .setKey(newPublicKey)
                .setInitialBalance(Hbar(1000))
                .execute(self.client))
        receipt = resp.getReceipt(self.client)
        print("Account ID = ",  receipt.accountId.toString())
        return receipt.accountId.toString(), newKey.toString()