from kubegrapher.utils.source import Hedera
import json

from kubegrapher.conf import (
    HEDERA_NODE_URL,
    NETWORK_ACCOUNT_ID,
    HEDERA_MIRROR_NODE_URL,
    OPERATOR_ACCOUNT_ID,
    OPERATOR_PRIVATE_KEY
)

def write_config(filename, config):
    with open(filename, 'w') as fp:
        json.dump(config, fp, indent=4)


if __name__ == "__main__":

    config_file = "config.json"

    config_dict = {
        "network": {
            NETWORK_ACCOUNT_ID: HEDERA_NODE_URL
        },
        "operator": {
            "accountId": OPERATOR_ACCOUNT_ID,
            "privateKey": OPERATOR_PRIVATE_KEY
        },
        "mirrorNetwork": [HEDERA_MIRROR_NODE_URL]
    }

    write_config(config_file, config_dict)

    client = Hedera(config_file)
    accountId, privateKey = client.create_account()

    config_dict["operator"]["accountId"] = accountId
    config_dict["operator"]["privateKey"] = privateKey

    write_config(config_file, config_dict)
