import os
import json
from kubegrapher.utils.hedera.utils import get_client, create_account

if __name__ == "__main__":

    client = get_client("default_config.json")
    accountId, privateKey = create_account(client)

    config = {
            "network": {
                "0.0.3": "10.18.1.35:50211"
            },
            "operator": {
                "accountId": accountId,
                "privateKey": privateKey
            },
            "mirrorNetwork": ["10.18.1.35:5600"]
            }
    
    with open('config.json', 'w') as fp:
        json.dump(config, fp, indent=4)