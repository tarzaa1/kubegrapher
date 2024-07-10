# kubegrapher

`kubegrapher` is a Python project that listens to a Kafka topic or a Hedera DLT topic, processes events reflecting the state update of a Kubernetes cluster, and writes them to a property graph in a Neo4j database instance.

## Table of Contents
- [Prerequisites](#prerequisites)
- [Installation](#installation)
- [Setup](#setup)
- [Running the Neo4j Instance](#running-the-neo4j-instance)
- [Usage](#usage)

## Prerequisites
- Python 3.7+
- Docker and Docker Compose
- Kafka or Hedera instance with a topic already created

## Installation

1. Clone the repository.

2. Install the required Python packages:
    ```sh
    cd kubegrapher
    pip install -r requirements.txt
    ```
3. Install Java and make sure $JAVA_HOME is set

## Setup

1. Create a `.env` file from the provided `example.env`:
    ```sh
    cp example.env .env
    ```

2. Edit the `.env` file to configure the necessary environment variables for Kafka or Hedera, and the Neo4j connection details.

## Running the Neo4j Instance

A `docker-compose.yml` file is provided to run a Neo4j database instance. To start the Neo4j instance, run:
```sh
docker compose up -d
```

## Usage

1. Ensure your Kafka or Hedera instance is running and the topic specified in the ".env" file is created.

2. If using Hedera, create an account:
```sh
python kubegrapher/setup_hedera.py
```

3. Run the application:
```sh
python kubegrapher/run.py
```


