# Financial Analytics Project

This project consists of multiple modules that collectively form a financial analytics system.

## Modules

### 1. finance-producer

The `finance-producer` module is responsible for fetching historical financial data from a real-time data source (e.g., Yahoo Finance API) and streaming the data to a Kafka topic. It serves as the producer component in the financial analytics data flow.

### 2. finance-consumer

The `finance-consumer` module acts as the consumer component in the financial analytics data flow. It subscribes to the Kafka topic where the financial data is being streamed and processes the data for further analysis or storage.

### 3. finance-bootstrap

The `finance-bootstrap` module serves as the initialization and orchestration point for the financial analytics application. It may include any setup, configuration, or initialization logic required before the actual data flow begins.

### 4. finance-utils

The `finance-utils` module contains utility classes and functions that are shared among multiple modules within the financial analytics project. It encapsulates common functionalities that can be reused across different components.

### 5. finance-datastore

The `finance-datastore` module is responsible for persisting the financial data received from the Kafka topic to a data store, such as HBase. It serves as the component that handles the storage and retrieval of financial data.

## Getting Started

To run the financial analytics application, follow the instructions in each module's respective README file.

### Prerequisites

- Java 8 or higher
- Apache Kafka (ensure it is running locally or update configuration accordingly)
- Other module-specific dependencies (check individual module README files for details)

## KAFKA

Follow these steps to start and stop Kafka:

### Start Kafka

1. Change directory to the Kafka installation directory:

   ```bash
   cd /path/to/kafka/directory
   
2. Start ZooKeeper:
    ```bash
    ./bin/zookeeper-server-start.sh config/zookeeper.properties

3. Start Kafka Broker:
    ```bash
    ./bin/kafka-server-start.sh config/server.properties