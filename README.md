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
   
2. Start ZooKeeper if not running:
    ```bash
    ./bin/zookeeper-server-start.sh config/zookeeper.properties

3. Start Kafka Broker:
    ```bash
    ./bin/kafka-server-start.sh config/server.properties
   
4. Start KafKa Producer:
   ```run Bootstrap main()``` 
   which will trigger API call, send events from Producer to Kafka Broker. Kafka then receives events and persist data to HBase.
5. Start Generator:
   ```run FinanceGenerator main()``` 
   which will read data from HBase, create Data Frame by Spark, make desired query and write result to csv file in HDFS.
6. Start Hive CLI:
   Create and use database
   ```bash
   CREATE DATABASE financialanalytics;
   USE financialanalytics;
   ```
      Create external table and link to hdfs file location
   ```bash
   CREATE EXTERNAL TABLE stock (key String, date INT, open Double, high Double, low Double, close Double, volume INT, adjclose Double) COMMENT 'MSFT Stock table' ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde' WITH SERDEPROPERTIES('seperatorChar' =',', 'quoteChar' = '"', 'escapeChar'='\\', 'serde.null.format' = '', 'skip.header.line.count' = '1') STORED AS TEXTFILE LOCATION '/user/cloudera/FinanceDataSummary';
   ```
      Create the view to resolve the issue with data type as after loading data from csv file, all data types become String
   ```bash
   CREATE VIEW stockview AS SELECT st.key key, CAST(st.date as INT) date, CAST(st.open as Double) open, CAST(st.high as Double) high, CAST(st.low as Double) low, CAST(st.close as Double) close, CAST(st.volume as INT) volume, CAST(st.adjclose as Double) adjclose FROM stock st;
   ```
   
7. Start tableau. Using tableau Desktop version, install ODBC driver for cloudera hadoop
   - Connect to Cloudera Hadoop:
     - Connection: HiveServer2
     - Server: --input the ip address of vmw machine--
     - Port: 10000(default)
     - Authentication: Username
     - Username: cloudera
   - Select schema: input the database which created in Hive, "financialanalytics" for example
   - Select table: input the table name which created in Hive, "stockview" for example
   - Now it's ready for visualizations.