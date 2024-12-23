# Kafka Setup Guide

This guide provides step-by-step instructions for setting up Apache Kafka on a Windows machine. Kafka requires **Java 8 or higher**, **Zookeeper**, and **Maven** or **Gradle** for dependency management (optional).

## Step-by-Step Guide to Setting Up Kafka

### 1. Install Java (at least version 8)

Ensure that you have **Java 8** or a higher version installed on your machine. If not, download and install Java from the official [Oracle JDK page](https://www.oracle.com/java/technologies/javase-jdk11-downloads.html).

### 2. Download and Extract Kafka

- Go to the [Apache Kafka download page](https://kafka.apache.org/downloads) and download the binary for your operating system.
- Extract the ZIP file to a directory, such as `C:\kafka`.

### 3. Move to the Kafka Directory
After extracting the Kafka ZIP file, navigate to the directory bin\windows\

### 4.a. Start Zookeeper (old way)
Kafka uses **Zookeeper** for managing and coordinating distributed services. You can start Zookeeper using the included Zookeeper server that comes with Kafka.
run the following command to start Zookeeper:
```bash
zookeeper-server-start.bat ../../config/zookeeper.properties
```

### 4.b. Kraft
KRaft simplifies the architecture of Kafka by eliminating the need for Zookeeper, and it allows Kafka to directly manage its metadata and leader election through the Raft consensus protocol.
```bash
kafka-storage.bat random-uuid
set KAFKA_CLUSTER_ID={above_id}
kafka-storage.bat format -t %KAFKA_CLUSTER_ID% -c ../../config/kraft/server.properties
```

### 5.a. Start Kafka Server(old way-using zookeeper)
```bash
kafka-server-start.bat ./../config/server.properties
```
### 5.b. Start Kafka Server(new way-using Kraft)
```bash
kafka-server-start.bat ../../config/kraft/server.properties
```
### 6. Create a Kafka Topic
Kafka topics are where the data will be stored. To create a topic, open another Command Prompt window and run the following command:
```bash
kafka-topics.bat --create --topic my-topic --bootstrap-server localhost:9092 --partitions 1 
```
### 7. To check created Topic
```bash
kafka-topics.bat --list --bootstrap-server localhost:9092
```
### 8. To produce messages
```bash
kafka-console-producer.bat --topic test --bootstrap-server localhost:9092
```
### 9. To consume messages
```bash
kafka-console-consumer.bat --topic my-topic --from-beginning --bootstrap-server localhost:9092
```