#!/bin/sh

set -ex

# wait for kafka server to load
sleep 5
docker exec -e JMX_PORT=5557 -i kafka-pixy_kafka_1 bin/kafka-topics.sh --create --zookeeper=zookeeper:2181 --topic test.1 --partitions 1 --replication-factor 1
docker exec -e JMX_PORT=5557 -i kafka-pixy_kafka_1 bin/kafka-topics.sh --create --zookeeper=zookeeper:2181 --topic test.4 --partitions 4 --replication-factor 1
docker exec -e JMX_PORT=5557 -i kafka-pixy_kafka_1 bin/kafka-topics.sh --create --zookeeper=zookeeper:2181 --topic test.64 --partitions 64 --replication-factor 1