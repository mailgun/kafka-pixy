#!/bin/sh

set -ex

for i in 1 2 3 4 5; do
    KAFKA_PORT=`expr $i + 9090`
    cd ${KAFKA_INSTALL_ROOT}/kafka-${KAFKA_PORT}
    bin/kafka-server-stop.sh config/server.properties

    while nc -q 1 localhost ${KAFKA_PORT} </dev/null; do
        echo "Waiting for Kafka at ${KAFKA_PORT} to stop..."
        sleep 1
    done
done

for i in 1 2 3 4 5; do
    KAFKA_PORT=`expr $i + 9090`
    cd ${KAFKA_INSTALL_ROOT}/kafka-${KAFKA_PORT}
    bin/zookeeper-server-stop.sh config/zookeeper.properties

    ZOOKEEPER_PORT=`expr $i + 2080`
    while nc -q 1 localhost ${ZOOKEEPER_PORT} </dev/null; do
        echo "Waiting for ZooKeeper at ${ZOOKEEPER_PORT} to stop..."
        sleep 1
    done
done
