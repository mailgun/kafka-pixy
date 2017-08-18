#!/bin/sh

set -ex

# Launch and wait for toxiproxy if it is configured
if [ -n "${TOXIPROXY_VERSION}" ]; then
    ${REPOSITORY_ROOT}/vagrant/run_toxiproxy.sh &
    while ! nc -q 1 localhost 2181 </dev/null; do echo "Waiting"; sleep 1; done
    while ! nc -q 1 localhost 9092 </dev/null; do echo "Waiting"; sleep 1; done
    ZK_BASE_PORT=21800
    KAFKA_BASE_PORT=29090
else
    ZK_BASE_PORT=2180
    KAFKA_BASE_PORT=9090
fi

# Launch and wait for Zookeeper
for i in $(seq 1 ${ZK_NODE_COUNT}); do
    ZK_PORT=$((2180 + ${i}))
    ${INSTALL_ROOT}/kafka/bin/zookeeper-server-start.sh -daemon ${INSTALL_ROOT}/zookeeper-${ZK_PORT}/config/zookeeper.properties
done
for i in $(seq 1 ${ZK_NODE_COUNT}); do
    ZK_REAL_PORT=$((${ZK_BASE_PORT} + ${i}))
    while ! nc -q 1 localhost ${ZK_REAL_PORT} </dev/null; do
        echo "Wait for ZooKeeper at ${ZK_REAL_PORT} to start ...";
        sleep 1;
    done
done

# Launch and wait for Kafka
for i in $(seq 1 ${KAFKA_NODE_COUNT}); do
    KAFKA_PORT=$((9090 + ${i}))
    ${INSTALL_ROOT}/kafka/bin/kafka-server-start.sh -daemon ${INSTALL_ROOT}/kafka-${KAFKA_PORT}/config/server.properties
done
for i in $(seq 1 ${KAFKA_NODE_COUNT}); do
    KAFKA_REAL_PORT=$((${KAFKA_BASE_PORT} + ${i}))
    while ! nc -q 1 localhost ${KAFKA_REAL_PORT} </dev/null; do
        echo "Wait for Kafka at ${KAFKA_REAL_PORT} to start ...";
        sleep 1;
    done
done
