#!/bin/sh

set -ex

if [ -n "${TOXIPROXY_VERSION}" ]; then
    stop toxiproxy || true
    cp ${REPOSITORY_ROOT}/vagrant/toxiproxy.conf /etc/init/toxiproxy.conf
    cp ${REPOSITORY_ROOT}/vagrant/run_toxiproxy.sh ${INSTALL_ROOT}/
    start toxiproxy

    ZK_BASE_PORT=21800
    KAFKA_BASE_PORT=29090
else
    ZK_BASE_PORT=2180
    KAFKA_BASE_PORT=9090
fi


for i in $(seq 1 ${ZK_NODE_COUNT}); do
    ZK_PORT=$((2180 + ${i}))
    stop zookeeper-${ZK_PORT} || true
    cp ${REPOSITORY_ROOT}/vagrant/zookeeper.conf /etc/init/zookeeper-${ZK_PORT}.conf
    sed -i s/ZK_PORT/${ZK_PORT}/g /etc/init/zookeeper-${ZK_PORT}.conf
    start zookeeper-${ZK_PORT}
done
for i in $(seq 1 ${ZK_NODE_COUNT}); do
    ZK_REAL_PORT=$((${ZK_BASE_PORT} + ${i}))
    while ! nc -q 1 localhost ${ZK_REAL_PORT} </dev/null; do
        echo "Wait for ZooKeeper at ${ZK_REAL_PORT} to start ...";
        sleep 1;
    done
done

for i in $(seq 1 ${KAFKA_NODE_COUNT}); do
    KAFKA_PORT=$((9090 + ${i}))
    ZK_PORT=$((2180 + ${i}))
    stop kafka-${KAFKA_PORT} || true
    cp ${REPOSITORY_ROOT}/vagrant/kafka.conf /etc/init/kafka-${KAFKA_PORT}.conf
    sed -i s/KAFKA_PORT/${KAFKA_PORT}/g /etc/init/kafka-${KAFKA_PORT}.conf
    sed -i s/ZK_PORT/${ZK_PORT}/g /etc/init/kafka-${KAFKA_PORT}.conf
    start kafka-${KAFKA_PORT}
done
for i in $(seq 1 ${KAFKA_NODE_COUNT}); do
    KAFKA_REAL_PORT=$((${KAFKA_BASE_PORT} + ${i}))
    while ! nc -q 1 localhost ${KAFKA_REAL_PORT} </dev/null; do
        echo "Wait for Kafka at ${KAFKA_REAL_PORT} to start ...";
        sleep 1;
    done
done
