#!/bin/sh

set -ex

mkdir -p ${INSTALL_ROOT}
if [ ! -f ${INSTALL_ROOT}/kafka-${KAFKA_VERSION}.tgz ]; then
    wget --quiet http://apache.mirror.gtcomm.net/kafka/${KAFKA_VERSION}/kafka_2.10-${KAFKA_VERSION}.tgz -O ${INSTALL_ROOT}/kafka-${KAFKA_VERSION}.tgz
fi

if [ -n "${TOXIPROXY_VERSION}" ]; then
    if [ ! -f ${INSTALL_ROOT}/toxiproxy-${TOXIPROXY_VERSION} ]; then
        wget --quiet https://github.com/Shopify/toxiproxy/releases/download/v${TOXIPROXY_VERSION}/toxiproxy-linux-amd64 -O ${INSTALL_ROOT}/toxiproxy-${TOXIPROXY_VERSION}
        chmod +x ${INSTALL_ROOT}/toxiproxy-${TOXIPROXY_VERSION}
    fi
    rm -f ${INSTALL_ROOT}/toxiproxy
    ln -s ${INSTALL_ROOT}/toxiproxy-${TOXIPROXY_VERSION} ${INSTALL_ROOT}/toxiproxy

    ZK_BASE_PORT=21800
    KAFKA_BASE_PORT=29090
else
    ZK_BASE_PORT=2180
    KAFKA_BASE_PORT=9090
fi

# unpack kafka
mkdir -p ${INSTALL_ROOT}/kafka
tar xzf ${INSTALL_ROOT}/kafka-${KAFKA_VERSION}.tgz -C ${INSTALL_ROOT}/kafka --strip-components 1

for i in $(seq 1 ${KAFKA_NODE_COUNT}); do
    KAFKA_PORT=$((9090 + ${i}))
    KAFKA_PORT_REAL=$((${KAFKA_BASE_PORT} + ${i}))

    # broker configuration
    mkdir -p ${INSTALL_ROOT}/kafka-${KAFKA_PORT}/config
    cp ${REPOSITORY_ROOT}/vagrant/server.properties ${INSTALL_ROOT}/kafka-${KAFKA_PORT}/config/
    KAFKA_CFG_FILE=${INSTALL_ROOT}/kafka-${KAFKA_PORT}/config/server.properties

    sed -i s/KAFKAID/${KAFKA_PORT}/g ${KAFKA_CFG_FILE}
    sed -i s/KAFKAPORT/${KAFKA_PORT_REAL}/g ${KAFKA_CFG_FILE}
    sed -i s/KAFKA_HOSTNAME/${KAFKA_HOSTNAME}/g ${KAFKA_CFG_FILE}

    KAFKA_DATADIR="${INSTALL_ROOT}/kafka-${KAFKA_PORT}/data"
    mkdir -p ${KAFKA_DATADIR}
    sed -i s#KAFKA_DATADIR#${KAFKA_DATADIR}#g ${KAFKA_CFG_FILE}
done

for i in $(seq 1 ${ZK_NODE_COUNT}); do
    ZK_PORT=$((2180 + ${i}))
    ZK_PORT_REAL=$((${ZK_BASE_PORT} + ${i}))

    # zookeeper configuration
    mkdir -p ${INSTALL_ROOT}/zookeeper-${ZK_PORT}/config
    cp ${REPOSITORY_ROOT}/vagrant/zookeeper.properties ${INSTALL_ROOT}/zookeeper-${ZK_PORT}/config/
    ZK_CFG_FILE=${INSTALL_ROOT}/zookeeper-${ZK_PORT}/config/zookeeper.properties

    sed -i s/ZK_PORT/${ZK_PORT_REAL}/g ${ZK_CFG_FILE}
    # add a list of all zookeeper nodes to the config
    for j in $(seq 1 ${ZK_NODE_COUNT}); do
        echo "server.${j}=localhost:$((2280 + ${j})):$((2380 + ${j}))" >> ${ZK_CFG_FILE}
    done

    ZK_DATADIR="${INSTALL_ROOT}/zookeeper-${ZK_PORT}/data"
    mkdir -p ${ZK_DATADIR}
    sed -i s#ZK_DATADIR#${ZK_DATADIR}#g ${ZK_CFG_FILE}

    echo $i > ${ZK_DATADIR}/myid
done
