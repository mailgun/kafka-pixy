#!/bin/sh

set -ex

cd ${INSTALL_ROOT}/kafka
bin/kafka-topics.sh --create --partitions 1 --replication-factor ${REPLICATION_FACTOR} --topic test.1 --zookeeper localhost:2181
bin/kafka-topics.sh --create --partitions 4 --replication-factor ${REPLICATION_FACTOR} --topic test.4 --zookeeper localhost:2181
bin/kafka-topics.sh --create --partitions 64 --replication-factor ${REPLICATION_FACTOR} --topic test.64  --zookeeper localhost:2181
