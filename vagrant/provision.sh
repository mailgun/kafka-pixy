#!/bin/sh

set -ex

apt-get update
yes | apt-get install default-jre

export INSTALL_ROOT=/opt
export KAFKA_HOSTNAME=192.168.100.67
export KAFKA_VERSION=1.1.1
export KAFKA_NODE_COUNT=5
export ZK_NODE_COUNT=3
export REPLICATION_FACTOR=2
export REPOSITORY_ROOT=/vagrant

sh /vagrant/vagrant/install_cluster.sh
sh /vagrant/vagrant/setup_services.sh
sh /vagrant/vagrant/create_topics.sh
