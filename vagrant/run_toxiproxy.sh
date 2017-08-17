#!/bin/sh

set -ex

${INSTALL_ROOT}/toxiproxy -port 8474 -host 0.0.0.0 &
export TOXYPROXY_PID=$!

while ! nc -q 1 localhost 8474 </dev/null; do echo "Waiting"; sleep 1; done

for i in $(seq 1 ${ZK_NODE_COUNT}); do
    ZK_PORT=$((2180 + ${i}))
    ZK_REAL_PORT=$((21800 + ${i}))
    wget -O/dev/null -S --post-data="{\"name\":\"zk${i}\", \"upstream\":\"localhost:${ZK_REAL_PORT}\", \"listen\":\"0.0.0.0:${ZK_PORT}\"}" localhost:8474/proxies
done

for i in $(seq 1 ${KAFKA_NODE_COUNT}); do
    KAFKA_PORT=$((9090 + ${i}))
    KAFKA_REAL_PORT=$((29090 + ${i}))
    wget -O/dev/null -S --post-data="{\"name\":\"kafka${i}\", \"upstream\":\"localhost:${KAFKA_REAL_PORT}\", \"listen\":\"0.0.0.0:${KAFKA_PORT}\"}" localhost:8474/proxies
done

wait $TOXYPROXY_PID

