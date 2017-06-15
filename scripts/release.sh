#!/bin/sh

# Make sure the script fails fast.
set -e
set -u

PROJECT_ROOT=${GOPATH}/src/github.com/mailgun/kafka-pixy
BUILD_DIR="${GOPATH}/bin"
RELEASE_DIR=${PROJECT_ROOT}/releases

make all

# Figure out the release parameters.
VER=$(git describe --tag)
OS=$(uname -s | awk '{print tolower($0)}')
ARCH=$(uname -m)
if [ ${ARCH} == "x86_64" ]; then
    ARCH="amd64"
fi
TARGET="kafka-pixy-${VER}-${OS}-${ARCH}"

# Create a release directory.
TARGET_DIR=${RELEASE_DIR}/${TARGET}
mkdir -p ${TARGET_DIR}

# Assemble release artifacts.
for bin in kafka-pixy testconsumer testproducer; do
    cp ${BUILD_DIR}/${bin} ${TARGET_DIR}
done
cp ${PROJECT_ROOT}/README.md ${TARGET_DIR}
cp ${PROJECT_ROOT}/CHANGELOG.md ${TARGET_DIR}
cp ${PROJECT_ROOT}/LICENSE ${TARGET_DIR}
cp ${PROJECT_ROOT}/default.yaml ${TARGET_DIR}
cp ${PROJECT_ROOT}/kafkapixy.proto ${TARGET_DIR}
mkdir ${TARGET_DIR}/grpc_stubs
cp -r ${PROJECT_ROOT}/gen/* ${TARGET_DIR}/grpc_stubs


# Make an archived distribution.
cd ${RELEASE_DIR}
if [ ${OS} == "linux" ]; then
    ARTIFACT="${TARGET}.tar.gz"
    tar cfz ${TARGET}.tar.gz ${TARGET}
    echo "Wrote ${TARGET}.tar.gz"
else
    ARTIFACT="${TARGET}.zip"
    zip -qr ${TARGET}.zip ${TARGET}
    echo "Wrote ${TARGET}.zip"
fi
