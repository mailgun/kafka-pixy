#!/bin/sh
# This script assumes that protoc is installed and available on the PATH.

# Make sure the script fails fast.
set -eux

SCRIPT_PATH=$(dirname "$0")                  # relative
REPO_ROOT=$(cd "${SCRIPT_PATH}/.." && pwd )  # absolutized and normalized
SRC_DIR=$REPO_ROOT
PYTHON_DST_DIR=$REPO_ROOT/gen/python
GOLANG_DST_DIR=$REPO_ROOT/gen/golang

# Build Golang stabs
go get google.golang.org/protobuf/cmd/protoc-gen-go
go install google.golang.org/protobuf/cmd/protoc-gen-go
go get google.golang.org/grpc/cmd/protoc-gen-go-grpc
go install google.golang.org/grpc/cmd/protoc-gen-go-grpc

mkdir -p "$GOLANG_DST_DIR"
go install google.golang.org/protobuf/cmd/protoc-gen-go
go install google.golang.org/grpc/cmd/protoc-gen-go-grpc
export PATH=$PATH:$(go env GOPATH)/bin
protoc -I=$SRC_DIR \
    --go_out=$GOLANG_DST_DIR \
    --go_opt=paths=source_relative \
    --go-grpc_out=$GOLANG_DST_DIR \
    --go-grpc_opt=paths=source_relative \
    $SRC_DIR/*.proto

# Build Python stabs
mkdir -p "$PYTHON_DST_DIR"
pip install grpcio
pip install grpcio-tools
python -m grpc.tools.protoc \
    -I=$SRC_DIR \
    --python_out=$PYTHON_DST_DIR \
    --grpc_python_out=$PYTHON_DST_DIR \
    $SRC_DIR/*.proto
touch $PYTHON_DST_DIR/__init__.py
