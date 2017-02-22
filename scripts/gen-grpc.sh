#!/bin/sh
# This script is supposed to run from the repository root.
echo $(pwd)

# Make sure the script fails fast.
set -e
set -u
set -x

repo_root=.
proto_file=$repo_root/grpc.proto
include_dir=$repo_root
out_dir=$repo_root/gen

# Generate Python code
python_out_dir=$out_dir/python
python -m grpc.tools.protoc -I=$include_dir --python_out=$python_out_dir --grpc_python_out=$python_out_dir $proto_file

# Generate Golang code
golang_out_dir=$out_dir/golang
protoc $proto_file --go_out=plugins=grpc:$golang_out_dir
