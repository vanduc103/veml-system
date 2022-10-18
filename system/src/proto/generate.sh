#!/bin/bash

../../thirdparty/protobuf-lib/bin/protoc --grpc_out ../../interface --cpp_out ../../interface -I . --plugin=protoc-gen-grpc=../../thirdparty/grpc/bin/grpc_cpp_plugin ./interface.proto
python -m grpc_tools.protoc -I . --python_out=../../interface --grpc_python_out=../../interface ./interface.proto
