#!/usr/bin/env sh
#

# Generate core protobuf files
protoc --go_out=./core --go_opt=module=github.com/opencost/opencost/core \
    --go-grpc_out=./core --go-grpc_opt=module=github.com/opencost/opencost/core \
    protos/**/*.proto

# Generate agent protobuf files
protoc --proto_path=pkg/agent/protos --go_out=pkg/agent/model/pb --go_opt=paths=source_relative pkg/agent/protos/*.proto
