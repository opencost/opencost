#!/usr/bin/env sh
#

# Generate core protobuf files
protoc --proto_path=protos --go_out=./core --go_opt=module=github.com/opencost/opencost/core \
    --go-grpc_out=./core --go-grpc_opt=module=github.com/opencost/opencost/core \
    protos/**/*.proto