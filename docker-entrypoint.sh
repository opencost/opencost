#!/bin/sh
set -e

cp -rv /opt/configs/models/* /models

echo "Starting OpenCost cost-model"

# check for cli parameter -d for "debug mode"
if [ "$1" = '-d' ]; then
    echo "Starting in debug mode"
    exec /go/bin/dlv exec --listen=:40000 --api-version=2 --headless=true --accept-multiclient --log --continue /app/main
else
    echo "Starting in normal mode"
    exec /go/bin/app
fi