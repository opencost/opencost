#!/bin/bash

# Quick test script for DM2 POC with improved error handling

set -e

echo "=== Quick DM2 POC Test ==="
echo ""

# Get pod name
POD=$(kubectl get pods -l app.kubernetes.io/name=opencost -o jsonpath='{.items[0].metadata.name}' 2>/dev/null)

if [ -z "$POD" ]; then
    echo "ERROR: OpenCost pod not found. Is Tilt running with 'tilt up -f Tiltfile.dm2'?"
    exit 1
fi

echo "Found OpenCost pod: $POD"
echo ""

# Check environment variables
echo "Checking DM2 environment variables..."
kubectl exec $POD -- env | grep DM2 || echo "WARNING: No DM2 env vars found"
echo ""

# Check logs for DM2 initialization
echo "Checking logs for DM2 initialization..."
kubectl logs $POD --tail=100 | grep -i dm2 || echo "No DM2 messages in recent logs"
echo ""

# Check for output files
echo "Checking for DM2 output files in /tmp/dm2-output/..."
kubectl exec $POD -- ls -la /tmp/dm2-output/ 2>/dev/null || echo "Directory not found or empty"
echo ""

# Try to get a file if it exists
FILES_COUNT=$(kubectl exec $POD -- ls /tmp/dm2-output/ 2>/dev/null | grep -c ".pb.gz" || echo "0")

if [ "$FILES_COUNT" != "0" ]; then
    echo "Found $FILES_COUNT DM2 files!"
    
    # Get the latest file
    FILE=$(kubectl exec $POD -- ls -t /tmp/dm2-output/ | head -n1)
    echo "Latest file: $FILE"
    
    # Copy and decode
    echo "Copying file to local system..."
    kubectl cp $POD:/tmp/dm2-output/$FILE /tmp/$FILE
    
    # Build decoder if needed
    if [ ! -f "./dm2decode" ]; then
        echo "Building decoder..."
        go build -tags dm2emitter -o dm2decode ./cmd/dm2decode
    fi
    
    echo ""
    echo "Decoding file:"
    ./dm2decode /tmp/$FILE -v
else
    echo "No DM2 files found yet."
    echo ""
    echo "Possible reasons:"
    echo "1. DM2 emitter just started (wait 30 seconds)"
    echo "2. DM2 not properly initialized (check integration)"
    echo "3. Permissions issue writing to /tmp/dm2-output"
    echo ""
    echo "To monitor file generation, run:"
    echo "  kubectl exec $POD -- sh -c 'while true; do ls -la /tmp/dm2-output/ 2>/dev/null; sleep 10; done'"
fi