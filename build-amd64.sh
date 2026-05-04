#!/bin/bash
set -e

echo "Building OpenCost for AMD64 (OpenShift)..."

# Set environment variables for cross-compilation
export GOOS=linux
export GOARCH=amd64
export CGO_ENABLED=0

# Build the binary
go build \
  -ldflags "-extldflags \"-static\" -s -w" \
  -o costmodel \
  ./cmd/costmodel

# Verify the binary
echo ""
echo "Build complete! Verifying architecture..."
file costmodel

# Check if it's AMD64
if file costmodel | grep -q "x86-64"; then
    echo "✓ Binary is AMD64 (x86-64) - correct for OpenShift"
else
    echo "✗ ERROR: Binary is not AMD64!"
    file costmodel
    exit 1
fi

echo ""
echo "Binary ready for Docker build"

# Made with Bob
