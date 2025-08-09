#!/usr/bin/env bash
set -euo pipefail
repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"
protoc --go_out=. --go_opt=paths=source_relative protos/dm2/opencost_dm2.proto
echo "Generated DM2 Go types."
