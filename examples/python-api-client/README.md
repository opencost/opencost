# OpenCost Python API Client Example

A minimal, self-contained Python example for querying the [OpenCost](https://opencost.io) API.

## What It Does

This example demonstrates how to:

- Connect to an OpenCost instance via its REST API
- Query **namespace-level** cost allocation for the last 24 hours
- Query **deployment-level** cost allocation and display the top spenders

## Prerequisites

- Python 3.8+
- A running OpenCost instance (default: `http://localhost:9003`)

## Installation

```bash
cd examples/python-api-client
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## Usage

By default, the script connects to `http://localhost:9003`. Override this by setting the `OPENCOST_URL` environment variable:

```bash
# Default endpoint
python opencost_client.py

# Custom endpoint
export OPENCOST_URL="http://opencost.your-cluster.local:9003"
python opencost_client.py
```

## Sample Output

```
Connecting to OpenCost at http://localhost:9003 ...
------------------------------------------------------------

📊 Namespace Allocation (Last 24h)
Namespace                                 CPU         RAM        Total
----------------------------------------------------------------------
__idle__                              $0.0234     $0.0156     $0.0390
kube-system                           $0.0123     $0.0089     $0.0212
default                               $0.0056     $0.0034     $0.0090

📦 Top Deployments by Cost (Last 24h)
Deployment                                   Total Cost
-------------------------------------------------------
kube-system/coredns                        $0.0034
default/nginx-deployment                   $0.0021

✅ Done!
```

## Next Steps

- See the full [OpenCost API documentation](https://www.opencost.io/docs/api) for additional endpoints such as `/assets`, `/cloudCost`, and `/aggregator/allocation`.
- Adapt `OpenCostClient` to fit your own reporting or alerting workflows.
