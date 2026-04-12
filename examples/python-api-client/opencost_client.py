#!/usr/bin/env python3
"""
OpenCost Python API Client Example

A minimal example demonstrating how to query the OpenCost API
for Kubernetes cost allocation data using Python.

Requirements:
    pip install requests

Usage:
    export OPENCOST_URL="http://localhost:9003"
    python opencost_client.py
"""

import os
import sys
from datetime import datetime, timedelta
from urllib.parse import urljoin

import requests


class OpenCostClient:
    """Simple Python client for the OpenCost API."""

    def __init__(self, base_url: str = "http://localhost:9003"):
        self.base_url = base_url.rstrip("/")
        self.session = requests.Session()

    def _get(self, path: str, params: dict = None) -> dict:
        """Make a GET request to the OpenCost API."""
        url = urljoin(self.base_url + "/", path)
        response = self.session.get(url, params=params, timeout=30)
        response.raise_for_status()
        return response.json()

    def get_allocation(
        self,
        window: str = "1d",
        aggregate: str = "namespace",
        accumulate: bool = False,
    ) -> dict:
        """
        Fetch cost allocation data.

        Args:
            window: Time window (e.g. '1d', '7d', 'today', 'yesterday')
            aggregate: Aggregation dimension
                (e.g. 'namespace', 'deployment', 'pod', 'service')
            accumulate: If True, accumulate costs over the window

        Returns:
            Parsed JSON response from the OpenCost API
        """
        params = {
            "window": window,
            "aggregate": aggregate,
            "accumulate": str(accumulate).lower(),
        }
        return self._get("allocation/compute", params=params)

    def get_assets(self, window: str = "1d") -> dict:
        """
        Fetch asset cost data.

        Args:
            window: Time window (e.g. '1d', '7d')

        Returns:
            Parsed JSON response from the OpenCost API
        """
        params = {"window": window}
        return self._get("assets", params=params)


def fmt_cost(cost: float) -> str:
    """Format a cost value as currency."""
    return f"${cost:,.4f}"


def main() -> int:
    base_url = os.environ.get("OPENCOST_URL", "http://localhost:9003")
    client = OpenCostClient(base_url)

    print(f"Connecting to OpenCost at {base_url} ...")
    print("-" * 60)

    # Example 1: Allocation by namespace
    try:
        allocation = client.get_allocation(window="1d", aggregate="namespace")
        data = allocation.get("data", [])

        print("\n📊 Namespace Allocation (Last 24h)")
        print(f"{'Namespace':<30} {'CPU':>12} {'RAM':>12} {'Total':>12}")
        print("-" * 70)

        for window_data in data:
            for namespace, metrics in window_data.items():
                cpu = metrics.get("cpuCost", 0.0)
                ram = metrics.get("ramCost", 0.0)
                pv = metrics.get("pvCost", 0.0)
                network = metrics.get("networkCost", 0.0)
                total = metrics.get("totalCost", cpu + ram + pv + network)
                print(
                    f"{namespace:<30} "
                    f"{fmt_cost(cpu):>12} {fmt_cost(ram):>12} {fmt_cost(total):>12}"
                )
    except requests.RequestException as exc:
        print(f"\n❌ Failed to fetch allocation data: {exc}")
        return 1

    # Example 2: Allocation by deployment
    try:
        deployment_alloc = client.get_allocation(
            window="1d", aggregate="deployment"
        )
        dep_data = deployment_alloc.get("data", [])

        print("\n📦 Top Deployments by Cost (Last 24h)")
        print(f"{'Deployment':<40} {'Total Cost':>12}")
        print("-" * 55)

        deployments = []
        for window_data in dep_data:
            for deployment, metrics in window_data.items():
                total = metrics.get("totalCost", 0.0)
                deployments.append((deployment, total))

        deployments.sort(key=lambda x: x[1], reverse=True)
        for deployment, total in deployments[:10]:
            print(f"{deployment:<40} {fmt_cost(total):>12}")
    except requests.RequestException as exc:
        print(f"\n❌ Failed to fetch deployment data: {exc}")

    print("\n✅ Done!")
    return 0


if __name__ == "__main__":
    sys.exit(main())
