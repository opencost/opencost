#!/usr/bin/env python3
"""
Hetzner Cloud Pricing CSV Generator for OpenCost

This script fetches current pricing from the Hetzner Cloud API and generates
an OpenCost-compatible CSV file for cost tracking.

Requirements:
    pip install requests

Usage:
    # Set your Hetzner Cloud API token
    export HCLOUD_TOKEN="your-api-token"
    
    # Run the script
    python3 hetzner_pricing_generator.py
    
    # Output will be written to hetzner_pricing.csv

API Documentation:
    https://docs.hetzner.cloud/reference/cloud#pricing
"""

import csv
import os
import sys
from datetime import datetime
from typing import Any

try:
    import requests
except ImportError:
    print("Error: 'requests' library is required. Install with: pip install requests")
    sys.exit(1)


HETZNER_API_URL = "https://api.hetzner.cloud/v1"

# Hetzner regions
REGIONS = ["fsn1", "nbg1", "hel1", "ash", "hil", "sin"]

# EU regions (pricing in the CSV will use EU rates by default)
EU_REGIONS = ["fsn1", "nbg1", "hel1"]


def get_api_token() -> str:
    """Get Hetzner Cloud API token from environment."""
    token = os.environ.get("HCLOUD_TOKEN")
    if not token:
        print("Error: HCLOUD_TOKEN environment variable not set")
        print("Get your token from: https://console.hetzner.cloud/projects/*/security/tokens")
        sys.exit(1)
    return token


def fetch_pricing(token: str) -> dict[str, Any]:
    """Fetch pricing data from Hetzner Cloud API."""
    headers = {"Authorization": f"Bearer {token}"}
    response = requests.get(f"{HETZNER_API_URL}/pricing", headers=headers)
    
    if response.status_code != 200:
        print(f"Error fetching pricing: {response.status_code}")
        print(response.text)
        sys.exit(1)
    
    return response.json()


def fetch_server_types(token: str) -> dict[str, Any]:
    """Fetch server types from Hetzner Cloud API."""
    headers = {"Authorization": f"Bearer {token}"}
    response = requests.get(f"{HETZNER_API_URL}/server_types", headers=headers)
    
    if response.status_code != 200:
        print(f"Error fetching server types: {response.status_code}")
        print(response.text)
        sys.exit(1)
    
    return response.json()


def fetch_load_balancer_types(token: str) -> dict[str, Any]:
    """Fetch load balancer types from Hetzner Cloud API."""
    headers = {"Authorization": f"Bearer {token}"}
    response = requests.get(f"{HETZNER_API_URL}/load_balancer_types", headers=headers)
    
    if response.status_code != 200:
        print(f"Error fetching load balancer types: {response.status_code}")
        print(response.text)
        sys.exit(1)
    
    return response.json()


def parse_hourly_price(price_str: str) -> float:
    """Parse price string to float."""
    try:
        return float(price_str)
    except (ValueError, TypeError):
        return 0.0


def generate_csv(output_file: str = "hetzner_pricing.csv"):
    """Generate OpenCost-compatible CSV from Hetzner API data."""
    token = get_api_token()
    
    print("Fetching pricing data from Hetzner Cloud API...")
    pricing_data = fetch_pricing(token)
    server_types_data = fetch_server_types(token)
    lb_types_data = fetch_load_balancer_types(token)
    
    version = datetime.now().strftime("%Y.%m")
    rows = []
    
    # Header
    header = [
        "EndTimestamp", "InstanceID", "Region", "AssetClass",
        "InstanceIDField", "InstanceType", "MarketPriceHourly", "Version"
    ]
    
    # Process server types
    print("Processing server types...")
    for server_type in server_types_data.get("server_types", []):
        name = server_type["name"]
        
        # Get pricing for each location
        for price_info in server_type.get("prices", []):
            location = price_info.get("location")
            hourly_price = parse_hourly_price(
                price_info.get("price_hourly", {}).get("net", "0")
            )
            
            if hourly_price > 0:
                rows.append([
                    "",  # EndTimestamp
                    name,  # InstanceID
                    location,  # Region
                    "node",  # AssetClass
                    "metadata.labels.node.kubernetes.io/instance-type",  # InstanceIDField
                    name,  # InstanceType
                    f"{hourly_price:.6f}",  # MarketPriceHourly
                    version  # Version
                ])
    
    # Process load balancer types
    print("Processing load balancer types...")
    for lb_type in lb_types_data.get("load_balancer_types", []):
        name = lb_type["name"]
        
        for price_info in lb_type.get("prices", []):
            location = price_info.get("location")
            hourly_price = parse_hourly_price(
                price_info.get("price_hourly", {}).get("net", "0")
            )
            
            if hourly_price > 0:
                rows.append([
                    "",  # EndTimestamp
                    name,  # InstanceID
                    location,  # Region
                    "node",  # AssetClass (using node for LBs to track costs)
                    "metadata.labels.load-balancer.hetzner.cloud/type",  # InstanceIDField
                    name,  # InstanceType
                    f"{hourly_price:.6f}",  # MarketPriceHourly
                    version  # Version
                ])
    
    # Process volume pricing
    print("Processing volume pricing...")
    pricing = pricing_data.get("pricing", {})
    volume_pricing = pricing.get("volume", {})
    
    # Volume pricing is per GB per month, convert to hourly
    volume_monthly = parse_hourly_price(
        volume_pricing.get("price_per_gb_month", {}).get("net", "0.052")
    )
    volume_hourly = volume_monthly / 730  # Average hours per month
    
    for region in EU_REGIONS:
        rows.append([
            "",  # EndTimestamp
            "ssd",  # InstanceID
            region,  # Region
            "pv",  # AssetClass
            "spec.storageClassName",  # InstanceIDField
            "hcloud-volumes",  # InstanceType
            f"{volume_hourly:.8f}",  # MarketPriceHourly
            version  # Version
        ])
    
    # Write CSV
    print(f"Writing {len(rows)} pricing entries to {output_file}...")
    with open(output_file, "w", newline="") as f:
        # Write header comment
        f.write("# Hetzner Cloud Pricing for OpenCost CSV Provider\n")
        f.write(f"# Generated: {datetime.now().isoformat()}\n")
        f.write("# Source: Hetzner Cloud API (https://api.hetzner.cloud/v1/pricing)\n")
        f.write("# Prices in EUR per hour (net, excluding VAT)\n")
        f.write("#\n")
        
        writer = csv.writer(f)
        writer.writerow(header)
        writer.writerows(rows)
    
    print(f"Done! Generated {output_file} with {len(rows)} pricing entries.")
    print("\nTo use with OpenCost:")
    print("  1. Copy the CSV to your OpenCost deployment")
    print("  2. Set USE_CSV_PROVIDER=true")
    print("  3. Set CSV_PATH to the file location")


def main():
    """Main entry point."""
    import argparse
    
    parser = argparse.ArgumentParser(
        description="Generate OpenCost-compatible pricing CSV from Hetzner Cloud API"
    )
    parser.add_argument(
        "-o", "--output",
        default="hetzner_pricing.csv",
        help="Output CSV file path (default: hetzner_pricing.csv)"
    )
    
    args = parser.parse_args()
    generate_csv(args.output)


if __name__ == "__main__":
    main()
