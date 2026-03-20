#!/usr/bin/env python3
"""
Fetch and display deployment logs from DigitalOcean.

Usage:
    export DO_API_TOKEN=your_token
    python scripts/fetch_deployment_logs.py [deployment_id]
    
If deployment_id is not provided, fetches logs from the most recent failed deployment.
"""
import os
import sys
import requests
import json
from datetime import datetime

BASE_URL = "https://api.digitalocean.com/v2"
APP_ID = "b4f45c80-9a75-4d4f-b16a-1b84e0c79ed4"
COMPONENT = "worker"


def _token() -> str:
    t = os.environ.get("DO_API_TOKEN") or os.environ.get("DIGITALOCEAN_API_TOKEN")
    if not t:
        print("❌ Set DO_API_TOKEN or DIGITALOCEAN_API_TOKEN")
        sys.exit(1)
    return t


def _headers() -> dict:
    return {"Authorization": f"Bearer {_token()}", "Content-Type": "application/json"}


def get_deployments():
    """Get recent deployments."""
    response = requests.get(f"{BASE_URL}/apps/{APP_ID}/deployments", headers=_headers())
    if response.status_code != 200:
        print(f"❌ Error: {response.status_code} - {response.text}")
        return None
    return response.json().get("deployments", [])


def get_logs_url(deployment_id: str, log_type: str = "RUN"):
    """Get logs URL for a deployment."""
    response = requests.get(
        f"{BASE_URL}/apps/{APP_ID}/deployments/{deployment_id}/components/{COMPONENT}/logs",
        headers=_headers(),
        params={"type": log_type, "tail_lines": 200}
    )
    
    if response.status_code != 200:
        return None
    
    data = response.json()
    if "live_url" in data:
        return data["live_url"]
    elif "historic_urls" in data and data["historic_urls"]:
        return data["historic_urls"][0]
    return None


def main():
    deployment_id = sys.argv[1] if len(sys.argv) > 1 else None
    
    if not deployment_id:
        print("📋 Finding most recent failed deployment...")
        deployments = get_deployments()
        if not deployments:
            print("❌ No deployments found")
            return
        
        # Find most recent failed deployment
        for dep in deployments:
            if dep.get("phase") == "ERROR":
                deployment_id = dep.get("id")
                created = dep.get("created_at", "")
                print(f"✅ Found failed deployment: {deployment_id[:8]}... (created: {created})")
                break
        
        if not deployment_id:
            print("❌ No failed deployments found")
            print("\nRecent deployments:")
            for dep in deployments[:5]:
                phase = dep.get("phase", "UNKNOWN")
                dep_id = dep.get("id", "")[:8]
                created = dep.get("created_at", "")
                print(f"  - {dep_id}... {phase} ({created})")
            return
    
    print(f"\n📋 Fetching logs for deployment: {deployment_id[:8]}...")
    
    # Try RUN logs first
    print("\n🔍 Runtime (RUN) logs:")
    run_url = get_logs_url(deployment_id, "RUN")
    if run_url:
        print(f"✅ Logs available at:")
        print(f"   {run_url}")
        print(f"\n💡 To view logs, open this URL in your browser")
        print(f"   Or use: curl '{run_url}'")
    else:
        print("❌ No RUN logs available (deployment may be in cleanup phase)")
    
    # Try BUILD logs
    print("\n🔍 Build logs:")
    build_url = get_logs_url(deployment_id, "BUILD")
    if build_url:
        print(f"✅ Build logs available at:")
        print(f"   {build_url}")
    else:
        print("❌ No BUILD logs available")
    
    # Get deployment details
    print("\n📊 Deployment details:")
    response = requests.get(
        f"{BASE_URL}/apps/{APP_ID}/deployments/{deployment_id}",
        headers=_headers()
    )
    
    if response.status_code == 200:
        deployment = response.json().get("deployment", {})
        phase = deployment.get("phase", "UNKNOWN")
        created = deployment.get("created_at", "")
        updated = deployment.get("updated_at", "")
        
        print(f"  Phase: {phase}")
        print(f"  Created: {created}")
        print(f"  Updated: {updated}")
        
        # Check components
        components = deployment.get("components", [])
        if components:
            print(f"\n  Components:")
            for comp in components:
                name = comp.get("name", "Unknown")
                comp_phase = comp.get("phase", "UNKNOWN")
                message = comp.get("message", "")
                reason = comp.get("reason", "")
                
                print(f"    - {name}: {comp_phase}")
                if message:
                    print(f"      Message: {message}")
                if reason:
                    print(f"      Reason: {reason}")


if __name__ == "__main__":
    main()
