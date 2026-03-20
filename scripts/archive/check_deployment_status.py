"""
Check deployment status and logs for DigitalOcean App Platform.

Usage:
  export DO_API_TOKEN=your_token
  python scripts/check_deployment_status.py
"""
import os
import sys
import requests
import json
import time

BASE_URL = "https://api.digitalocean.com/v2"
APP_NAME = "tradingsystem"
APP_ID = "b4f45c80-9a75-4d4f-b16a-1b84e0c79ed4"


def _token() -> str:
    t = os.environ.get("DO_API_TOKEN") or os.environ.get("DIGITALOCEAN_API_TOKEN")
    if not t:
        print("Set DO_API_TOKEN or DIGITALOCEAN_API_TOKEN")
        sys.exit(1)
    return t


def _headers() -> dict:
    return {"Authorization": f"Bearer {_token()}", "Content-Type": "application/json"}


def get_deployments():
    """Get recent deployments."""
    print(f"Fetching deployments for app {APP_ID}...")
    response = requests.get(f"{BASE_URL}/apps/{APP_ID}/deployments", headers=_headers())
    if response.status_code != 200:
        print(f"Error: {response.status_code} - {response.text}")
        return None
    
    deployments = response.json().get('deployments', [])
    return deployments


def get_deployment_details(deployment_id):
    """Get detailed information about a deployment."""
    print(f"\nFetching details for deployment {deployment_id}...")
    response = requests.get(
        f"{BASE_URL}/apps/{APP_ID}/deployments/{deployment_id}",
        headers=_headers()
    )
    if response.status_code != 200:
        print(f"Error fetching deployment details: {response.status_code} - {response.text}")
        return None
    
    return response.json().get('deployment', {})


def get_component_logs(deployment_id, component_name, log_type="RUN"):
    """Get logs for a specific component."""
    print(f"\nFetching {log_type} logs for component '{component_name}'...")
    response = requests.get(
        f"{BASE_URL}/apps/{APP_ID}/deployments/{deployment_id}/components/{component_name}/logs",
        headers=_headers(),
        params={"type": log_type, "tail_lines": 100}
    )
    if response.status_code != 200:
        print(f"Error fetching logs: {response.status_code} - {response.text}")
        return None
    
    return response.json()


def get_app_status():
    """Get current app status."""
    print(f"Fetching app status for {APP_ID}...")
    response = requests.get(f"{BASE_URL}/apps/{APP_ID}", headers=_headers())
    if response.status_code != 200:
        print(f"Error: {response.status_code} - {response.text}")
        return None
    
    return response.json().get('app', {})


def main():
    # Get app status
    app = get_app_status()
    if app:
        print(f"\n📊 App Status:")
        print(f"  Name: {app.get('spec', {}).get('name')}")
        print(f"  Active Deployment: {app.get('active_deployment', {}).get('id', 'None')}")
        print(f"  Ingress: {app.get('ingress', {}).get('default', {}).get('component', {}).get('name', 'None')}")
    
    # Get deployments
    deployments = get_deployments()
    if not deployments:
        print("No deployments found")
        return
    
    print(f"\n📦 Recent Deployments ({len(deployments)}):")
    print("=" * 80)
    
    for i, dep in enumerate(deployments[:5]):  # Show last 5
        dep_id = dep.get('id')
        phase = dep.get('phase', 'UNKNOWN')
        created = dep.get('created_at', '')
        updated = dep.get('updated_at', '')
        
        print(f"\n{i+1}. Deployment {dep_id[:8]}...")
        print(f"   Phase: {phase}")
        print(f"   Created: {created}")
        print(f"   Updated: {updated}")
        
        # Get detailed status
        if phase in ['PENDING_BUILD', 'BUILDING', 'PENDING_DEPLOY', 'DEPLOYING']:
            print(f"   Status: ⏳ In Progress")
        elif phase == 'ACTIVE':
            print(f"   Status: ✅ Active")
        elif phase in ['ERROR', 'CANCELED', 'SUPERSEDED']:
            print(f"   Status: ❌ {phase}")
            
            # Get detailed deployment info for failed deployments
            if phase == 'ERROR':
                details = get_deployment_details(dep_id)
                if details:
                    progress_steps = details.get('progress', {}).get('steps', [])
                    if progress_steps:
                        print(f"\n   📋 Progress Steps:")
                        for step in progress_steps[-5:]:  # Last 5 steps
                            step_name = step.get('name', 'Unknown')
                            step_status = step.get('status', 'UNKNOWN')
                            step_reason = step.get('reason', '')
                            status_icon = "✅" if step_status == "SUCCESS" else "❌" if step_status == "ERROR" else "⏳"
                            print(f"      {status_icon} {step_name}: {step_status}")
                            if step_reason:
                                print(f"         Reason: {step_reason}")
        
        # Get component statuses
        components = dep.get('components', [])
        if components:
            print(f"\n   Components:")
            for comp in components:
                comp_name = comp.get('name', 'Unknown')
                comp_phase = comp.get('phase', 'UNKNOWN')
                comp_reason = comp.get('reason', '')
                comp_message = comp.get('message', '')
                print(f"      - {comp_name}: {comp_phase}")
                if comp_reason:
                    print(f"        Reason: {comp_reason}")
                if comp_message:
                    print(f"        Message: {comp_message}")
                
                # Get logs for failed components
                if comp_phase == 'ERROR' and phase == 'ERROR':
                    logs = get_component_logs(dep_id, comp_name, "RUN")
                    if logs and 'live_url' in logs:
                        print(f"        Logs URL: {logs['live_url']}")
                    elif logs and 'historic_urls' in logs:
                        print(f"        Historic logs: {logs['historic_urls']}")
        
        print("-" * 80)


if __name__ == "__main__":
    main()
