import os
import requests
import json
import time

API_TOKEN = "dop_v1_153558ff94f1f7f6be775692dba269d755b8cac87042bda4c23d75717fce490d"
BASE_URL = "https://api.digitalocean.com/v2"

headers = {
    "Authorization": f"Bearer {API_TOKEN}",
    "Content-Type": "application/json"
}

def list_apps():
    print("Fetching App Platform apps...")
    response = requests.get(f"{BASE_URL}/apps", headers=headers)
    if response.status_code == 200:
        apps = response.json().get("apps", [])
        for app in apps:
            print(f"App ID: {app['id']}, Name: {app['spec']['name']}, Live Domain: {app.get('live_domain')}")
        return apps
    else:
        print(f"Error fetching apps: {response.status_code} - {response.text}")
        return []

def get_deployments(app_id):
    print(f"Fetching deployments for App ID: {app_id}...")
    response = requests.get(f"{BASE_URL}/apps/{app_id}/deployments", headers=headers)
    if response.status_code == 200:
        deployments = response.json().get("deployments", [])
        if deployments:
            latest = deployments[0]
            print(f"Latest Deployment ID: {latest['id']}, Phase: {latest['phase']}, Cause: {latest['cause']}")
            return latest
    else:
        print(f"Error fetching deployments: {response.status_code} - {response.text}")
    return None

def monitor_deployment(app_id):
    print("Monitoring deployment...")
    while True:
        deployment = get_deployments(app_id)
        if deployment:
            phase = deployment['phase']
            print(f"Current Phase: {phase}")
            
            if phase == 'ACTIVE':
                print("Deployment SUCCESSFUL and ACTIVE!")
                return True
            elif phase in ['FAILED', 'CANCELED']:
                print(f"Deployment FAILED with phase: {phase}")
                return False
            elif phase in ['PENDING_BUILD', 'BUILDING', 'DEPLOYING']:
                print("Deployment in progress...")
                time.sleep(10)
            else:
                print(f"Unknown phase: {phase}")
                time.sleep(10)
        else:
            print("No deployment found.")
            return False

if __name__ == "__main__":
    apps = list_apps()
    if apps:
        # Assuming we want to track the first app found, or the one named 'trading-system-v2' if possible
        target_app = None
        for app in apps:
            # You might want to filter by name if you know it, e.g.
            # if app['spec']['name'] == 'trading-system':
            target_app = app
            break # Just take the first one for now
        
        if target_app:
            monitor_deployment(target_app['id'])
        else:
            print("No apps found to monitor.")
