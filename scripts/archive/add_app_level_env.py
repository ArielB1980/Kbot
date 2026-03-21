"""
Add DATABASE_URL as app-level environment variable in DigitalOcean App Platform.

This ensures DATABASE_URL is available to all components and during migration phase.
"""
import os
import sys
import requests

BASE_URL = "https://api.digitalocean.com/v2"
APP_ID = "b4f45c80-9a75-4d4f-b16a-1b84e0c79ed4"


def _token() -> str:
    t = os.environ.get("DO_API_TOKEN") or os.environ.get("DIGITALOCEAN_API_TOKEN")
    if not t:
        print("Set DO_API_TOKEN or DIGITALOCEAN_API_TOKEN")
        sys.exit(1)
    return t


def _headers() -> dict:
    return {"Authorization": f"Bearer {_token()}", "Content-Type": "application/json"}


def add_app_level_database_url():
    """Add DATABASE_URL at app level."""
    print(f"Fetching current app spec...")
    response = requests.get(f"{BASE_URL}/apps/{APP_ID}", headers=_headers())
    if response.status_code != 200:
        print(f"Error: {response.status_code} - {response.text}")
        sys.exit(1)
    
    app_data = response.json()
    spec = app_data.get('app', {}).get('spec', {})
    
    # Check if DATABASE_URL already exists at app level
    app_envs = spec.get('envs', [])
    db_url_exists = any(env.get('key') == 'DATABASE_URL' for env in app_envs)
    
    if db_url_exists:
        print("✅ DATABASE_URL already exists at app level")
        return
    
    # Get DATABASE_URL from worker component to copy the secret reference
    services = spec.get('services', [])
    worker_db_url = None
    for service in services:
        if service.get('name') == 'worker':
            envs = service.get('envs', [])
            for env in envs:
                if env.get('key') == 'DATABASE_URL':
                    worker_db_url = env
                    break
            break
    
    if not worker_db_url:
        print("❌ DATABASE_URL not found in worker component. Cannot copy to app level.")
        sys.exit(1)
    
    # Add DATABASE_URL to app-level envs
    if 'envs' not in spec:
        spec['envs'] = []
    
    spec['envs'].append({
        'key': 'DATABASE_URL',
        'scope': 'RUN_TIME',
        'type': 'SECRET'
    })
    
    print("📝 Adding DATABASE_URL to app-level environment variables...")
    print("   This ensures it's available to all components and during migration phase")
    
    # Update the app
    update_response = requests.put(
        f"{BASE_URL}/apps/{APP_ID}",
        headers=_headers(),
        json={"spec": spec}
    )
    
    if update_response.status_code == 200:
        print("✅ Successfully added DATABASE_URL at app level")
        print("   Deployment will be triggered automatically")
    else:
        print(f"❌ Failed to update app: {update_response.status_code}")
        print(f"   Response: {update_response.text}")
        sys.exit(1)


if __name__ == "__main__":
    add_app_level_database_url()
