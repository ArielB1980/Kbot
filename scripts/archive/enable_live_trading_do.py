"""
Enable live trading on DigitalOcean App Platform (tradingbot).

Sets TRADING_* and DRY_RUN env vars on workers, then submits updated spec (triggers deploy).

Usage:
  export DO_API_TOKEN=your_token   # or DIGITALOCEAN_API_TOKEN
  python scripts/enable_live_trading_do.py
"""
import os
import sys
import requests

BASE_URL = "https://api.digitalocean.com/v2"
APP_NAME = "tradingbot"


def _token() -> str:
    t = os.environ.get("DO_API_TOKEN") or os.environ.get("DIGITALOCEAN_API_TOKEN")
    if not t:
        print("Set DO_API_TOKEN or DIGITALOCEAN_API_TOKEN")
        sys.exit(1)
    return t


def _headers() -> dict:
    return {"Authorization": f"Bearer {_token()}", "Content-Type": "application/json"}


def get_app_id():
    print(f"Finding App ID for '{APP_NAME}'...")
    response = requests.get(f"{BASE_URL}/apps", headers=_headers())
    if response.status_code != 200:
        print(f"Error fetching apps: {response.text}")
        sys.exit(1)
        
    apps = response.json().get("apps", [])
    for app in apps:
        if app['spec']['name'] == APP_NAME:
            return app['id']
            
    # If not found by exact name, try to find the one we deployed to recently
    if apps:
        print(f"App '{APP_NAME}' not found. Using first available app: {apps[0]['spec']['name']}")
        return apps[0]['id']
        
    print("No apps found.")
    sys.exit(1)

def enable_trading(app_id):
    print(f"Fetching App Spec for {app_id}...")
    response = requests.get(f"{BASE_URL}/apps/{app_id}", headers=_headers())
    if response.status_code != 200:
        print(f"Error fetching app: {response.text}")
        sys.exit(1)
        
    app_data = response.json()
    spec = app_data.get('app', {}).get('spec', {})
    
    # Define flags to enable (live execution, top performance)
    LIVE_FLAGS = {
        "TRADING_NEW_ENTRIES_ENABLED": "true",
        "TRADING_REVERSALS_ENABLED": "true",
        "TRADING_PARTIALS_ENABLED": "true",
        "TRADING_TRAILING_ENABLED": "true",
        "USE_STATE_MACHINE_V2": "true",
        "SYSTEM_DRY_RUN": "false",
        "DRY_RUN": "false",
    }
    
    updated = False
    
    # Update env vars in services/workers
    components = spec.get('services', []) + spec.get('workers', [])
    
    for component in components:
        print(f"Updating configuration for component: {component['name']}")
        envs = component.get('envs', [])
        
        # Convert existing envs to dict for easy update
        env_dict = {e['key']: e for e in envs}
        
        for key, value in LIVE_FLAGS.items():
            if key in env_dict:
                if env_dict[key].get('value') != value:
                    print(f"  - Changing {key}: {env_dict[key].get('value')} -> {value}")
                    env_dict[key]['value'] = value
                    updated = True
                else:
                    print(f"  - {key} is already {value}")
            else:
                print(f"  - Adding {key}: {value}")
                envs.append({"key": key, "value": value})
                updated = True
                
        component['envs'] = envs

    if not updated:
        print("Configuration is already set for Live Trading. No changes needed.")
        return False

    print("Submitting updated App Spec to DigitalOcean...")
    update_response = requests.put(
        f"{BASE_URL}/apps/{app_id}",
        headers=_headers(),
        json={"spec": spec}
    )
    
    if update_response.status_code == 200:
        deployment = update_response.json().get('deployment', {})
        print(f"✅ Update successful! Deployment ID: {deployment.get('id')}")
        print("The system is restarting in LIVE TRADING MODE.")
        return True
    else:
        print(f"❌ Update failed: {update_response.status_code} - {update_response.text}")
        return False

if __name__ == "__main__":
    app_id = get_app_id()
    enable_trading(app_id)
