#!/usr/bin/env python3
"""
Script to add environment variables to DigitalOcean App Platform.

Requires DigitalOcean API token with write access.

Usage:
    export DIGITALOCEAN_API_TOKEN=your_token
    python scripts/add_env_vars.py
"""
import os
import sys
import requests
import json

# API Keys (from user)
API_KEYS = {
    "KRAKEN_API_KEY": "Yn8ZB7+enpsmhv6QCC+QitHe/qz4+Tu25kqWfZmgwKckY24IAr4FD5lN",
    "KRAKEN_API_SECRET": "HdPZPP2GYy4gGa1gvG5DvtmlzlcE2foIsIozOQRg6oovaswSuXSc1lvS3abJ+WEWK9r/GBKOtHbUxMNNgmp6PA==",
    "KRAKEN_FUTURES_API_KEY": "2k1daXUJari2fsDGsQ21rNgF1xeL3obeT+ojmNcpuS44SPMYXaKV6KMx",
    "KRAKEN_FUTURES_API_SECRET": "4h77HOI0onjBh4zgklakpVwLrbCg0GZNrCeOBOUQPMOIVcciOFEJ9yljOy2Fm746UznwVCpSqPbKsMqyxNOUmBoM",
    "ENVIRONMENT": "prod"
}

# App details
APP_ID = "tradingbot-2tdzi"  # This might need to be the actual app ID/UUID
BASE_URL = "https://api.digitalocean.com/v2"

def get_api_token():
    """Get API token from environment."""
    token = os.getenv("DIGITALOCEAN_API_TOKEN")
    if not token:
        print("❌ Error: DIGITALOCEAN_API_TOKEN environment variable not set")
        print("\nTo get your API token:")
        print("1. Go to https://cloud.digitalocean.com/account/api/tokens")
        print("2. Generate a new token with write access")
        print("3. Run: export DIGITALOCEAN_API_TOKEN=your_token")
        sys.exit(1)
    return token

def get_apps(token):
    """Get list of apps."""
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }
    
    response = requests.get(f"{BASE_URL}/apps", headers=headers)
    if response.status_code != 200:
        print(f"❌ Failed to get apps: {response.status_code}")
        print(response.text)
        return None
    
    return response.json().get("apps", [])

def find_app_by_name(apps, name):
    """Find app by name."""
    for app in apps:
        if name in app.get("spec", {}).get("name", "").lower():
            return app
    return None

def get_app_env_vars(token, app_id):
    """Get current environment variables."""
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }
    
    response = requests.get(f"{BASE_URL}/apps/{app_id}", headers=headers)
    if response.status_code != 200:
        print(f"❌ Failed to get app: {response.status_code}")
        return None
    
    app_data = response.json().get("app", {})
    spec = app_data.get("spec", {})
    
    # Find worker component (or web component)
    components = spec.get("services", []) + spec.get("workers", [])
    env_vars = {}
    
    for component in components:
        env = component.get("envs", [])
        for e in env:
            env_vars[e.get("key")] = e.get("value")
    
    return env_vars, spec

def update_app_env_vars(token, app_id, spec, new_vars):
    """Update app with new environment variables."""
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }
    
    # Add new vars to all components
    components = spec.get("services", []) + spec.get("workers", [])
    
    for component in components:
        existing_envs = component.get("envs", [])
        
        # Add new vars
        for key, value in new_vars.items():
            # Check if already exists
            found = False
            for env in existing_envs:
                if env.get("key") == key:
                    env["value"] = value
                    found = True
                    break
            
            if not found:
                existing_envs.append({
                    "key": key,
                    "value": value,
                    "scope": "RUN_TIME",
                    "type": "GENERAL"
                })
        
        component["envs"] = existing_envs
    
    # Update spec
    update_data = {
        "spec": spec
    }
    
    response = requests.put(f"{BASE_URL}/apps/{app_id}", 
                          headers=headers, 
                          json=update_data)
    
    return response

def main():
    print("=" * 60)
    print("DigitalOcean App Platform - Environment Variables Setup")
    print("=" * 60)
    print()
    
    # Get API token
    token = get_api_token()
    print("✅ API token found")
    
    # Get apps
    print("\n📋 Fetching apps...")
    apps = get_apps(token)
    if not apps:
        print("❌ No apps found or error occurred")
        return
    
    print(f"✅ Found {len(apps)} app(s)")
    
    # Find our app
    app = find_app_by_name(apps, "tradingbot")
    if not app:
        print("\n❌ App 'tradingbot' not found")
        print("\nAvailable apps:")
        for a in apps:
            print(f"  - {a.get('spec', {}).get('name', 'Unknown')} (ID: {a.get('id', 'Unknown')})")
        return
    
    app_id = app.get("id")
    app_name = app.get("spec", {}).get("name", "Unknown")
    print(f"✅ Found app: {app_name} (ID: {app_id})")
    
    # Get current env vars
    print("\n📋 Getting current environment variables...")
    current_vars, spec = get_app_env_vars(token, app_id)
    if current_vars is None:
        print("❌ Failed to get current environment variables")
        return
    
    print(f"✅ Found {len(current_vars)} existing environment variables")
    
    # Show what will be added
    print("\n📝 Environment variables to add/update:")
    for key, value in API_KEYS.items():
        masked_value = value[:10] + "..." if len(value) > 10 else value
        status = "UPDATE" if key in current_vars else "ADD"
        print(f"  {status}: {key} = {masked_value}")
    
    # Confirm
    print("\n⚠️  This will update your app and trigger a restart.")
    confirm = input("Continue? (yes/no): ")
    if confirm.lower() != "yes":
        print("❌ Cancelled")
        return
    
    # Update app
    print("\n🔄 Updating app...")
    response = update_app_env_vars(token, app_id, spec, API_KEYS)
    
    if response.status_code in [200, 201]:
        print("✅ Environment variables updated successfully!")
        print("\n⏳ App will restart automatically...")
        print("   Check status at: https://cloud.digitalocean.com/apps")
        print("\n🔍 Verify after restart:")
        print("   curl https://tradingbot-2tdzi.ondigitalocean.app/quick-test")
    else:
        print(f"❌ Failed to update: {response.status_code}")
        print(response.text)

if __name__ == "__main__":
    main()
