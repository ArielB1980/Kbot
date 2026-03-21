#!/usr/bin/env python3
"""
Fetch DigitalOcean App Platform logs via API.
"""
import requests
import sys
from datetime import datetime

APP_ID = "b4f45c80-9a75-4d4f-b16a-1b84e0c79ed4"
API_TOKEN = "dop_v1_153558ff94f1f7f6be775692dba269d755b8cac87042bda4c23d75717fce490d"

def fetch_logs():
    """Fetch logs from DigitalOcean App Platform."""

    headers = {
        "Authorization": f"Bearer {API_TOKEN}",
        "Content-Type": "application/json"
    }

    # Get app info first
    print("Fetching app info...")
    url = f"https://api.digitalocean.com/v2/apps/{APP_ID}"
    response = requests.get(url, headers=headers)

    if response.status_code != 200:
        print(f"Error fetching app info: {response.status_code}")
        print(response.text)
        return

    app_data = response.json()
    print(f"App name: {app_data['app']['spec']['name']}")

    # Get deployments
    print("\nFetching deployments...")
    url = f"https://api.digitalocean.com/v2/apps/{APP_ID}/deployments"
    response = requests.get(url, headers=headers)

    if response.status_code != 200:
        print(f"Error fetching deployments: {response.status_code}")
        print(response.text)
        return

    deployments = response.json()['deployments']
    if not deployments:
        print("No deployments found")
        return

    latest_deployment = deployments[0]
    deployment_id = latest_deployment['id']
    print(f"Latest deployment ID: {deployment_id}")
    print(f"Status: {latest_deployment['phase']}")

    # Get component name
    if 'services' in app_data['app']['spec'] and app_data['app']['spec']['services']:
        component_name = app_data['app']['spec']['services'][0]['name']
    elif 'workers' in app_data['app']['spec'] and app_data['app']['spec']['workers']:
        component_name = app_data['app']['spec']['workers'][0]['name']
    else:
        print("No services or workers found in spec")
        return
    print(f"Component: {component_name}")

    # Fetch logs
    print(f"\nFetching logs for component '{component_name}'...")
    url = f"https://api.digitalocean.com/v2/apps/{APP_ID}/deployments/{deployment_id}/components/{component_name}/logs"

    params = {
        "type": "RUN",
        "follow": "false",
        "tail_lines": 1000
    }

    response = requests.get(url, headers=headers, params=params)

    if response.status_code != 200:
        print(f"Error fetching logs: {response.status_code}")
        print(response.text)
        return

    logs_data = response.json()

    if 'live_url' in logs_data:
        # Logs are streamed via URL
        print("Logs available via streaming URL")
        live_url = logs_data['live_url']
        
        # Helper function to fetch logs asynchronously
        async def fetch_ws_logs():
            ws_url = live_url
            if ws_url.startswith("https://"):
                ws_url = ws_url.replace("https://", "wss://")
            
            print(f"Connecting to {ws_url[:50]}...")
            ssl_context = ssl.create_default_context()
            ssl_context.check_hostname = False
            ssl_context.verify_mode = ssl.CERT_NONE

            try:
                async with websockets.connect(ws_url, ssl=ssl_context) as websocket:
                    print("Connected. Downloading logs...")
                    with open("server_logs_live.txt", "w") as f:
                        try:
                            while True:
                                message = await asyncio.wait_for(websocket.recv(), timeout=3.0)
                                print(message, file=f)
                        except asyncio.TimeoutError:
                            print("Finished downloading live logs (timeout).")
                        except Exception as e:
                            print(f"Error reading stream: {e}")
            except Exception as e:
                print(f"Connection error: {e}")

        # Run the async loop
        import asyncio
        import websockets
        import ssl
        
        asyncio.run(fetch_ws_logs())
        print("Live logs saved to server_logs_live.txt")

    if 'historic_urls' in logs_data:
        # Historical logs
        print(f"Found {len(logs_data['historic_urls'])} historic log URLs")
        with open("server_logs_historic.txt", "w") as f:
            for url_info in logs_data['historic_urls']:
                print(f"\nFetching historical logs from {url_info}...")
                log_response = requests.get(url_info)
                f.write(log_response.text)
        print("Historic logs saved to server_logs_historic.txt")
    else:
        print("No historic logs found in response")

if __name__ == "__main__":
    fetch_logs()
