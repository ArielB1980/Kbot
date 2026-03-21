#!/usr/bin/env python3
"""
DigitalOcean App Platform Performance Monitor

This script checks the health and performance of the trading system
deployed on DigitalOcean App Platform.

Usage:
    python scripts/check_do_performance.py [--api-token YOUR_TOKEN]
    
If no API token is provided, it will check public health endpoints only.
To get full metrics, create a DigitalOcean API token at:
https://cloud.digitalocean.com/account/api/tokens
"""

import os
import sys
import json
import time
import argparse
from datetime import datetime
from typing import Dict, Any, Optional
import requests

# App configuration
APP_URL = "https://tradingbot-2tdzi.ondigitalocean.app"
APP_ID = "b4f45c80-9a75-4d4f-b16a-1b84e0c79ed4"

# Health endpoints
HEALTH_ENDPOINTS = {
    "root": "/",
    "health": "/health",
    "ready": "/ready",
    "api_health": "/api/health",
}

# Color codes for terminal output
class Colors:
    GREEN = '\033[92m'
    YELLOW = '\033[93m'
    RED = '\033[91m'
    BLUE = '\033[94m'
    CYAN = '\033[96m'
    BOLD = '\033[1m'
    END = '\033[0m'


def print_header(text: str):
    """Print a formatted header"""
    print(f"\n{Colors.BOLD}{Colors.CYAN}{'=' * 80}{Colors.END}")
    print(f"{Colors.BOLD}{Colors.CYAN}{text.center(80)}{Colors.END}")
    print(f"{Colors.BOLD}{Colors.CYAN}{'=' * 80}{Colors.END}\n")


def print_status(label: str, status: str, color: str = Colors.GREEN):
    """Print a status line"""
    print(f"{Colors.BOLD}{label:.<50}{Colors.END} {color}{status}{Colors.END}")


def check_health_endpoint(endpoint_name: str, path: str) -> Dict[str, Any]:
    """Check a single health endpoint"""
    url = f"{APP_URL}{path}"
    
    try:
        start_time = time.time()
        response = requests.get(url, timeout=10)
        response_time = (time.time() - start_time) * 1000  # Convert to ms
        
        result = {
            "endpoint": endpoint_name,
            "url": url,
            "status_code": response.status_code,
            "response_time_ms": round(response_time, 2),
            "success": response.status_code == 200,
        }
        
        # Try to parse JSON response
        try:
            result["data"] = response.json()
        except:
            result["data"] = response.text[:200]  # First 200 chars
        
        return result
        
    except requests.exceptions.Timeout:
        return {
            "endpoint": endpoint_name,
            "url": url,
            "error": "Request timeout (>10s)",
            "success": False,
        }
    except requests.exceptions.ConnectionError:
        return {
            "endpoint": endpoint_name,
            "url": url,
            "error": "Connection failed",
            "success": False,
        }
    except Exception as e:
        return {
            "endpoint": endpoint_name,
            "url": url,
            "error": str(e),
            "success": False,
        }


def check_do_api(api_token: str) -> Optional[Dict[str, Any]]:
    """Check DigitalOcean API for app metrics"""
    if not api_token:
        return None
    
    headers = {
        "Authorization": f"Bearer {api_token}",
        "Content-Type": "application/json",
    }
    
    try:
        # Get app details
        response = requests.get(
            f"https://api.digitalocean.com/v2/apps/{APP_ID}",
            headers=headers,
            timeout=10
        )
        
        if response.status_code == 200:
            return response.json()
        else:
            return {
                "error": f"API returned status {response.status_code}",
                "message": response.text[:200]
            }
            
    except Exception as e:
        return {"error": str(e)}


def analyze_health_response(data: Any) -> Dict[str, Any]:
    """Analyze health check response data"""
    analysis = {
        "overall_status": "unknown",
        "issues": [],
        "warnings": [],
    }
    
    if isinstance(data, dict):
        # Check for common health indicators
        if "status" in data:
            analysis["overall_status"] = data["status"]
        
        if "database" in data:
            db_status = data["database"]
            if isinstance(db_status, dict):
                if db_status.get("connected") == False:
                    analysis["issues"].append("Database not connected")
                elif db_status.get("status") == "error":
                    analysis["issues"].append(f"Database error: {db_status.get('error', 'Unknown')}")
        
        if "services" in data:
            for service, status in data["services"].items():
                if status != "healthy" and status != "ok":
                    analysis["warnings"].append(f"Service {service}: {status}")
    
    return analysis


def main():
    parser = argparse.ArgumentParser(description="Check DigitalOcean App Platform performance")
    parser.add_argument(
        "--api-token",
        help="DigitalOcean API token (optional, for detailed metrics)",
        default=os.environ.get("DO_API_TOKEN")
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Output results as JSON"
    )
    
    args = parser.parse_args()
    
    # Collect all results
    results = {
        "timestamp": datetime.utcnow().isoformat(),
        "app_url": APP_URL,
        "health_checks": [],
        "api_metrics": None,
        "summary": {
            "total_endpoints": len(HEALTH_ENDPOINTS),
            "healthy_endpoints": 0,
            "failed_endpoints": 0,
            "average_response_time_ms": 0,
        }
    }
    
    if not args.json:
        print_header("DigitalOcean App Platform Performance Check")
        print(f"{Colors.BOLD}App URL:{Colors.END} {APP_URL}")
        print(f"{Colors.BOLD}Timestamp:{Colors.END} {results['timestamp']}")
    
    # Check all health endpoints
    if not args.json:
        print_header("Health Endpoint Checks")
    
    total_response_time = 0
    
    for endpoint_name, path in HEALTH_ENDPOINTS.items():
        result = check_health_endpoint(endpoint_name, path)
        results["health_checks"].append(result)
        
        if not args.json:
            if result["success"]:
                status_color = Colors.GREEN
                status_text = f"✓ {result['status_code']} ({result['response_time_ms']}ms)"
                results["summary"]["healthy_endpoints"] += 1
                total_response_time += result["response_time_ms"]
            else:
                status_color = Colors.RED
                status_text = f"✗ {result.get('error', 'Failed')}"
                results["summary"]["failed_endpoints"] += 1
            
            print_status(f"{endpoint_name} ({path})", status_text, status_color)
            
            # Show response data if available
            if result.get("data") and isinstance(result["data"], dict):
                analysis = analyze_health_response(result["data"])
                if analysis["issues"]:
                    for issue in analysis["issues"]:
                        print(f"  {Colors.RED}⚠ {issue}{Colors.END}")
                if analysis["warnings"]:
                    for warning in analysis["warnings"]:
                        print(f"  {Colors.YELLOW}⚠ {warning}{Colors.END}")
    
    # Calculate average response time
    if results["summary"]["healthy_endpoints"] > 0:
        results["summary"]["average_response_time_ms"] = round(
            total_response_time / results["summary"]["healthy_endpoints"], 2
        )
    
    # Check DigitalOcean API if token provided
    if args.api_token:
        if not args.json:
            print_header("DigitalOcean API Metrics")
        
        api_data = check_do_api(args.api_token)
        results["api_metrics"] = api_data
        
        if not args.json:
            if api_data and "error" not in api_data:
                app_info = api_data.get("app", {})
                print_status("App Name", app_info.get("spec", {}).get("name", "N/A"))
                print_status("Region", app_info.get("region", {}).get("slug", "N/A"))
                print_status("Tier", app_info.get("tier_slug", "N/A"))
                print_status("Active Deployment", app_info.get("active_deployment", {}).get("id", "N/A")[:8])
                
                # Show component status
                components = app_info.get("spec", {}).get("services", [])
                if components:
                    print(f"\n{Colors.BOLD}Components:{Colors.END}")
                    for comp in components:
                        print(f"  • {comp.get('name', 'Unknown')}: {comp.get('instance_size_slug', 'N/A')}")
            else:
                print_status("API Access", f"Failed: {api_data.get('error', 'Unknown error')}", Colors.RED)
    else:
        if not args.json:
            print(f"\n{Colors.YELLOW}ℹ No API token provided. Skipping detailed metrics.{Colors.END}")
            print(f"{Colors.YELLOW}  To get full metrics, provide --api-token or set DO_API_TOKEN env var{Colors.END}")
    
    # Print summary
    if not args.json:
        print_header("Summary")
        
        total = results["summary"]["total_endpoints"]
        healthy = results["summary"]["healthy_endpoints"]
        failed = results["summary"]["failed_endpoints"]
        avg_time = results["summary"]["average_response_time_ms"]
        
        health_percentage = (healthy / total * 100) if total > 0 else 0
        
        if health_percentage == 100:
            overall_color = Colors.GREEN
            overall_status = "✓ ALL SYSTEMS OPERATIONAL"
        elif health_percentage >= 75:
            overall_color = Colors.YELLOW
            overall_status = "⚠ DEGRADED PERFORMANCE"
        else:
            overall_color = Colors.RED
            overall_status = "✗ SYSTEM ISSUES DETECTED"
        
        print(f"{Colors.BOLD}{overall_color}{overall_status}{Colors.END}\n")
        print_status("Healthy Endpoints", f"{healthy}/{total}", Colors.GREEN if healthy == total else Colors.YELLOW)
        print_status("Failed Endpoints", f"{failed}/{total}", Colors.RED if failed > 0 else Colors.GREEN)
        print_status("Average Response Time", f"{avg_time}ms", 
                    Colors.GREEN if avg_time < 500 else Colors.YELLOW if avg_time < 1000 else Colors.RED)
        
        # Recommendations
        print(f"\n{Colors.BOLD}Recommendations:{Colors.END}")
        if failed > 0:
            print(f"  {Colors.RED}• Investigate failed endpoints immediately{Colors.END}")
        if avg_time > 1000:
            print(f"  {Colors.YELLOW}• Response times are slow (>1s). Consider scaling up.{Colors.END}")
        elif avg_time > 500:
            print(f"  {Colors.YELLOW}• Response times are acceptable but could be improved.{Colors.END}")
        else:
            print(f"  {Colors.GREEN}• Response times are excellent (<500ms).{Colors.END}")
        
        print(f"\n{Colors.BOLD}Next Steps:{Colors.END}")
        print(f"  • Check logs: https://cloud.digitalocean.com/apps/{APP_ID}/logs")
        print(f"  • View metrics: https://cloud.digitalocean.com/apps/{APP_ID}/metrics")
        print(f"  • App settings: https://cloud.digitalocean.com/apps/{APP_ID}/settings")
        
    else:
        # JSON output
        print(json.dumps(results, indent=2))
    
    # Exit with appropriate code
    if results["summary"]["failed_endpoints"] > 0:
        sys.exit(1)
    else:
        sys.exit(0)


if __name__ == "__main__":
    main()
