#!/bin/bash
# Deployment script for Paper Trading

# Ensure we are in the project root
cd "$(dirname "$0")"

# Set environment to paper
export ENVIRONMENT=paper

# Run the paper trading CLI
echo "Starting Paper Trading..."
echo "Press Ctrl+C to stop"
echo "--------------------------------"

python3 run.py paper --config src/config/config.yaml
