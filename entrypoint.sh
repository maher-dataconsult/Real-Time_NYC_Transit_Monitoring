#!/bin/bash
set -e

echo "🚀 Starting NYC Transit Data Pipeline Container..."

# Start Prefect Server in the background
echo "📡 Starting Prefect Server..."
prefect server start --host 0.0.0.0 &

# Wait for Prefect Server to be ready
echo "⏳ Waiting for Prefect Server to be ready..."
sleep 15

# Set the API URL for the worker
export PREFECT_API_URL="http://localhost:4200/api"

echo "✅ Prefect Server should be ready!"
echo "🌐 Prefect UI available at: http://localhost:4200"

# Run the main pipeline script
echo "🏃 Starting NYC Transit Pipeline..."
cd /app
python script00_prefect_pipeline.py
