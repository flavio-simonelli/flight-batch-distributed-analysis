#!/bin/bash

# Deployment script for the Metrics Node of the Sky Analytics Engine.
# This instance runs Redis Stack (for performance metrics) and Grafana.
# It provides the visualization layer for the entire cluster.

echo "[DEPLOY] Starting Metrics Node Deployment..."

# S3 Bucket name provided as first argument or defaulting to standard project bucket.
BUCKET_NAME="${1:-spark-flight-analysis}"

# Navigate to the home directory for setup.
cd /home/ec2-user

# Download the metrics-specific compose file and Grafana provisioning assets from S3.
echo "[DEPLOY] Downloading Metrics configuration and Grafana dashboards..."
aws s3 cp "s3://${BUCKET_NAME}/deploy/compose/metrics-compose.yml" "docker-compose.yml"
aws s3 cp "s3://${BUCKET_NAME}/grafana/" "grafana/" --recursive

# Move the environment file to the current directory for Docker Compose to read.
if [ -f "/home/ec2-user/.env" ]; then
    mv /home/ec2-user/.env .env
fi

# Launch the Metrics stack via Docker Compose.
# Grafana is configured to auto-install the Redis datasource plugin.
echo "[DEPLOY] Launching Metrics containers..."
docker-compose up -d

# Transfer ownership of the application directory to the ec2-user.
echo "[DEPLOY] Finalizing permissions and ownership..."
sudo chown -R ec2-user:ec2-user /home/ec2-user

echo "[DEPLOY] Metrics Node deployed successfully."
