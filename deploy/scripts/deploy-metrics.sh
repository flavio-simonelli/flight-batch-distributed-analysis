#!/bin/bash

echo "[DEPLOY] Starting Metrics Node Deployment (Redis + Grafana)..."

BUCKET_NAME="${1:-spark-flight-analysis}"

cd /home/ec2-user

# Download files
aws s3 cp "s3://${BUCKET_NAME}/deploy/compose/metrics-compose.yml" "docker-compose.yml"
aws s3 cp "s3://${BUCKET_NAME}/grafana/" "grafana/" --recursive

mv /home/ec2-user/.env .env

# Start containers
docker-compose up -d

# Fix ownership
sudo chown -R ec2-user:ec2-user /home/ec2-user

echo "[DEPLOY] Metrics Node deployed successfully."
