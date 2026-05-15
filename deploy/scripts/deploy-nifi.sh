#!/bin/bash

echo "[DEPLOY] Starting NiFi Node Deployment..."

BUCKET_NAME="${1:-spark-flight-analysis}"

cd /home/ec2-user
mkdir -p data/raw data/conv

# Download files
echo "[DEPLOY] Downloading configurations, extensions, and raw data..."
aws s3 cp "s3://${BUCKET_NAME}/deploy/compose/nifi-compose.yml" "docker-compose.yml"
aws s3 cp "s3://${BUCKET_NAME}/deploy/configs/" "hadoop-config/" --recursive
aws s3 cp "s3://${BUCKET_NAME}/nifi/extensions/" "nar_extensions/" --recursive
aws s3 cp "s3://${BUCKET_NAME}/data/raw/" "data/raw/" --recursive

mv /home/ec2-user/.env .env

# Start containers
echo "[DEPLOY] Launching NiFi..."
docker-compose up -d

# Fix ownership
sudo chown -R ec2-user:ec2-user /home/ec2-user

echo "[DEPLOY] NiFi Node deployed successfully."
