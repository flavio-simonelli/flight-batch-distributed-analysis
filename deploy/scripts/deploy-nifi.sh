#!/bin/bash

# Deployment script for the NiFi Node of the Sky Analytics Engine.
# This instance handles the ingestion layer, downloading raw data 
# and processing it through NiFi flows before storage in HDFS.

echo "[DEPLOY] Starting NiFi Node Deployment..."

# S3 Bucket name provided as first argument or defaulting to standard project bucket.
BUCKET_NAME="${1:-spark-flight-analysis}"

# Prepare the local environment for NiFi operations.
cd /home/ec2-user
echo "[DEPLOY] Creating data and extension directories..."
mkdir -p data/raw data/conv nar_extensions

# Download configurations, NAR extensions, and raw input data from S3.
echo "[DEPLOY] Downloading NiFi configurations, extensions, and raw datasets..."
aws s3 cp "s3://${BUCKET_NAME}/deploy/compose/nifi-compose.yml" "docker-compose.yml"
aws s3 cp "s3://${BUCKET_NAME}/deploy/configs/" "hadoop/config/" --recursive
aws s3 cp "s3://${BUCKET_NAME}/nifi/extensions/" "nifi/extensions/" --recursive
aws s3 cp "s3://${BUCKET_NAME}/data/" "data/" --recursive

# Move the environment file to the working directory for Docker Compose.
if [ -f "/home/ec2-user/.env" ]; then
    mv /home/ec2-user/.env .env
fi

# Launch the NiFi container using Docker Compose.
# NiFi uses host networking to expose its Web UI and Site-to-Site ports.
echo "[DEPLOY] Launching NiFi containers..."
docker-compose up -d

# Grant ownership of all downloaded assets to the ec2-user.
echo "[DEPLOY] Finalizing permissions and ownership..."
sudo chown -R ec2-user:ec2-user /home/ec2-user

echo "[DEPLOY] NiFi Node deployed successfully."
