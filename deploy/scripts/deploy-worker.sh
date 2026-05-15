#!/bin/bash

echo "[DEPLOY] Starting Flight Analytics Engine Deployment on Worker Node..."

BUCKET_NAME="${1:-spark-flight-analysis}"

# Prepare directory
cd /home/ec2-user
mkdir -p data

# Download the Docker Compose file from S3
echo "[DEPLOY] Downloading Docker Compose file..."
aws s3 cp "s3://${BUCKET_NAME}/deploy/compose/worker-compose.yml" "docker-compose.yml"
aws s3 cp "s3://${BUCKET_NAME}/deploy/configs/core-site.xml" "core-site.xml"
aws s3 cp "s3://${BUCKET_NAME}/deploy/configs/hdfs-site.xml" "hdfs-site.xml"

# Move the .env file to the current directory (downloaded previously by UserData)
mv /home/ec2-user/.env .env

# Pull and Start the containers
echo "[DEPLOY] Launching containers..."
docker-compose up -d

# Fix ownership
sudo chown -R ec2-user:ec2-user /home/ec2-user

echo "[DEPLOY] Worker Node deployed successfully."
