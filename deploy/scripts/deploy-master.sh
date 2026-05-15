#!/bin/bash

echo "[DEPLOY] Starting Flight Analytics Engine Deployment on Master Node..."

BUCKET_NAME="${1:-spark-flight-analysis}"

# Prepare directory
cd /home/ec2-user

# Download the Data folder from S3
echo "[DEPLOY] Downloading Data folder..."
aws s3 cp "s3://${BUCKET_NAME}/data/" "data/" --recursive
aws s3 cp "s3://${BUCKET_NAME}/flight-analysis.jar" "spark/flight-analysis.jar"

# Download the Docker Compose file from S3
echo "[DEPLOY] Downloading Docker Compose file..."
aws s3 cp "s3://${BUCKET_NAME}/deploy/compose/master-compose.yml" "docker-compose.yml"
aws s3 cp "s3://${BUCKET_NAME}/deploy/configs/workers" "workers"
aws s3 cp "s3://${BUCKET_NAME}/deploy/configs/core-site.xml" "core-site.xml"
aws s3 cp "s3://${BUCKET_NAME}/deploy/configs/hdfs-site.xml" "hdfs-site.xml"

# Move the .env file to the current directory (downloaded previously by UserData)
mv /home/ec2-user/.env .env

# Pull and Start the containers
echo "[DEPLOY] Launching containers..."
docker-compose up -d

echo "[DEPLOY] Master Engine deployed successfully."
