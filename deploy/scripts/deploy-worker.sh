#!/bin/bash

# Deployment script for a Worker Node of the Sky Analytics Engine.
# Each worker instance runs an HDFS DataNode and a Spark Worker container.
# These nodes connect back to the master to join the distributed cluster.

echo "[DEPLOY] Starting Worker Node Deployment..."

# Retrieve the S3 Bucket name from the first argument or use the project default.
BUCKET_NAME="${1:-spark-flight-analysis}"

# Prepare the local environment in the home directory.
cd /home/ec2-user
echo "[DEPLOY] Provisioning local data directories..."
mkdir -p data

# Download the specific worker configuration and shared Hadoop settings from S3.
echo "[DEPLOY] Synchronizing deployment files and Hadoop configurations..."
aws s3 cp "s3://${BUCKET_NAME}/deploy/compose/worker-compose.yml" "docker-compose.yml"
aws s3 cp "s3://${BUCKET_NAME}/deploy/configs/core-site.xml" "core-site.xml"
aws s3 cp "s3://${BUCKET_NAME}/deploy/configs/hdfs-site.xml" "hdfs-site.xml"

# Move the node-specific environment variables file to the root directory.
# This file contains the private IP of the master and AWS credentials.
if [ -f "/home/ec2-user/.env" ]; then
    mv /home/ec2-user/.env .env
fi

# Launch the Worker containers using Docker Compose.
# Both DataNode and Spark Worker are configured to use the host network.
echo "[DEPLOY] Launching worker containers..."
docker-compose up -d

# Adjust ownership of all files to ec2-user for maintenance tasks.
echo "[DEPLOY] Finalizing ownership settings..."
sudo chown -R ec2-user:ec2-user /home/ec2-user

echo "[DEPLOY] Worker Node deployed successfully."
