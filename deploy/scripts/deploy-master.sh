#!/bin/bash

# Deployment script for the Master Node of the Sky Analytics Engine.
# This instance hosts the HDFS NameNode and the Spark Master.
# It is responsible for orchestrating the storage and processing layers.

echo "[DEPLOY] Starting Master Node Deployment..."

# S3 Bucket name provided as first argument or defaulting to standard project bucket.
BUCKET_NAME="${1:-spark-flight-analysis}"

# Navigate to the user's home directory to prepare the environment.
cd /home/ec2-user

# Provision the local directory structure for Spark scripts and data staging.
echo "[DEPLOY] Creating local directory structure..."
mkdir -p data spark

# Download application artifacts, configuration files, and helper scripts from S3.
# These include the Spark application JAR, HDFS configurations, and orchestrator scripts.
echo "[DEPLOY] Downloading application artifacts and configurations..."
aws s3 cp "s3://${BUCKET_NAME}/spark/" "spark/" --recursive
aws s3 cp "s3://${BUCKET_NAME}/deploy/compose/master-compose.yml" "docker-compose.yml"
aws s3 cp "s3://${BUCKET_NAME}/deploy/configs/workers" "workers"
aws s3 cp "s3://${BUCKET_NAME}/deploy/configs/core-site.xml" "core-site.xml"
aws s3 cp "s3://${BUCKET_NAME}/deploy/configs/hdfs-site.xml" "hdfs-site.xml"
aws s3 cp "s3://${BUCKET_NAME}/deploy/scripts/spark-submit.sh" "spark-submit.sh"
aws s3 cp "s3://${BUCKET_NAME}/deploy/scripts/hdfs-helper.sh" "hdfs-helper.sh"

# Ensure all downloaded helper scripts have the executable bit set.
chmod +x spark-submit.sh hdfs-helper.sh

# Move the environment file to the working directory.
# The UserData script downloads this file from S3 to the home directory initially.
if [ -f "/home/ec2-user/.env" ]; then
    mv /home/ec2-user/.env .env
fi

# Launch the Master stack using Docker Compose.
# This starts the HDFS NameNode and Spark Master containers.
echo "[DEPLOY] Launching containers via Docker Compose..."
docker-compose up -d

# Transfer ownership of all home directory contents to ec2-user.
# This ensures that subsequent manual operations (like Spark submissions) do not require sudo.
echo "[DEPLOY] Finalizing file permissions and ownership..."
sudo chown -R ec2-user:ec2-user /home/ec2-user

echo "[DEPLOY] Master Node deployed successfully."
