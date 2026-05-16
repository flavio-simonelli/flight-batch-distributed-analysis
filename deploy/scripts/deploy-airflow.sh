#!/bin/bash

# Deployment script for the Airflow Node on EC2
# This script handles the initialization of the Airflow environment, 
# including downloading configurations, building custom Docker images, 
# and launching the Airflow stack using Docker Compose.

echo "[DEPLOY] Starting Airflow Node Deployment..."

# S3 Bucket name passed as an argument or using default
BUCKET_NAME="${1:-spark-flight-analysis}"

# Navigate to the home directory
cd /home/ec2-user
mkdir -p data/airflow-postgres

# Download deployment configurations and necessary application folders from S3
echo "[DEPLOY] Downloading deployment configurations and application folders..."
aws s3 cp "s3://${BUCKET_NAME}/deploy/compose/airflow-compose.yml" "docker-compose.yml"
aws s3 cp "s3://${BUCKET_NAME}/airflow/" "airflow/" --recursive
aws s3 cp "s3://${BUCKET_NAME}/livy/" "livy/" --recursive
aws s3 cp "s3://${BUCKET_NAME}/spark/" "spark/" --recursive

# Move the environment file to the current directory
# This file is downloaded automatically by the instance UserData from S3
if [ -f "/home/ec2-user/.env" ]; then
    mv /home/ec2-user/.env .env
fi

# Build custom images and start the Airflow stack
# Using --build to ensure custom Dockerfiles in airflow/ and livy/ are processed
echo "[DEPLOY] Launching Airflow stack and Livy server..."
docker-compose up --build -d

# Fix ownership of all files to ec2-user to allow easier management without sudo
echo "[DEPLOY] Finalizing file permissions..."
sudo chown -R ec2-user:ec2-user /home/ec2-user

echo "[DEPLOY] Airflow Node deployed successfully."
