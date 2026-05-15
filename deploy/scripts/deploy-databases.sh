#!/bin/bash

echo "[DEPLOY] Starting Databases Node Deployment (Postgres, Redis, HBase)..."

BUCKET_NAME="${1:-spark-flight-analysis}"

cd /home/ec2-user
mkdir -p data/postgres

# Download files
aws s3 cp "s3://${BUCKET_NAME}/deploy/compose/databases-compose.yml" "docker-compose.yml"
aws s3 cp "s3://${BUCKET_NAME}/deploy/scripts/db-selector.sh" "db-selector.sh"
chmod +x db-selector.sh

mv /home/ec2-user/.env .env

# Fix ownership
sudo chown -R ec2-user:ec2-user /home/ec2-user

# We don't start any DB automatically as per user request to use selector
echo "[DEPLOY] Databases Node deployed. Use ./db-selector.sh to start a database."
