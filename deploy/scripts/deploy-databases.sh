#!/bin/bash

# Deployment script for the Databases Node of the Sky Analytics Engine.
# This instance hosts various storage engines (Postgres, MongoDB, HBase, Redis Output).
# It is used for persisting the final results of the analytical queries.

echo "[DEPLOY] Starting Databases Node Deployment..."

# S3 Bucket name provided as first argument or defaulting to standard project bucket.
BUCKET_NAME="${1:-spark-flight-analysis}"

# Set up the local directory structure for data persistence.
cd /home/ec2-user
echo "[DEPLOY] Provisioning local database storage..."
mkdir -p data/postgres

# Download the database-specific compose file and the DB selector script from S3.
echo "[DEPLOY] Downloading database configurations and management scripts..."
aws s3 cp "s3://${BUCKET_NAME}/deploy/compose/databases-compose.yml" "docker-compose.yml"
aws s3 cp "s3://${BUCKET_NAME}/deploy/scripts/db-selector.sh" "db-selector.sh"

# Ensure the database selector script is executable.
chmod +x db-selector.sh

# Move the node-specific environment file to the root directory.
if [ -f "/home/ec2-user/.env" ]; then
    mv /home/ec2-user/.env .env
fi

# We do not start any database automatically. 
# The operator must use the ./db-selector.sh script to activate desired databases.
echo "[DEPLOY] Database Node setup complete. Use ./db-selector.sh to manage services."

# Transfer ownership of the directory to the ec2-user.
echo "[DEPLOY] Finalizing ownership settings..."
sudo chown -R ec2-user:ec2-user /home/ec2-user

echo "[DEPLOY] Databases Node deployed successfully."
