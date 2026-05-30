#!/bin/bash

# --- ENVIRONMENT INITIALIZATION ---
# Determine the location of the deployment scripts to ensure all relative 
# path resolutions are consistent regardless of the working directory.
SCRIPTS_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
PROJECT_ROOT="${SCRIPTS_DIR}/.."

# Load environment variables from the .env file located in the deploy folder.
# This provides the script with required S3 bucket names and AWS region settings.
source "${SCRIPTS_DIR}/load_env.sh"
if [ $? -ne 0 ]; then
    echo "[ERROR] Failed to initialize environment variables. Ensure .env exists."
    exit 1
fi

# Validate that essential AWS variables are properly defined before proceeding.
if [ -z "$BUCKET_NAME" ]; then
    echo "[ERROR] BUCKET_NAME is not defined in the environment."
    exit 1
fi

echo "----------------------------------------------------"
echo "[INFO] AWS S3 BUCKET PROVISIONING"
echo "----------------------------------------------------"
echo "BUCKET: ${BUCKET_NAME}"
echo "REGION: ${REGION}"
echo "----------------------------------------------------"

# --- BUCKET VALIDATION AND CREATION ---
# Check if the target S3 bucket exists;
# create it if it is missing in the specified region.
aws s3api head-bucket --bucket "${BUCKET_NAME}" >/dev/null 2>&1
if [ $? -eq 0 ]; then
    echo "[INFO] Bucket already exists. Skipping creation."
else
    echo "[INFO] Bucket does not exist. Creating it now in ${REGION}..."
    aws s3 mb "s3://${BUCKET_NAME}" --region "${REGION}"
    if [ $? -ne 0 ]; then
        echo "[ERROR] Failed to create bucket. Verify IAM permissions or naming rules."
        exit 1
    fi
    echo "[INFO] Bucket created successfully."
fi

# --- FOLDER STRUCTURE PROVISIONING ---
# Create the logical folder structure within the bucket.
"${SCRIPTS_DIR}/create_bucket_folders.sh" "logs" "deploy" "data" "data/raw" "data/conv" "data/res"

# --- DEPLOYMENT ASSETS SYNCHRONIZATION ---
# Sync the local deploy directory containing templates, compose files, and bash scripts.
echo "[INFO] Syncing deployment scripts and configurations..."
aws s3 sync "${SCRIPTS_DIR}/" "s3://${BUCKET_NAME}/deploy/" --exclude "*.sh" --exclude ".env" --exclude "template/*"

# Sync NiFi custom extensions, flows and configurations from the project structure.
if [ -d "${PROJECT_ROOT}/nifi/" ]; then
    echo "[INFO] Syncing NiFi application assets..."
    aws s3 sync "${PROJECT_ROOT}/nifi/" "s3://${BUCKET_NAME}/nifi/"
fi

# Sync Grafana dashboard and datasource provisioning configurations.
if [ -d "${PROJECT_ROOT}/grafana/" ]; then
    echo "[INFO] Syncing Grafana dashboards and datasources..."
    aws s3 sync "${PROJECT_ROOT}/grafana/" "s3://${BUCKET_NAME}/grafana/"
fi

# Sync Airflow DAGs, plugins, and custom configurations.
if [ -d "${PROJECT_ROOT}/airflow/" ]; then
    echo "[INFO] Syncing Airflow orchestration assets..."
    aws s3 sync "${PROJECT_ROOT}/airflow/" "s3://${BUCKET_NAME}/airflow/"
fi

# Sync Livy server custom configurations and Docker files.
if [ -d "${PROJECT_ROOT}/livy/" ]; then
    echo "[INFO] Syncing Livy server assets..."
    aws s3 sync "${PROJECT_ROOT}/livy/" "s3://${BUCKET_NAME}/livy/"
fi

# Upload the Spark application JAR and configuration files.
# We avoid syncing the entire 'spark/' directory to exclude source code and build overhead.
if [ -f "${PROJECT_ROOT}/spark/target/flight-analysis.jar" ]; then
    echo "[INFO] Uploading Spark application JAR..."
    aws s3 cp "${PROJECT_ROOT}/spark/target/flight-analysis.jar" "s3://${BUCKET_NAME}/spark/flight-analysis.jar"
fi

if [ -d "${PROJECT_ROOT}/spark/src/main/resources/" ]; then
    echo "[INFO] Syncing Spark configuration files..."
    aws s3 sync "${PROJECT_ROOT}/spark/src/main/resources/" "s3://${BUCKET_NAME}/spark/" --exclude "*" --include "*.yml"
fi

# --- DATASET SYNCHRONIZATION ---
# Upload the raw project data used as input for the processing pipeline.
if [ -d "${PROJECT_ROOT}/data/zip/" ]; then
    echo "[INFO] Syncing raw input datasets to the storage layer..."
    aws s3 sync "${PROJECT_ROOT}/data/zip/" "s3://${BUCKET_NAME}/data/zip/"
fi

echo ""
echo "----------------------------------------------------"
echo "[INFO] S3 Synchronization completed successfully!"
echo "Assets location: s3://${BUCKET_NAME}/"
echo "----------------------------------------------------"

exit 0