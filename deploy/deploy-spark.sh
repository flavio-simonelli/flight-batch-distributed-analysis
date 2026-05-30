#!/bin/bash

# --- SPARK CLUSTER DEPLOYMENT ORCHESTRATOR ---
# This script manages the provisioning of an Amazon EMR cluster configured 
# for Spark analytical processing, utilizing the project's S3 assets.

# --- ENVIRONMENT INITIALIZATION ---
source load_env.sh
if [ $? -ne 0 ]; then
    echo "[ERROR] Environment initialization failed."
    exit 1
fi

echo "----------------------------------------------------"
echo "[INFO] SPARK EMR CLUSTER PROVISIONING"
echo "Cluster Name: ${SPARK_CLUSTER_NAME}"
echo "Core Nodes:   ${SPARK_CORE_COUNT}"
echo "----------------------------------------------------"

echo "[INFO] Initiating pre-deployment validation..."

# --- SSH ACCESS VERIFICATION ---
# Ensure that the cryptographic key required for node access is properly provisioned.
source setup_ssh_key.sh
if [ $? -ne 0 ]; then
    echo "[ERROR] SSH key setup failed. Provisioning aborted."
    exit 1
fi

# --- NETWORK STATE VALIDATION ---
# Discover the target subnet using its Name tag to ensure the cluster is launched in the correct VPC segment.
echo "[INFO] Discovering network infrastructure context..."

SUBNET_ID=$(aws ec2 describe-subnets --filters "Name=tag:Name,Values=${SUBNET_NAME}" --query "Subnets[0].SubnetId" --output text 2>/dev/null)

# Verify that the network segment exists; EMR deployment requires a pre-existing subnet.
if [ -z "$SUBNET_ID" ] || [ "$SUBNET_ID" == "None" ]; then
    echo "[ERROR] Network infrastructure '${SUBNET_NAME}' not found. Run deploy-network.sh first."
    exit 1
fi

echo "[INFO] Target network segment identified: ${SUBNET_ID}"

# --- EMR CLUSTER PROVISIONING ---
# Deploy the EMR cluster stack via CloudFormation using the spark-emr template.
echo "[INFO] Launching Spark EMR Infrastructure stack..."

CLUSTER_STACK_NAME="${SPARK_CLUSTER_NAME}-stack-${RANDOM}"

aws cloudformation create-stack \
  --stack-name "${CLUSTER_STACK_NAME}" \
  --template-body "file://template/spark-emr.yaml" \
  --parameters \
      ParameterKey=ClusterName,ParameterValue="${SPARK_CLUSTER_NAME}" \
      ParameterKey=KeyName,ParameterValue="${SSH_KEY_NAME}" \
      ParameterKey=SubnetId,ParameterValue="${SUBNET_ID}" \
      ParameterKey=CoreInstanceCount,ParameterValue="${SPARK_CORE_COUNT}"

if [ $? -eq 0 ]; then
    echo "[INFO] EMR Cluster provisioning request successfully submitted to CloudFormation."
else
    echo "[ERROR] Failed to initiate Spark EMR Cluster stack creation."
fi

exit 0