#!/bin/bash

# --- DEPLOYMENT ORCHESTRATOR FOR SKY ANALYTICS ENGINE ---
# This script manages the deployment of various node types across the EC2 cluster.
# It supports targeted deployment (master, worker, metrics, databases, nifi, airflow)
# or full cluster deployment (all).

# --- PARAMETER CHECK ---
TARGET="${1:-all}"
TARGET=$(echo "$TARGET" | tr '[:upper:]' '[:lower:]')

# Validate the provided target against supported node types.
if [[ ! "$TARGET" =~ ^(master|worker|metrics|databases|nifi|airflow|all)$ ]]; then
    echo "[ERROR] Invalid deployment target: ${TARGET}"
    echo "Usage: ./deploy-node.sh [master | worker | metrics | databases | nifi | airflow | all]"
    exit 1
fi

# --- ENVIRONMENT INITIALIZATION ---
source load_env.sh
if [ $? -ne 0 ]; then
    echo "[ERROR] Failed to load environment variables. Ensure .env exists in the current directory."
    exit 1
fi

# --- SSH KEY PROVISIONING ---
source setup-ssh-key.sh
if [ $? -ne 0 ]; then
    echo "[ERROR] SSH key setup failed. Deployment aborted."
    exit 1
fi

echo "[INFO] Retrieving Cloud Infrastructure context..."

# --- INFRASTRUCTURE DISCOVERY ---
SUBNET_ID=$(aws ec2 describe-subnets --filters "Name=tag:Name,Values=${SUBNET_NAME}" --query "Subnets[0].SubnetId" --output text 2>/dev/null)
SG_ID=$(aws ec2 describe-security-groups --filters "Name=group-name,Values=${SG_NAME}" --query "SecurityGroups[0].GroupId" --output text 2>/dev/null)

RAW_ZONE_ID=$(aws route53 list-hosted-zones-by-name --dns-name "${PRIVATE_DOMAIN_NAME}" --query "HostedZones[0].Id" --output text 2>/dev/null)
ZONE_ID=${RAW_ZONE_ID#/hostedzone/}

echo "[INFO] Infrastructure Discovery Results:"
echo "       Subnet: ${SUBNET_ID}"
echo "       Security Group: ${SG_ID}"
echo "       DNS Zone: ${ZONE_ID}"

if [ -z "$SUBNET_ID" ] || [ "$SUBNET_ID" == "None" ]; then
    echo "[ERROR] Required network infrastructure is missing. Run deploy-network.sh first."
    read -p "Press any key to exit..." -n1 -s
    exit 1
fi

# --- SUBROUTINES ---
deploy_worker() {
    local N=$1
    # Contains HDFS DataNode and Spark Worker.
    echo "[INFO] Cleaning up stale SSH host keys for worker-node-${N}..."
    if [ -f "$KH_PATH" ]; then sed -i.bak "/worker-node-${N}/d" "$KH_PATH"; fi

    echo "[INFO] Launching Worker Instance ${N}..."
    aws cloudformation deploy \
      --stack-name "${SPARK_CLUSTER_NAME}-worker-node-${N}" \
      --template-file "template/cluster-node.yaml" \
      --parameter-overrides \
          SubnetId="${SUBNET_ID}" \
          SecurityGroupId="${SG_ID}" \
          HostedZoneId="${ZONE_ID}" \
          KeyName="${SSH_KEY_NAME}" \
          NodeHostname="worker-${N}.${PRIVATE_DOMAIN_NAME}" \
          InstanceName="Worker-Node-${N}" \
          S3Bucket="${BUCKET_NAME}" \
          EnvironmentFile="worker.env" \
          DeployScripts="deploy-worker.sh" \
      --capabilities CAPABILITY_IAM \
      --no-fail-on-empty-changeset

    echo "[INFO] Creating DNS aliases for Worker ${N} (hdfs-worker-${N}, spark-worker-${N})..."
    local DNS_BATCH_FILE="/tmp/dns-worker-${N}-aliases.json"
cat <<EOF > "$DNS_BATCH_FILE"
{
  "Comment": "Creating aliases for Worker ${N}",
  "Changes": [
    { "Action": "UPSERT", "ResourceRecordSet": { "Name": "hdfs-worker-${N}.${PRIVATE_DOMAIN_NAME}", "Type": "CNAME", "TTL": 300, "ResourceRecords": [{ "Value": "worker-${N}.${PRIVATE_DOMAIN_NAME}" }] } },
    { "Action": "UPSERT", "ResourceRecordSet": { "Name": "spark-worker-${N}.${PRIVATE_DOMAIN_NAME}", "Type": "CNAME", "TTL": 300, "ResourceRecords": [{ "Value": "worker-${N}.${PRIVATE_DOMAIN_NAME}" }] } }
  ]
}
EOF

    aws route53 change-resource-record-sets --hosted-zone-id "${ZONE_ID}" --change-batch file://"${DNS_BATCH_FILE}"
    if [ $? -eq 0 ]; then
        echo "[INFO] DNS aliases for Worker ${N} created successfully."
    else
        echo "[WARN] Failed to create DNS aliases for Worker ${N}."
    fi
    rm -f "$DNS_BATCH_FILE"
}

# --- NODE DEPLOYMENT PHASE ---
echo "[INFO] Initiating deployment for target: ${TARGET}..."

KH_PATH="${HOME}/.ssh/known_hosts"

# --- MASTER NODE DEPLOYMENT ---
# Contains HDFS NameNode and Spark Master.
if [[ "$TARGET" == "master" || "$TARGET" == "all" ]]; then
    MASTER_STACK_NAME="${SPARK_CLUSTER_NAME}-master-node"

    echo "[INFO] Cleaning up stale SSH host keys for master-node..."
    if [ -f "$KH_PATH" ]; then sed -i.bak '/master-node/d' "$KH_PATH"; fi

    echo "[INFO] Deploying Master Node via CloudFormation..."
    aws cloudformation deploy \
      --stack-name "${MASTER_STACK_NAME}" \
      --template-file "template/cluster-node.yaml" \
      --parameter-overrides \
          SubnetId="${SUBNET_ID}" \
          SecurityGroupId="${SG_ID}" \
          HostedZoneId="${ZONE_ID}" \
          KeyName="${SSH_KEY_NAME}" \
          NodeHostname="master.${PRIVATE_DOMAIN_NAME}" \
          InstanceName="Master-Node" \
          S3Bucket="${BUCKET_NAME}" \
          EnvironmentFile="master.env" \
          DeployScripts="deploy-master.sh" \
      --capabilities CAPABILITY_IAM \
      --no-fail-on-empty-changeset

    echo "[INFO] Creating additional DNS aliases for Master (hdfs-master, spark-master)..."
    DNS_BATCH_FILE="/tmp/dns-master-aliases.json"
cat <<EOF > "$DNS_BATCH_FILE"
{
  "Comment": "Creating aliases for HDFS and Spark",
  "Changes": [
    { "Action": "UPSERT", "ResourceRecordSet": { "Name": "hdfs-master.${PRIVATE_DOMAIN_NAME}", "Type": "CNAME", "TTL": 300, "ResourceRecords": [{ "Value": "master.${PRIVATE_DOMAIN_NAME}" }] } },
    { "Action": "UPSERT", "ResourceRecordSet": { "Name": "spark-master.${PRIVATE_DOMAIN_NAME}", "Type": "CNAME", "TTL": 300, "ResourceRecords": [{ "Value": "master.${PRIVATE_DOMAIN_NAME}" }] } }
  ]
}
EOF

    aws route53 change-resource-record-sets --hosted-zone-id "${ZONE_ID}" --change-batch file://"${DNS_BATCH_FILE}"
    if [ $? -eq 0 ]; then
        echo "[INFO] DNS aliases for Master created successfully."
    else
        echo "[WARN] Failed to create DNS aliases for Master."
    fi
    rm -f "$DNS_BATCH_FILE"
fi

# --- WORKER NODES DEPLOYMENT ---
# Distributed instances for HDFS DataNodes and Spark Workers.
if [[ "$TARGET" == "worker" || "$TARGET" == "all" ]]; then
    echo "[INFO] Deploying ${WORKER_COUNT} Worker Nodes..."
    for (( N=1; N<=WORKER_COUNT; N++ )); do
        deploy_worker "$N"
    done
fi

# --- METRICS NODE DEPLOYMENT ---
# Visualization layer with Redis Stack and Grafana.
if [[ "$TARGET" == "metrics" || "$TARGET" == "all" ]]; then
    echo "[INFO] Cleaning up stale SSH host keys for metrics-node..."
    if [ -f "$KH_PATH" ]; then sed -i.bak '/metrics-node/d' "$KH_PATH"; fi

    echo "[INFO] Deploying Metrics Node via CloudFormation..."
    aws cloudformation deploy \
      --stack-name "${SPARK_CLUSTER_NAME}-metrics-node" \
      --template-file "template/cluster-node.yaml" \
      --parameter-overrides \
          SubnetId="${SUBNET_ID}" \
          SecurityGroupId="${SG_ID}" \
          HostedZoneId="${ZONE_ID}" \
          KeyName="${SSH_KEY_NAME}" \
          NodeHostname="metrics.${PRIVATE_DOMAIN_NAME}" \
          InstanceName="Metrics-Node" \
          S3Bucket="${BUCKET_NAME}" \
          EnvironmentFile="metrics.env" \
          DeployScripts="deploy-metrics.sh" \
      --capabilities CAPABILITY_IAM \
      --no-fail-on-empty-changeset
fi

# --- NIFI NODE DEPLOYMENT ---
# Ingestion layer with Apache NiFi and local raw datasets.
if [[ "$TARGET" == "nifi" || "$TARGET" == "all" ]]; then
    echo "[INFO] Cleaning up stale SSH host keys for nifi-node..."
    if [ -f "$KH_PATH" ]; then sed -i.bak '/nifi-node/d' "$KH_PATH"; fi

    echo "[INFO] Deploying NiFi Node via CloudFormation..."
    aws cloudformation deploy \
      --stack-name "${SPARK_CLUSTER_NAME}-nifi-node" \
      --template-file "template/cluster-node.yaml" \
      --parameter-overrides \
          SubnetId="${SUBNET_ID}" \
          SecurityGroupId="${SG_ID}" \
          HostedZoneId="${ZONE_ID}" \
          KeyName="${SSH_KEY_NAME}" \
          NodeHostname="nifi.${PRIVATE_DOMAIN_NAME}" \
          InstanceName="NiFi-Node" \
          S3Bucket="${BUCKET_NAME}" \
          EnvironmentFile="nifi.env" \
          DeployScripts="deploy-nifi.sh" \
      --capabilities CAPABILITY_IAM \
      --no-fail-on-empty-changeset
fi

# --- AIRFLOW NODE DEPLOYMENT ---
# Orchestration layer with Apache Airflow and Livy server.
if [[ "$TARGET" == "airflow" || "$TARGET" == "all" ]]; then
    echo "[INFO] Cleaning up stale SSH host keys for airflow-node..."
    if [ -f "$KH_PATH" ]; then sed -i.bak '/airflow-node/d' "$KH_PATH"; fi

    echo "[INFO] Deploying Airflow Node via CloudFormation..."
    aws cloudformation deploy \
      --stack-name "${SPARK_CLUSTER_NAME}-airflow-node" \
      --template-file "template/cluster-node.yaml" \
      --parameter-overrides \
          SubnetId="${SUBNET_ID}" \
          SecurityGroupId="${SG_ID}" \
          HostedZoneId="${ZONE_ID}" \
          KeyName="${SSH_KEY_NAME}" \
          NodeHostname="airflow.${PRIVATE_DOMAIN_NAME}" \
          InstanceName="Airflow-Node" \
          S3Bucket="${BUCKET_NAME}" \
          EnvironmentFile="airflow.env" \
          DeployScripts="deploy-airflow.sh" \
          InstanceType="t3.medium" \
      --capabilities CAPABILITY_IAM \
      --no-fail-on-empty-changeset

    echo "[INFO] Creating additional DNS aliases for Airflow (livy-airflow)..."
    DNS_BATCH_FILE="/tmp/dns-airflow-aliases.json"
cat <<EOF > "$DNS_BATCH_FILE"
{
  "Comment": "Creating aliases for Livy",
  "Changes": [
    { "Action": "UPSERT", "ResourceRecordSet": { "Name": "livy-airflow.${PRIVATE_DOMAIN_NAME}", "Type": "CNAME", "TTL": 300, "ResourceRecords": [{ "Value": "airflow.${PRIVATE_DOMAIN_NAME}" }] } }
  ]
}
EOF

    aws route53 change-resource-record-sets --hosted-zone-id "${ZONE_ID}" --change-batch file://"${DNS_BATCH_FILE}"
    if [ $? -eq 0 ]; then
        echo "[INFO] DNS aliases for Airflow created successfully."
    else
        echo "[WARN] Failed to create DNS aliases for Airflow."
    fi
    rm -f "$DNS_BATCH_FILE"
fi

# --- DATABASES NODE DEPLOYMENT ---
# Persistence layer with PostgreSQL, HBase, and Redis Output.
if [[ "$TARGET" == "databases" || "$TARGET" == "all" ]]; then
    echo "[INFO] Cleaning up stale SSH host keys for databases-node..."
    if [ -f "$KH_PATH" ]; then sed -i.bak '/databases-node/d' "$KH_PATH"; fi

    echo "[INFO] Deploying Databases Node via CloudFormation..."
    aws cloudformation deploy \
      --stack-name "${SPARK_CLUSTER_NAME}-databases-node" \
      --template-file "template/cluster-node.yaml" \
      --parameter-overrides \
          SubnetId="${SUBNET_ID}" \
          SecurityGroupId="${SG_ID}" \
          HostedZoneId="${ZONE_ID}" \
          KeyName="${SSH_KEY_NAME}" \
          NodeHostname="databases.${PRIVATE_DOMAIN_NAME}" \
          InstanceName="Databases-Node" \
          S3Bucket="${BUCKET_NAME}" \
          EnvironmentFile="databases.env" \
          DeployScripts="deploy-databases.sh" \
      --capabilities CAPABILITY_IAM \
      --no-fail-on-empty-changeset

    echo "[INFO] Creating additional DNS aliases for Databases (postgres, redis, hbase)..."
    DNS_BATCH_FILE="/tmp/dns-databases-aliases.json"
cat <<EOF > "$DNS_BATCH_FILE"
{
  "Comment": "Creating aliases for Databases",
  "Changes": [
    { "Action": "UPSERT", "ResourceRecordSet": { "Name": "postgres-databases.${PRIVATE_DOMAIN_NAME}", "Type": "CNAME", "TTL": 300, "ResourceRecords": [{ "Value": "databases.${PRIVATE_DOMAIN_NAME}" }] } },
    { "Action": "UPSERT", "ResourceRecordSet": { "Name": "cockroachdb-databases.${PRIVATE_DOMAIN_NAME}", "Type": "CNAME", "TTL": 300, "ResourceRecords": [{ "Value": "databases.${PRIVATE_DOMAIN_NAME}" }] } },
    { "Action": "UPSERT", "ResourceRecordSet": { "Name": "redis-databases.${PRIVATE_DOMAIN_NAME}", "Type": "CNAME", "TTL": 300, "ResourceRecords": [{ "Value": "databases.${PRIVATE_DOMAIN_NAME}" }] } },
    { "Action": "UPSERT", "ResourceRecordSet": { "Name": "hbase-databases.${PRIVATE_DOMAIN_NAME}", "Type": "CNAME", "TTL": 300, "ResourceRecords": [{ "Value": "databases.${PRIVATE_DOMAIN_NAME}" }] } }
  ]
}
EOF

    aws route53 change-resource-record-sets --hosted-zone-id "${ZONE_ID}" --change-batch file://"${DNS_BATCH_FILE}"
    if [ $? -eq 0 ]; then
        echo "[INFO] DNS aliases for Databases created successfully."
    else
        echo "[WARN] Failed to create DNS aliases for Databases."
    fi
    rm -f "$DNS_BATCH_FILE"
fi

echo "----------------------------------------------------"
echo "[SUCCESS] Deployment completed for target: ${TARGET}"
echo "----------------------------------------------------"
exit 0