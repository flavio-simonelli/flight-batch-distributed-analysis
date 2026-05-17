@echo off
setlocal enabledelayedexpansion

:: --- DEPLOYMENT ORCHESTRATOR FOR SKY ANALYTICS ENGINE ---
:: This script manages the deployment of various node types across the EC2 cluster.
:: It supports targeted deployment (master, worker, metrics, databases, nifi, airflow)
:: or full cluster deployment (all).

:: --- PARAMETER CHECK ---
:: The first argument defines the deployment target. Defaults to 'all' if omitted.
set "TARGET=%~1"
if "%TARGET%"=="" set "TARGET=all"

:: Validate the provided target against supported node types.
if /I "%TARGET%" NEQ "master" if /I "%TARGET%" NEQ "worker" if /I "%TARGET%" NEQ "metrics" if /I "%TARGET%" NEQ "databases" if /I "%TARGET%" NEQ "nifi" if /I "%TARGET%" NEQ "airflow" if /I "%TARGET%" NEQ "all" (
    echo [ERROR] Invalid deployment target: %TARGET%
    echo Usage: deploy-node.bat [master ^^| worker ^| metrics ^| databases ^| nifi ^| airflow ^| all]
    exit /b 1
)

:: --- ENVIRONMENT INITIALIZATION ---
:: Load configuration variables from the .env file.
call load_env.bat
if %ERRORLEVEL% neq 0 (
    echo [ERROR] Failed to load environment variables. Ensure .env exists in the current directory.
    exit /b 1
)

:: --- SSH KEY PROVISIONING ---
:: Ensure the SSH key pair exists in AWS and is saved locally with restrictive permissions.
call setup-ssh-key.bat
if %ERRORLEVEL% neq 0 (
    echo [ERROR] SSH key setup failed. Deployment aborted.
    exit /b 1
)

echo [INFO] Retrieving Cloud Infrastructure context...

:: --- INFRASTRUCTURE DISCOVERY ---
:: Retrieve the Subnet ID using its Name tag to target the correct VPC segment.
set "SUBNET_ID=None"
for /f "tokens=*" %%i in ('aws ec2 describe-subnets --filters "Name=tag:Name,Values=%SUBNET_NAME%" --query "Subnets[0].SubnetId" --output text') do set "SUBNET_ID=%%i"

:: Retrieve the Security Group ID to allow traffic between cluster nodes.
set "SG_ID=None"
for /f "tokens=*" %%i in ('aws ec2 describe-security-groups --filters "Name=group-name,Values=%SG_NAME%" --query "SecurityGroups[0].GroupId" --output text') do set "SG_ID=%%i"

:: Retrieve the Route53 Hosted Zone ID to manage private DNS records for the cluster.
set "ZONE_ID=None"
for /f "tokens=*" %%i in ('aws route53 list-hosted-zones-by-name --dns-name %PRIVATE_DOMAIN_NAME% --query "HostedZones[0].Id" --output text') do (
    set "RAW_ZONE_ID=%%i"
    set "ZONE_ID=!RAW_ZONE_ID:/hostedzone/=!"
)

echo [INFO] Infrastructure Discovery Results:
echo        Subnet: %SUBNET_ID%
echo        Security Group: %SG_ID%
echo        DNS Zone: %ZONE_ID%

:: Verify that all required network components were successfully identified.
if "%SUBNET_ID%"=="None" (
    echo [ERROR] Required network infrastructure is missing. Run deploy-network.bat first.
    pause
    exit /b 1
)

:: --- NODE DEPLOYMENT PHASE ---
echo [INFO] Initiating deployment for target: %TARGET%...

:: Define the local path to the SSH known_hosts file for cleanup operations.
set "KH_PATH=%USERPROFILE%\.ssh\known_hosts"

:: --- MASTER NODE DEPLOYMENT ---
:: Contains HDFS NameNode and Spark Master.
if /I "%TARGET%"=="worker" goto :skip_master
if /I "%TARGET%"=="metrics" goto :skip_master
if /I "%TARGET%"=="databases" goto :skip_master
if /I "%TARGET%"=="nifi" goto :skip_master
if /I "%TARGET%"=="airflow" goto :skip_master

set MASTER_STACK_NAME="%SPARK_CLUSTER_NAME%-master-node"

echo [INFO] Cleaning up stale SSH host keys for master-node...
powershell -Command "if (Test-Path '%KH_PATH%') { $c = Get-Content '%KH_PATH%'; $c | Where-Object { $_ -notmatch 'master-node' } | Set-Content '%KH_PATH%' }"

echo [INFO] Deploying Master Node via CloudFormation...
aws cloudformation deploy ^
  --stack-name "%MASTER_STACK_NAME%" ^
  --template-file "template/cluster-node.yaml" ^
  --parameter-overrides ^
      SubnetId=%SUBNET_ID% ^
      SecurityGroupId=%SG_ID% ^
      HostedZoneId=%ZONE_ID% ^
      KeyName=%SSH_KEY_NAME% ^
      NodeHostname="master.flight-analysis.local" ^
      InstanceName="Master-Node" ^
      S3Bucket=%BUCKET_NAME% ^
      EnvironmentFile="master.env" ^
      DeployScripts="deploy-master.sh" ^
  --capabilities CAPABILITY_IAM ^
  --no-fail-on-empty-changeset

echo [INFO] Creating additional DNS aliases for Master (hdfs-master, spark-master)...
set "DNS_BATCH_FILE=%TEMP%\dns-master-aliases.json"
echo { "Comment": "Creating aliases for HDFS and Spark", "Changes": [ { "Action": "UPSERT", "ResourceRecordSet": { "Name": "hdfs-master.%PRIVATE_DOMAIN_NAME%", "Type": "CNAME", "TTL": 300, "ResourceRecords": [{ "Value": "master.%PRIVATE_DOMAIN_NAME%" }] } }, { "Action": "UPSERT", "ResourceRecordSet": { "Name": "spark-master.%PRIVATE_DOMAIN_NAME%", "Type": "CNAME", "TTL": 300, "ResourceRecords": [{ "Value": "master.%PRIVATE_DOMAIN_NAME%" }] } } ] } > "!DNS_BATCH_FILE!"

aws route53 change-resource-record-sets --hosted-zone-id %ZONE_ID% --change-batch file://"!DNS_BATCH_FILE!"
if !ERRORLEVEL! equ 0 (
    echo [INFO] DNS aliases created successfully.
) else (
    echo [WARN] Failed to create DNS aliases.
)
del "!DNS_BATCH_FILE!"

:skip_master

:: --- WORKER NODES DEPLOYMENT ---
:: Distributed instances for HDFS DataNodes and Spark Workers.
if /I "%TARGET%"=="master" if /I "%TARGET%" NEQ "all" goto :skip_worker
if /I "%TARGET%"=="metrics" goto :skip_worker
if /I "%TARGET%"=="databases" goto :skip_worker
if /I "%TARGET%"=="nifi" goto :skip_worker
if /I "%TARGET%"=="airflow" goto :skip_worker

set WORKER_STACK_NAME="%SPARK_CLUSTER_NAME%-worker-node"

echo [INFO] Deploying %WORKER_COUNT% Worker Nodes...

for /L %%N in (1,1,%WORKER_COUNT%) do (
    echo [INFO] Cleaning up stale SSH host keys for worker-node-%%N...
    powershell -Command "if (Test-Path '%KH_PATH%') { $c = Get-Content '%KH_PATH%'; $c | Where-Object { $_ -notmatch 'worker-node-%%N' } | Set-Content '%KH_PATH%' }"

    echo [INFO] Launching Worker Instance %%N...
    aws cloudformation deploy ^
      --stack-name "%WORKER_STACK_NAME%-%%N" ^
      --template-file "template/cluster-node.yaml" ^
      --parameter-overrides ^
          SubnetId=%SUBNET_ID% ^
          SecurityGroupId=%SG_ID% ^
          HostedZoneId=%ZONE_ID% ^
          KeyName=%SSH_KEY_NAME% ^
          NodeHostname="worker-%%N.flight-analysis.local" ^
          InstanceName="Worker-Node-%%N" ^
          S3Bucket=%BUCKET_NAME% ^
          EnvironmentFile="worker.env" ^
          DeployScripts="deploy-worker.sh" ^
          --capabilities CAPABILITY_IAM ^
          --no-fail-on-empty-changeset

          echo [INFO] Creating DNS aliases for Worker %%N (hdfs-worker-%%N, spark-worker-%%N)...
          set "DNS_BATCH_FILE=%TEMP%\dns-worker-%%N-aliases.json"
          echo { "Comment": "Creating aliases for Worker %%N", "Changes": [ { "Action": "UPSERT", "ResourceRecordSet": { "Name": "hdfs-worker-%%N.%PRIVATE_DOMAIN_NAME%", "Type": "CNAME", "TTL": 300, "ResourceRecords": [{ "Value": "worker-%%N.%PRIVATE_DOMAIN_NAME%" }] } }, { "Action": "UPSERT", "ResourceRecordSet": { "Name": "spark-worker-%%N.%PRIVATE_DOMAIN_NAME%", "Type": "CNAME", "TTL": 300, "ResourceRecords": [{ "Value": "worker-%%N.%PRIVATE_DOMAIN_NAME%" }] } } ] } > "!DNS_BATCH_FILE!"

          aws route53 change-resource-record-sets --hosted-zone-id %ZONE_ID% --change-batch file://"!DNS_BATCH_FILE!"
          if !ERRORLEVEL! equ 0 (
            echo [INFO] DNS aliases for Worker %%N created successfully.
          ) else (
            echo [WARN] Failed to create DNS aliases for Worker %%N.
          )
          del "!DNS_BATCH_FILE!"
          )

          :skip_worker
:: --- METRICS NODE DEPLOYMENT ---
:: Visualization layer with Redis Stack and Grafana.
if /I "%TARGET%"=="master" goto :skip_metrics
if /I "%TARGET%"=="worker" goto :skip_metrics
if /I "%TARGET%"=="databases" goto :skip_metrics
if /I "%TARGET%"=="nifi" goto :skip_metrics
if /I "%TARGET%"=="airflow" goto :skip_metrics

set METRICS_STACK_NAME="%SPARK_CLUSTER_NAME%-metrics-node"

echo [INFO] Cleaning up stale SSH host keys for metrics-node...
powershell -Command "if (Test-Path '%KH_PATH%') { $c = Get-Content '%KH_PATH%'; $c | Where-Object { $_ -notmatch 'metrics-node' } | Set-Content '%KH_PATH%' }"

echo [INFO] Deploying Metrics Node via CloudFormation...
aws cloudformation deploy ^
  --stack-name "%METRICS_STACK_NAME%" ^
  --template-file "template/cluster-node.yaml" ^
  --parameter-overrides ^
      SubnetId=%SUBNET_ID% ^
      SecurityGroupId=%SG_ID% ^
      HostedZoneId=%ZONE_ID% ^
      KeyName=%SSH_KEY_NAME% ^
      NodeHostname="metrics.flight-analysis.local" ^
      InstanceName="Metrics-Node" ^
      S3Bucket=%BUCKET_NAME% ^
      EnvironmentFile="metrics.env" ^
      DeployScripts="deploy-metrics.sh" ^
  --capabilities CAPABILITY_IAM ^
  --no-fail-on-empty-changeset

:skip_metrics

:: --- NIFI NODE DEPLOYMENT ---
:: Ingestion layer with Apache NiFi and local raw datasets.
if /I "%TARGET%"=="master" goto :skip_nifi
if /I "%TARGET%"=="worker" goto :skip_nifi
if /I "%TARGET%"=="metrics" goto :skip_nifi
if /I "%TARGET%"=="databases" goto :skip_nifi
if /I "%TARGET%"=="airflow" goto :skip_nifi

set NIFI_STACK_NAME="%SPARK_CLUSTER_NAME%-nifi-node"

echo [INFO] Cleaning up stale SSH host keys for nifi-node...
powershell -Command "if (Test-Path '%KH_PATH%') { $c = Get-Content '%KH_PATH%'; $c | Where-Object { $_ -notmatch 'nifi-node' } | Set-Content '%KH_PATH%' }"

echo [INFO] Deploying NiFi Node via CloudFormation...
aws cloudformation deploy ^
  --stack-name "%NIFI_STACK_NAME%" ^
  --template-file "template/cluster-node.yaml" ^
  --parameter-overrides ^
      SubnetId=%SUBNET_ID% ^
      SecurityGroupId=%SG_ID% ^
      HostedZoneId=%ZONE_ID% ^
      KeyName=%SSH_KEY_NAME% ^
      NodeHostname="nifi.flight-analysis.local" ^
      InstanceName="NiFi-Node" ^
      S3Bucket=%BUCKET_NAME% ^
      EnvironmentFile="nifi.env" ^
      DeployScripts="deploy-nifi.sh" ^
  --capabilities CAPABILITY_IAM ^
  --no-fail-on-empty-changeset

:skip_nifi

:: --- AIRFLOW NODE DEPLOYMENT ---
:: Orchestration layer with Apache Airflow and Livy server.
if /I "%TARGET%"=="master" goto :skip_airflow
if /I "%TARGET%"=="worker" goto :skip_airflow
if /I "%TARGET%"=="metrics" goto :skip_airflow
if /I "%TARGET%"=="databases" goto :skip_airflow
if /I "%TARGET%"=="nifi" goto :skip_airflow

set AIRFLOW_STACK_NAME="%SPARK_CLUSTER_NAME%-airflow-node"

echo [INFO] Cleaning up stale SSH host keys for airflow-node...
powershell -Command "if (Test-Path '%KH_PATH%') { $c = Get-Content '%KH_PATH%'; $c | Where-Object { $_ -notmatch 'airflow-node' } | Set-Content '%KH_PATH%' }"

echo [INFO] Deploying Airflow Node via CloudFormation...
aws cloudformation deploy ^
  --stack-name "%AIRFLOW_STACK_NAME%" ^
  --template-file "template/cluster-node.yaml" ^
  --parameter-overrides ^
      SubnetId=%SUBNET_ID% ^
      SecurityGroupId=%SG_ID% ^
      HostedZoneId=%ZONE_ID% ^
      KeyName=%SSH_KEY_NAME% ^
      NodeHostname="airflow.flight-analysis.local" ^
      InstanceName="Airflow-Node" ^
      S3Bucket=%BUCKET_NAME% ^
      EnvironmentFile="airflow.env" ^
      DeployScripts="deploy-airflow.sh" ^
  --capabilities CAPABILITY_IAM ^
  --no-fail-on-empty-changeset

:skip_airflow

:: --- DATABASES NODE DEPLOYMENT ---
:: Persistence layer with PostgreSQL, MongoDB, HBase, and Redis Output.
if /I "%TARGET%"=="master" goto :skip_databases
if /I "%TARGET%"=="worker" goto :skip_databases
if /I "%TARGET%"=="metrics" goto :skip_databases
if /I "%TARGET%"=="nifi" goto :skip_databases
if /I "%TARGET%"=="airflow" goto :skip_databases

set DATABASES_STACK_NAME="%SPARK_CLUSTER_NAME%-databases-node"

echo [INFO] Cleaning up stale SSH host keys for databases-node...
powershell -Command "if (Test-Path '%KH_PATH%') { $c = Get-Content '%KH_PATH%'; $c | Where-Object { $_ -notmatch 'databases-node' } | Set-Content '%KH_PATH%' }"

echo [INFO] Deploying Databases Node via CloudFormation...
aws cloudformation deploy ^
  --stack-name "%DATABASES_STACK_NAME%" ^
  --template-file "template/cluster-node.yaml" ^
  --parameter-overrides ^
      SubnetId=%SUBNET_ID% ^
      SecurityGroupId=%SG_ID% ^
      HostedZoneId=%ZONE_ID% ^
      KeyName=%SSH_KEY_NAME% ^
      NodeHostname="databases.flight-analysis.local" ^
      InstanceName="Databases-Node" ^
      S3Bucket=%BUCKET_NAME% ^
      EnvironmentFile="databases.env" ^
      DeployScripts="deploy-databases.sh" ^
  --capabilities CAPABILITY_IAM ^
  --no-fail-on-empty-changeset

:skip_databases

echo ----------------------------------------------------
echo [SUCCESS] Deployment completed for target: %TARGET%
echo ----------------------------------------------------
