@echo off
setlocal enabledelayedexpansion

:: --- PARAMETER CHECK ---
:: Target can be: master, worker, metrics, databases, nifi, or all (default)
set "TARGET=%~1"
if "%TARGET%"=="" set "TARGET=all"

:: Validate input target
if /I "%TARGET%" NEQ "master" if /I "%TARGET%" NEQ "worker" if /I "%TARGET%" NEQ "metrics" if /I "%TARGET%" NEQ "databases" if /I "%TARGET%" NEQ "nifi" if /I "%TARGET%" NEQ "all" (
    echo [ERROR] Invalid target: %TARGET%
    echo Usage: deploy-node.bat [master ^| worker ^| metrics ^| databases ^| nifi ^| all]
    exit /b 1
)

:: --- LOAD ENVIRONMENT VARIABLES ---
call load_env.bat
if %ERRORLEVEL% neq 0 exit /b 1

:: --- SSH KEY CHECK ---
:: Ensure the key exists in AWS and locally with correct permissions
call setup-ssh-key.bat
if %ERRORLEVEL% neq 0 exit /b 1

echo [INFO] Searching for Infrastructure components by Name...

:: Retrieve SubnetId using the Name Tag
set "SUBNET_ID=None"
for /f "tokens=*" %%i in ('aws ec2 describe-subnets --filters "Name=tag:Name,Values=%SUBNET_NAME%" --query "Subnets[0].SubnetId" --output text') do set "SUBNET_ID=%%i"

:: Retrieve Security Group ID using the Group Name
set "SG_ID=None"
for /f "tokens=*" %%i in ('aws ec2 describe-security-groups --filters "Name=group-name,Values=%SG_NAME%" --query "SecurityGroups[0].GroupId" --output text') do set "SG_ID=%%i"

:: Retrieve Hosted Zone ID using the Domain Name (e.g., flight-analysis.local.)
:: Note: Route53 appends a trailing dot to the zone name
set "ZONE_ID=None"
for /f "tokens=*" %%i in ('aws route53 list-hosted-zones-by-name --dns-name %PRIVATE_DOMAIN_NAME% --query "HostedZones[0].Id" --output text') do (
    set "RAW_ZONE_ID=%%i"
    set "ZONE_ID=!RAW_ZONE_ID:/hostedzone/=!"
)

echo [INFO] Subnet Found: %SUBNET_ID%
echo [INFO] SG Found:     %SG_ID%
echo [INFO] Zone Found:   %ZONE_ID%

:: Check if required infrastructure is present
if "%SUBNET_ID%"=="None" ( echo [ERROR] Network infrastructure missing. & pause & exit /b 1 )

:: --- DEPLOY NODES ---
echo [INFO] Deploying Cluster Nodes for target: %TARGET%...

:: Define known_hosts path for PowerShell command
set "KH_PATH=%USERPROFILE%\\.ssh\\known_hosts"

:: --- DEPLOY MASTER NODE ---
if /I "%TARGET%"=="worker" goto :skip_master
if /I "%TARGET%"=="metrics" goto :skip_master
if /I "%TARGET%"=="databases" goto :skip_master
if /I "%TARGET%"=="nifi" goto :skip_master

set MASTER_STACK_NAME="%SPARK_CLUSTER_NAME%-master-node"

echo [INFO] Cleaning up known_hosts for master-node...
powershell -Command "if (Test-Path '%KH_PATH%') { (Get-Content '%KH_PATH%') | Where-Object { $_ -notmatch (Select-String -Path '%KH_PATH%' -Pattern 'master-node' | Select-Object -ExpandProperty Line) } | Set-Content '%KH_PATH%' }"

echo [INFO] Deploying MASTER Node...
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

:skip_master

:: --- DEPLOY WORKER NODES ---
if /I "%TARGET%"=="master" if /I "%TARGET%" NEQ "all" goto :skip_worker
if /I "%TARGET%"=="metrics" goto :skip_worker
if /I "%TARGET%"=="databases" goto :skip_worker
if /I "%TARGET%"=="nifi" goto :skip_worker

set WORKER_STACK_NAME="%SPARK_CLUSTER_NAME%-worker-node"

echo [INFO] Deploying WORKER Nodes...

for /L %%N in (1,1,%WORKER_COUNT%) do (
    echo [INFO] Cleaning up known_hosts for worker-node-%%N...
    powershell -Command "if (Test-Path '%KH_PATH%') { (Get-Content '%KH_PATH%') | Where-Object { $_ -notmatch (Select-String -Path '%KH_PATH%' -Pattern 'worker-node-%%N' | Select-Object -ExpandProperty Line) } | Set-Content '%KH_PATH%' }"

    echo [INFO] Launching Worker %%N...
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
)

:skip_worker

:: --- DEPLOY METRICS NODE ---
if /I "%TARGET%"=="master" goto :skip_metrics
if /I "%TARGET%"=="worker" goto :skip_metrics
if /I "%TARGET%"=="databases" goto :skip_metrics
if /I "%TARGET%"=="nifi" goto :skip_metrics

set METRICS_STACK_NAME="%SPARK_CLUSTER_NAME%-metrics-node"

echo [INFO] Cleaning up known_hosts for metrics-node...
powershell -Command "if (Test-Path '%KH_PATH%') { (Get-Content '%KH_PATH%') | Where-Object { $_ -notmatch (Select-String -Path '%KH_PATH%' -Pattern 'metrics-node' | Select-Object -ExpandProperty Line) } | Set-Content '%KH_PATH%' }"

echo [INFO] Deploying METRICS Node...
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

:: --- DEPLOY NIFI NODE ---
if /I "%TARGET%"=="master" goto :skip_nifi
if /I "%TARGET%"=="worker" goto :skip_nifi
if /I "%TARGET%"=="metrics" goto :skip_nifi
if /I "%TARGET%"=="databases" goto :skip_nifi

set NIFI_STACK_NAME="%SPARK_CLUSTER_NAME%-nifi-node"

echo [INFO] Cleaning up known_hosts for nifi-node...
powershell -Command "if (Test-Path '%KH_PATH%') { (Get-Content '%KH_PATH%') | Where-Object { $_ -notmatch (Select-String -Path '%KH_PATH%' -Pattern 'nifi-node' | Select-Object -ExpandProperty Line) } | Set-Content '%KH_PATH%' }"

echo [INFO] Deploying NIFI Node...
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

:: --- DEPLOY DATABASES NODE ---
if /I "%TARGET%"=="master" goto :skip_databases
if /I "%TARGET%"=="worker" goto :skip_databases
if /I "%TARGET%"=="metrics" goto :skip_databases
if /I "%TARGET%"=="nifi" goto :skip_databases

set DATABASES_STACK_NAME="%SPARK_CLUSTER_NAME%-databases-node"

echo [INFO] Cleaning up known_hosts for databases-node...
powershell -Command "if (Test-Path '%KH_PATH%') { (Get-Content '%KH_PATH%') | Where-Object { $_ -notmatch (Select-String -Path '%KH_PATH%' -Pattern 'databases-node' | Select-Object -ExpandProperty Line) } | Set-Content '%KH_PATH%' }"

echo [INFO] Deploying DATABASES Node...
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

echo [SUCCESS] Deployment completed for target: %TARGET%
