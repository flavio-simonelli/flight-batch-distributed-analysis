@echo off
setlocal enabledelayedexpansion

:: --- SPARK CLUSTER DEPLOYMENT ORCHESTRATOR ---
:: This script manages the provisioning of an Amazon EMR cluster configured 
:: for Spark analytical processing, utilizing the project's S3 assets.

:: --- ENVIRONMENT INITIALIZATION ---
call load_env.bat
if %ERRORLEVEL% neq 0 (
    echo [ERROR] Environment initialization failed.
    exit /b 1
)

echo ----------------------------------------------------
echo [INFO] SPARK EMR CLUSTER PROVISIONING
echo Cluster Name: %SPARK_CLUSTER_NAME%
echo Core Nodes:   %SPARK_CORE_COUNT%
echo ----------------------------------------------------

echo [INFO] Initiating pre-deployment validation...

:: --- SSH ACCESS VERIFICATION ---
:: Ensure that the cryptographic key required for node access is properly provisioned.
call setup_ssh_key.bat
if %ERRORLEVEL% neq 0 (
    echo [ERROR] SSH key setup failed. Provisioning aborted.
    exit /b 1
)

:: --- NETWORK STATE VALIDATION ---
:: Discover the target subnet using its Name tag to ensure the cluster is launched in the correct VPC segment.
echo [INFO] Discovering network infrastructure context...
set "SUBNET_ID=None"
for /f "tokens=*" %%i in ('aws ec2 describe-subnets --filters "Name=tag:Name,Values=%SUBNET_NAME%" --query "Subnets[0].SubnetId" --output text 2^>nul') do (
    set "SUBNET_ID=%%i"
)

:: Verify that the network segment exists; EMR deployment requires a pre-existing subnet.
if "%SUBNET_ID%"=="None" (
    echo [ERROR] Network infrastructure '%SUBNET_NAME%' not found. Run deploy-network.bat first.
    exit /b 1
)

echo [INFO] Target network segment identified: %SUBNET_ID%

:: --- EMR CLUSTER PROVISIONING ---
:: Deploy the EMR cluster stack via CloudFormation using the spark-emr template.
echo [INFO] Launching Spark EMR Infrastructure stack...

set CLUSTER_STACK_NAME=%SPARK_CLUSTER_NAME%-stack-%RANDOM%

aws cloudformation create-stack ^
  --stack-name %CLUSTER_STACK_NAME% ^
  --template-body "file://template/spark-emr.yaml" ^
  --parameters ^
      ParameterKey=ClusterName,ParameterValue="%SPARK_CLUSTER_NAME%" ^
      ParameterKey=KeyName,ParameterValue="%SSH_KEY_NAME%" ^
      ParameterKey=SubnetId,ParameterValue="%SUBNET_ID%" ^
      ParameterKey=CoreInstanceCount,ParameterValue=%SPARK_CORE_COUNT%

if %ERRORLEVEL% equ 0 (
    echo [INFO] EMR Cluster provisioning request successfully submitted to CloudFormation.
) else (
    echo [ERROR] Failed to initiate Spark EMR Cluster stack creation.
)

exit /b 0
