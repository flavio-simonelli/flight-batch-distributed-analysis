@echo off
setlocal enabledelayedexpansion

:: --- LOAD ENVIRONMENT VARIABLES ---
call load_env.bat
if %ERRORLEVEL% neq 0 exit /b 1

echo ----------------------------------------------------
echo SPARK CLUSTER DEPLOYMENT
echo Cluster Name:  %SPARK_CLUSTER_NAME%
echo Core Count:    %SPARK_CORE_COUNT%
echo ----------------------------------------------------

echo [INFO] Starting Pre-Deployment Checks...

:: --- SSH KEY CHECK ---
:: Check if the local directory for keys exists
if not exist "%SSH_KEY_DIR%" mkdir "%SSH_KEY_DIR%"

:: Check if the key pair exists in AWS EC2
aws ec2 describe-key-pairs --key-names %SSH_KEY_NAME% 2>nul
if %ERRORLEVEL% neq 0 (
    echo [INFO] SSH Key %SSH_KEY_NAME% not found in AWS. Creating...
    aws ec2 create-key-pair --key-name %SSH_KEY_NAME% --query "KeyMaterial" --output text > "%SSH_KEY_DIR%\%SSH_KEY_NAME%.pem"
    icacls "%SSH_KEY_DIR%\%SSH_KEY_NAME%.pem" /inheritance:r
    icacls "%SSH_KEY_DIR%\%SSH_KEY_NAME%.pem" /grant:r "${env:UserName}:(R)"
    echo [INFO] Key created and saved locally to %SSH_KEY_DIR%.
) else (
    if not exist "%SSH_KEY_DIR%\%SSH_KEY_NAME%.pem" (
        echo [WARNING] Key exists in AWS but the .pem file is missing in %SSH_KEY_DIR%.
        echo [WARNING] Ensure you have the correct file to access instances.
    ) else (
        echo [INFO] SSH Key is ready both locally and in AWS.
    )
)

:: --- NETWORK CHECK ---
echo [INFO] Searching for Subnet by Name: %SUBNET_NAME%...

:: Try to retrieve SubnetId using the Name tag
set "SUBNET_ID=None"
for /f "tokens=*" %%i in ('aws ec2 describe-subnets --filters "Name=tag:Name,Values=%SUBNET_NAME%" --query "Subnets[0].SubnetId" --output text 2^>nul') do set "SUBNET_ID=%%i"

:: If Subnet doesn't exist, deploy the VPC infrastructure stack
:: If Subnet doesn't exist or is "None"
if "%SUBNET_ID%"=="None" (
    echo [ERROR] Subnet '%SUBNET_NAME%' not found. Please run deploy-network.bat first.
    exit /b 1
)
if "%SUBNET_ID%"=="" (
    echo [ERROR] Subnet '%SUBNET_NAME%' search returned empty. Please run deploy-network.bat first.
    exit /b 1
)

:: If it passes the checks above, it means we have an ID
echo [INFO] Found existing Subnet: %SUBNET_ID%

:: --- LAUNCH SPARK CLUSTER ---
echo [INFO] Launching Spark EMR Stack...

set CLUSTER_STACK_NAME=%SPARK_CLUSTER_NAME%-stack

:: Using specific Spark template and parameters
aws cloudformation create-stack ^
  --stack-name %CLUSTER_STACK_NAME% ^
  --template-body "file://template/spark-emr.yaml" ^
  --parameters ^
      ParameterKey=ClusterName,ParameterValue="%SPARK_CLUSTER_NAME%" ^
      ParameterKey=KeyName,ParameterValue="%SSH_KEY_NAME%" ^
      ParameterKey=SubnetId,ParameterValue="%SUBNET_ID%" ^
      ParameterKey=LogUri,ParameterValue="s3://%BUCKET_NAME%/%LOGS_FOLDER_NAME%/" ^
      ParameterKey=CoreInstanceCount,ParameterValue=%SPARK_CORE_COUNT%

if %ERRORLEVEL% equ 0 (
    echo [INFO] Spark Cluster stack creation initiated.
) else (
    echo [ERROR] Failed to launch Spark Cluster stack.
)