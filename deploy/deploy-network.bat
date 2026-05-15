@echo off
setlocal enabledelayedexpansion

:: --- LOAD ENVIRONMENT VARIABLES ---
call load_env.bat
if %ERRORLEVEL% neq 0 exit /b 1

echo ----------------------------------------------------
echo NETWORK DEPLOYMENT AND CHECK
echo VPC Name:    %VPC_NAME%
echo Subnet Name: %SUBNET_NAME%
echo ----------------------------------------------------

:: Set the stack name based on VPC name
set "VPC_STACK_NAME=%VPC_NAME%-stack"

:: Check if the subnet already exists using its Name tag
echo [INFO] Searching for existing subnet: %SUBNET_NAME%...
aws ec2 describe-subnets --filters "Name=tag:Name,Values=%SUBNET_NAME%" --query "Subnets[0].SubnetId" --output text 2>nul | findstr /v "None" >nul

if %ERRORLEVEL% equ 0 (
    echo [INFO] Network infrastructure with name '%SUBNET_NAME%' already exists.
    echo [INFO] Skipping CloudFormation deployment.
) else (
    echo [INFO] Network not found. Deploying VPC Infrastructure stack...

    :: Deploy the CloudFormation template
    aws cloudformation deploy ^
      --stack-name "%VPC_STACK_NAME%" ^
      --template-file "template/cluster-vpc.yaml" ^
      --parameter-overrides ^
          VpcName="%VPC_NAME%" ^
          SubnetName="%SUBNET_NAME%" ^
          VpcCIDR=%VPC_CIDR% ^
          PublicSubnetCIDR=%SUBNET_CIDR% ^
      --no-fail-on-empty-changeset

    if !ERRORLEVEL! neq 0 (
        echo [ERROR] CloudFormation deploy command failed.
        exit /b 1
    )
)

:: --- FINAL NETWORK VERIFICATION ---
echo [INFO] Verifying network status...

set "FOUND_SUBNET_ID=None"

:: Retrieve SubnetId after potential deployment
for /f "tokens=*" %%i in ('aws ec2 describe-subnets --filters "Name=tag:Name,Values=%SUBNET_NAME%" --query "Subnets[0].SubnetId" --output text 2^>nul') do (
    set "FOUND_SUBNET_ID=%%i"
)

:: Validation logic
if "%FOUND_SUBNET_ID%"=="None" (
    echo [ERROR] Subnet '%SUBNET_NAME%' could not be found or created.
    set "EXIT_CODE=1"
) else if "%FOUND_SUBNET_ID%"=="" (
    echo [ERROR] Received an empty Subnet ID.
    set "EXIT_CODE=1"
) else (
    echo [INFO] Network is ready!
    echo [INFO] Subnet ID: %FOUND_SUBNET_ID%

    :: Get the associated VPC ID for confirmation
    for /f "tokens=*" %%j in ('aws ec2 describe-subnets --subnet-ids %FOUND_SUBNET_ID% --query "Subnets[0].VpcId" --output text') do (
        echo [INFO] Parent VPC ID: %%j
    )
    set "EXIT_CODE=0"
)

exit /b %EXIT_CODE%