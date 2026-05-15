@echo off
:: This script ensures the SSH key pair exists in AWS and locally with correct permissions.
:: It is meant to be called by other deployment scripts.

:: --- LOAD ENVIRONMENT VARIABLES ---
if "%BUCKET_NAME%"=="" (
    call load_env.bat
    if %ERRORLEVEL% neq 0 exit /b 1
)

echo [INFO] Ensuring SSH Key: %SSH_KEY_NAME%

:: Check if the local directory for keys exists
if not exist "%SSH_KEY_DIR%" mkdir "%SSH_KEY_DIR%"

:: Check if the key pair exists in AWS EC2
aws ec2 describe-key-pairs --key-names %SSH_KEY_NAME% 2>nul
if %ERRORLEVEL% neq 0 (
    echo [INFO] SSH Key %SSH_KEY_NAME% not found in AWS. Creating...
    aws ec2 create-key-pair --key-name %SSH_KEY_NAME% --query "KeyMaterial" --output text > "%SSH_KEY_DIR%\%SSH_KEY_NAME%.pem"
    
    :: Set restrictive permissions (Required for OpenSSH on Windows)
    icacls "%SSH_KEY_DIR%\%SSH_KEY_NAME%.pem" /inheritance:r
    icacls "%SSH_KEY_DIR%\%SSH_KEY_NAME%.pem" /grant:r "%USERNAME%:(R)"
    
    echo [INFO] Key created and saved locally to %SSH_KEY_DIR% with correct permissions.
) else (
    if not exist "%SSH_KEY_DIR%\%SSH_KEY_NAME%.pem" (
        echo [WARNING] Key exists in AWS but the .pem file is missing in %SSH_KEY_DIR%.
        echo [WARNING] Ensure you have the correct file to access instances.
    ) else (
        :: Even if it exists, re-apply permissions to be safe
        icacls "%SSH_KEY_DIR%\%SSH_KEY_NAME%.pem" /inheritance:r
        icacls "%SSH_KEY_DIR%\%SSH_KEY_NAME%.pem" /grant:r "%USERNAME%:(R)"
        echo [INFO] SSH Key is ready both locally and in AWS.
    )
)

exit /b 0
