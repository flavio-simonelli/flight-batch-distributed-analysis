@echo off
setlocal enabledelayedexpansion

:: --- ENVIRONMENT INITIALIZATION ---
:: Determine the location of the deployment scripts to ensure all relative 
:: path resolutions are consistent regardless of the working directory.
set "SCRIPTS_DIR=%~dp0"
set "PROJECT_ROOT=%SCRIPTS_DIR%..\"

:: Load environment variables from the .env file located in the deploy folder.
:: This provides the script with required S3 bucket names and AWS region settings.
call "%SCRIPTS_DIR%load_env.bat"
if %ERRORLEVEL% neq 0 (
    echo [ERROR] Failed to initialize environment variables. Ensure .env exists.
    exit /b 1
)

:: Validate that essential AWS variables are properly defined before proceeding.
if "%BUCKET_NAME%"=="" (
    echo [ERROR] BUCKET_NAME is not defined in the environment.
    exit /b 1
)

echo ----------------------------------------------------
echo [INFO] AWS S3 BUCKET PROVISIONING
echo ----------------------------------------------------
echo BUCKET: %BUCKET_NAME%
echo REGION: %REGION%
echo ----------------------------------------------------

:: --- BUCKET VALIDATION AND CREATION ---
:: Check if the target S3 bucket exists; create it if it is missing in the specified region.
aws s3api head-bucket --bucket %BUCKET_NAME% 2>nul
if %ERRORLEVEL% equ 0 (
    echo [INFO] Bucket already exists. Skipping creation.
) else (
    echo [INFO] Bucket does not exist. Creating it now in %REGION%...
    aws s3 mb s3://%BUCKET_NAME% --region %REGION%
    if !ERRORLEVEL! neq 0 (
        echo [ERROR] Failed to create bucket. Verify IAM permissions or naming rules.
        exit /b 1
    )
    echo [INFO] Bucket created successfully.
)

:: --- FOLDER STRUCTURE PROVISIONING ---
:: Create the logical folder structure within the bucket.
call "%SCRIPTS_DIR%create_bucket_folders.bat" "logs" "deploy" "data" "data/raw" "data/conv" "data/res"

:: --- DEPLOYMENT ASSETS SYNCHRONIZATION ---
:: Sync the local deploy directory containing templates, compose files, and bash scripts.
echo [INFO] Syncing deployment scripts and configurations...
aws s3 sync "%SCRIPTS_DIR%." "s3://%BUCKET_NAME%/deploy/" ^
    --exclude "*.bat" --include "*/*.bat" ^
    --exclude "*.sh" --include "*/*.sh" ^
    --exclude ".env*" --include "envs/*.env" ^
    --exclude "template/*"

:: Sync NiFi custom extensions, flows and configurations from the project structure.
if exist "%PROJECT_ROOT%nifi\" (
    echo [INFO] Syncing NiFi application assets...
    aws s3 sync "%PROJECT_ROOT%nifi/" "s3://%BUCKET_NAME%/nifi/"
)

:: Sync Grafana dashboard and datasource provisioning configurations.
if exist "%PROJECT_ROOT%grafana\" (
    echo [INFO] Syncing Grafana dashboards and datasources...
    aws s3 sync "%PROJECT_ROOT%grafana/" "s3://%BUCKET_NAME%/grafana/"
)

:: Sync Airflow DAGs, plugins, and custom configurations.
if exist "%PROJECT_ROOT%airflow\" (
    echo [INFO] Syncing Airflow orchestration assets...
    aws s3 sync "%PROJECT_ROOT%airflow/" "s3://%BUCKET_NAME%/airflow/"
)

:: Sync Livy server custom configurations and Docker files.
if exist "%PROJECT_ROOT%livy\" (
    echo [INFO] Syncing Livy server assets...
    aws s3 sync "%PROJECT_ROOT%livy/" "s3://%BUCKET_NAME%/livy/"
)

:: Upload the Spark application JAR and configuration files.
:: We avoid syncing the entire 'spark/' directory to exclude source code and build overhead.
if exist "%PROJECT_ROOT%spark\target\flight-analysis.jar" (
    echo [INFO] Uploading Spark application JAR...
    aws s3 cp "%PROJECT_ROOT%spark/target/flight-analysis.jar" "s3://%BUCKET_NAME%/spark/flight-analysis.jar"
)

if exist "%PROJECT_ROOT%spark\src\main\resources\" (
    echo [INFO] Syncing Spark configuration files...
    aws s3 sync "%PROJECT_ROOT%spark/src/main/resources/" "s3://%BUCKET_NAME%/spark/" --exclude "*" --include "*.yml"
)

:: --- DATASET SYNCHRONIZATION ---
:: Upload the raw project data used as input for the processing pipeline.
if exist "%PROJECT_ROOT%data\zip\" (
    echo [INFO] Syncing raw input datasets to the storage layer...
    aws s3 sync "%PROJECT_ROOT%data/zip/" "s3://%BUCKET_NAME%/data/zip/"
)

echo.
echo ----------------------------------------------------
echo [INFO] S3 Synchronization completed successfully!
echo Assets location: s3://%BUCKET_NAME%/
echo ----------------------------------------------------

exit /b 0
