@echo off
setlocal enabledelayedexpansion

:: --- LOAD ENVIRONMENT VARIABLES ---
:: Call the external library script
call load_env.bat
if %ERRORLEVEL% neq 0 (
    echo [FATAL] Failed to initialize environment.
    exit /b 1
)

echo ----------------------------------------------------
echo S3 BUCKET MANAGEMENT
echo ----------------------------------------------------
echo BUCKET: %BUCKET_NAME%
echo REGION: %REGION%
echo ----------------------------------------------------

set SPARK_JAR=../spark/target/flight-analysis.jar
set DEPLOY_FOLDER=.
set RAW_DATA_FOLDER=../data/raw
set NIFI_FOLDER=../nifi
set GRAFANA_FOLDER=../grafana

:: Check if the bucket already exists
:: '2>nul' hides the error output if the bucket is missing
aws s3api head-bucket --bucket %BUCKET_NAME% 2>nul

if %ERRORLEVEL% equ 0 (
    echo [INFO] Bucket already exists. Skipping creation.
) else (
    echo [INFO] Bucket does not exist. Creating it now...
    aws s3 mb s3://%BUCKET_NAME% --region %REGION%

    :: Wait 1 seconds for AWS propagation
    timeout /t 1 /nobreak >nul

    :: Double check if it actually exists now
    aws s3api head-bucket --bucket %BUCKET_NAME% 2>nul
    if !ERRORLEVEL! neq 0 (
        echo [ERROR] Failed to create bucket. Check permissions or naming rules.
        exit /b 1
    )
    echo [OK] Bucket is ready.
)

:: Check and create specific folder inside bucket
call create_bucket_folders.bat "%LOGS_FOLDER_NAME%" "%DEPLOY_FOLDER_NAME%" "%DATA_FOLDER_NAME%" "%DATA_FOLDER_NAME%/%RAW_FOLDER_NAME%" "%DATA_FOLDER_NAME%/%CONV_FOLDER_NAME%" "%DATA_FOLDER_NAME%/%RES_FOLDER_NAME%"

if exist "%DEPLOY_FOLDER%\" (
    :: Upload folder using sync
    echo [INFO] Syncing directory: %DEPLOY_FOLDER%...
    aws s3 sync "%DEPLOY_FOLDER%" "s3://%BUCKET_NAME%/%DEPLOY_FOLDER_NAME%/" --exclude "*.bat" --exclude ".env" --exclude "template/*"
    echo [INFO] Directory synced successfully.
) else (
     echo [WARN] Directory %DEPLOY_FOLDER% non trovata.
)

if exist "%NIFI_FOLDER%/\" (
    echo [INFO] Syncing NiFi...
    aws s3 sync "%NIFI_FOLDER%/" "s3://%BUCKET_NAME%/nifi/"
)

if exist "%GRAFANA_FOLDER%\" (
    echo [INFO] Syncing Grafana...
    aws s3 sync "%GRAFANA_FOLDER%" "s3://%BUCKET_NAME%/grafana/"
)


if exist "%RAW_DATA_FOLDER%\" (
    :: Upload folder using sync
    echo [INFO] Syncing directory: %RAW_DATA_FOLDER%...
    aws s3 sync "%RAW_DATA_FOLDER%" "s3://%BUCKET_NAME%/%DATA_FOLDER_NAME%/%RAW_FOLDER_NAME%/" --exclude "*.bat" --exclude ".env" --exclude "template/*"
    echo [INFO] Directory synced successfully.
) else (
     echo [WARN] Directory %RAW_DATA_FOLDER% non trovata.
)

if exist "%SPARK_JAR%" (
    :: Upload file using cp
    echo [INFO] Copying file: %SPARK_JAR%...
    aws s3 cp "%SPARK_JAR%" "s3://%BUCKET_NAME%/"
    echo [INFO] File copied successfully.
) else (
    echo [WARN] File %SPARK_JAR% non trovato.
)

if %ERRORLEVEL% equ 0 (
    echo.
    echo ----------------------------------------------------
    echo [INFO] Operation completed!
    echo Files are available at: s3://%BUCKET_NAME%/
    echo ----------------------------------------------------
) else (
    echo [ERROR] An error occurred during the sync process.
)

exit /b 0