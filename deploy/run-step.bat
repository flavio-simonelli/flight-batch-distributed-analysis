@echo off
setlocal enabledelayedexpansion

:: --- LOAD ENVIRONMENT VARIABLES ---
call load_env.bat
if %ERRORLEVEL% neq 0 exit /b 1

:: --- PARAMETER CHECK ---
if "%~1"=="" (
    echo [ERROR] Cluster ID is missing.
    echo Usage: run-step.bat j-XXXXXXXXXXXXX
    exit /b 1
)

set "CLUSTER_ID=%~1"
set "SPARK_JAR_PATH=s3://%BUCKET_NAME%/flight-analysis.jar"

echo ----------------------------------------------------
echo SUBMITTING SPARK STEPS TO EMR
echo Cluster ID: %CLUSTER_ID%
echo JAR Path:   %SPARK_JAR_PATH%
echo ----------------------------------------------------

:: %%Q represents the Query type
:: %%B represents the Execution Backend
for %%Q in (monthly_performance arrival_delay_ranking hourly_delay_percentiles) do (
    for %%B in (rdd dataframe sql) do (

        echo [INFO] Submitting Step: Query=%%Q ^| Backend=%%B

        :: Add step to the EMR cluster
        aws emr add-steps ^
          --cluster-id %CLUSTER_ID% ^
          --steps Type=Spark,Name="Flight Analysis - %%Q - %%B",ActionOnFailure=CONTINUE,Args=[%SPARK_JAR_PATH%,--config,aws-config.yml,--query,%%Q,--backend,%%B] >nul

        if !ERRORLEVEL! equ 0 (
            echo [INFO] Step submitted successfully.
        ) else (
            echo [ERROR] Failed to submit step: %%Q with %%B.
        )
    )
)

echo.
echo ----------------------------------------------------
echo [INFO] All steps have been submitted to the cluster!
echo ----------------------------------------------------