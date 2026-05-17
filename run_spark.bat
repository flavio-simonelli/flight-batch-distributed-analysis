@echo off
setlocal enabledelayedexpansion

:: --- CONFIGURATION ---
set CONTAINER_NAME=spark-master
set CONFIG_FILE=local-config.yml
:: ---------------------

echo Starting Spark analysis on Docker container: %CONTAINER_NAME%
echo.

:: %%Q represents the Query
:: %%B represents the Backend
FOR %%Q IN (monthly_performance arrival_delay_ranking hourly_delay_percentiles) DO (
    FOR %%B IN (rdd dataframe sql) DO (
        call submit_spark.bat %%Q %%B %CONFIG_FILE%
    )
)

echo All processing in the container has finished!