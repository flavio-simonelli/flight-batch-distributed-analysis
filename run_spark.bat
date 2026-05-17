@echo off
setlocal enabledelayedexpansion

:: --- CONFIGURATION ---
set CONTAINER_NAME=spark-master
set CONFIG_FILE=local-config.yml
:: ---------------------

echo Avvio analisi Spark sul container Docker: %CONTAINER_NAME%
echo.

:: %%Q rappresenta la Query
:: %%B rappresenta il Backend
FOR %%Q IN (monthly_performance arrival_delay_ranking hourly_delay_percentiles) DO (
    FOR %%B IN (rdd dataframe sql) DO (
        call submit_spark.bat %%Q %%B %CONFIG_FILE%
    )
)

echo Tutte le elaborazioni nel container sono terminate!