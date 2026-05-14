@echo off
setlocal enabledelayedexpansion

:: --- CONFIGURATION ---
set CONTAINER_NAME=spark-master
set JAR_PATH_IN_CONTAINER=/opt/spark/scripts/target/flight-analysis.jar
set CONFIG_FILE=compose-config.yml
:: ---------------------

echo Avvio analisi Spark sul container Docker: %CONTAINER_NAME%
echo.

:: %%Q rappresenta la Query
:: %%B rappresenta il Backend
FOR %%Q IN (monthly_performance arrival_delay_ranking hourly_delay_percentiles) DO (
    FOR %%B IN (rdd dataframe sql) DO (

        echo ------------------------------------------------------------
        echo ESECUZIONE: Query = %%Q ^| Backend = %%B
        echo ------------------------------------------------------------

        :: Usiamo 'docker exec' per lanciare il comando dentro il container
        docker exec %CONTAINER_NAME% /opt/spark/bin/spark-submit ^
          --packages org.apache.hadoop:hadoop-aws:3.3.4 ^
          --class it.uniroma2.sae.FlightAnalysisApp ^
          --master spark://spark-master:7077 ^
          %JAR_PATH_IN_CONTAINER% ^
          --config %CONFIG_FILE% ^
          --query %%Q ^
          --backend %%B

        if %ERRORLEVEL% NEQ 0 (
            echo [ERRORE] Fase %%Q con backend %%B fallita.
        )
        echo.
    )
)

echo Tutte le elaborazioni nel container sono terminate!