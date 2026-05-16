@echo off
setlocal enabledelayedexpansion

:: Mappatura degli argomenti in ingresso
set QUERY=%1
set BACKEND=%2

set CONFIG_FILE=local-config.yml
if not "%~3"=="" set CONFIG_FILE=%~3

:: --- CONFIGURATION ---
set CONTAINER_NAME=spark-master
set JAR_PATH_IN_CONTAINER=/opt/spark/scripts/target/flight-analysis.jar
:: ---------------------

echo ------------------------------------------------------------
echo ESECUZIONE: Query = %QUERY% ^| Backend = %BACKEND%
echo ------------------------------------------------------------

docker exec %CONTAINER_NAME% /opt/spark/bin/spark-submit ^
  --packages org.apache.hadoop:hadoop-aws:3.3.4 ^
  --class it.uniroma2.sae.FlightAnalysisApp ^
  --master spark://spark-master.flight-analysis.local:7077 ^
  %JAR_PATH_IN_CONTAINER% ^
  --config %CONFIG_FILE% ^
  --query %QUERY% ^
  --backend %BACKEND%

if !ERRORLEVEL! NEQ 0 (
    echo [ERRORE] Fase %QUERY% con backend %BACKEND% fallita.
)
echo.