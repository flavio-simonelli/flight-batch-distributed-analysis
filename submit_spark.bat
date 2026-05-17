@echo off
setlocal enabledelayedexpansion

:: Input argument mapping
set QUERY=%1
set BACKEND=%2

set CONFIG_FILE=local-config.yml
if not "%~3"=="" set CONFIG_FILE=%~3

set INPUT=hdfs
if not "%~4"=="" set INPUT=%~4

set OUTPUT=hdfs
if not "%~5"=="" set OUTPUT=%~5

:: --- CONFIGURATION ---
set CONTAINER_NAME=spark-master
set JAR_PATH_IN_CONTAINER=/opt/spark/jars/flight-analysis.jar
:: ---------------------

docker cp spark\target\flight-analysis.jar %CONTAINER_NAME%:%JAR_PATH_IN_CONTAINER%

echo -----------------------------------------------------------------------------------------------------------
echo EXECUTION: Query = %QUERY% ^| Backend = %BACKEND% ^| Input = %INPUT% ^| Output = %OUTPUT%
echo -----------------------------------------------------------------------------------------------------------

docker exec %CONTAINER_NAME% /opt/spark/bin/spark-submit ^
  --packages org.apache.hadoop:hadoop-aws:3.3.4 ^
  --class it.uniroma2.sae.FlightAnalysisApp ^
  --master spark://spark-master.flight-analysis.local:7077 ^
  %JAR_PATH_IN_CONTAINER% ^
  --config %CONFIG_FILE% ^
  --query %QUERY% ^
  --backend %BACKEND% ^
  --input-type %INPUT% ^
  --output-type %OUTPUT%

if !ERRORLEVEL! NEQ 0 (
    echo [ERROR] Phase %QUERY% with backend %BACKEND% failed.
)
echo.