@echo off
setlocal enabledelayedexpansion

:: --- CONFIGURATION ---
set CONTAINER_NAME=hdfs-master
set JAR_NAME=flight-analysis.jar
set LOCAL_PATH=.\spark\target\%JAR_NAME%
set STAGE_PATH=/home/%JAR_NAME%
set REMOTE_PATH=/bin/%JAR_NAME%
:: ---------------------

echo Avvio upload jar sul container Docker: %CONTAINER_NAME%
echo.

docker cp %LOCAL_PATH% %CONTAINER_NAME%:%STAGE_PATH%
docker exec -it %CONTAINER_NAME% hdfs dfs -rm %REMOTE_PATH%
docker exec -it %CONTAINER_NAME% hdfs dfs -put %STAGE_PATH% %REMOTE_PATH%

echo Upload nel container terminato!