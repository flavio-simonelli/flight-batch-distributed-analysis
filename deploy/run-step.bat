@echo off

if "%~1"=="" (
    echo ERRORE: Devi passare il Cluster ID come primo parametro.
    echo Esempio di utilizzo: lancia-fasi.bat j-1ZT8KX6GGLVLW
    exit /b
)

set CLUSTER_ID=%~1

echo Invio della fase Spark al cluster EMR %CLUSTER_ID% in corso...

set SPARK_SCRIPT=s3://spark-flight-analysis/flight-analysis.jar

:: %%Q rappresenta la Query
:: %%B rappresenta il Backend
FOR %%Q IN (monthly_performance arrival_delay_ranking hourly_delay_percentiles) DO (
    FOR %%B IN (rdd dataframe sql) DO (

        echo Inviando fase: Query = %%Q ^| Backend = %%B

        aws emr add-steps ^
          --cluster-id %CLUSTER_ID% ^
          --steps Type=Spark,Name="Flight Analysis - %%Q - %%B",ActionOnFailure=CONTINUE,Args=[%SPARK_SCRIPT%,--config,aws-config.yml,--query,%%Q,--backend,%%B]

    )
)

echo.
echo Tutti gli step sono stati inviati con successo!