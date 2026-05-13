@echo off

if "%~1"=="" (
    echo ERRORE: Devi passare il KeyName come primo parametro.
    echo Esempio di utilizzo: lancia-stack.bat "Cluster Spark" subnet-06293b490d5e99854
    exit /b
)
if "%~2"=="" (
    echo ERRORE: Devi passare la SubnetId come secondo parametro.
    echo Esempio di utilizzo: lancia-stack.bat "Cluster Spark" subnet-06293b490d5e99854
    exit /b
)

set MY_KEY_NAME=%~1
set MY_SUBNET_ID=%~2

echo Lancio dello stack CloudFormation in corso...
echo KeyName utilizzata: %MY_KEY_NAME%
echo SubnetId utilizzata: %MY_SUBNET_ID%

aws cloudformation create-stack ^
  --stack-name ClusterSpark2 ^
  --template-body file://spark-emr.yaml ^
  --parameters ^
      ParameterKey=KeyName,ParameterValue="%MY_KEY_NAME%" ^
      ParameterKey=SubnetId,ParameterValue="%MY_SUBNET_ID%" ^
      ParameterKey=LogUri,ParameterValue=s3://spark-flight-analysis/logs/

echo.