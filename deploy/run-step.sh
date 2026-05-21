#!/bin/bash

# --- LOAD ENVIRONMENT VARIABLES ---
source load_env.sh
if [ $? -ne 0 ]; then
    exit 1
fi

# --- PARAMETER CHECK ---
if [ -z "$1" ]; then
    echo "[ERROR] Cluster ID is missing."
    echo "Usage: ./run-step.sh j-XXXXXXXXXXXXX"
    exit 1
fi

CLUSTER_ID="$1"
SPARK_JAR_PATH="s3://${BUCKET_NAME}/flight-analysis.jar"

echo "----------------------------------------------------"
echo "SUBMITTING SPARK STEPS TO EMR"
echo "Cluster ID: ${CLUSTER_ID}"
echo "JAR Path:   ${SPARK_JAR_PATH}"
echo "----------------------------------------------------"

# Q represents the Query type
# B represents the Execution Backend
for Q in monthly_performance arrival_delay_ranking hourly_delay_percentiles; do
    for B in rdd dataframe sql; do

        echo "[INFO] Submitting Step: Query=${Q} | Backend=${B}"

        # Add step to the EMR cluster
        aws emr add-steps \
          --cluster-id "${CLUSTER_ID}" \
          --steps Type=Spark,Name="Flight Analysis - ${Q} - ${B}",ActionOnFailure=CONTINUE,Args=[${SPARK_JAR_PATH},--config,aws-config.yml,--query,${Q},--backend,${B}] > /dev/null 2>&1

        if [ $? -eq 0 ]; then
            echo "[INFO] Step submitted successfully."
        else
            echo "[ERROR] Failed to submit step: ${Q} with ${B}."
        fi
    done
done

echo ""
echo "----------------------------------------------------"
echo "[INFO] All steps have been submitted to the cluster!"
echo "----------------------------------------------------"