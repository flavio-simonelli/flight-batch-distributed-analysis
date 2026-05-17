#!/bin/bash

# Script for automated execution of Spark jobs on EC2
# Executes all possible combinations of Query and Backend

# Definition of datasets to be processed
QUERIES=("monthly_performance" "arrival_delay_ranking" "hourly_delay_percentiles")  # Add or remove project queries
BACKENDS=("rdd" "dataframe" "sql")                                                  # The required approaches
OUTPUT_TYPE="postgres"                                                              # Default output (e.g., hdfs or redis)

echo "[SPARK-BENCHMARK] Starting batch execution of Spark jobs..."
echo "--------------------------------------------------------"

# Nested loops to cycle through every combination
for QUERY_TYPE in "${QUERIES[@]}"; do
    for BACKEND_TYPE in "${BACKENDS[@]}"; do

        echo ""
        echo "========================================================"
        echo "[SPARK] Starting new Job"
        echo "========================================================"

        # Execution of the command inside the spark-master container
        ./spark-submit.sh $QUERY_TYPE $BACKEND_TYPE $OUTPUT_TYPE

        # Check exit status of the previous job
        if [ $? -eq 0 ]; then
            echo "[SUCCESS] Job completed successfully: $QUERY_TYPE ($BACKEND_TYPE)"
        else
            echo "[ERROR] Job encountered a failure: $QUERY_TYPE ($BACKEND_TYPE)"
            # Optional: uncomment the line below if you want to stop the script on a single job failure
            exit 1
        fi

        echo "--------------------------------------------------------"
        # Small sleep to allow the cluster to breathe and give Redis/HDFS time to release resources
        sleep 3

    done
done

echo "[SPARK-BENCHMARK] All jobs have been submitted to the cluster."