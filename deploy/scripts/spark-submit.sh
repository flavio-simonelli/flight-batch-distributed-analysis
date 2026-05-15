#!/bin/bash

# Helper script to submit Spark jobs on EC2
# Usage: ./spark-submit.sh <query_type> <backend_type>
# Example: ./spark-submit.sh monthly_performance dataframe

QUERY_TYPE=${1:-monthly_performance}
BACKEND_TYPE=${2:-dataframe}
OUTPUT_TYPE=${3:-hdfs}

echo "[SPARK] Submitting job: Query=$QUERY_TYPE, Backend=$BACKEND_TYPE, Output=$OUTPUT_TYPE"

docker exec spark-master /opt/spark/bin/spark-submit \
  --master spark://master.flight-analysis.local:7077 \
  --class it.uniroma2.sae.FlightAnalysisApp \
  /opt/spark/scripts/flight-analysis.jar \
  --config ec2-$OUTPUT_TYPE-config.yml \
  --query "$QUERY_TYPE" \
  --backend "$BACKEND_TYPE"

echo "[SPARK] Submission command sent to container."
