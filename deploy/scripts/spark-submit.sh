#!/bin/bash

# Helper script for submitting Spark analytical jobs on the EC2 cluster.
# This script automates the 'docker exec' command targeting the spark-master container.

# Usage: ./spark-submit.sh <query_type> <backend_type>
# Supported Query Types: monthly_performance, arrival_delay_ranking, hourly_delay_percentiles
# Supported Backends: rdd, dataframe, sql

# Retrieve query and backend types from arguments, defaulting to common values if omitted.
QUERY_TYPE=${1:-monthly_performance}
BACKEND_TYPE=${2:-dataframe}

echo "[SPARK] Initiating Spark job submission..."
echo "[SPARK] Configuration: Query=$QUERY_TYPE, Backend=$BACKEND_TYPE"

# Execute the spark-submit binary within the spark-master container.
# Configuration is loaded from the internal container path mapped to the spark application folder.
docker exec spark-master /opt/spark/bin/spark-submit \
  --master spark://master.flight-analysis.local:7077 \
  --class it.uniroma2.sae.FlightAnalysisApp \
  /opt/spark/scripts/flight-analysis.jar \
  --config /opt/spark/scripts/ec2-config.yml \
  --query "$QUERY_TYPE" \
  --backend "$BACKEND_TYPE"

echo "[SPARK] Submission command successfully transmitted to the master container."
