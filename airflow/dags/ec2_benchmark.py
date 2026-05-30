"""
Flight Analysis Sequential Benchmark DAG

This DAG executes all possible combinations of Spark queries and backends 
sequentially to generate complete performance comparison data.
"""

from __future__ import annotations

import os
from datetime import datetime
from airflow.sdk import dag, Param
from airflow.providers.apache.livy.operators.livy import LivyOperator

# Spark application parameters (consistent with HDFS deployment)
JAR_PATH = "hdfs://hdfs-master.flight-analysis.local:54310/bin/flight-analysis.jar"
JAR_CLASS = "it.uniroma2.sae.FlightAnalysisApp"
SPARK_CONFIG_PATH = "local-config.yml"

# All available combinations from Spark Java configuration
AVAILABLE_QUERIES = ["monthly_performance", "arrival_delay_ranking", "hourly_delay_percentiles", "airline_clustering"]
AVAILABLE_BACKENDS = ["rdd", "dataframe", "sql"]
AVAILABLE_CONFIGS = ["local-config.yml", "ec2-config.yml", "emr-config.yml"]
AVAILABLE_INPUTS = ["local", "hdfs", "s3"]
AVAILABLE_OUTPUTS = ["local", "hdfs", "s3", "cockroach", "postgres", "hbase", "redis"]
AVAILABLE_METRICS = ["redis"]

@dag(
    dag_id="ec2_benchmark",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["distributed", "benchmark", "performance", "spark", "processing", "ec2"],
    params={
        "config_file": Param("ec2-config.yml", enum=AVAILABLE_CONFIGS, description="Select the configuration file for the Spark job."),
        "input_type": Param("hdfs", enum=AVAILABLE_INPUTS, description="Select the input type for the Spark job."),
        "output_type": Param("cockroach", enum=AVAILABLE_OUTPUTS, description="Select the output type for the Spark job."),
        "metrics_type": Param("redis", enum=AVAILABLE_METRICS, description="Select the metrics type for the Spark job."),
    },
    description="Runs all query/backend combinations sequentially for benchmarking on AWS EC2."
)
def flight_benchmark():

    prev_task = None

    # Loop through all combinations of queries and backends to create a LivyOperator task for each
    for query in AVAILABLE_QUERIES:
        for backend in AVAILABLE_BACKENDS:

            # Skip incompatible combinations (airline_clustering doesn't support rdd or sql)
            if query == "airline_clustering" and (backend == "rdd" or backend == "sql"):
                continue

            task_id = f"spark_{query}_{backend}"

            # Initialize LivyOperator for this specific combination
            spark_task = LivyOperator(
                task_id=task_id,
                livy_conn_id="livy_default",
                file=JAR_PATH,
                class_name=JAR_CLASS,
                args=[
                    "--query", query, 
                    "--backend", backend, 
                    "--config", "{{ params.config_file }}",
                    "--input-type", "{{ params.input_type }}",
                    "--output-type", "{{ params.output_type }}",
                    "--metrics-type", "{{ params.metrics_type }}"
                ],
                name=f"benchmark-{query}-{backend}-{{{{ run_id }}}}",
                polling_interval=5
            )

            # Wire sequentially
            if prev_task:
                prev_task >> spark_task
            
            prev_task = spark_task

flight_benchmark()
