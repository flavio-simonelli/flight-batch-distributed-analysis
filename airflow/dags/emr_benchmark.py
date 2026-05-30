"""
Flight Analysis Sequential Benchmark DAG

This DAG executes all possible combinations of Spark queries and backends 
sequentially to generate complete performance comparison data.
"""

from __future__ import annotations

import os
from datetime import datetime
from airflow.sdk import dag, Param
from airflow.providers.amazon.aws.operators.emr import EmrAddStepsOperator
from airflow.providers.amazon.aws.sensors.emr import EmrStepSensor

# Spark application parameters (MUST be hosted on S3 for EMR)
S3_BUCKET = "spark-flight-analysis"
JAR_PATH = f"s3://{S3_BUCKET}/spark/flight-analysis.jar"
JAR_CLASS = "it.uniroma2.sae.FlightAnalysisApp"

# All available combinations from Spark Java configuration
AVAILABLE_QUERIES = ["monthly_performance", "arrival_delay_ranking", "hourly_delay_percentiles", "airline_clustering"]
AVAILABLE_BACKENDS = ["rdd", "dataframe", "sql"]
AVAILABLE_CONFIGS = ["local-config.yml", "ec2-config.yml", "emr-config.yml"]
AVAILABLE_INPUTS = ["local", "hdfs", "s3"]
AVAILABLE_OUTPUTS = ["local", "hdfs", "s3", "cockroach", "postgres", "hbase", "redis"]
AVAILABLE_METRICS = ["redis"]

@dag(
    dag_id="emr_benchmark",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["distributed", "benchmark", "performance", "spark", "processing", "emr"],
    params={
        "job_flow_id": Param("j-XXXXXXXXXXXXX", type="string", description="The EMR Cluster ID"),
        "config_file": Param("emr-config.yml", enum=AVAILABLE_CONFIGS, description="Select the configuration file for the Spark job."),
        "input_type": Param("s3", enum=AVAILABLE_INPUTS, description="Select the input type for the Spark job."),
        "output_type": Param("s3", enum=AVAILABLE_OUTPUTS, description="Select the output type for the Spark job."),
        "metrics_type": Param("redis", enum=AVAILABLE_METRICS, description="Select the metrics type for the Spark job."),
    },
    description="Runs all query/backend combinations sequentially for benchmarking on AWS EMR."
)
def flight_benchmark_emr():

    prev_task = None

    # Loop through all combinations of queries and backends to create a LivyOperator task for each
    for query in AVAILABLE_QUERIES:
        for backend in AVAILABLE_BACKENDS:

            # Skip incompatible combinations (airline_clustering doesn't support rdd or sql)
            if query == "airline_clustering" and (backend == "rdd" or backend == "sql"):
                continue

            step_name = f"benchmark-{query}-{backend}"
            add_step_task_id = f"add_step_{query}_{backend}"
            sensor_task_id = f"watch_step_{query}_{backend}"

            # Define the EMR Step
            spark_step = {
                "Name": step_name,
                "ActionOnFailure": "CONTINUE",
                "HadoopJarStep": {
                    "Jar": "command-runner.jar",
                    "Args": [
                        "spark-submit",
                        "--deploy-mode", "client",
                        "--class", JAR_CLASS,
                        JAR_PATH,
                        "--query", query,
                        "--backend", backend,
                        "--config", "{{ params.config_file }}",
                        "--input-type", "{{ params.input_type }}",
                        "--output-type", "{{ params.output_type }}",
                        "--metrics-type", "{{ params.metrics_type }}"
                    ],
                },
            }

            # Operator to add the step to the EMR cluster
            add_step = EmrAddStepsOperator(
                task_id=add_step_task_id,
                job_flow_id="{{ params.job_flow_id }}",
                aws_conn_id="aws_default",
                steps=[spark_step],
            )

            # Sensor to poll the step status until COMPLETED (or FAILED)
            watch_step = EmrStepSensor(
                task_id=sensor_task_id,
                job_flow_id="{{ params.job_flow_id }}",
                # Pull the dynamically generated Step ID from the AddStepsOperator
                step_id=f"{{{{ ti.xcom_pull(task_ids='{add_step_task_id}')[0] }}}}",
                aws_conn_id="aws_default",
                poke_interval=30,
            )

            # Wire sequentially
            # Previous Sensor -> Current Add Step -> Current Sensor
            add_step >> watch_step
            if prev_task:
                prev_task >> add_step

            prev_task = watch_step

flight_benchmark_emr()
