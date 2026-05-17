"""
Flight Analysis Sequential Benchmark DAG

This DAG executes all possible combinations of Spark queries and backends 
sequentially to generate complete performance comparison data.

Total Tasks: 4 Queries * 3 Backends = 12 Spark Jobs.
"""

from __future__ import annotations

import os
from datetime import datetime
from airflow.sdk import dag
from airflow.providers.apache.livy.operators.livy import LivyOperator

# Spark application parameters (consistent with HDFS deployment)
JAR_PATH = "hdfs://hdfs-master.flight-analysis.local:54310/bin/flight-analysis.jar"
JAR_CLASS = "it.uniroma2.sae.FlightAnalysisApp"
SPARK_CONFIG_PATH = "local-config.yml"

# All available combinations from Spark Java configuration
AVAILABLE_QUERIES = ["monthly_performance", "arrival_delay_ranking", "hourly_delay_percentiles", "airline_clustering"]
AVAILABLE_BACKENDS = ["rdd", "dataframe", "sql"]

@dag(
    dag_id="flight_benchmark",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["benchmark", "spark", "performance", "airflow-3"],
    description="Runs all query/backend combinations sequentially for benchmarking."
)
def flight_benchmark():

    prev_task = None

    for query in AVAILABLE_QUERIES:
        for backend in AVAILABLE_BACKENDS:
            task_id = f"spark_{query}_{backend}"
            
            # Initialize LivyOperator for this specific combination
            spark_task = LivyOperator(
                task_id=task_id,
                livy_conn_id="livy_default",
                file=JAR_PATH,
                class_name=JAR_CLASS,
                args=["--query", query, "--backend", backend, "--config", SPARK_CONFIG_PATH],
                name=f"benchmark-{query}-{backend}-{{{{ run_id }}}}",
                polling_interval=15
            )

            # Wire sequentially
            if prev_task:
                prev_task >> spark_task
            
            prev_task = spark_task

flight_benchmark()
