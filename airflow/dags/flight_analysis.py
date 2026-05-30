"""
Flight Analysis Distributed Orchestrator DAG

This DAG orchestrates the multi-stage analytical pipeline, supporting conditional 
execution of the Ingestion and Processing layers.
"""

from __future__ import annotations

import json
import os
import re
import requests
from datetime import datetime

from airflow.sdk import dag, task, Param, Variable
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.apache.livy.operators.livy import LivyOperator

# Path resolution for configuration files and templates
DAGS_DIR = os.path.dirname(__file__)
TEMPLATES_DIR = os.path.join(DAGS_DIR, "..", "templates")
PAYLOAD_TEMPLATE_PATH = os.path.join(TEMPLATES_DIR, "nifi_payload.json")

# Spark application parameters and paths within the distributed nodes
JAR_PATH = "hdfs://hdfs-master.flight-analysis.local:54310/bin/flight-analysis.jar"
JAR_CLASS = "it.uniroma2.sae.FlightAnalysisApp"

# Available configuration options
AVAILABLE_QUERIES = ["monthly_performance", "arrival_delay_ranking", "hourly_delay_percentiles", "airline_clustering"]
AVAILABLE_BACKENDS = ["rdd", "dataframe", "sql"]
AVAILABLE_CONFIGS = ["local-config.yml", "ec2-config.yml", "emr-config.yml"]
AVAILABLE_INPUTS = ["local", "hdfs", "s3"]
AVAILABLE_OUTPUTS = ["local", "hdfs", "s3", "cockroach", "postgres", "hbase", "redis"]
AVAILABLE_METRICS = ["redis"]

# Hardcoded storage configurations for NiFi
STORAGE_MAPPINGS = {
    "HDFS": {"bucket": "", "raw_path": "/data/raw", "preprocessed_path": "/data/conv"},
    "S3": {"bucket": "spark-flight-analysis", "region": "us-east-1", "raw_path": "data/raw", "preprocessed_path": "data/conv"}
}

@dag(
    dag_id="flight_analysis",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["distributed", "spark", "nifi", "orchestration", "ingestion", "processing"],
    params={
        "run_mode": Param("Both", enum=["Both", "Ingest Only", "Execution Only"], description="Select 'Ingest Only' to run just the NiFi ingestion, 'Execution Only' to run just the Spark processing, or 'Both' to run the full pipeline."),
        "raw_storage_type": Param("HDFS", enum=["HDFS", "S3"], description="Storage type for raw data."),
        "optimization_strategy": Param("PREDICATE_PUSHDOWN", enum=["PREDICATE_PUSHDOWN", "PARTITION_PRUNING"], description="Optimization strategy for the Parquet conversion."),
        "preprocessed_storage_type": Param("HDFS", enum=["HDFS", "S3"], description="Storage type for preprocessed data."),
        "selected_query": Param("monthly_performance", enum=AVAILABLE_QUERIES, description="Select which analytical query to run in Spark."),
        "spark_backend": Param("dataframe", enum=AVAILABLE_BACKENDS, description="Select the backend for the Spark job."),
        "config_file": Param("ec2-config.yml", enum=AVAILABLE_CONFIGS, description="Select the configuration file for the Spark job."),
        "input_type": Param("hdfs", enum=AVAILABLE_INPUTS, description="Select the input type for the Spark job."),
        "output_type": Param("cockroach", enum=AVAILABLE_OUTPUTS, description="Select the output type for the Spark job."),
        "metrics_type": Param("redis", enum=AVAILABLE_METRICS, description="Select the metrics type for the Spark job."),
        "output_partitions": Param(0, type="integer", minimum=0, description="Number of output partitions. 0 means no coalescing.")
    },
    description="Orchestrates the full flight analysis pipeline with conditional execution paths for ingestion and processing."
)
def flight_analysis():

    @task
    def sanitize_id(**context) -> str:
        """Removes special characters from run_id to ensure compatibility."""
        clean_id = re.sub(r'[^a-zA-Z0-9]', '_', context.get("run_id", "unknown_run"))
        print(f"[SAE] Sanitized Run ID: {clean_id}")
        return clean_id

    @task.branch
    def route_start(**context) -> str:
        """Branches the execution based on the selected run mode at the very start."""
        params = context.get("params", {})
        run_mode = params.get("run_mode", "Both")

        if run_mode == "Execution Only":
            print("[SAE] Run mode is 'Execution Only'. Skipping NiFi ingestion and proceeding directly to Spark execution.")
            return "start_spark"

        print(f"[SAE] Run mode is '{run_mode}'. Proceeding with NiFi ingestion.")
        return "trigger_nifi_backend"

    @task
    def trigger_nifi_backend(clean_id: str, **context) -> str:
        """Initializes the Variable, fetches JWT, builds the payload, and triggers NiFi."""
        params = context.get("params", {})
        var_name = f"nifi_done_{clean_id}"

        # Initialize Variable via Airflow  SDK
        Variable.set(var_name, "false")
        print(f"[SAE] Variable '{var_name}' initialized.")

        # Fetch JWT Token for the callback
        af_host = os.getenv("AIRFLOW_HOST", "airflow.flight-analysis.local")
        af_user = os.getenv("AIRFLOW_USER", "admin")
        af_pass = os.getenv("AIRFLOW_PASSWORD", "admin_password")
        
        login_url = f"http://{af_host}:8088/auth/token"
        login_res = requests.post(login_url, json={"username": af_user, "password": af_pass})
        login_res.raise_for_status()
        jwt_token = login_res.json().get("access_token")
        print("[SAE] JWT Token fetched successfully.")

        # Build Payload
        if not os.path.exists(PAYLOAD_TEMPLATE_PATH):
            raise FileNotFoundError(f"Template not found at {PAYLOAD_TEMPLATE_PATH}")

        with open(PAYLOAD_TEMPLATE_PATH, "r") as f:
            payload = json.load(f)

        raw_storage_type = params.get("raw_storage_type")
        preprocessed_storage_type = params.get("preprocessed_storage_type")
        raw_info = STORAGE_MAPPINGS.get(raw_storage_type)
        pre_info = STORAGE_MAPPINGS.get(preprocessed_storage_type)
        if not raw_info or not pre_info:
            raise ValueError(f"Invalid storage type(s) provided: raw='{raw_storage_type}', preprocessed='{preprocessed_storage_type}'")
        print(f"[SAE] Storage configurations resolved: RAW={raw_info}, PREPROCESSED={pre_info}")

        payload["job_id"] = f"FLIGHT_INGEST_{clean_id}"
        print(f"[SAE] Job ID initialized: {payload['job_id']}")

        # Configure RAW storage
        payload["storage_raw"].update({
            "type": params.get("raw_storage_type"),
            "bucket": raw_info.get("bucket", ""),
            "region": raw_info.get("region", "us-east-1"),
            "path": raw_info.get("raw_path", "")
        })
        print(f"[SAE] RAW storage configured: {payload['storage_raw']}")
        
        # Configure PREPROCESSED storage
        payload["storage_preprocessed"].update({
            "type": params.get("preprocessed_storage_type"),
            "bucket": pre_info.get("bucket", ""),
            "region": pre_info.get("region", "us-east-1"),
            "path": pre_info.get("preprocessed_path", "")
        })
        print(f"[SAE] PREPROCESSED storage configured: {payload['storage_preprocessed']}")
        
        # Mapping processing strategy
        opt_strategy = params.get("optimization_strategy", "PARTITION_PRUNING")
        payload["processing"]["optimization_strategy"] = opt_strategy
        print(f"[SAE] Optimization strategy configured: {opt_strategy}")

        # Ensure NiFi callback is configured for a PATCH request
        payload["callback"].update({
            "run_id": clean_id,
            "variable_key": var_name,
            "url": f"http://airflow.flight-analysis.local:8088/api/v2/variables/{var_name}",
            "method": "PATCH",
            "headers": {
                "Authorization": f"Bearer {jwt_token}",
                "Content-Type": "application/json"
            }
        })
        print(f"[SAE] Callback configuration set: {payload['callback']}")

        # Trigger NiFi Endpoint
        endpoint = f"http://nifi.flight-analysis.local:8085/experiment"
        nifi_res = requests.post(endpoint, json=payload)
        nifi_res.raise_for_status()
        print(f"[SAE] NiFi triggered successfully! Status: {nifi_res.status_code}")

        return clean_id

    @task.sensor(poke_interval=15, timeout=3600, mode="reschedule")
    def wait_nifi_done(clean_id: str, **context) -> bool:
        """Polls the Airflow Variable until NiFi updates it to 'true'."""
        var_name = f"nifi_done_{clean_id}"
        try:
            # Safely fetch the variable, defaulting to "false"
            val = str(Variable.get(var_name, default="false")).strip().lower()
            print(f"[SAE] Sensor poke for {var_name}: {val}")
            return val == "true"
        except Exception as e:
            # Prevent the sensor from crashing on temporary DB locks
            print(f"[SAE] Sensor exception (ignoring and retrying): {e}")
            return False

    @task.branch
    def route_after_ingest(**context) -> str:
        """Branches the execution after ingestion is fully completed."""

        params = context.get("params", {})
        run_mode = params.get("run_mode", "Both")

        if run_mode == "Ingest Only":
            print("[SAE] Run mode is 'Ingest Only'. Skipping Spark execution and proceeding to cleanup.")
            return "cleanup_variable"

        print(f"[SAE] Run mode is '{run_mode}'. Proceeding with Spark execution.")
        return "start_spark"

    @task(trigger_rule="none_failed_min_one_success")
    def cleanup_variable(clean_id: str):
        """Safely removes the temporary variable only when the active workflow completes."""
        var_name = f"nifi_done_{clean_id}"
        try:
            Variable.delete(var_name)
            print(f"[SAE] Temporary variable {var_name} successfully deleted.")
        except Exception as e:
            print(f"[SAE] Cleanup skipped or failed (safe to ignore): {e}")

    # =================================================================
    # DAG WIRING & INSTANTIATION
    # =================================================================

    # Synchronization node to safely join branches before Spark
    spark_join = EmptyOperator(
        task_id="start_spark",
        trigger_rule="none_failed_min_one_success"
    )

    clean_id_str = sanitize_id()
    branch_1 = route_start()
    nifi_task = trigger_nifi_backend(clean_id_str)
    wait_task = wait_nifi_done(clean_id_str)
    branch_2 = route_after_ingest()
    cleanup_task = cleanup_variable(clean_id_str)

    @task(trigger_rule="none_failed_min_one_success")
    def prepare_spark_args(**context) -> list[str]:
        """Prepares the arguments for the Spark task based on user selection."""
        params = context.get("params", {})
        run_mode = params.get("run_mode", "Both")
        
        if run_mode == "Ingest Only":
            return []
            
        args = [
            "--query", params.get("selected_query"),
            "--backend", params.get("spark_backend"),
            "--config", params.get("config_file"),
            "--input-type", params.get("input_type"),
            "--output-type", params.get("output_type"),
            "--metrics-type", params.get("metrics_type")
        ]

        if params.get("output_partitions", 0) > 0:
            args.extend(["--partitions", str(params.get("output_partitions"))])

        return args

    spark_args = prepare_spark_args()

    # Set Livy task to only run if Spark execution is selected,
    # and ensure it waits for the NiFi branch to complete if needed
    spark_task = LivyOperator(
        task_id="spark_execution",
        livy_conn_id="livy_default",
        file=JAR_PATH,
        class_name=JAR_CLASS,
        args=spark_args,
        name=f"spark-{{{{ params.selected_query }}}}-{{{{ ti.xcom_pull(task_ids='sanitize_id') }}}}",
        polling_interval=5
    )

    # Routing from Start
    branch_1 >> [nifi_task, spark_join]
    nifi_task >> wait_task >> branch_2

    # Routing after Ingestion
    branch_2 >> [spark_join, cleanup_task]

    # Spark Execution Logic
    spark_join >> spark_args >> spark_task >> cleanup_task

flight_analysis()
