"""
Flight Analysis Distributed Orchestrator DAG

This DAG orchestrates the multi-stage analytical pipeline, supporting conditional 
execution of the Ingestion (NiFi) and Processing (Spark/Livy) layers.

Fully compliant with Apache Airflow 3.0 TaskFlow API and Standard Providers.
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
PAYLOAD_TEMPLATE_PATH = os.path.join(DAGS_DIR, "payload.json")

# Spark application parameters and paths within the distributed nodes
JAR_PATH = "hdfs://hdfs-master.flight-analysis.local:54310/bin/flight-analysis.jar"
JAR_CLASS = "it.uniroma2.sae.FlightAnalysisApp"

# Available configuration options
AVAILABLE_QUERIES = ["monthly_performance", "arrival_delay_ranking", "hourly_delay_percentiles", "airline_clustering"]
AVAILABLE_BACKENDS = ["rdd", "dataframe", "sql"]
AVAILABLE_CONFIGS = ["local-config.yml", "ec2-config.yml", "emr-config.yml"]
AVAILABLE_INPUTS = ["hdfs", "s3", "local"]
AVAILABLE_OUTPUTS = ["hdfs", "postgres", "redis", "hbase", "s3", "local", "cockroach"]
AVAILABLE_METRICS = ["redis"]

# Hardcoded storage configurations for NiFi (Internal Airflow mapping)
STORAGE_MAPPINGS = {
    "HDFS": {"bucket": "", "raw_path": "/data/raw", "preprocessed_path": "/data/conv"},
    "S3": {"bucket": "spark-flight-analysis", "region": "us-east-1", "raw_path": "data/raw", "preprocessed_path": "data/conv"}
}

@dag(
    dag_id="flight_analysis",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["distributed", "spark", "nifi", "orchestration", "airflow-3"],
    params={
        "run_mode": Param("Both", enum=["Both", "Ingest Only", "Execution Only"]),
        "raw_storage_type": Param("HDFS", enum=["HDFS", "S3"]),
        "optimization_strategy": Param("PREDICATE_PUSHDOWN", enum=["PREDICATE_PUSHDOWN", "PARTITION_PRUNING"]),
        "preprocessed_storage_type": Param("HDFS", enum=["HDFS", "S3"]),
        "selected_query": Param("monthly_performance", enum=AVAILABLE_QUERIES),
        "spark_backend": Param("dataframe", enum=AVAILABLE_BACKENDS),
        "config_file": Param("ec2-config.yml", enum=AVAILABLE_CONFIGS),
        "input_type": Param("hdfs", enum=AVAILABLE_INPUTS),
        "output_type": Param("postgres", enum=AVAILABLE_OUTPUTS),
        "metrics_type": Param("redis", enum=AVAILABLE_METRICS)
    },
)
def flight_analysis():

    @task
    def sanitize_id(**context) -> str:
        """Removes special characters from run_id to ensure compatibility."""
        clean_id = re.sub(r'[^a-zA-Z0-9]', '_', context["run_id"])
        print(f"[AIRFLOW] Sanitized Run ID: {clean_id}")
        return clean_id

    @task.branch
    def route_start(clean_id: str, **context) -> str:
        """Branches the execution based on the selected run mode at the very start."""
        if context["params"]["run_mode"] == "Execution Only":
            return "start_spark"
        return "trigger_nifi_backend"

    @task
    def trigger_nifi_backend(clean_id: str, **context) -> str:
        """Initializes the Variable, fetches JWT, builds the payload, and triggers NiFi."""
        params = context["params"]
        var_name = f"nifi_done_{clean_id}"

        # 1. Initialize Variable via Airflow 3 SDK
        Variable.set(var_name, "false")
        print(f"[AIRFLOW] Variable '{var_name}' initialized.")

        # 2. Fetch JWT Token for the callback
        af_host = os.getenv("AIRFLOW_HOST", "airflow.flight-analysis.local")
        af_user = os.getenv("AIRFLOW_USER", "admin")
        af_pass = os.getenv("AIRFLOW_PASSWORD", "admin_password")
        
        login_url = f"http://{af_host}:8088/auth/token"
        login_res = requests.post(login_url, json={"username": af_user, "password": af_pass})
        login_res.raise_for_status()
        jwt_token = login_res.json().get("access_token")

        # 3. Build Payload
        if not os.path.exists(PAYLOAD_TEMPLATE_PATH):
            raise FileNotFoundError(f"Template not found at {PAYLOAD_TEMPLATE_PATH}")

        with open(PAYLOAD_TEMPLATE_PATH, "r") as f:
            payload = json.load(f)

        raw_info = STORAGE_MAPPINGS[params["raw_storage_type"]]
        pre_info = STORAGE_MAPPINGS[params["preprocessed_storage_type"]]

        payload["job_id"] = f"FLIGHT_INGEST_{clean_id}"
        
        # Configure RAW storage
        payload["storage_raw"].update({
            "type": params["raw_storage_type"],
            "bucket": raw_info["bucket"],
            "region": raw_info["region"] if "region" in raw_info else "us-east-1",
            "path": raw_info["raw_path"]
        })
        
        # Configure PREPROCESSED storage
        payload["storage_preprocessed"].update({
            "type": params["preprocessed_storage_type"],
            "bucket": pre_info["bucket"],
            "region": pre_info["region"] if "region" in raw_info else "us-east-1",
            "path": pre_info["preprocessed_path"]
        })
        
        # Mapping processing strategy
        payload["processing"]["optimization_strategy"] = params["optimization_strategy"]

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

        # 4. Trigger NiFi Endpoint
        endpoint = f"http://nifi.flight-analysis.local:8085/experiment"
        nifi_res = requests.post(endpoint, json=payload)
        nifi_res.raise_for_status()
        print(f"[AIRFLOW] NiFi triggered successfully! Status: {nifi_res.status_code}")

        return clean_id

    @task.sensor(poke_interval=30, timeout=3600, mode="reschedule")
    def wait_nifi_done(clean_id: str, **context) -> bool:
        """Polls the Airflow Variable until NiFi updates it to 'true'."""
        var_name = f"nifi_done_{clean_id}"
        try:
            # Safely fetch the variable, defaulting to "false"
            val = str(Variable.get(var_name, default="false")).strip().lower()
            print(f"[AIRFLOW] Sensor poke for {var_name}: {val}")
            return val == "true"
        except Exception as e:
            # Prevent the sensor from crashing on temporary DB locks
            print(f"[AIRFLOW] Sensor exception (ignoring and retrying): {e}")
            return False

    @task.branch
    def route_after_ingest(clean_id: str, **context) -> str:
        """Branches the execution after ingestion is fully completed."""
        if context["params"]["run_mode"] == "Ingest Only":
            return "cleanup_variable"
        return "start_spark"

    @task(trigger_rule="none_failed_min_one_success")
    def cleanup_variable(clean_id: str):
        """Safely removes the temporary variable only when the active workflow completes."""
        var_name = f"nifi_done_{clean_id}"
        try:
            Variable.delete(var_name)
            print(f"[AIRFLOW] Temporary variable {var_name} successfully deleted.")
        except Exception as e:
            print(f"[AIRFLOW] Cleanup skipped or failed (safe to ignore): {e}")

    # =================================================================
    # DAG WIRING & INSTANTIATION
    # =================================================================

    # Synchronization node to safely join branches before Spark
    spark_join = EmptyOperator(
        task_id="start_spark",
        trigger_rule="none_failed_min_one_success"
    )

    clean_id_str = sanitize_id()
    branch_1 = route_start(clean_id_str)
    nifi_task = trigger_nifi_backend(clean_id_str)
    wait_task = wait_nifi_done(clean_id_str)
    branch_2 = route_after_ingest(clean_id_str)
    cleanup_task = cleanup_variable(clean_id_str)

    @task(trigger_rule="none_failed_min_one_success")
    def prepare_spark_args(**context) -> list[str]:
        """Prepares the arguments for the Spark task based on user selection."""
        params = context["params"]
        if params["run_mode"] == "Ingest Only":
            return []
            
        return [
            "--query", params["selected_query"],
            "--backend", params["spark_backend"],
            "--config", params["config_file"],
            "--input-type", params["input_type"],
            "--output-type", params["output_type"],
            "--metrics-type", params["metrics_type"]
        ]

    spark_args = prepare_spark_args()

    # Simple Livy task
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
