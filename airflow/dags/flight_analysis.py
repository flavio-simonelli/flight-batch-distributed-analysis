"""
Flight Analysis Distributed Orchestrator DAG

This DAG orchestrates the multi-stage analytical pipeline, supporting conditional 
execution of the Ingestion (NiFi) and Processing (Spark/Livy) layers.

Flow:
  1. Sanitize the run_id to be compatible with Airflow Variable keys and URLs.
  2. Read the base NiFi template from 'payload.json'.
  3. Dynamically inject user-selected parameters and callback metadata.
  4. Trigger NiFi preprocessing via HTTP.
  5. Wait for NiFi to signal completion via Airflow REST API (PATCH Variable).
  6. Execute Spark analytical queries via Apache Livy.
  7. Cleanup the temporary variable.
"""

from __future__ import annotations

import json
import os
import re
import base64
from datetime import datetime

from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.models.param import Param
from airflow.providers.apache.livy.operators.livy import LivyOperator
from airflow.providers.http.operators.http import HttpOperator
from airflow.sensors.python import PythonSensor
from airflow.operators.empty import EmptyOperator

# Path resolution for configuration files and templates
DAGS_DIR = os.path.dirname(__file__)
PAYLOAD_TEMPLATE_PATH = os.path.join(DAGS_DIR, "payload.json")

# Spark application parameters and paths within the distributed nodes
JAR_PATH = "/opt/spark/scripts/flight-analysis.jar"
JAR_CLASS = "it.uniroma2.sae.FlightAnalysisApp"
SPARK_CONFIG_PATH = "/opt/spark/scripts/ec2-config.yml"

# Hardcoded storage configurations mapping engine types to buckets and paths.
STORAGE_MAPPINGS = {
    "HDFS": {
        "bucket": "", 
        "raw_path": "/data/raw",
        "preprocessed_path": "/data/conv"
    },
    "S3": {
        "bucket": "spark-flight-analysis",
        "raw_path": "/data/raw",
        "preprocessed_path": "/data/conv"
    }
}

QUERIES = [
    "monthly_performance",
    "arrival_delay_ranking",
    "hourly_delay_percentiles",
]

@dag(
    dag_id="flight_analysis",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["distributed", "spark", "nifi", "orchestration"],
    params={
        "run_mode": Param(
            "Both",
            type="string",
            description="Select which parts of the pipeline to execute.",
            enum=["Both", "Ingest Only", "Execution Only"]
        ),
        "raw_storage_type": Param(
            "HDFS",
            type="string",
            description="Select the storage backend for RAW data.",
            enum=["HDFS", "S3"]
        ),
        "preprocessed_storage_type": Param(
            "HDFS",
            type="string",
            description="Select the storage backend for PREPROCESSED data.",
            enum=["HDFS", "S3"]
        ),
        "optimization_strategy": Param(
            "PREDICATE_PUSHDOWN",
            type="string",
            description="Select the Spark optimization strategy for the processing phase.",
            enum=["PREDICATE_PUSHDOWN", "PARTITION_PRUNING"]
        )
    },
)
def flight_analysis():

    @task
    def sanitize_id(**context) -> str:
        """Removes special characters from run_id to ensure compatibility with Airflow APIs."""
        raw_id = context["run_id"]
        # Replace :, +, - and other non-alphanumeric chars with underscore
        clean_id = re.sub(r'[^a-zA-Z0-9]', '_', raw_id)
        print(f"[AIRFLOW] Sanitized Run ID: {clean_id}")
        return clean_id

    @task.branch
    def determine_workflow_path(clean_id: str, **context) -> str:
        """Branches the execution based on the selected run mode."""
        mode = context["params"]["run_mode"]
        if mode == "Execution Only":
            return "spark_monthly_performance"
        return "build_nifi_payload"

    @task
    def build_nifi_payload(clean_id: str, **context) -> str:
        """Reads 'payload.json' and populates it with dynamic parameters and callback auth."""
        params = context["params"]
        raw_info = STORAGE_MAPPINGS[params["raw_storage_type"]]
        pre_info = STORAGE_MAPPINGS[params["preprocessed_storage_type"]]
        
        if not os.path.exists(PAYLOAD_TEMPLATE_PATH):
            raise FileNotFoundError(f"NiFi payload template not found at {PAYLOAD_TEMPLATE_PATH}")

        with open(PAYLOAD_TEMPLATE_PATH, "r") as f:
            payload = json.load(f)
        
        # Pre-create the variable in Airflow to avoid 404 errors during sensing
        var_name = f"nifi_done_{clean_id}"
        Variable.set(var_name, "false")
        
        # Prepare Basic Auth header for NiFi callback (using admin credentials from env)
        # In a real scenario, use Airflow Connections to store these safely.
        user_pass = f"admin:admin_password"
        encoded_auth = base64.b64encode(user_pass.encode()).decode()

        # Inject dynamic identifiers
        payload["job_id"] = f"FLIGHT_INGEST_{clean_id}"
        
        # Configure RAW storage
        payload["storage_raw"]["type"] = params["raw_storage_type"]
        payload["storage_raw"]["bucket"] = raw_info["bucket"]
        payload["storage_raw"]["path"] = raw_info["raw_path"]
        
        # Configure PREPROCESSED storage
        payload["storage_preprocessed"]["type"] = params["preprocessed_storage_type"]
        payload["storage_preprocessed"]["bucket"] = pre_info["bucket"]
        payload["storage_preprocessed"]["path"] = pre_info["preprocessed_path"]
        
        # Mapping processing strategy
        payload["processing"]["optimization_strategy"] = params["optimization_strategy"]
        
        # Configure callback with Basic Auth and Sanitized URL
        payload["callback"]["run_id"] = clean_id
        payload["callback"]["url"] = f"http://airflow.flight-analysis.local:8088/api/v2/variables/{var_name}"
        payload["callback"]["headers"] = {
            "Authorization": f"Basic {encoded_auth}",
            "Content-Type": "application/json"
        }
        
        return json.dumps(payload)

    # Workflow Orchestration
    clean_run_id = sanitize_id()
    branch_op = determine_workflow_path(clean_run_id)
    
    payload_op = build_nifi_payload(clean_run_id)
    branch_op >> payload_op

    # Trigger Ingestion
    trigger_nifi = HttpOperator(
        task_id="trigger_nifi",
        http_conn_id="nifi_http",
        method="POST",
        endpoint="/experiment",
        data="{{ ti.xcom_pull(task_ids='build_nifi_payload') }}",
        headers={"Content-Type": "application/json"},
        response_check=lambda response: response.status_code in (200, 202),
    )
    payload_op >> trigger_nifi

    # Wait for Callback
    def _is_ingest_finished(clean_id: str, **context) -> bool:
        var_name = f"nifi_done_{clean_id}"
        return Variable.get(var_name, default_var="false") == "true"

    wait_nifi_callback = PythonSensor(
        task_id="wait_nifi_done",
        python_callable=_is_ingest_finished,
        op_args=[clean_run_id],
        poke_interval=30,
        timeout=3600,
        mode="reschedule",
    )
    trigger_nifi >> wait_nifi_callback

    # Post-Ingest Branching
    @task.branch
    def check_execution_required(mode: str) -> str:
        return "end_ingest_only" if mode == "Ingest Only" else "spark_monthly_performance"

    execution_branch = check_execution_required(context["params"]["run_mode"])
    wait_nifi_callback >> execution_branch

    end_ingest = EmptyOperator(task_id="end_ingest_only")
    execution_branch >> end_ingest

    # Spark Processing
    spark_tasks = []
    for query in QUERIES:
        t = LivyOperator(
            task_id=f"spark_{query}",
            livy_conn_id="livy_default",
            file=JAR_PATH,
            class_name=JAR_CLASS,
            args=[
                "--query", query, 
                "--backend", "dataframe", 
                "--config", SPARK_CONFIG_PATH
            ],
            name=f"spark-{query}-" + "{{ ti.xcom_pull(task_ids='sanitize_id') }}",
            polling_interval=15,
            trigger_rule="one_success" 
        )
        spark_tasks.append(t)

    # Cleanup Variable
    @task(trigger_rule="all_done")
    def cleanup_variable(clean_id: str):
        var_name = f"nifi_done_{clean_id}"
        Variable.delete(var_name)
        print(f"[AIRFLOW] Deleted temporary variable: {var_name}")

    cleanup_op = cleanup_variable(clean_run_id)

    # Wiring the Graph
    execution_branch >> spark_tasks[0]
    branch_op >> spark_tasks[0] # Execution Only path

    for i in range(1, len(spark_tasks)):
        spark_tasks[0] >> spark_tasks[i]
    
    spark_tasks[-1] >> cleanup_op

flight_analysis()
