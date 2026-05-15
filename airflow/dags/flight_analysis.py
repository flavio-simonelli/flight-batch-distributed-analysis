"""
Flight Analysis DAG

Flow:
  1. Trigger NiFi preprocessing via HTTP, passing the run_id so NiFi can
     reference it in the callback.
  2. Wait until NiFi calls back Airflow by setting Variable 'nifi_done_<run_id>'.
  3. Run the three Spark queries in parallel via Apache Livy.

NiFi callback setup:
  NiFi must send a PATCH request to Airflow when preprocessing is complete:
    PATCH http://airflow-apiserver:8080/api/v2/variables/nifi_done_<run_id>
    Authorization: Basic <base64(user:password)>
    Content-Type: application/json
    Body: {"key": "nifi_done_<run_id>", "value": "true"}
"""

from __future__ import annotations

import json
import os
from datetime import datetime

from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.providers.apache.livy.operators.livy import LivyOperator
from airflow.providers.http.operators.http import HttpOperator
from airflow.sensors.python import PythonSensor

JAR_PATH = "/opt/spark/scripts/flight-analysis-spark-1.0-SNAPSHOT.jar"
JAR_CLASS = "it.uniroma2.sae.FlightAnalysisApp"
SPARK_CONFIG_PATH = "/opt/spark/scripts/src/main/resources/compose-config.yml"
TEMPLATE_PATH = os.path.join(os.path.dirname(__file__), "../templates/nifi_job_template.json")

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
    tags=["flight", "spark", "nifi"],
)
def flight_analysis():

    @task
    def build_nifi_payload(**context) -> str:
        """Legge il template, compila la sezione callback con i dati del run corrente."""
        run_id = context["run_id"]
        with open(TEMPLATE_PATH) as f:
            payload = json.load(f)
        payload["callback"]["run_id"] = run_id
        payload["callback"]["url"] = (
            f"http://airflow-apiserver:8080/api/v2/variables/nifi_done_{run_id}"
        )
        return json.dumps(payload)

    payload = build_nifi_payload()

    trigger_nifi = HttpOperator(
        task_id="trigger_nifi",
        http_conn_id="nifi_http",
        method="POST",
        endpoint="/",
        data="{{ ti.xcom_pull(task_ids='build_nifi_payload') }}",
        headers={"Content-Type": "application/json"},
        response_check=lambda response: response.status_code in (200, 202),
    )

    def _nifi_done(**context) -> bool:
        """Returns True once NiFi has set the callback Variable."""
        value = Variable.get(f"nifi_done_{context['run_id']}", default_var=None)
        return value == "true"

    wait_nifi = PythonSensor(
        task_id="wait_nifi_done",
        python_callable=_nifi_done,
        poke_interval=30,
        timeout=3600,
        mode="reschedule",
    )

    spark_tasks = [
        LivyOperator(
            task_id=f"spark_{query}",
            livy_conn_id="livy_default",
            file=JAR_PATH,
            class_name=JAR_CLASS,
            args=["--query", query, "--backend", "dataframe", "--config", SPARK_CONFIG_PATH],
            name=f"flight-analysis-{query}",
            polling_interval=15,
        )
        for query in QUERIES
    ]

    payload >> trigger_nifi >> wait_nifi >> spark_tasks


flight_analysis()
