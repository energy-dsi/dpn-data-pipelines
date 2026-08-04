# Copyright DSI Project
#
# Licensed under the Apache License, Version 2.0 (the "License");
# +---------+----------------------------------------------------------+---------------+-------------+
# | Version | Description                                              | Change Owner  | Change Date |
# +---------+----------------------------------------------------------+---------------+-------------+
# | 1.0.0   | Initial version — Kafka trigger pattern                  | DSI Team      | 2026-05-26  |
# +---------+----------------------------------------------------------+---------------+-------------+
"""
Airflow DAG — Producer · File · HP Natural Oil (Kafka trigger pattern)

HOW THIS DAG WORKS
──────────────────
The pipeline pods (adaptor, schema mapper) are already running in Kubernetes
as long-lived Deployments. This DAG does NOT create new pods. Instead it
orchestrates the existing pods by publishing trigger messages to Kafka.

  Task 1: trigger_adaptor
    Publishes a JSON trigger to dpn-pipeline-control.
    The hp-natural-oil adaptor pod receives it, runs one cycle (list files
    → copy to staging → publish file-ready events), then publishes a
    completion status to dpn-pipeline-status.

  Task 2: wait_adaptor_done
    Polls dpn-pipeline-status until it finds a "completed" message matching
    this DAG run's run_id and stage="adaptor".
    Only then moves to task 3.

  Task 3: trigger_schema_mapper
    Publishes a trigger to dpn-pipeline-control for stage="schema_mapper".
    The schema mapper pod enters "drain mode": processes all file-ready
    events the adaptor just published, then signals completion.
    (Without this explicit trigger the schema mapper would process the
    events eventually on its own, but Airflow would have no way to know
    when it finished.)

  Task 4: wait_schema_mapper_done
    Polls dpn-pipeline-status until it finds a "completed" message matching
    this run_id and stage="schema_mapper".
    DAG succeeds when this task completes.

DEPENDENCY CHAIN
────────────────
  trigger_adaptor
       ↓
  wait_adaptor_done          (Airflow waits here — no new pods)
       ↓
  trigger_schema_mapper
       ↓
  wait_schema_mapper_done    (Airflow waits here — no new pods)

WHY NOT KubernetesPodOperator?
──────────────────────────────
KubernetesPodOperator creates a NEW pod per task run.
That would leave us with two adaptor pods running simultaneously:
  - the existing always-on Kubernetes Deployment
  - plus the new Airflow-created temporary pod
This DAG avoids that entirely. The existing pods do all the work.

WHAT CHANGES PER PRODUCT
────────────────────────
To add eq-neso-oil, copy this file and change:
  PRODUCT = "eq-neso-oil"
  dag_id  = "producer_topic_eq_neso_oil"   (also change file and pipeline type)
  SCHEDULE env var name
  import   (from producer.topic.eq_neso_oil...)
Everything else is identical.
"""
from __future__ import annotations

import json
import os
import sys
from datetime import timedelta

from airflow import DAG
from airflow.models import BaseOperator
from airflow.operators.python import PythonOperator
from airflow.sensors.base import BaseSensorOperator
from airflow.utils.context import Context
from airflow.utils.dates import days_ago

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Kafka trigger helpers — no pipeline business logic imported here
from utils.kafka_trigger import (
    build_trigger_message,
    get_status_start_offset,
    poll_for_status,
    publish_trigger,
)

from dotenv import load_dotenv

load_dotenv()

# ── Product identity ──────────────────────────────────────────────────────────
# These are the only three lines that change when you copy this DAG for another product.
PRODUCT         = "hp-natural-oil"
PIPELINE_TYPE   = "file"
PIPELINE_ROLE   = "producer"

BOOTSTRAP_SERVER = "dpn-kafka-src:9092"
# SCHEDULE         = os.getenv("BP_NATURAL_GAS_SCHEDULE", "@hourly")
SCHEDULE  = None
# How long the sensor polls before timing out the whole task (and triggering retry)
SENSOR_TIMEOUT_SECS    = int(os.getenv("SENSOR_TIMEOUT_SECS",    "600"))
# How long each individual poke looks in the status topic before returning False
SENSOR_POKE_SECS       = int(os.getenv("SENSOR_POKE_SECS",       "20"))
# How often (seconds) Airflow calls poke()
SENSOR_POKE_INTERVAL   = int(os.getenv("SENSOR_POKE_INTERVAL",   "30"))


# ---------------------------------------------------------------------------
# Custom sensor: waits for a pipeline status message on dpn-pipeline-status
# ---------------------------------------------------------------------------

class PipelineStatusSensor(BaseSensorOperator):
    """
    Polls dpn-pipeline-status until a matching completion message arrives.

    Parameters
    ----------
    expected_stage:
        "adaptor" or "schema_mapper" — so the adaptor sensor does not
        accidentally match the schema mapper's completion message.
    status_start_offset_xcom_key:
        XCom key where the trigger task stored the Kafka start offset.
        The sensor seeks to this offset so it never misses a message
        published between the trigger and the first poke.
    """

    template_fields = ("run_id",)

    def __init__(
        self,
        *,
        expected_stage: str,
        status_start_offset_xcom_task_id: str,
        run_id: str = "{{ run_id }}",
        bootstrap_server: str,
        product: str,
        poke_timeout_secs: float = 20.0,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.expected_stage                  = expected_stage
        self.status_start_offset_xcom_task_id = status_start_offset_xcom_task_id
        self.run_id                          = run_id
        self.bootstrap_server                = bootstrap_server
        self.product                         = product
        self.poke_timeout_secs               = poke_timeout_secs

    def poke(self, context: Context) -> bool:
        # Read the Kafka start offset that the trigger task recorded in XCom.
        # This tells us where to start scanning the status topic.
        start_offset: int = context["ti"].xcom_pull(
            task_ids=self.status_start_offset_xcom_task_id,
            key="status_start_offset",
        ) or 0

        self.log.info(
            "PipelineStatusSensor poke — run_id=%s stage=%s product=%s start_offset=%d",
            self.run_id[:20], self.expected_stage, self.product, start_offset,
        )

        result = poll_for_status(
            expected_run_id  = self.run_id,
            expected_stage   = self.expected_stage,
            expected_product = self.product,
            bootstrap_server = self.bootstrap_server,
            start_offset     = start_offset,
            timeout_secs     = self.poke_timeout_secs,
        )

        if result is None:
            self.log.info(
                "Status not yet available — will poke again in %ds",
                self.poke_interval,
            )
            return False

        # Got a result — check if it was a success or failure
        status = result.get("status", "unknown")
        self.log.info(
            "Pipeline status received — stage=%s status=%s duration_ms=%.0f",
            self.expected_stage, status, result.get("duration_ms", 0),
        )

        if status == "failed":
            raise RuntimeError(
                f"Pipeline {self.expected_stage} for {self.product} FAILED "
                f"(run_id={self.run_id}): {result.get('error', 'unknown error')}"
            )

        if status == "skipped":
            self.log.warning(
                "Pipeline %s was skipped (EXECUTION_MODE=manual) — "
                "treating as success, downstream tasks will run",
                self.expected_stage,
            )

        return True   # "completed" or "skipped" → task SUCCESS


# ---------------------------------------------------------------------------
# Task callables
# ---------------------------------------------------------------------------

def _trigger_adaptor(**kwargs) -> None:
    """
    Publish a trigger to dpn-pipeline-control for the adaptor stage.

    Also records the current end offset of dpn-pipeline-status in XCom
    BEFORE publishing the trigger. The sensor uses this offset to seek
    to the right position and never miss a completion message.
    """
    dag_run = kwargs["dag_run"]
    dag     = kwargs["dag"]
    run_id  = dag_run.run_id
    ti      = kwargs["ti"]

    # Step 1: record current status topic offset BEFORE triggering
    # (in case the adaptor is very fast and completes before the sensor's first poke)
    start_offset = get_status_start_offset(BOOTSTRAP_SERVER)
    ti.xcom_push(key="status_start_offset", value=start_offset)

    # Step 2: build and publish the trigger message
    message = build_trigger_message(
        stage          = "adaptor",
        pipeline_type  = PIPELINE_TYPE,
        pipeline_role  = PIPELINE_ROLE,
        product        = PRODUCT,
        run_id         = run_id,
        dag_run_id     = run_id,
        dag_id         = dag.dag_id,
        execution_mode = "automatic",
    )

    publish_trigger(message, BOOTSTRAP_SERVER)

    kwargs["ti"].log.info(
        "Adaptor trigger published — product=%s run_id=%s start_offset=%d",
        PRODUCT, run_id[:20], start_offset,
    )


def _trigger_schema_mapper(**kwargs) -> None:
    """
    Publish a trigger to dpn-pipeline-control for the schema_mapper stage.

    The schema mapper enters drain mode: processes all file-ready events
    the adaptor just published, then signals completion.
    """
    dag_run = kwargs["dag_run"]
    dag     = kwargs["dag"]
    run_id  = dag_run.run_id
    ti      = kwargs["ti"]

    # Record status offset before triggering (same reason as adaptor trigger)
    start_offset = get_status_start_offset(BOOTSTRAP_SERVER)
    ti.xcom_push(key="status_start_offset", value=start_offset)

    message = build_trigger_message(
        stage          = "schema_mapper",
        pipeline_type  = PIPELINE_TYPE,
        pipeline_role  = PIPELINE_ROLE,
        product        = PRODUCT,
        run_id         = run_id,
        dag_run_id     = run_id,
        dag_id         = dag.dag_id,
        execution_mode = "automatic",
    )

    publish_trigger(message, BOOTSTRAP_SERVER)

    kwargs["ti"].log.info(
        "Schema mapper trigger published — product=%s run_id=%s start_offset=%d",
        PRODUCT, run_id[:20], start_offset,
    )


# ---------------------------------------------------------------------------
# DAG definition
# ---------------------------------------------------------------------------

_DEFAULT_ARGS = {
    "owner":            "dpn",
    "depends_on_past":  False,
    "retries":          2,
    "retry_delay":      timedelta(minutes=3),
    "email_on_failure": False,
}

with DAG(
    dag_id          = "producer_file_hp_natural_oil",
    default_args    = _DEFAULT_ARGS,
    description     = "HP Natural Oil file pipeline — Kafka trigger, no new pods",
    schedule_interval = SCHEDULE,
    start_date      = days_ago(1),
    catchup         = False,
    max_active_runs = 1,
    tags            = ["dpn", "producer", "file", "hp-natural-oil", "kafka-trigger"],
) as dag:

    # ── Task 1: publish trigger to adaptor ───────────────────────────────────
    trigger_adaptor = PythonOperator(
        task_id         = "trigger_adaptor",
        python_callable = _trigger_adaptor,
        provide_context = True,
        doc_md = (
            "Publishes a trigger message to dpn-pipeline-control for the hp-natural-oil adaptor. "
            "The already-running adaptor pod receives it and executes one file-ingestion cycle."
        ),
    )

    # ── Task 2: wait for adaptor completion ──────────────────────────────────
    wait_adaptor_done = PipelineStatusSensor(
        task_id                           = "wait_adaptor_done",
        expected_stage                    = "adaptor",
        status_start_offset_xcom_task_id  = "trigger_adaptor",
        bootstrap_server                  = BOOTSTRAP_SERVER,
        product                           = PRODUCT,
        poke_timeout_secs                 = SENSOR_POKE_SECS,
        poke_interval                     = SENSOR_POKE_INTERVAL,
        timeout                           = SENSOR_TIMEOUT_SECS,
        mode                              = "reschedule",  # frees the worker slot while waiting
        doc_md = (
            "Polls dpn-pipeline-status until the adaptor publishes a 'completed' message "
            "matching this DAG run's run_id. Fails the DAG if the adaptor reports failure."
        ),
    )

    # ── Task 3: publish trigger to schema mapper ─────────────────────────────
    trigger_schema_mapper = PythonOperator(
        task_id         = "trigger_schema_mapper",
        python_callable = _trigger_schema_mapper,
        provide_context = True,
        doc_md = (
            "Publishes a trigger to the schema mapper pod. "
            "The pod drains its queue (file-ready events from the adaptor) "
            "and publishes completion when idle."
        ),
    )

    # ── Task 4: wait for schema mapper completion ────────────────────────────
    wait_schema_mapper_done = PipelineStatusSensor(
        task_id                           = "wait_schema_mapper_done",
        expected_stage                    = "schema_mapper",
        status_start_offset_xcom_task_id  = "trigger_schema_mapper",
        bootstrap_server                  = BOOTSTRAP_SERVER,
        product                           = PRODUCT,
        poke_timeout_secs                 = SENSOR_POKE_SECS,
        poke_interval                     = SENSOR_POKE_INTERVAL,
        timeout                           = SENSOR_TIMEOUT_SECS,
        mode                              = "reschedule",
        doc_md = (
            "Polls dpn-pipeline-status until the schema mapper publishes a 'completed' message. "
            "DAG succeeds when this task completes."
        ),
    )

    # ── Dependency chain ─────────────────────────────────────────────────────
    # trigger_adaptor → wait_adaptor_done → trigger_schema_mapper → wait_schema_mapper_done
    trigger_adaptor >> wait_adaptor_done >> trigger_schema_mapper >> wait_schema_mapper_done
