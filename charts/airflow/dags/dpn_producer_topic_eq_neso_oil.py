# Copyright DSI Project — Apache 2.0
# v1.0.0 2026-05-26 — Kafka trigger pattern — no new pods created
"""
Airflow DAG: producer_topic_eq_neso_oil

Orchestrates the already-running Kubernetes pods for eq-neso-oil via Kafka.
No new pods are created. Existing deployments receive trigger messages on
dpn-pipeline-control and publish completion to dpn-pipeline-status.

Task chain:
  trigger_adaptor → wait_adaptor_done → trigger_schema_mapper → wait_schema_mapper_done
"""
from __future__ import annotations
import os, sys
from datetime import timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.base import BaseSensorOperator
from airflow.utils.dates import days_ago
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils.kafka_trigger import (build_trigger_message, get_status_start_offset,
                                  poll_for_status, publish_trigger)
from dotenv import load_dotenv

load_dotenv()

PRODUCT  = "eq-neso-oil"
P_TYPE   = "topic"
P_ROLE   = "producer"
STAGE1   = "adaptor"
# BOOTSTRAP = os.getenv("bootstrapServer","")
BOOTSTRAP = "dpn-kafka-src:9092"
# SCHEDULE  = os.getenv("EQ_NESO_OIL_SCHEDULE","@hourly")
SCHEDULE  = '*/3 * * * *'
SENSOR_TIMEOUT   = int(os.getenv("SENSOR_TIMEOUT_SECS","600"))
SENSOR_POKE_SECS = int(os.getenv("SENSOR_POKE_SECS","20"))
SENSOR_POKE_INT  = int(os.getenv("SENSOR_POKE_INTERVAL","30"))


class PipelineStatusSensor(BaseSensorOperator):
    template_fields = ("run_id",)
    def __init__(self,*,expected_stage,xcom_task_id,run_id="{{ run_id }}",
                 bootstrap_server,product,poke_timeout=20.0,**kwargs):
        super().__init__(**kwargs)
        self.expected_stage=expected_stage; self.xcom_task_id=xcom_task_id
        self.run_id=run_id; self.bootstrap_server=bootstrap_server
        self.product=product; self.poke_timeout=poke_timeout
    def poke(self,context):
        offset=context["ti"].xcom_pull(task_ids=self.xcom_task_id,key="status_start_offset") or 0
        self.log.info("poke stage=%s run_id=%s offset=%d",self.expected_stage,self.run_id[:20],offset)
        result=poll_for_status(expected_run_id=self.run_id,expected_stage=self.expected_stage,
            expected_product=self.product,bootstrap_server=self.bootstrap_server,
            start_offset=offset,timeout_secs=self.poke_timeout)
        if result is None: return False
        status=result.get("status","unknown")
        if status=="failed": raise RuntimeError(f"{self.expected_stage} FAILED: {result.get('error')}")
        return True


def _trigger(stage):
    def _callable(**kwargs):
        dag_run=kwargs["dag_run"]; dag=kwargs["dag"]; run_id=dag_run.run_id
        offset=get_status_start_offset(BOOTSTRAP)
        kwargs["ti"].xcom_push(key="status_start_offset",value=offset)
        publish_trigger(build_trigger_message(stage=stage,pipeline_type=P_TYPE,
            pipeline_role=P_ROLE,product=PRODUCT,run_id=run_id,dag_run_id=run_id,
            dag_id=dag.dag_id,execution_mode="automatic"),BOOTSTRAP)
        kwargs["ti"].log.info("trigger published stage=%s product=%s run_id=%s",stage,PRODUCT,run_id[:20])
    return _callable


_DEFAULT = {"owner":"dpn","depends_on_past":False,"retries":2,
            "retry_delay":timedelta(minutes=3),"email_on_failure":False}

with DAG(dag_id="producer_topic_eq_neso_oil",default_args=_DEFAULT,
         description="eq-neso-oil — Kafka trigger, existing pods, no new K8s pods",
         schedule_interval=SCHEDULE,start_date=days_ago(1),
         catchup=False,max_active_runs=1,
         tags=["dpn","producer","topic","eq-neso-oil","kafka-trigger"]) as dag:

    t1=PythonOperator(task_id=f"trigger_{STAGE1}",python_callable=_trigger(STAGE1),provide_context=True)
    t2=PipelineStatusSensor(task_id=f"wait_{STAGE1}_done",expected_stage=STAGE1,
        xcom_task_id=f"trigger_{STAGE1}",bootstrap_server=BOOTSTRAP,product=PRODUCT,
        poke_timeout=SENSOR_POKE_SECS,poke_interval=SENSOR_POKE_INT,
        timeout=SENSOR_TIMEOUT,mode="reschedule")
    t3=PythonOperator(task_id="trigger_schema_mapper",
        python_callable=_trigger("schema_mapper"),provide_context=True)
    t4=PipelineStatusSensor(task_id="wait_schema_mapper_done",expected_stage="schema_mapper",
        xcom_task_id="trigger_schema_mapper",bootstrap_server=BOOTSTRAP,product=PRODUCT,
        poke_timeout=SENSOR_POKE_SECS,poke_interval=SENSOR_POKE_INT,
        timeout=SENSOR_TIMEOUT,mode="reschedule")

    t1 >> t2 >> t3 >> t4
