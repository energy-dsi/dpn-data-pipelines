# Copyright DSI Project
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# +---------+----------------------------------------------------------+---------------+-------------+
# | Version | Description                                              | Change Owner  | Change Date |
# +---------+----------------------------------------------------------+---------------+-------------+
# | 1.0.0   | Initial version                                          | DSI Team      | 2026-05-01  |
# +---------+----------------------------------------------------------+---------------+-------------+


"""
Scheduler Backend Abstraction

This module defines a pluggable scheduler backend system that decouples
pipeline execution from orchestration frameworks (Airflow, Kafka, etc.).

Purpose:
--------
- Allow pipelines to run in different environments without code changes
- Provide a unified `execute()` interface across schedulers
- Enable dynamic backend selection via environment variables

Supported Backends:
-------------------
- kafka-trigger → KafkaTriggerBackend (Kubernetes recommended)
- airflow       → AirflowBackend (PythonOperator)
- interval      → IntervalBackend (schedule-based loop)
- standalone    → StandaloneBackend (one-time execution)
"""

from __future__ import annotations

import os
import time
import uuid
from typing import Any, Callable, Protocol, runtime_checkable

import schedule
from dotenv import load_dotenv

from utils.pipeline_context import (
    ExecutionMode,
    PipelineContext,
    PipelineRole,
    PipelineStage,
    PipelineType,
    TriggeredBy,
)
from utils.otel_logger import OtelLogger as Logging

# Load environment variables
load_dotenv()


# ---------------------------------------------------------------------------
# Scheduler Backend Protocol
# ---------------------------------------------------------------------------

@runtime_checkable
class SchedulerBackend(Protocol):
    """
    Structural interface for scheduler backends.

    Any backend implementing this method is considered valid.
    """

    def execute(
        self,
        run_fn: Callable[[PipelineContext], None],
        *,
        pipeline_stage: PipelineStage,
        pipeline_type: PipelineType,
        pipeline_role: PipelineRole,
        component_name: str = "",
    ) -> None:
        """Execute pipeline run."""


# ---------------------------------------------------------------------------
# Internal Helpers
# ---------------------------------------------------------------------------

def _resolve_execution_mode() -> ExecutionMode:
    """
    Resolve execution mode from environment variable.

    Returns:
        ExecutionMode: "automatic" or "manual"
    """
    raw = os.getenv("EXECUTION_MODE", "automatic").strip().lower()
    return "manual" if raw == "manual" else "automatic"


def _build_context(
    *,
    triggered_by: TriggeredBy,
    pipeline_stage: PipelineStage,
    pipeline_type: PipelineType,
    pipeline_role: PipelineRole,
    run_id: str | None = None,
    dag_run_id: str = "",
    dag_id: str = "",
    component_name: str = "",
) -> PipelineContext:
    """
    Build a PipelineContext object.

    Args:
        triggered_by: Execution source
        pipeline_stage: Current stage
        pipeline_type: File or topic
        pipeline_role: Producer or consumer
        run_id: Optional run ID
        dag_run_id: Airflow DAG run ID
        dag_id: Airflow DAG ID
        component_name: Same identifier passed to this blueprint's
            HeartbeatLogger, so pipeline-step logs and heartbeat logs
            share one component.name attribute.

    Returns:
        PipelineContext
    """
    return PipelineContext(
        run_id=run_id or str(uuid.uuid4()),
        mode=_resolve_execution_mode(),
        triggered_by=triggered_by,
        pipeline_stage=pipeline_stage,
        pipeline_type=pipeline_type,
        pipeline_role=pipeline_role,
        dag_run_id=dag_run_id,
        dag_id=dag_id,
        component_name=component_name,
    )


# ---------------------------------------------------------------------------
# Manual (run-once) trigger
# ---------------------------------------------------------------------------

def run_once(
    run_fn: Callable[[PipelineContext], None],
    *,
    pipeline_stage: PipelineStage,
    pipeline_type: PipelineType,
    pipeline_role: PipelineRole,
    component_name: str = "",
) -> None:
    """
    Execute `run_fn` exactly once, unconditionally.

    Every SchedulerBackend.execute() above skips the run entirely when
    EXECUTION_MODE=manual (that's the whole point of "manual" — a paused
    data product should sit idle). This function is the deliberate escape
    hatch for that: it's what main.py's `--run-once` CLI flag calls when
    invoked via `podman exec` / `kubectl exec` against an already-running
    container (see service/podman_ops.py's and service/k8s_ops.py's
    run_once(), wired to the self-service UI's "Run Now" button) — an
    operator forcing a single cycle on demand, whether the product is
    currently paused or not.

    ctx.mode still reflects the container's real EXECUTION_MODE (so logs
    show whether this ad-hoc run happened while paused); only
    ctx.triggered_by is "manual", so downstream run() implementations can
    tell an ad-hoc invocation apart from a scheduled one — e.g. the topic
    pathway's run() bounds it with TOPIC_TASK_TIMEOUT_SECS the same way it
    already does for "interval"/"kafka-trigger", rather than blocking
    forever the way an untimed "standalone" run would.
    """
    logger = Logging().create_logger()
    ctx = _build_context(
        triggered_by="manual",
        pipeline_stage=pipeline_stage,
        pipeline_type=pipeline_type,
        pipeline_role=pipeline_role,
        component_name=component_name,
    )
    logger.info(
        "Manual run-once triggered",
        extra={"event.name": "scheduler.manual.start", **ctx.as_log_extra()},
    )
    run_fn(ctx)


# ---------------------------------------------------------------------------
# Airflow Backend
# ---------------------------------------------------------------------------

class AirflowBackend:
    """
    Scheduler backend for Airflow PythonOperator.

    Extracts execution context from Airflow runtime and builds
    a PipelineContext for downstream pipeline execution.
    """

    def __init__(self, airflow_context: dict[str, Any]) -> None:
        self._ctx = airflow_context
        self._logger = Logging().create_logger()

    def execute(
        self,
        run_fn: Callable[[PipelineContext], None],
        *,
        pipeline_stage: PipelineStage,
        pipeline_type: PipelineType,
        pipeline_role: PipelineRole,
        component_name: str = "",
    ) -> None:
        """Execute pipeline within Airflow context."""
        dag_run = self._ctx.get("dag_run")
        dag = self._ctx.get("dag")

        ctx = _build_context(
            triggered_by="airflow",
            pipeline_stage=pipeline_stage,
            pipeline_type=pipeline_type,
            pipeline_role=pipeline_role,
            run_id=str(dag_run.run_id) if dag_run else None,
            dag_run_id=str(dag_run.run_id) if dag_run else "",
            dag_id=str(dag.dag_id) if dag else "",
            component_name=component_name,
        )

        self._logger.info(
            "AirflowBackend — execution started",
            extra={
                "event.name": "scheduler.execute.start",
                **ctx.as_log_extra(),
            },
        )

        if ctx.mode == "manual":
            self._logger.info(
                "EXECUTION_MODE=manual — skipping Airflow execution",
                extra={
                    "event.name": "scheduler.execute.skipped",
                    **ctx.as_log_extra(),
                },
            )
            return

        run_fn(ctx)


# ---------------------------------------------------------------------------
# Interval Backend
# ---------------------------------------------------------------------------

class IntervalBackend:
    """
    Scheduler backend using the `schedule` library.

    Runs pipeline at fixed time intervals.
    Useful when no external orchestrator is available.
    """

    def __init__(self, interval_seconds: int | None = None) -> None:
        self._interval = interval_seconds or int(
            os.getenv("scheduleInterval", "60")
        )
        self._logger = Logging().create_logger()

    def execute(
        self,
        run_fn: Callable[[PipelineContext], None],
        *,
        pipeline_stage: PipelineStage,
        pipeline_type: PipelineType,
        pipeline_role: PipelineRole,
        component_name: str = "",
    ) -> None:
        """Run pipeline continuously at configured interval."""
        self._logger.info(
            "IntervalBackend started",
            extra={
                "event.name": "scheduler.interval.started",
                "interval_seconds": self._interval,
            },
        )

        def _tick() -> None:
            ctx = _build_context(
                triggered_by="interval",
                pipeline_stage=pipeline_stage,
                pipeline_type=pipeline_type,
                pipeline_role=pipeline_role,
                component_name=component_name,
            )

            if ctx.mode == "manual":
                self._logger.info(
                    "EXECUTION_MODE=manual — skipping interval tick",
                    extra={
                        "event.name": "scheduler.execute.skipped",
                        **ctx.as_log_extra(),
                    },
                )
                return

            run_fn(ctx)

        # Execute immediately once
        _tick()

        # Schedule recurring execution
        schedule.every(self._interval).seconds.do(_tick)

        while True:
            schedule.run_pending()
            time.sleep(1)


# ---------------------------------------------------------------------------
# Standalone Backend
# ---------------------------------------------------------------------------

class StandaloneBackend:
    """
    One-time execution backend.

    Runs the pipeline once and exits.
    Default fallback backend.
    """

    def __init__(self) -> None:
        self._logger = Logging().create_logger()

    def execute(
        self,
        run_fn: Callable[[PipelineContext], None],
        *,
        pipeline_stage: PipelineStage,
        pipeline_type: PipelineType,
        pipeline_role: PipelineRole,
        component_name: str = "",
    ) -> None:
        """Execute pipeline once."""
        ctx = _build_context(
            triggered_by="standalone",
            pipeline_stage=pipeline_stage,
            pipeline_type=pipeline_type,
            pipeline_role=pipeline_role,
            component_name=component_name,
        )

        if ctx.mode == "manual":
            self._logger.info(
                "EXECUTION_MODE=manual — skipping standalone run",
                extra={
                    "event.name": "scheduler.execute.skipped",
                    **ctx.as_log_extra(),
                },
            )
            return

        run_fn(ctx)


# ---------------------------------------------------------------------------
# Backend Factory
# ---------------------------------------------------------------------------

def get_backend(
    airflow_context: dict[str, Any] | None = None,
    interval_seconds: int | None = None,
) -> SchedulerBackend:
    """
    Select and return active scheduler backend.

    Selection is based on `SCHEDULER_BACKEND` environment variable.

    Supported Values:
    -----------------
    kafka-trigger → KafkaTriggerBackend (Kubernetes deployments)
    airflow       → AirflowBackend
    interval      → IntervalBackend
    standalone    → StandaloneBackend (default)

    Args:
        airflow_context: Airflow context (required for AirflowBackend)
        interval_seconds: Custom interval override

    Returns:
        SchedulerBackend instance
    """
    key = os.getenv("SCHEDULER_BACKEND", "standalone").strip().lower()

    if key == "kafka-trigger":
        # Lazy import prevents Kafka dependency in non-Kafka environments
        from utils.kafka_trigger import KafkaTriggerBackend
        return KafkaTriggerBackend()

    if key == "airflow":
        if airflow_context is None:
            raise ValueError(
                "SCHEDULER_BACKEND=airflow requires airflow_context"
            )
        return AirflowBackend(airflow_context)

    if key == "interval":
        return IntervalBackend(interval_seconds=interval_seconds)

    return StandaloneBackend()