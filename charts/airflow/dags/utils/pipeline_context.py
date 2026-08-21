# Copyright DSI Project
#
# Licensed under the Apache License, Version 2.0
# ...

"""
Pipeline Context

This module defines the `PipelineContext` class, which represents
shared execution metadata for a single pipeline run.

Purpose:
--------
- Provides a consistent context object across all pipeline stages
- Enables end-to-end log correlation using a shared `run_id`
- Abstracts scheduler-specific details (Airflow, Kafka trigger, etc.)

Key Features:
-------------
- Immutable metadata container
- Standardized logging fields
- Compatible with multiple execution backends
- Supports context creation from Kafka trigger messages

Version Notes:
--------------
v1.1:
- Added `from_trigger_message()` to integrate with KafkaTriggerBackend
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any, Literal


# ---------------------------------------------------------------------------
# Type Definitions
# ---------------------------------------------------------------------------

PipelineStage = Literal[
    "adaptor",
    "extractor",
    "schema_mapper",
    "unknown",
]

PipelineType = Literal["file", "topic"]

PipelineRole = Literal["producer", "consumer"]

ExecutionMode = Literal["manual", "automatic"]

TriggeredBy = Literal[
    "airflow",
    "interval",
    "standalone",
    "kafka-trigger",
]


# ---------------------------------------------------------------------------
# PipelineContext
# ---------------------------------------------------------------------------

@dataclass
class PipelineContext:
    """
    Immutable container for pipeline execution metadata.

    This object is created once per pipeline run and passed to all stages.

    Attributes
    ----------
    run_id : str
        Unique identifier for the pipeline execution.
        Source depends on scheduler:
        - Airflow: dag_run.run_id
        - Kafka Trigger: same Airflow run_id (from message)
        - Standalone/Interval: generated UUID

    mode : ExecutionMode
        Execution mode:
        - "automatic"
        - "manual"

    triggered_by : TriggeredBy
        Origin of execution (scheduler/backend type)

    pipeline_stage : PipelineStage
        Stage name (adaptor, extractor, schema_mapper)

    pipeline_type : PipelineType
        Pipeline type (file or topic)

    pipeline_role : PipelineRole
        Role of the pipeline (producer or consumer)

    dag_run_id : str
        Airflow DAG run identifier (if applicable)

    dag_id : str
        Airflow DAG identifier (if applicable)

    started_at : str
        ISO-8601 UTC timestamp when context was created
    """

    run_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    mode: ExecutionMode = "automatic"
    triggered_by: TriggeredBy = "standalone"
    pipeline_stage: PipelineStage = "unknown"
    pipeline_type: PipelineType = "file"
    pipeline_role: PipelineRole = "producer"
    dag_run_id: str = ""
    dag_id: str = ""
    started_at: str = field(
        default_factory=lambda: datetime.now(UTC).isoformat()
    )

    # -----------------------------------------------------------------------
    # Logging Helper
    # -----------------------------------------------------------------------

    def as_log_extra(self) -> dict[str, Any]:
        """
        Build structured logging metadata.

        Returns a flat dictionary that can be passed to logging
        frameworks (e.g., OpenTelemetry, structured logging systems).

        Returns
        -------
        dict[str, Any]
            Structured log metadata
        """
        return {
            "pipeline.run_id": self.run_id,
            "pipeline.mode": self.mode,
            "pipeline.triggered_by": self.triggered_by,
            "pipeline.stage": self.pipeline_stage,
            "pipeline.type": self.pipeline_type,
            "pipeline.role": self.pipeline_role,
            "pipeline.dag_run_id": self.dag_run_id,
            "pipeline.dag_id": self.dag_id,
            "pipeline.started_at": self.started_at,
        }

    # -----------------------------------------------------------------------
    # Factory Methods
    # -----------------------------------------------------------------------

    @classmethod
    def from_trigger_message(
        cls,
        payload: dict[str, Any],
        *,
        pipeline_stage: PipelineStage,
        pipeline_type: PipelineType,
        pipeline_role: PipelineRole,
    ) -> "PipelineContext":
        """
        Create a PipelineContext from Kafka trigger message.

        Used by KafkaTriggerBackend to convert control-topic payloads
        into execution context objects.

        Important:
        ----------
        - `run_id` is preserved from Airflow trigger message
        - `triggered_by` is always set to "kafka-trigger"
        - Pipeline identity is NOT taken from payload (security-safe)

        Parameters
        ----------
        payload : dict[str, Any]
            Kafka trigger message payload

        pipeline_stage : PipelineStage
            Current stage (injected by backend)

        pipeline_type : PipelineType
            Pipeline type (topic/file)

        pipeline_role : PipelineRole
            Pipeline role (producer/consumer)

        Returns
        -------
        PipelineContext
            Initialized context object

        Example
        -------
        payload = {
            "run_id": "scheduled__2026-05-26T08:00:00+00:00",
            "dag_run_id": "scheduled__2026-05-26T08:00:00+00:00",
            "dag_id": "producer_topic_pipeline",
            "execution_mode": "automatic",
            "triggered_by": "airflow",
        }

        ctx = PipelineContext.from_trigger_message(
            payload,
            pipeline_stage="adaptor",
            pipeline_type="topic",
            pipeline_role="producer",
        )

        # ctx.run_id == payload["run_id"]
        # ctx.triggered_by == "kafka-trigger"
        """

        raw_mode = payload.get("execution_mode", "automatic")
        raw_mode = raw_mode.strip().lower()

        # Normalize execution mode
        mode: ExecutionMode = (
            "manual" if raw_mode == "manual" else "automatic"
        )

        return cls(
            run_id=payload.get("run_id", str(uuid.uuid4())),
            mode=mode,
            triggered_by="kafka-trigger",
            pipeline_stage=pipeline_stage,
            pipeline_type=pipeline_type,
            pipeline_role=pipeline_role,
            dag_run_id=payload.get("dag_run_id", ""),
            dag_id=payload.get("dag_id", ""),
        )