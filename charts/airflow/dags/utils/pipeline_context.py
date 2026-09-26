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

    component_name : str
        Same identifier passed to this blueprint's HeartbeatLogger
        (e.g. "consumer-file-extractor"), so every log emitted through
        this context shares one component.name with the heartbeat.
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
    component_name: str = ""

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
        extra: dict[str, Any] = {}
        if self.component_name:
            extra["component.name"] = self.component_name
        extra.update({
            "pipeline.run_id": self.run_id,
            "pipeline.mode": self.mode,
            "pipeline.triggered_by": self.triggered_by,
            "pipeline.stage": self.pipeline_stage,
            "pipeline.type": self.pipeline_type,
            "pipeline.role": self.pipeline_role,
            "pipeline.dag_run_id": self.dag_run_id,
            "pipeline.dag_id": self.dag_id,
            "pipeline.started_at": self.started_at,
        })
        return extra

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
        component_name: str = "",
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

        component_name : str
            Same identifier passed to this blueprint's HeartbeatLogger
            (e.g. "consumer-topic-mapper"), so pipeline-step logs and
            heartbeat logs report under the same component.name attribute.

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
            component_name="producer-topic-adaptor",
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
            component_name=component_name,
        )