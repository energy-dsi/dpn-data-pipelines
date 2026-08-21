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
Kafka Trigger Backend

This module implements a Kafka-based scheduler backend that:
- Listens to a control topic (`dpn-pipeline-control`)
- Triggers pipeline execution
- Publishes pipeline execution status to a status topic

Key Features:
-------------
- Explicit offset commit (auto.commit disabled) for reliability
- Safe consumer lifecycle (closed before execution)
- In-flight execution guard (prevents parallel runs per pod)
- Idempotency via run_id tracking
- Status publishing for observability

Execution Flow:
---------------
1. WAIT phase:
    - Poll control topic
    - Match trigger message
    - Commit offset
    - Close consumer

2. EXECUTE phase:
    - Run pipeline with no active Kafka consumer
    - Publish status (success/failure)

Important Fixes:
----------------
v1.2.0:
    - enable.auto.commit=False with explicit commit

v1.1.0:
    - Close consumer before pipeline execution

v1.1.1:
    - In-flight concurrency guard added
"""

from __future__ import annotations

import json
import os
import threading
import time
from datetime import UTC, datetime
from typing import Any, Callable

from confluent_kafka import (
    Consumer,
    KafkaError,
    KafkaException,
    Producer,
    TopicPartition,
)

from utils.otel_logger import OtelLogger as Logging
from utils.pipeline_context import (
    PipelineContext,
    PipelineRole,
    PipelineStage,
    PipelineType,
)

# ---------------------------------------------------------------------------
# Environment Configuration
# ---------------------------------------------------------------------------

CONTROL_TOPIC = os.getenv("PIPELINE_CONTROL_TOPIC", "dpn-pipeline-control")
STATUS_TOPIC = os.getenv("PIPELINE_STATUS_TOPIC", "dpn-pipeline-status")

# ---------------------------------------------------------------------------
# Status Publisher
# ---------------------------------------------------------------------------


class StatusPublisher:
    """
    Publish pipeline execution status messages.

    Sends structured messages to Kafka `STATUS_TOPIC`
    for monitoring, observability, and Airflow sensors.
    """

    def __init__(self, bootstrap_server: str, logger) -> None:
        self._producer = Producer({"bootstrap.servers": bootstrap_server})
        self._logger = logger

    def publish(
        self,
        ctx: PipelineContext,
        *,
        status: str,
        error: str | None = None,
        duration_ms: float = 0.0,
    ) -> None:
        """
        Publish pipeline status.

        Args:
            ctx (PipelineContext): Execution context
            status (str): completed | failed | skipped
            error (str | None): Error message if failed
            duration_ms (float): Execution duration
        """
        payload = {
            "stage": ctx.pipeline_stage,
            "pipeline_type": ctx.pipeline_type,
            "pipeline_role": ctx.pipeline_role,
            "product": os.getenv("PRODUCT_NAME", "unknown"),
            "run_id": ctx.run_id,
            "dag_run_id": ctx.dag_run_id,
            "dag_id": ctx.dag_id,
            "status": status,
            "error": error,
            "duration_ms": round(duration_ms, 3),
            "timestamp": datetime.now(UTC).isoformat(),
        }

        self._producer.produce(
            STATUS_TOPIC,
            value=json.dumps(payload).encode("utf-8"),
            key=ctx.run_id.encode("utf-8"),
        )
        self._producer.flush()

        self._logger.info(
            "[%s] status published — %s (run_id=%s)",
            ctx.pipeline_stage,
            status,
            ctx.run_id[:8],
            extra={
                "event.name": "pipeline.status.published",
                "pipeline.status": status,
                "pipeline.duration_ms": duration_ms,
                **ctx.as_log_extra(),
            },
        )


# ---------------------------------------------------------------------------
# Kafka Trigger Backend
# ---------------------------------------------------------------------------


class KafkaTriggerBackend:
    """
    Kafka-based scheduler backend.

    Responsible for:
    - Listening for trigger messages
    - Executing pipeline functions
    - Managing execution lifecycle safely

    Guarantees:
    -----------
    - No offset loss (manual commit)
    - No MAX_POLL_EXCEEDED errors
    - Single pipeline execution per pod
    """

    def __init__(self) -> None:
        self._bootstrap = os.getenv("bootstrapServer", "")
        self._product = os.getenv("PRODUCT_NAME", "unknown")
        self._logger = Logging().create_logger()

        print("bootstrapServer from kafka trigger")
        print(self._bootstrap)
        print("PRODUCT_NAME")
        print(self._product)

        # State tracking
        self._last_processed_run_id: str | None = None
        self._pipeline_running = threading.Event()
        self._component_name: str = ""

    # -----------------------------------------------------------------------
    # Public Execution Loop
    # -----------------------------------------------------------------------

    def execute(
        self,
        run_fn: Callable[[PipelineContext], None],
        *,
        pipeline_stage: PipelineStage,
        pipeline_type: PipelineType,
        pipeline_role: PipelineRole,
        component_name: str = "",
    ) -> None:
        """Main execution loop for trigger-based pipeline."""
        # Stashed on the instance so every log line emitted by this backend
        # (waiting/starting/trigger-received/error) carries the same
        # component.name as the pipeline-step logs, letting Kibana show one
        # consistent component across the whole run — not just the logs
        # that happen to receive a PipelineContext.
        self._component_name = component_name
        status_pub = StatusPublisher(self._bootstrap, self._logger)
        group_id = f"{self._product}-{pipeline_stage}-control"

        self._logger.info(
            "KafkaTriggerBackend starting — product=%s stage=%s group=%s",
            self._product,
            pipeline_stage,
            group_id,
            extra={
                "event.name": "kafka_trigger.starting",
                "control.topic": CONTROL_TOPIC,
                "status.topic": STATUS_TOPIC,
                "consumer.group_id": group_id,
                "component.name": component_name,
            },
        )

        while True:
            payload = self._wait_for_trigger(
                group_id=group_id,
                pipeline_stage=pipeline_stage,
                pipeline_type=pipeline_type,
                pipeline_role=pipeline_role,
            )

            self._execute_pipeline(
                payload=payload,
                run_fn=run_fn,
                status_pub=status_pub,
                pipeline_stage=pipeline_stage,
                pipeline_type=pipeline_type,
                pipeline_role=pipeline_role,
                component_name=component_name,
            )

    # -----------------------------------------------------------------------
    # WAIT Phase
    # -----------------------------------------------------------------------

    def _wait_for_trigger(
        self,
        *,
        group_id: str,
        pipeline_stage: PipelineStage,
        pipeline_type: PipelineType,
        pipeline_role: PipelineRole,
    ) -> dict[str, Any]:
        """
        Wait for a matching trigger message.

        Returns only when:
        - Valid message received
        - Offset committed
        - Consumer safely closed
        """
        consumer = Consumer(
            {
                "bootstrap.servers": self._bootstrap,
                "group.id": group_id,
                "auto.offset.reset": "earliest",
                "enable.auto.commit": False,  # critical fix
                "max.poll.interval.ms": 600000,
                "session.timeout.ms": 30000,
            }
        )

        consumer.subscribe([CONTROL_TOPIC])

        self._logger.info(
            "Waiting for trigger",
            extra={
                "event.name": "kafka_trigger.waiting",
                "component.name": self._component_name,
            },
        )

        try:
            while True:
                msg = consumer.poll(timeout=1.0)

                if msg is None:
                    continue

                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    raise KafkaException(msg.error())

                # Decode payload
                try:
                    payload = json.loads(msg.value().decode("utf-8"))
                except Exception as exc:
                    self._logger.warning(
                        "Invalid control message",
                        extra={
                            "error": str(exc),
                            "component.name": self._component_name,
                        },
                    )
                    consumer.commit(message=msg, asynchronous=False)
                    continue

                # Filter irrelevant messages
                if (
                    payload.get("stage") != pipeline_stage
                    or payload.get("pipeline_type") != pipeline_type
                    or payload.get("pipeline_role") != pipeline_role
                    or payload.get("product") != self._product
                ):
                    consumer.commit(message=msg, asynchronous=False)
                    continue

                self._logger.info(
                    "Trigger received",
                    extra={
                        "run_id": payload.get("run_id"),
                        "component.name": self._component_name,
                    },
                )

                # Commit before returning (critical)
                consumer.commit(message=msg, asynchronous=False)
                return payload

        finally:
            try:
                consumer.close()
            except Exception:
                pass

    # -----------------------------------------------------------------------
    # EXECUTE Phase
    # -----------------------------------------------------------------------

    def _execute_pipeline(
        self,
        *,
        payload: dict[str, Any],
        run_fn: Callable[[PipelineContext], None],
        status_pub: StatusPublisher,
        pipeline_stage: PipelineStage,
        pipeline_type: PipelineType,
        pipeline_role: PipelineRole,
        component_name: str = "",
    ) -> None:
        """Execute pipeline for a given trigger payload."""
        run_id = payload.get("run_id", "unknown")

        # In-flight guard
        if self._pipeline_running.is_set():
            ctx = PipelineContext.from_trigger_message(
                payload,
                pipeline_stage=pipeline_stage,
                pipeline_type=pipeline_type,
                pipeline_role=pipeline_role,
                component_name=component_name,
            )
            status_pub.publish(
                ctx,
                status="failed",
                error="Pipeline already running on this pod",
            )
            return

        # Idempotency
        if run_id == self._last_processed_run_id:
            ctx = PipelineContext.from_trigger_message(
                payload,
                pipeline_stage=pipeline_stage,
                pipeline_type=pipeline_type,
                pipeline_role=pipeline_role,
                component_name=component_name,
            )
            status_pub.publish(ctx, status="completed", duration_ms=0.0)
            return

        ctx = PipelineContext.from_trigger_message(
            payload,
            pipeline_stage=pipeline_stage,
            pipeline_type=pipeline_type,
            pipeline_role=pipeline_role,
            component_name=component_name,
        )

        if ctx.mode == "manual":
            status_pub.publish(ctx, status="skipped")
            return

        self._pipeline_running.set()
        start_time = time.perf_counter()

        try:
            run_fn(ctx)

            duration_ms = (time.perf_counter() - start_time) * 1000
            self._last_processed_run_id = run_id

            status_pub.publish(
                ctx,
                status="completed",
                duration_ms=duration_ms,
            )

        except Exception as exc:
            duration_ms = (time.perf_counter() - start_time) * 1000
            self._last_processed_run_id = run_id

            status_pub.publish(
                ctx,
                status="failed",
                error=f"{type(exc).__name__}: {exc}",
                duration_ms=duration_ms,
            )

            self._logger.error(
                "Pipeline execution failed",
                extra={"component.name": self._component_name},
                exc_info=True,
            )

        finally:
            self._pipeline_running.clear()


# ---------------------------------------------------------------------------
# Helper Functions
# ---------------------------------------------------------------------------

def build_trigger_message(
    *,
    stage: str,
    pipeline_type: str,
    pipeline_role: str,
    product: str,
    run_id: str,
    dag_run_id: str,
    dag_id: str,
    execution_mode: str = "automatic",
) -> dict[str, Any]:
    """Build Kafka trigger message payload."""
    return {
        "stage": stage,
        "pipeline_type": pipeline_type,
        "pipeline_role": pipeline_role,
        "product": product,
        "run_id": run_id,
        "dag_run_id": dag_run_id,
        "dag_id": dag_id,
        "execution_mode": execution_mode,
        "triggered_by": "airflow",
        "timestamp": datetime.now(UTC).isoformat(),
    }


def publish_trigger(message: dict[str, Any], bootstrap_server: str) -> None:
    """Publish trigger message to control topic."""
    producer = Producer({"bootstrap.servers": bootstrap_server})

    def _on_delivery(err, msg):
        if err:
            raise RuntimeError(f"Trigger delivery failed: {err}")

    producer.produce(
        CONTROL_TOPIC,
        value=json.dumps(message).encode("utf-8"),
        key=message["run_id"].encode("utf-8"),
        callback=_on_delivery,
    )
    producer.flush(timeout=10)


def get_status_start_offset(bootstrap_server: str) -> int:
    """Get starting offset for status topic."""
    consumer = Consumer(
        {
            "bootstrap.servers": bootstrap_server,
            "group.id": "airflow-offset-probe",
            "auto.offset.reset": "earliest",
        }
    )

    try:
        tp = TopicPartition(STATUS_TOPIC, 0)
        _, high = consumer.get_watermark_offsets(tp, timeout=5.0)
        return max(0, high)
    except Exception:
        return 0
    finally:
        consumer.close()


def poll_for_status(
    *,
    expected_run_id: str,
    expected_stage: str,
    expected_product: str,
    bootstrap_server: str,
    start_offset: int,
    timeout_secs: float = 20.0,
) -> dict[str, Any] | None:
    """Poll Kafka status topic for specific pipeline result."""
    safe_id = expected_run_id.replace(":", "-")[:50]
    group_id = f"airflow-sensor-{safe_id}-{expected_stage}"

    consumer = Consumer(
        {
            "bootstrap.servers": bootstrap_server,
            "group.id": group_id,
            "enable.auto.commit": False,
            "auto.offset.reset": "earliest",
        }
    )

    try:
        tp = TopicPartition(STATUS_TOPIC, 0, start_offset)
        consumer.assign([tp])

        deadline = time.monotonic() + timeout_secs

        while time.monotonic() < deadline:
            msg = consumer.poll(timeout=1.0)

            if msg is None:
                continue

            if msg.error():
                continue

            try:
                payload = json.loads(msg.value().decode("utf-8"))
            except Exception:
                continue

            if (
                payload.get("run_id") == expected_run_id
                and payload.get("stage") == expected_stage
                and payload.get("product") == expected_product
            ):
                return payload

        return None

    finally:
        consumer.close()
        