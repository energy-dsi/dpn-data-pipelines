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
Topic Forwarder (v1.1.0 — Drain Until Idle)

This module implements a Kafka topic forwarder that replaces
fixed-time window processing with a drain-until-idle approach.

Problem with Fixed Timeout:
---------------------------
- Consumer stayed alive for entire timeout period (e.g. 600s)
- Messages arriving during this window were processed immediately
- Result: behaved like continuous consumer instead of trigger-based

Solution:
---------
- Consume until topic becomes idle (no messages for N seconds)
- Exit cleanly once queue is drained

Execution Behavior:
-------------------
No Trigger:
    - Pod idle
    - No subscription to source topic
    - Messages accumulate in Kafka

On Trigger:
    - Subscribe to source topic
    - Forward all messages
    - Exit after DRAIN_IDLE_SECS of silence
    - Pod returns to idle

Environment Variables:
----------------------
DRAIN_IDLE_SECS → idle timeout threshold (default: 15 seconds)
srcGroupId      → Kafka consumer group
"""

from __future__ import annotations

import os
import time
from datetime import UTC, datetime

from confluent_kafka import Consumer, KafkaError, KafkaException, Producer

from utils.otel_logger import OtelLogger as Logging
from utils.topic_utils import TopicResolver, KafkaTopicManager
from utils.pipeline_context import PipelineContext
from utils.step_logger import StepLogger


# ---------------------------------------------------------------------------
# Runtime Configuration
# ---------------------------------------------------------------------------

_DRAIN_IDLE_SECS: int = int(os.getenv("DRAIN_IDLE_SECS", "15"))


# ---------------------------------------------------------------------------
# TopicForwarder
# ---------------------------------------------------------------------------

class TopicForwarder:
    """
    Kafka Topic Forwarder.

    Responsibilities:
    -----------------
    - Consume from source topic
    - Forward messages to mapper topic
    - Exit cleanly after idle period

    Key Design Principle:
    ---------------------
    Consumer is NOT created at initialization.
    It is created only during execution so the pod remains idle
    between Kafka trigger events.
    """

    def __init__(self, service_name: str) -> None:
        """Initialize forwarder configuration."""
        self.logger = Logging().create_logger()
        self.service_name = service_name

        # Kafka configuration
        self.bootstrap = os.getenv("bootstrapServer", "")
        self.src_topic = os.getenv("srcTopicName", "")
        self.group_id = os.getenv("srcGroupId", service_name)
        self.retry_delay = int(os.getenv("consumerRetryDelaySecs", "5"))

        # Resolve mapper topic
        resolver = TopicResolver()
        self.mapper_topic = resolver.resolve(
            os.getenv("mapperTopicName"),
            self.src_topic,
            "trfm",
        )

        # Ensure topic exists
        topic_manager = KafkaTopicManager(
            bootstrap_server=self.bootstrap,
            logger=self.logger,
        )
        topic_manager.ensure_exists(self.mapper_topic)

        # Kafka producer (persistent)
        self.producer = Producer(
            {"bootstrap.servers": self.bootstrap}
        )

        self.logger.info(
            "TopicForwarder initialized (idle, not subscribed)",
            extra={
                "service": service_name,
                "src_topic": self.src_topic,
                "mapper_topic": self.mapper_topic,
                "note": "Consumer created only on trigger",
            },
        )

    # -----------------------------------------------------------------------
    # Entry Point
    # -----------------------------------------------------------------------

    def run(self, ctx: PipelineContext, step_log: StepLogger) -> None:
        """
        Execute forwarder.

        Args:
            ctx: Pipeline execution context
            step_log: Step logger
        """
        if ctx.triggered_by == "kafka-trigger":
            self._run_drain(ctx, step_log)
        else:
            self._run_continuous(ctx, step_log)

    # -----------------------------------------------------------------------
    # Drain Mode
    # -----------------------------------------------------------------------

    def _run_drain(
        self,
        ctx: PipelineContext,
        step_log: StepLogger,
    ) -> None:
        """
        Run forwarder in drain-until-idle mode.

        Exits when:
            No message received for DRAIN_IDLE_SECS
        """
        self.logger.info(
            "[%s] Drain mode started",
            ctx.pipeline_stage,
            extra={
                "event.name": "topic_forwarder.drain_start",
                "src_topic": self.src_topic,
                "drain_idle_secs": _DRAIN_IDLE_SECS,
                **ctx.as_log_extra(),
            },
        )

        consumer = Consumer(
            {
                "bootstrap.servers": self.bootstrap,
                "group.id": self.group_id,
                "auto.offset.reset": "earliest",
                "enable.auto.commit": False,
                "max.poll.interval.ms": 600000,
            }
        )
        consumer.subscribe([self.src_topic])

        step_log.step_start(ctx, "drain_forward")

        forwarded = 0
        last_message_time = time.monotonic()

        try:
            while True:
                msg = consumer.poll(timeout=1.0)

                if msg is None:
                    idle_time = time.monotonic() - last_message_time

                    # Exit condition: idle timeout reached
                    if idle_time >= _DRAIN_IDLE_SECS:
                        self.logger.info(
                            "[%s] Drain complete — %d messages forwarded",
                            ctx.pipeline_stage,
                            forwarded,
                            extra={
                                "event.name": "topic_forwarder.drain_complete",
                                "messages_forwarded": forwarded,
                                "idle_seconds": round(idle_time, 1),
                                **ctx.as_log_extra(),
                            },
                        )
                        break

                    continue

                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    raise KafkaException(msg.error())

                # Forward message
                self._forward_one(msg, ctx, step_log)

                # Commit after success
                consumer.commit(message=msg, asynchronous=False)

                forwarded += 1
                last_message_time = time.monotonic()

        finally:
            self.producer.flush()
            consumer.close()

            self.logger.info(
                "[%s] Consumer closed — returning to idle",
                ctx.pipeline_stage,
                extra={
                    "event.name": "topic_forwarder.consumer_closed",
                    **ctx.as_log_extra(),
                },
            )

        step_log.step_end(
            ctx,
            "drain_forward",
            extra={"messages_forwarded": forwarded},
        )

    # -----------------------------------------------------------------------
    # Continuous Mode
    # -----------------------------------------------------------------------

    def _run_continuous(
        self,
        ctx: PipelineContext,
        step_log: StepLogger,
    ) -> None:
        """Run in continuous mode (no trigger-based exit)."""
        self.logger.info(
            "[%s] Continuous mode started",
            ctx.pipeline_stage,
            extra={
                "event.name": "topic_forwarder.continuous_start",
                **ctx.as_log_extra(),
            },
        )

        step_log.step_start(ctx, "continuous_forward")

        while True:
            consumer = Consumer(
                {
                    "bootstrap.servers": self.bootstrap,
                    "group.id": self.group_id,
                    "auto.offset.reset": "earliest",
                    "enable.auto.commit": True,
                }
            )
            consumer.subscribe([self.src_topic])

            try:
                while True:
                    msg = consumer.poll(timeout=1.0)

                    if msg is None:
                        continue

                    if msg.error():
                        if msg.error().code() == KafkaError._PARTITION_EOF:
                            continue
                        raise KafkaException(msg.error())

                    self._forward_one(msg, ctx, step_log)

            except KafkaException as exc:
                self.logger.error(
                    "Kafka error — retrying",
                    extra={"error": str(exc), **ctx.as_log_extra()},
                )

            except Exception as exc:
                step_log.step_failed(ctx, "continuous_forward", exc=exc)

                self.logger.error(
                    "Unexpected error — retrying",
                    extra={"error": str(exc), **ctx.as_log_extra()},
                    exc_info=True,
                )

            finally:
                self.producer.flush()
                consumer.close()

            time.sleep(self.retry_delay)
            step_log.step_start(ctx, "continuous_forward")

    # -----------------------------------------------------------------------
    # Message Forwarding
    # -----------------------------------------------------------------------

    def _forward_one(
        self,
        msg,
        ctx: PipelineContext,
        step_log: StepLogger,
    ) -> None:
        """Forward a single Kafka message."""
        operation = "forward_message"
        start_time = datetime.now(UTC)

        step_log.step_start(
            ctx,
            operation,
            extra={
                "partition": msg.partition(),
                "offset": msg.offset(),
                "bytes": len(msg.value()) if msg.value() else 0,
            },
        )

        try:
            self.producer.produce(
                topic=self.mapper_topic,
                value=msg.value(),
                key=msg.key(),
                headers=msg.headers() or [],
                callback=self._on_delivery,
            )

            self.producer.poll(0)

            duration_ms = int(
                (datetime.now(UTC) - start_time).total_seconds() * 1000
            )

            step_log.step_end(
                ctx,
                operation,
                extra={
                    "dst": self.mapper_topic,
                    "duration_ms": duration_ms,
                },
            )

        except Exception as exc:
            step_log.step_failed(ctx, operation, exc=exc)
            raise

    def _on_delivery(self, err, msg) -> None:
        """Kafka delivery callback."""
        if err:
            self.logger.error(
                "Delivery failed",
                extra={
                    "error": str(err),
                    "dst": self.mapper_topic,
                },
            )