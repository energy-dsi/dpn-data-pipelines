# Copyright DSI Project — Apache 2.0
# v1.0.0 Initial | v1.1.0 Kafka trigger + StepLogger 2026-05-26

"""
Kafka Topic Adaptor (Producer)

This module implements a Kafka topic forwarder responsible for:
- Consuming messages from a source Kafka topic
- Forwarding them to a mapper (transformation) topic

Deployment Notes:
- Designed to run in Kubernetes with scheduler backend support
- Uses environment variables for configuration
- Supports Kafka-triggered and interval-based execution modes

Example Runtime Config:
    SCHEDULER_BACKEND=kafka-trigger
    PRODUCT_NAME=eqbd-pg-gas

File Location:
    /home/claude/dpn-complete/producer/topic/eqbd-pg-gas/adaptor/main.py
"""

from __future__ import annotations

import os
import time
from datetime import UTC, datetime

from confluent_kafka import Consumer, KafkaError, KafkaException, Producer
from dotenv import load_dotenv

from utils.otel_logger import OtelLogger as Logging
from utils.topic_utils import TopicResolver, KafkaTopicManager
from utils.pipeline_context import PipelineContext
from utils.scheduler_backend import get_backend
from utils.step_logger import StepLogger


# Load environment variables from .env file
load_dotenv()

# Task timeout (in seconds)
_TIMEOUT = int(os.getenv("TOPIC_TASK_TIMEOUT_SECS", "600"))


class TopicForwarder:
    """
    Kafka Topic Forwarder.

    Responsible for:
    - Subscribing to a source Kafka topic
    - Forwarding messages to a mapper topic
    - Handling retries, logging, and delivery status

    Attributes:
        bootstrap (str): Kafka bootstrap server
        src_topic (str): Source Kafka topic
        mapper_topic (str): Destination (mapper) topic
        group_id (str): Consumer group ID
        retry_delay (int): Delay between retries (seconds)
    """

    SERVICE_NAME = "producer-topic-eqbd-pg-gas-adaptor"

    def __init__(self) -> None:
        """Initialize Kafka producer, topics, and configuration."""
        self.logger = Logging().create_logger()

        # Kafka configuration from environment
        self.bootstrap = os.getenv("bootstrapServer", "")
        self.src_topic = os.getenv("srcTopicName", "")
        self.group_id = os.getenv("srcGroupId", self.SERVICE_NAME)
        self.retry_delay = int(os.getenv("consumerRetryDelaySecs", "5"))

        # Resolve mapper topic dynamically
        topic_resolver = TopicResolver()
        self.mapper_topic = topic_resolver.resolve(
            os.getenv("mapperTopicName"),
            self.src_topic,
            "trfm",
        )

        # Ensure target topic exists
        topic_manager = KafkaTopicManager(
            bootstrap_server=self.bootstrap,
            logger=self.logger,
        )
        topic_manager.ensure_exists(self.mapper_topic)

        # Initialize Kafka producer
        self.producer = Producer({"bootstrap.servers": self.bootstrap})

        # Log initialization
        self.logger.info(
            "Adaptor initialised",
            extra={
                "srcTopicName": self.src_topic,
                "mapperTopicName": self.mapper_topic,
            },
        )

        # Runtime debug configuration (console print)
        print(
            "\n--------------------------------------------"
            " adaptor runtime config "
            "--------------------------------------------"
        )
        print(
            "bootstrapServer:", self.bootstrap,
            "srcTopicName:", self.src_topic,
            "mapperTopicName:", self.mapper_topic,
            "srcGroupId:", self.group_id,
            "PRODUCT_NAME:", os.getenv("PRODUCT_NAME"),
            "SCHEDULER_BACKEND:", os.getenv("SCHEDULER_BACKEND"),
            "TOPIC_TASK_TIMEOUT_SECS:", os.getenv("TOPIC_TASK_TIMEOUT_SECS"),
        )

    def _on_delivery(self, err, msg) -> None:
        """
        Kafka delivery callback.

        Args:
            err: Delivery error (if any)
            msg: Kafka message
        """
        if err:
            self.logger.error(
                "Delivery failed",
                extra={"error": str(err)},
            )

    def _forward(self, msg, ctx: PipelineContext, step_log: StepLogger) -> None:
        """
        Forward a single Kafka message to mapper topic.

        Args:
            msg: Kafka message
            ctx (PipelineContext): Pipeline execution context
            step_log (StepLogger): Step-level logger
        """
        operation = "forward"
        start_time = datetime.now(UTC)

        step_log.step_start(
            ctx,
            operation,
            extra={
                "partition": msg.partition(),
                "offset": msg.offset(),
            },
        )

        try:
            # Produce message to mapper topic
            self.producer.produce(
                topic=self.mapper_topic,
                value=msg.value(),
                key=msg.key(),
                headers=msg.headers() or [],
                callback=self._on_delivery,
            )

            # Trigger delivery callbacks
            self.producer.poll(0)

            # Compute processing time
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

    def run_window(
        self,
        ctx: PipelineContext,
        step_log: StepLogger,
        stop_after: int | None = None,
    ) -> None:
        """
        Run message processing loop for a specified duration.

        Args:
            ctx (PipelineContext): Pipeline execution context
            step_log (StepLogger): Step logger
            stop_after (int | None): Stop execution after given seconds
        """
        deadline = time.monotonic() + stop_after if stop_after else None

        while True:
            if deadline and time.monotonic() >= deadline:
                break

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
                    if deadline and time.monotonic() >= deadline:
                        break

                    msg = consumer.poll(timeout=1.0)

                    if msg is None:
                        continue

                    if msg.error():
                        if msg.error().code() == KafkaError._PARTITION_EOF:
                            continue
                        raise KafkaException(msg.error())

                    # Forward message
                    self._forward(msg, ctx, step_log)

            except KafkaException as exc:
                self.logger.error(
                    "Kafka error",
                    extra={"error": str(exc), "retry": self.retry_delay},
                )

            except Exception as exc:
                self.logger.error(
                    "Unexpected error",
                    extra={"error": str(exc)},
                    exc_info=True,
                )

            finally:
                # Ensure resources are flushed and closed
                self.producer.flush()
                consumer.close()

            if deadline and time.monotonic() >= deadline:
                break

            time.sleep(self.retry_delay)


def run(ctx: PipelineContext) -> None:
    """
    Entry point for pipeline execution.

    Args:
        ctx (PipelineContext): Execution context
    """
    forwarder = TopicForwarder()
    step_log = StepLogger(forwarder.logger)

    # Log pipeline start banner
    step_log.pipeline_banner(
        ctx,
        service_name="producer-topic-eqbd-pg-gas-adaptor",
        config_summary={
            "srcTopicName": forwarder.src_topic,
            "mapperTopicName": forwarder.mapper_topic,
            "SCHEDULER_BACKEND": os.getenv("SCHEDULER_BACKEND", "standalone"),
            "PRODUCT_NAME": os.getenv("PRODUCT_NAME", "eqbd-pg-gas"),
        },
    )

    operation = "adaptor_window"
    step_log.step_start(ctx, operation)

    # Determine timeout based on trigger type
    timeout = (
        _TIMEOUT
        if ctx.triggered_by in ["kafka-trigger", "interval"]
        else None
    )

    try:
        forwarder.run_window(ctx, step_log, stop_after=timeout)
        step_log.step_end(ctx, operation)

    except Exception as exc:
        step_log.step_failed(ctx, operation, exc=exc)
        raise


if __name__ == "__main__":
    """
    Script entry point.

    Executes the adaptor using configured scheduler backend.
    """
    get_backend({"task_id": "trigger_adaptor"}).execute(
        run,
        pipeline_stage="adaptor",
        pipeline_type="topic",
        pipeline_role="producer",
    )
