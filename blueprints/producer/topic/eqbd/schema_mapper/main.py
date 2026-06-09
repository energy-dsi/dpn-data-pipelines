# Copyright DSI Project — Apache 2.0

"""
Kafka Topic Schema Mapper (Producer)

Overview:
---------
This service consumes messages from a RAW Kafka topic and forwards them
to a dynamically resolved TARGET topic. It enriches each message with
standardized metadata headers derived from environment configuration.

Design Principles:
------------------
- ✅ Static routing (ENV-driven)
- ✅ No dependency on message headers for routing
- ✅ Target topic created at startup (idempotent)
- ✅ Supports fallback to 'unknown' metadata
- ✅ Reliable delivery with explicit flush
- ✅ Suitable for Kubernetes / event-driven pipelines

Pipeline Flow:
--------------
RAW Topic  ──►  Schema Mapper (this service)  ──►  TARGET Topic

Key Features:
-------------
- Deterministic topic naming convention
- Automatic topic creation using Kafka Admin API
- Message enrichment with processing metadata
- Safe Kafka consumption using retry loop
- Debug logging for observability

Environment Variables:
----------------------
bootstrapServer        : Kafka bootstrap servers
mapperTopicName        : RAW topic name (source)
schemaType             : Schema identifier (optional)
orgName                : Organization name (optional)
productType            : Product identifier (optional)

Example:
--------
RAW:    dpn-producer-eqbd-pg-gas-raw
TARGET: dpn-producer-neso-eqbd-eqbdpggas-target

Deployment:
-----------
- Designed for Kubernetes
- Supports Kafka-trigger / interval execution
"""

from __future__ import annotations

import os
import re
import time
from datetime import UTC, datetime

from confluent_kafka import Consumer, KafkaError, KafkaException, Producer
from dotenv import load_dotenv

from utils.otel_logger import OtelLogger as Logging
from utils.topic_utils import TopicResolver, KafkaTopicManager
from utils.pipeline_context import PipelineContext
from utils.scheduler_backend import get_backend
from utils.step_logger import StepLogger


# Load environment variables from .env
load_dotenv()

# Maximum execution timeout (used for scheduled / triggered execution)
_TIMEOUT = int(os.getenv("TOPIC_TASK_TIMEOUT_SECS", "600"))


def _sanitize(value: str | None, default: str = "unknown") -> str:
    """
    Normalize input string for Kafka-safe topic naming.

    Rules:
    - Removes non-alphanumeric characters
    - Converts to lowercase
    - Falls back to 'unknown' if empty or None

    Args:
        value (str | None): Input value
        default (str): Default fallback

    Returns:
        str: Sanitized string
    """
    cleaned = re.sub(r"[^A-Za-z0-9]", "", value or default).lower()
    return cleaned or default


class TopicSchemaMapper:
    """
    Kafka Producer Schema Mapper

    Responsibilities:
    -----------------
    - Consume messages from RAW topic
    - Enrich messages with metadata headers
    - Resolve target topic deterministically
    - Produce messages to target topic

    Characteristics:
    ----------------
    - Static routing (ENV-based)
    - Topic resolved once at startup
    - No per-message topic computation
    """

    def __init__(self) -> None:
        """
        Initialize Kafka clients, metadata, and topics.
        """
        self.logger = Logging().create_logger()

        # Kafka configuration
        self.bootstrap = os.getenv("bootstrapServer", "")
        self.src_topic = os.getenv("mapperTopicName", "")  # RAW topic
        self.group_id = os.getenv("mapperGroupId", "producer_mapper")
        self.retry_delay = int(os.getenv("consumerRetryDelaySecs", "5"))

        # Metadata (ENV-driven, supports fallback)
        self.schema_type = _sanitize(os.getenv("schemaType"))
        self.org_name = _sanitize(os.getenv("orgName"))
        self.product_type = _sanitize(os.getenv("productType"))

        # Topic resolver utility (handles topic naming conventions)
        self.tr = TopicResolver()

        # Kafka Admin manager (used to ensure topic existence)
        self.km = KafkaTopicManager(
            bootstrap_server=self.bootstrap,
            logger=self.logger,
        )

        # ✅ Ensure RAW topic exists (defensive)
        self.km.ensure_exists(self.src_topic)

        # ✅ Construct TARGET topic
        topic_name = (
            f"dpn-producer-{self.org_name}-"
            f"{self.schema_type}-"
            f"{self.product_type}-target"
        )

        # Resolve topic (supports naming strategies / environments)
        self.target_topic = self.tr.resolve(
            topic_name,
            self.src_topic,
            "target",
        )

        # ✅ Ensure TARGET topic exists
        self.logger.info(f"Ensuring target topic exists: {self.target_topic}")
        self.km.ensure_exists(self.target_topic)

        # Kafka producer client
        self.producer = Producer(
            {"bootstrap.servers": self.bootstrap}
        )

    def _on_delivery(self, err, msg) -> None:
        """
        Kafka delivery callback.

        Logs success or failure of message delivery.

        Args:
            err: Delivery error (if any)
            msg: Kafka message metadata
        """
        if err:
            self.logger.error(
                "Delivery failed",
                extra={"error": str(err)},
            )

    def _process(
        self,
        msg,
        ctx: PipelineContext,
        step_log: StepLogger,
    ) -> None:
        """
        Process a single Kafka message.

        Steps:
        -------
        1. Log message reception
        2. Enrich headers with metadata
        3. Produce message to target topic
        4. Ensure delivery

        Args:
            msg: Kafka message
            ctx (PipelineContext): Execution context
            step_log (StepLogger): Pipeline logger
        """
        operation = "mapper_msg"
        start_time = datetime.now(UTC)

        step_log.step_start(ctx, operation)

        try:

            # Enrich metadata headers
            enriched_headers = [
                ("schema_type", self.schema_type.encode()),
                ("org_name", self.org_name.encode()),
                ("product_type", self.product_type.encode()),
                ("processed_at", datetime.now(UTC).isoformat().encode()),
            ]

            # Produce message
            self.producer.produce(
                topic=self.target_topic,
                value=msg.value(),
                key=msg.key(),
                headers=enriched_headers,
                callback=self._on_delivery,
            )

            # Trigger delivery
            self.producer.poll(0)

            # Ensure message is delivered (synchronous safety)
            self.producer.flush()

            duration_ms = int(
                (datetime.now(UTC) - start_time).total_seconds() * 1000
            )

            step_log.step_end(
                ctx,
                operation,
                extra={
                    "dst": self.target_topic,
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
        Run Kafka consumer loop.

        Polls messages from RAW topic and processes them
        until timeout or termination.

        Args:
            ctx (PipelineContext): Execution context
            step_log (StepLogger): Pipeline logger
            stop_after (int | None): Optional timeout
        """
        deadline = time.monotonic() + stop_after if stop_after else None

        while True:
            if deadline and time.monotonic() >= deadline:
                break

            # Kafka consumer config
            consumer = Consumer(
                {
                    "bootstrap.servers": self.bootstrap,
                    "group.id": self.group_id,
                    "auto.offset.reset": "earliest",  # ensures existing data is read
                    "enable.auto.commit": True,
                }
            )

            # Subscribe to RAW topic
            consumer.subscribe([self.src_topic])

            try:
                while True:
                    if deadline and time.monotonic() >= deadline:
                        break

                    msg = consumer.poll(1.0)

                    if msg is None:
                        continue

                    if msg.error():
                        if msg.error().code() == KafkaError._PARTITION_EOF:
                            continue
                        raise KafkaException(msg.error())

                    self._process(msg, ctx, step_log)

            finally:
                self.producer.flush()
                consumer.close()

            time.sleep(self.retry_delay)


def run(ctx: PipelineContext) -> None:
    """
    Pipeline entry point.

    Initializes mapper and starts processing loop.

    Args:
        ctx (PipelineContext): Execution context
    """
    mapper = TopicSchemaMapper()
    step_log = StepLogger(mapper.logger)

    # Banner for observability
    step_log.pipeline_banner(
        ctx,
        service_name="producer-schema-mapper",
        config_summary={
            "source_topic": mapper.src_topic,
            "target_topic": mapper.target_topic,
        },
    )

    timeout = (
        _TIMEOUT
        if ctx.triggered_by in ["kafka-trigger", "interval"]
        else None
    )

    mapper.run_window(ctx, step_log, stop_after=timeout)


if __name__ == "__main__":
    """
    Application entry point.

    Executes schema mapper via configured scheduler backend.
    """
    get_backend({"task_id": "trigger_schema_mapper"}).execute(
        run,
        pipeline_stage="schema_mapper",
        pipeline_type="topic",
        pipeline_role="producer",
    )