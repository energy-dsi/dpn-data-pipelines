# Copyright 2026 DSI Project
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
# | 1.1.0   | OTEL Collector Integration                               | DSI Team      | 2026-06-26  |
# | 1.2.0   | Airflow Integration Release                              | DSI Team      | 2026-06-27  |
# +---------+----------------------------------------------------------+---------------+-------------+

"""
Kafka Topic Schema Mapper (Producer).

This module implements the producer schema mapper responsible for
consuming messages from a RAW Kafka topic, enriching them with
standardized metadata, and forwarding them to a downstream TARGET
Kafka topic.

The schema mapper supports scheduler-driven execution models,
OpenTelemetry observability, heartbeat monitoring, Kafka-triggered
execution, and interval-based processing patterns.

Features:
    * Kafka message consumption from RAW topics.
    * Environment-driven metadata enrichment.
    * Deterministic target topic resolution.
    * OpenTelemetry tracing and metrics collection.
    * Distributed trace context propagation.
    * Heartbeat monitoring and operational logging.
    * Scheduler backend integration for orchestrated execution.
    * Automatic Kafka topic validation and creation.

Environment Variables:
    bootstrapServer: Kafka bootstrap server endpoint.
    mapperTopicName: Source RAW Kafka topic name.
    mapperGroupId: Kafka consumer group identifier.
    consumerRetryDelaySecs: Consumer retry interval.
    schemaType: Schema identifier used for target topic generation.
    orgName: Organization identifier used for target topic generation.
    productType: Product identifier used for target topic generation.
    SCHEDULER_BACKEND: Execution backend configuration.
    TOPIC_TASK_TIMEOUT_SECS: Processing window timeout.

Pipeline Flow:
    RAW Topic --> Schema Mapper --> TARGET Topic

Example:
    RAW Topic:
        dpn-producer-eqbd-pg-gas-raw

    TARGET Topic:
        dpn-producer-neso-eqbd-eqbdpggas-target

File Location:
    Producer/topic/<data_product>/schema_mapper/main.py
"""

from __future__ import annotations

import os
import re
import time
from datetime import UTC, datetime

from confluent_kafka import Consumer, KafkaError, KafkaException, Producer
from opentelemetry import context as _otel_context, propagate as _otel_propagate
from dotenv import load_dotenv

from utils.topic_utils import TopicResolver, KafkaTopicManager
from utils.pipeline_context import PipelineContext
from utils.scheduler_backend import get_backend
from utils.step_logger import StepLogger
from dpn_observability_sdk.otel_tracer import OtelTracer
from dpn_observability_sdk.otel_metrics import OtelMetrics
from dpn_observability_sdk.otel_instrumentation import traced, timed_metric
from dpn_observability_sdk.heartbeat import HeartbeatLogger
from dpn_observability_sdk.otel_logger import OtelLogger as Logging


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

        # Initialize OpenTelemetry
        self.tracer = OtelTracer.initialize(
            service_name="producer-topic-schema-mapper",
            service_version="1.0.0"
        )
        self.meter = OtelMetrics.initialize(
            service_name="producer-topic-schema-mapper",
            service_version="1.0.0"
        )
        
        # Create metrics
        self.messages_forwarded = self.meter.create_counter(
            name="messages_forwarded_total",
            description="Total messages forwarded",
            unit="1",
        )
        
        self.forward_duration = self.meter.create_histogram(
            name="message_forward_duration",
            description="Message forwarding duration",
            unit="ms",
        )
        
        self.messages_consumed = self.meter.create_counter(
            name="messages_consumed_total",
            description="Total messages consumed from source topic",
            unit="1",
        )

        # Initialize heartbeat logger (will be started in main)
        self.heartbeat: HeartbeatLogger | None = None

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

    @traced(span_name="process_message")
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

        # Restore the extractor's trace context from Kafka headers so this span
        # becomes a child of the extractor span — same trace_id end-to-end.
        _carrier = {
            k: v.decode() if isinstance(v, bytes) else v
            for k, v in (msg.headers() or [])
        }
        _remote_ctx = _otel_propagate.extract(_carrier)
        _token = _otel_context.attach(_remote_ctx)

        with self.tracer.start_as_current_span("process_message") as span:

            operation = "mapper_msg"
            start_time = datetime.now(UTC)

            span.set_attribute("kafka.source_topic", msg.topic())
            span.set_attribute("kafka.partition", msg.partition())
            span.set_attribute("kafka.offset", msg.offset())

            step_log.step_start(ctx, operation)

            try:

                # Enrich metadata headers
                enriched_headers = [
                    ("schemaType", self.schema_type.encode()),
                    ("orgName", self.org_name.encode()),
                    ("productType", self.product_type.encode()),
                    ("processedAt", datetime.now(UTC).isoformat().encode()),
                    ("offset",str(msg.offset())),
                ]

                # Produce message
                self.producer.produce(
                    topic=self.target_topic,
                    value=msg.value(),
                    key=msg.key(),
                    headers=enriched_headers,
                    callback=self._on_delivery,
                )

                # Compute processing time
                duration_ms = int(
                    (datetime.now(UTC) - start_time).total_seconds() * 1000
                )

                span.set_attribute("processing.status", "success")
                span.set_attribute("processing.duration_ms", duration_ms)

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
                span.set_attribute("processing.status", "error")
                span.set_attribute("error.type", type(exc).__name__)
                span.record_exception(exc)
                step_log.step_failed(ctx, operation, exc=exc)
                raise

    @traced(span_name="schema_mapper_window")
    @timed_metric("schema_mapper_window_duration", "Duration of schema mapper window")
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

        with self.tracer.start_as_current_span("schema_mapper_window") as span:

            span.set_attribute("kafka.target_topic", self.target_topic)
            span.set_attribute("kafka.group_id", self.group_id)
            span.set_attribute("window.timeout_seconds", stop_after or 0)

            messages_processed = 0
            messages_skipped = 0

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

                        if self.target_topic:
                            messages_processed += 1
                        else:
                            messages_skipped += 1

                except Exception as unexpected_error:
                    span.set_attribute("error.type", "unexpected_error")
                    span.record_exception(unexpected_error)
                    self.logger.error(
                        "unexpected",
                        extra={"error": str(unexpected_error)},
                        exc_info=True
                    )


                finally:
                    self.producer.flush()
                    consumer.close()

                time.sleep(self.retry_delay)

            span.set_attribute("messages.processed", messages_processed)
            span.set_attribute("messages.skipped", messages_skipped)
            span.set_attribute("window.status", "completed")


@traced(span_name="producer_schema_mapper_pipeline")
@timed_metric("pipeline_total_duration", "Total pipeline execution time")
def run(ctx: PipelineContext) -> None:
    """
    Pipeline entry point.

    Initializes mapper and starts processing loop.

    Args:
        ctx (PipelineContext): Execution context
    """

    tracer = OtelTracer.get_tracer(__name__)
    
    with tracer.start_as_current_span("schema_mapper_pipeline") as span:
        span.set_attribute("pipeline.type", "producer-topic-schema-mapper")
        span.set_attribute("pipeline.triggered_by", ctx.triggered_by)
        span.set_attribute("pipeline.run_id", ctx.run_id)

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
        span.set_attribute("pipeline.timeout_seconds", timeout or 0)

        try:
            mapper.run_window(ctx, step_log, stop_after=timeout)
            span.set_attribute("pipeline.status", "success")

        except Exception as exc:
            span.set_attribute("pipeline.status", "error")
            span.set_attribute("error.type", type(exc).__name__)
            span.record_exception(exc)
            raise        


if __name__ == "__main__":
    """
    Application entry point.

    Executes schema mapper via configured scheduler backend.
    """
    backend = get_backend({"task_id": "trigger_schema_mapper"})
    temp_forwarder = TopicSchemaMapper()

    heartbeat = HeartbeatLogger(
        logger=temp_forwarder.logger,
        component_name="producer-topic-schema-mapper",
        metadata={
            "source_topic": temp_forwarder.src_topic,
            "target_topic": temp_forwarder.target_topic,
            "scheduler_backend": os.getenv("SCHEDULER_BACKEND", "standalone"),
        },
    )
    heartbeat.start()

    backend.execute(
        run,
        pipeline_stage="schema_mapper",
        pipeline_type="topic",
        pipeline_role="producer",
    )