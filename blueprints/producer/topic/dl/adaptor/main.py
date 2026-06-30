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
# | 1.1.0   | Kafka Trigger and StepLogger Integration                 | DSI Team      | 2026-05-26  |
# | 1.2.0   | OTEL Collector Integration                               | DSI Team      | 2026-06-26  |
# +---------+----------------------------------------------------------+---------------+-------------+

"""
Kafka Topic Adaptor (Producer).

This module implements the producer adaptor responsible for consuming
messages from a source Kafka topic and forwarding them to a downstream
mapper topic for transformation processing.

The adaptor supports scheduler-driven execution models, OpenTelemetry
observability, heartbeat monitoring, Kafka-triggered execution, and
interval-based processing patterns.

Features:
    * Kafka message consumption from source topics.
    * Dynamic mapper topic resolution.
    * OpenTelemetry tracing and metrics collection.
    * Message forwarding with distributed trace propagation.
    * Heartbeat monitoring and operational logging.
    * Scheduler backend integration for orchestrated execution.

Environment Variables:
    bootstrapServer: Kafka bootstrap server endpoint.
    srcTopicName: Source Kafka topic name.
    mapperTopicName: Mapper topic name or configuration.
    srcGroupId: Kafka consumer group identifier.
    consumerRetryDelaySecs: Consumer retry interval.
    PRODUCT_NAME: Data product identifier.
    SCHEDULER_BACKEND: Execution backend configuration.
    TOPIC_TASK_TIMEOUT_SECS: Processing window timeout.

File Location:
    Producer/topic/<data_product>/adaptor/main.py
"""

from __future__ import annotations

import os
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

        # Initialize OpenTelemetry
        self.tracer = OtelTracer.initialize(
            service_name="producer-topic-adaptor",
            service_version="1.0.0"
        )
        self.meter = OtelMetrics.initialize(
            service_name="producer-topic-adaptor",
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

    @traced(span_name="forward_message")
    def _forward(self, msg, ctx: PipelineContext, step_log: StepLogger) -> None:
        """
        Forward a single Kafka message to mapper topic.

        Args:
            msg: Kafka message
            ctx (PipelineContext): Pipeline execution context
            step_log (StepLogger): Step-level logger
        """
        with self.tracer.start_as_current_span("forward_message") as span:
            operation = "forward"
            start_time = datetime.now(UTC)

            # Add span attributes
            span.set_attribute("kafka.source_topic", msg.topic())
            span.set_attribute("kafka.destination_topic", self.mapper_topic)
            span.set_attribute("kafka.partition", msg.partition())
            span.set_attribute("kafka.offset", msg.offset())
            span.set_attribute("message.key", str(msg.key()) if msg.key() else "null")
            
            step_log.step_start(
                ctx,
                operation,
                extra={
                    "partition": msg.partition(),
                    "offset": msg.offset(),
                },
            )

            try:
                # schema_mapper pod can restore the same trace_id.
                carrier: dict[str, str] = {}
                _otel_propagate.inject(carrier)
                outgoing_headers = list(msg.headers() or []) + [
                    (k, v.encode() if isinstance(v, str) else v)
                    for k, v in carrier.items()
                ]
                # Produce message to mapper topic
                self.producer.produce(
                    topic=self.mapper_topic,
                    value=msg.value(),
                    key=msg.key(),
                    headers=outgoing_headers,
                    callback=self._on_delivery,
                )

                # Trigger delivery callbacks
                self.producer.poll(0)

                # Compute processing time
                duration_ms = int(
                    (datetime.now(UTC) - start_time).total_seconds() * 1000
                )
                
                # Record metrics
                self.messages_forwarded.add(1, {
                    "source_topic": msg.topic(),
                    "destination_topic": self.mapper_topic,
                    "status": "success"
                })
                
                self.forward_duration.record(duration_ms, {
                    "source_topic": msg.topic(),
                    "destination_topic": self.mapper_topic
                })
                
                span.set_attribute("forward.status", "success")
                span.set_attribute("forward.duration_ms", duration_ms)

                step_log.step_end(
                    ctx,
                    operation,
                    extra={
                        "dst": self.mapper_topic,
                        "duration_ms": duration_ms,
                    },
                )

            except Exception as exc:
                # Record error metrics
                self.messages_forwarded.add(1, {
                    "source_topic": msg.topic(),
                    "destination_topic": self.mapper_topic,
                    "status": "error"
                })
                
                span.set_attribute("forward.status", "error")
                span.set_attribute("error.type", type(exc).__name__)
                span.record_exception(exc)

                step_log.step_failed(ctx, operation, exc=exc)
                raise

    @traced(span_name="producer_window")
    @timed_metric("producer_window_duration", "Duration of producer window")
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

        with self.tracer.start_as_current_span("consumer_window") as span:
            span.set_attribute("kafka.source_topic", self.src_topic)
            span.set_attribute("kafka.mapper_topic", self.mapper_topic)
            span.set_attribute("kafka.group_id", self.group_id)
            span.set_attribute("window.timeout_seconds", stop_after or 0)
            
            messages_processed = 0

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

                        # Record message consumption
                        self.messages_consumed.add(1, {
                            "source_topic": self.src_topic,
                            "partition": str(msg.partition())
                        })
                        
                        messages_processed += 1

                        # Forward message
                        self._forward(msg, ctx, step_log)

                except KafkaException as exc:
                    span.set_attribute("error.type", "kafka_error")
                    span.record_exception(exc)
                    self.logger.error(
                        "Kafka error",
                        extra={"error": str(exc), "retry": self.retry_delay},
                    )

                except Exception as exc:
                    span.set_attribute("error.type", "unexpected_error")
                    span.record_exception(exc)
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
            span.set_attribute("messages.processed", messages_processed)
            span.set_attribute("window.status", "completed")


@traced(span_name="producer_topic_adaptor_pipeline")
@timed_metric("pipeline_total_duration", "Total pipeline execution time")
def run(ctx: PipelineContext) -> None:
    """
    Entry point for pipeline execution.

    Args:
        ctx (PipelineContext): Execution context
    """
    tracer = OtelTracer.get_tracer(__name__)
    
    with tracer.start_as_current_span("extractor_pipeline") as span:
        span.set_attribute("pipeline.type", "producer-topic-adaptor")
        span.set_attribute("pipeline.triggered_by", ctx.triggered_by)
        span.set_attribute("pipeline.run_id", ctx.run_id)
            
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
        span.set_attribute("pipeline.timeout_seconds", timeout or 0)

        try:
            forwarder.run_window(ctx, step_log, stop_after=timeout)
            step_log.step_end(ctx, operation)
            span.set_attribute("pipeline.status", "success")

        except Exception as exc:
            span.set_attribute("pipeline.status", "error")
            span.set_attribute("error.type", type(exc).__name__)
            span.record_exception(exc)
            step_log.step_failed(ctx, operation, exc=exc)
            raise


if __name__ == "__main__":
    """
    Script entry point.

    Executes the adaptor using configured scheduler backend.
    """
    backend = get_backend({"task_id": "trigger_adaptor"})
    temp_forwarder = TopicForwarder()
    heartbeat = HeartbeatLogger(
        logger=temp_forwarder.logger,
        component_name="producer-topic-adaptor",
        metadata={
            "source_topic": temp_forwarder.src_topic,
            "mapper_topic": temp_forwarder.mapper_topic,
            "scheduler_backend": os.getenv("SCHEDULER_BACKEND", "standalone"),
        },
    )
    heartbeat.start()

    backend.execute(
        run,
        pipeline_stage="adaptor",
        pipeline_type="topic",
        pipeline_role="producer",
    )

