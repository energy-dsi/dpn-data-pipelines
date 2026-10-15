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
# | 2.0.0   | Simplified data pipeline implementation                  | DSI Team      | 2026-10-11  |
# +---------+----------------------------------------------------------+---------------+-------------+

"""
Kafka Topic Adaptor (Producer) - Generic Data Product Runtime.

This module implements the producer adaptor responsible for consuming
messages from a source Kafka topic, enriching them with the same
standardized metadata the removed schema mapper always added, and
producing them straight to the target Kafka topic. There is no
intermediate "mapper"/RAW topic or separate mapper process any more — a
data product's adaptor is now the entire topic-pathway pipeline by
itself.

Unlike the per-schema-type blueprints under blueprints/producer/topic/,
this module carries no product- or schema-specific defaults: every
identifier (product name, source topic, group id, org) is supplied
entirely via environment variables at container start. One image serves
every data product.

The adaptor supports scheduler-driven execution models, OpenTelemetry
observability, heartbeat monitoring, Kafka-triggered execution, and
interval-based processing patterns.

Environment Variables:
    bootstrapServer: Kafka bootstrap server endpoint.
    srcTopicName: Source Kafka topic name — required, no default.
    srcGroupId: Kafka consumer group identifier.
    orgName: Organization identifier — used to compute the target topic
        name and stamped onto every message's headers.
    productType: Product identifier — used to compute the target topic
        name and stamped onto every message's headers.
    targetTopicName: Destination Kafka topic — a real user override from
        the self-service review screen; falls back to
        dpn-producer-<org>-<product>-target (computed from orgName/
        productType above) only when left unset entirely.
    consumerRetryDelaySecs: Consumer retry interval.
    PRODUCT_NAME: Data product identifier — required, no default.
    SCHEDULER_BACKEND: Execution backend configuration.
    TOPIC_TASK_TIMEOUT_SECS: Processing window timeout.

Pipeline Flow:
    Source Topic --> Adaptor (enrich) --> Target Topic

File Location:
    blueprints/producer/topic/adaptor/main.py
"""

from __future__ import annotations

import os
import re
import signal
import sys
import time
from datetime import UTC, datetime

from confluent_kafka import Consumer, KafkaError, KafkaException, Producer
from opentelemetry import propagate as _otel_propagate
from dotenv import load_dotenv

from utils.kafka_security import build_kafka_client_config
from utils.topic_utils import KafkaTopicManager
from utils.pipeline_context import PipelineContext
from utils.scheduler_backend import get_backend, run_once
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


def _sanitize(value: str | None, default: str = "unknown") -> str:
    """
    Normalize input string for Kafka-safe topic naming — identical to the
    removed schema mapper's own `_sanitize()`, preserved verbatim so the
    target topic name this adaptor computes is exactly what it always was.

    Rules:
    - Removes non-alphanumeric characters
    - Converts to lowercase
    - Falls back to 'unknown' if empty or None
    """
    cleaned = re.sub(r"[^A-Za-z0-9]", "", value or default).lower()
    return cleaned or default


class TopicForwarder:
    """
    Kafka Topic Forwarder for any registered data product.

    Responsible for:
    - Subscribing to the source Kafka topic
    - Enriching each message with metadata headers (org/product/timestamp/
      offset — exactly what the removed schema mapper always added)
    - Producing straight to the target Kafka topic
    - Handling retries, logging, and delivery status

    Attributes:
        bootstrap (str): Kafka bootstrap server
        src_topic (str): Source Kafka topic
        target_topic (str): Destination topic — dpn-producer-<org>-<product>-target
        group_id (str): Consumer group ID
        retry_delay (int): Delay between retries (seconds)
    """

    COMPONENT_PREFIX = "producer-topic-adaptor"

    def __init__(self) -> None:
        """Initialize Kafka producer, topics, and configuration."""

        self.logger = Logging().create_logger()
        self.product_name: str = os.getenv("PRODUCT_NAME", "")
        self.component_name = f"{self.COMPONENT_PREFIX}-{self.product_name or 'unknown'}"

        # Initialize OpenTelemetry
        self.tracer = OtelTracer.initialize(
            service_name="producer-topic-adaptor",
            service_version="3.0.0"
        )
        self.meter = OtelMetrics.initialize(
            service_name="producer-topic-adaptor",
            service_version="3.0.0"
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
        self.group_id = os.getenv("srcGroupId", self.component_name)
        self.retry_delay = int(os.getenv("consumerRetryDelaySecs", "5"))
        # Bounds the producer.flush() call below — without it, flush()
        # blocks forever on an undelivered backlog, freezing
        # IntervalBackend's single-threaded tick loop permanently: no
        # further cycles ever run, with no crash/restart to signal it.
        self.flush_timeout = int(os.getenv("producerFlushTimeoutSecs", "10"))

        # Metadata (ENV-driven, supports fallback) — absorbed from the
        # removed schema mapper, which used these to compute the target
        # topic name and to stamp every message's headers.
        self.org_name = _sanitize(os.getenv("orgName"))
        self.product_type = _sanitize(os.getenv("productType"))

        # Target topic: a real, user-provided value from the self-service
        # review screen (targetTopicName) — not silently recomputed here
        # every time. Falls back to the same dpn-producer-<org>-<product>-
        # target formula the removed schema mapper always used only when
        # nothing was ever supplied at all (e.g. a manual chart deploy
        # outside the self-service UI, or the field left blank) — see
        # service/models.py's default_topic_names().
        fallback_target_topic_name = f"dpn-producer-{self.org_name}-{self.product_type}-target"
        self.target_topic = os.getenv("targetTopicName", "").strip() or fallback_target_topic_name

        # Ensure source and target topics exist. The source topic is
        # normally owned by an upstream system, but a brand-new self-service
        # product may name one that doesn't exist yet on the broker — without
        # this, consumer.subscribe() below fails every poll with
        # UNKNOWN_TOPIC_OR_PART and the adaptor retry-loops forever.
        topic_manager = KafkaTopicManager(
            bootstrap_server=self.bootstrap,
            logger=self.logger,
        )
        topic_manager.ensure_exists(self.src_topic)
        topic_manager.ensure_exists(self.target_topic)

        # Initialize Kafka producer
        self.producer = Producer(build_kafka_client_config(self.bootstrap))

        # Log initialization
        self.logger.info(
            "Adaptor initialised",
            extra={
                "srcTopicName": self.src_topic,
                "targetTopicName": self.target_topic,
                **({"component.name": self.component_name} if self.component_name else {}),
            },
        )

    def _on_delivery(self, err, msg) -> None:
        """Kafka delivery callback."""
        if err:
            self.logger.error(
                "Delivery failed",
                extra={
                    "error": str(err),
                    **({"component.name": self.component_name} if self.component_name else {}),
                },
            )

    @traced(span_name="forward_message")
    def _forward(self, msg, ctx: PipelineContext, step_log: StepLogger) -> None:
        """
        Enrich and produce a single Kafka message straight to the target
        topic — one hop, not two: there is no mapper topic in between any
        more.
        """
        with self.tracer.start_as_current_span("forward_message") as span:
            operation = "forward"
            start_time = datetime.now(UTC)

            span.set_attribute("kafka.source_topic", msg.topic())
            span.set_attribute("kafka.destination_topic", self.target_topic)
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
                # Trace context, for any external consumer of the target
                # topic that wants to correlate — same propagation the
                # removed schema mapper's own downstream relied on.
                carrier: dict[str, str] = {}
                _otel_propagate.inject(carrier)

                # Metadata headers — identical to what the removed schema
                # mapper always stamped onto every message it forwarded.
                enriched_headers = list(msg.headers() or []) + [
                    ("orgName", self.org_name.encode()),
                    ("productType", self.product_type.encode()),
                    ("processedAt", datetime.now(UTC).isoformat().encode()),
                    ("offset", str(msg.offset())),
                ] + [
                    (k, v.encode() if isinstance(v, str) else v)
                    for k, v in carrier.items()
                ]

                self.producer.produce(
                    topic=self.target_topic,
                    value=msg.value(),
                    key=msg.key(),
                    headers=enriched_headers,
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
                    "destination_topic": self.target_topic,
                    "status": "success"
                })

                self.forward_duration.record(duration_ms, {
                    "source_topic": msg.topic(),
                    "destination_topic": self.target_topic
                })

                span.set_attribute("forward.status", "success")
                span.set_attribute("forward.duration_ms", duration_ms)

                step_log.step_end(
                    ctx,
                    operation,
                    extra={
                        "dst": self.target_topic,
                        "duration_ms": duration_ms,
                    },
                )

            except Exception as exc:
                # Record error metrics
                self.messages_forwarded.add(1, {
                    "source_topic": msg.topic(),
                    "destination_topic": self.target_topic,
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
        """Run message processing loop for a specified duration."""

        with self.tracer.start_as_current_span("consumer_window") as span:
            span.set_attribute("kafka.source_topic", self.src_topic)
            span.set_attribute("kafka.target_topic", self.target_topic)
            span.set_attribute("kafka.group_id", self.group_id)
            span.set_attribute("window.timeout_seconds", stop_after or 0)

            messages_processed = 0

            deadline = time.monotonic() + stop_after if stop_after else None

            while True:
                if deadline and time.monotonic() >= deadline:
                    break

                consumer = Consumer(
                    {
                        **build_kafka_client_config(self.bootstrap),
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

                        # Enrich and forward message straight to target
                        self._forward(msg, ctx, step_log)

                except KafkaException as exc:
                    span.set_attribute("error.type", "kafka_error")
                    span.record_exception(exc)
                    self.logger.error(
                        "Kafka error",
                        extra={
                            "error": str(exc),
                            "retry": self.retry_delay,
                            **ctx.as_log_extra(),
                        },
                    )

                except Exception as exc:
                    span.set_attribute("error.type", "unexpected_error")
                    span.record_exception(exc)
                    self.logger.error(
                        "Unexpected error",
                        extra={"error": str(exc), **ctx.as_log_extra()},
                        exc_info=True,
                    )

                finally:
                    # Ensure resources are flushed and closed — bounded,
                    # so an unreachable broker can't hang this forever
                    # (see self.flush_timeout's comment above).
                    pending = self.producer.flush(self.flush_timeout)
                    if pending:
                        self.logger.warning(
                            "Producer flush timed out with messages still undelivered",
                            extra={
                                "pending_messages": pending,
                                "flush_timeout_secs": self.flush_timeout,
                                **ctx.as_log_extra(),
                            },
                        )
                    consumer.close()

                if deadline and time.monotonic() >= deadline:
                    break

                time.sleep(self.retry_delay)
            span.set_attribute("messages.processed", messages_processed)
            span.set_attribute("window.status", "completed")


@traced(span_name="producer_topic_adaptor_pipeline")
@timed_metric("pipeline_total_duration", "Total pipeline execution time")
def run(ctx: PipelineContext) -> None:
    """Entry point for pipeline execution."""
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
            service_name=f"producer-topic-{forwarder.product_name or 'unknown'}-adaptor",
            config_summary={
                "srcTopicName": forwarder.src_topic,
                "targetTopicName": forwarder.target_topic,
                "SCHEDULER_BACKEND": os.getenv("SCHEDULER_BACKEND", "standalone"),
                "PRODUCT_NAME": forwarder.product_name,
            },
        )

        operation = "adaptor_window"
        step_log.step_start(ctx, operation)

        # Determine timeout based on trigger type. "manual" (the
        # self-service "Run Now" button, see utils/scheduler_backend.py's
        # run_once()) is bounded the same way "interval" is, so a manual
        # run consumes for one window and returns rather than blocking
        # forever the way an untimed "standalone" run would.
        timeout = (
            _TIMEOUT
            if ctx.triggered_by in ["kafka-trigger", "interval", "manual"]
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
    """Script entry point. Executes the adaptor using configured scheduler backend."""
    temp_forwarder = TopicForwarder()
    component_name = temp_forwarder.component_name

    if "--run-once" in sys.argv:
        # Self-service "Run Now" button (see service/podman_ops.py /
        # k8s_ops.py run_once()): execs this inside the already-running
        # container to force a single window on demand, bypassing the
        # scheduler entirely — no heartbeat needed for a process this
        # short-lived.
        run_once(
            run,
            pipeline_stage="adaptor",
            pipeline_type="topic",
            pipeline_role="producer",
            component_name=component_name,
        )
    else:
        backend = get_backend({"task_id": "trigger_adaptor"})
        heartbeat = HeartbeatLogger(
            logger=temp_forwarder.logger,
            component_name=component_name,
            metadata={
                "source_topic": temp_forwarder.src_topic,
                "target_topic": temp_forwarder.target_topic,
                "scheduler_backend": os.getenv("SCHEDULER_BACKEND", "standalone"),
            },
        )
        heartbeat.start()

        # Deployment deletion/rolling-update sends SIGTERM before the pod is
        # killed — catching it emits the SDK's own "heartbeat.stopped" event
        # (see dpn_observability_sdk.heartbeat.HeartbeatLogger.stop) within
        # the grace period, so this product's last-known-alive signal is
        # followed by an explicit stop rather than just silence.
        def _handle_sigterm(signum, frame):
            heartbeat.stop()
            sys.exit(0)

        signal.signal(signal.SIGTERM, _handle_sigterm)

        backend.execute(
            run,
            pipeline_stage="adaptor",
            pipeline_type="topic",
            pipeline_role="producer",
            component_name=component_name,
        )
