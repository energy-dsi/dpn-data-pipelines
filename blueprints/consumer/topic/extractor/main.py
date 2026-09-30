# Copyright DSI Project — Apache 2.0
# v1.0.0 Initial | v1.1.0 Kafka trigger + StepLogger 2026-05-26
# +---------+----------------------------------------------------------+---------------+-------------+
# | Version | Description                                              | Change Owner  | Change Date |
# +---------+----------------------------------------------------------+---------------+-------------+
# | 1.0.0   | Initial version                                          | DSI Team      | 2026-05-01  |
# | 1.1.0   | Kafka Trigger and StepLogger Integration                 | DSI Team      | 2026-05-26  |
# | 1.2.0   | OTEL Collector Integration                               | DSI Team      | 2026-06-26  |
# | 2.0.0   | Absorbed schema_mapper — consumes srcTopicName, resolves  | DSI Team      | 2026-09-15  |
# |         | + enriches + produces straight to the per-product target |               |             |
# |         | topic, no mapperTopicName relay                          |               |             |
# | 2.1.0   | Added --run-once so Run Now forces a window on demand;    | DSI Team      | 2026-09-24  |
# |         | manual trigger now bounded by _TIMEOUT like kafka-trigger |               |             |
# | 2.2.0   | Reverted the short-lived 3-stage stage -> raw -> target   | DSI Team      | 2026-09-30  |
# |         | flow — back to 2-stage stage -> target (no rawTopicName) |               |             |
# +---------+----------------------------------------------------------+---------------+-------------+

"""
/home/claude/dpn-complete/consumer/topic/extractor/main.py

Kafka topic extractor (consumer).

This service consumes messages from the source topic, stamps org/product/
processedAt/offset headers onto each one, and produces it straight to the
per-product target topic (dpn-consumer-<org>-<product>-target) — resolved
per message from the producer-side headers already on it (orgName/
productType, stamped there by blueprints/producer/topic/adaptor/main.py).

There is no separate schema_mapper stage any more — the extractor absorbed
its header-enrichment + dynamic target-topic-resolution logic entirely
(mapperTopicName relay removed), the same way
blueprints/producer/topic/adaptor/main.py absorbed its own schema mapper.

Kubernetes Environment Variables:
    SCHEDULER_BACKEND=kafka-trigger
    PRODUCT_NAME=consumer-topic
"""

from __future__ import annotations

import os
import re
import signal
import sys
import time
from datetime import UTC, datetime

from confluent_kafka import Consumer, KafkaError, KafkaException, Producer
from opentelemetry import context as _otel_context
from dotenv import load_dotenv


from dpn_observability_sdk.otel_logger import OtelLogger as Logging
from dpn_observability_sdk.otel_tracer import OtelTracer
from dpn_observability_sdk.otel_metrics import OtelMetrics
from dpn_observability_sdk.otel_instrumentation import traced, timed_metric
from dpn_observability_sdk.heartbeat import HeartbeatLogger
from utils.kafka_security import build_kafka_client_config
from utils.topic_utils import TopicResolver, KafkaTopicManager
from utils.pipeline_context import PipelineContext
from utils.scheduler_backend import get_backend, run_once
from utils.step_logger import StepLogger

load_dotenv()

# Timeout used when running in triggered mode
_TIMEOUT = int(os.getenv("TOPIC_TASK_TIMEOUT_SECS", "600"))


def _sanitize(value: str | None, default: str = "unknown") -> str:
    """
    Sanitize string for safe Kafka topic naming.
    """
    cleaned = re.sub(r"[^A-Za-z0-9]", "", value or default).lower()
    return cleaned or default


class TopicForwarder:
    """
    Kafka Topic Forwarder.

    Responsible for:
    - Consuming messages from the source topic
    - Resolving each message's own target topic from its headers
    - Enriching headers and producing straight to that target topic
    - Handling Kafka producer/consumer lifecycle
    - Logging and step tracking
    """

    SERVICE_NAME = "consumer-topic-extractor"

    def __init__(self) -> None:
        """Initialize Kafka producer, topic configuration, and logging."""

        # Same identifier used for the HeartbeatLogger and the scheduler
        # backend's component_name, so every log this class emits directly
        # (i.e. not via StepLogger/ctx) still carries a matching component.name.
        self.component_name = self.SERVICE_NAME

        # Initialize OpenTelemetry
        self.tracer = OtelTracer.initialize(
            service_name="consumer-topic-extractor",
            service_version="2.0.0"
        )
        self.meter = OtelMetrics.initialize(
            service_name="consumer-topic-extractor",
            service_version="2.0.0"
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

        self.logger = Logging().create_logger()

        # Kafka configuration from environment
        self.bootstrap = os.getenv("bootstrapServer", "")
        self.src_topic = os.getenv("srcTopicName", "")
        self.group_id = os.getenv("srcGroupId", self.SERVICE_NAME)
        self.retry_delay = int(os.getenv("consumerRetryDelaySecs", "5"))

        # Resolved per message from headers — see _resolve_target(). Set
        # to "" whenever a message's metadata is invalid, which skips the
        # produce for that message entirely (no dpn-consumer-unknown-*
        # topics are ever created).
        self.target_topic = ""

        self.tr = TopicResolver()
        self.km = KafkaTopicManager(
            bootstrap_server=self.bootstrap,
            logger=self.logger
        )
        self.km.ensure_exists(self.src_topic)

        # Kafka producer initialization
        self.producer = Producer(build_kafka_client_config(self.bootstrap))

        self.logger.info(
            "extractor initialised",
            extra={
                "srcTopicName": self.src_topic,
                "srcGroupId": self.group_id,
                "PRODUCT_NAME": os.getenv("PRODUCT_NAME"),
                "SCHEDULER_BACKEND": os.getenv("SCHEDULER_BACKEND"),
                "TOPIC_TASK_TIMEOUT_SECS": os.getenv("TOPIC_TASK_TIMEOUT_SECS"),
                "component.name": self.component_name,
            }
        )

    def _resolve_target(self, headers: dict) -> None:
        """
        Resolve this message's target topic from its headers.

        Skips messages with missing/invalid org_name or product_type
        (never creates a dpn-consumer-unknown-* topic).
        """
        headers = {k.lower(): v for k, v in headers.items()}

        org_name = _sanitize(headers.get("org_name") or headers.get("orgname"))
        product_type = _sanitize(headers.get("product_type") or headers.get("producttype"))

        if org_name == "unknown" or product_type == "unknown":
            self.logger.warning(
                "Skipping message due to invalid metadata",
                extra={
                    "org_name": org_name,
                    "product_type": product_type,
                    "component.name": self.component_name,
                },
            )
            self.target_topic = ""
            return

        topic_name = f"dpn-consumer-{org_name}-{product_type}-target"
        resolved = self.tr.resolve(topic_name, self.src_topic, "target")

        if resolved != self.target_topic:
            self.target_topic = resolved
            self.logger.info(
                f"Ensuring topic exists: {self.target_topic}",
                extra={"component.name": self.component_name},
            )
            try:
                self.km.ensure_exists(self.target_topic)
            except Exception as e:
                self.logger.error(
                    f"Topic creation failed: {e}",
                    extra={"component.name": self.component_name},
                )

    def _on_delivery(self, err, msg) -> None:
        """
        Kafka delivery callback.

        Args:
            err: Delivery error, if any.
            msg: Kafka message metadata.
        """
        if err:
            self.logger.error(
                "delivery failed",
                extra={"error": str(err), "component.name": self.component_name}
            )

    @traced(span_name="forward_message")
    def _forward(self, msg, ctx: PipelineContext, step_log: StepLogger) -> None:
        """
        Resolve, enrich and forward a single Kafka message to its target topic.

        Args:
            msg: Kafka message received from source topic.
            ctx: Pipeline execution context.
            step_log: StepLogger instance for structured logging.
        """
        # Empty context creates a new root span → new trace_id per message.
        # Without this, all messages in the same consumer_window share one trace_id.
        with self.tracer.start_as_current_span(
            "forward_message", context=_otel_context.Context()
        ) as span:
            operation = "forward"
            start_time = datetime.now(UTC)

            carrier = {
                k: v.decode() if isinstance(v, bytes) else v
                for k, v in (msg.headers() or [])
            }

            span.set_attribute("kafka.source_topic", msg.topic())
            span.set_attribute("kafka.partition", msg.partition())
            span.set_attribute("kafka.offset", msg.offset())
            span.set_attribute("message.key", str(msg.key()) if msg.key() else "null")

            step_log.step_start(
                ctx,
                operation,
                extra={"partition": msg.partition(), "offset": msg.offset()}
            )

            try:
                self._resolve_target(carrier)

                if not self.target_topic:
                    self.logger.warning(
                        "Skipping produce (no valid target topic)",
                        extra={"component.name": self.component_name},
                    )
                    span.set_attribute("forward.status", "skipped")
                    step_log.step_end(ctx, operation, extra={"status": "skipped"})
                    return

                span.set_attribute("kafka.destination_topic", self.target_topic)

                # Same case-insensitive lookup _resolve_target() uses above —
                # carrier's own keys are whatever case the producer sent
                # (orgName/productType, the actual convention stamped by
                # blueprints/producer/topic/adaptor/main.py), so reading the
                # raw dict here would silently fall through to "unknown"
                # even though resolution just succeeded against the same data.
                lower_carrier = {k.lower(): v for k, v in carrier.items()}
                enriched_headers = list(msg.headers() or []) + [
                    ("orgName", _sanitize(lower_carrier.get("org_name") or lower_carrier.get("orgname")).encode()),
                    ("productType", _sanitize(lower_carrier.get("product_type") or lower_carrier.get("producttype")).encode()),
                    ("processedAt", datetime.now(UTC).isoformat().encode()),
                    ("offset", str(msg.offset()).encode()),
                ]

                self.producer.produce(
                    topic=self.target_topic,
                    value=msg.value(),
                    key=msg.key(),
                    headers=enriched_headers,
                    callback=self._on_delivery
                )

                # Trigger delivery callback
                self.producer.poll(0)

                duration_ms = int(
                    (datetime.now(UTC) - start_time).total_seconds() * 1000
                )

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
                        "duration_ms": duration_ms
                    }
                )

            except Exception as exc:
                self.messages_forwarded.add(1, {
                    "source_topic": msg.topic(),
                    "destination_topic": self.target_topic or "unknown",
                    "status": "error"
                })

                span.set_attribute("forward.status", "error")
                span.set_attribute("error.type", type(exc).__name__)
                span.record_exception(exc)

                step_log.step_failed(ctx, operation, exc=exc)
                raise

    @traced(span_name="consumer_window")
    @timed_metric("consumer_window_duration", "Duration of consumer window")
    def run_window(
        self,
        ctx: PipelineContext,
        step_log: StepLogger,
        stop_after: int | None = None
    ) -> None:
        """
        Run the consumer loop for a specified duration or indefinitely.

        Args:
            ctx: Pipeline execution context.
            step_log: StepLogger instance.
            stop_after: Optional time limit (in seconds).
        """
        with self.tracer.start_as_current_span("consumer_window") as span:
            span.set_attribute("kafka.source_topic", self.src_topic)
            span.set_attribute("kafka.group_id", self.group_id)
            span.set_attribute("window.timeout_seconds", stop_after or 0)

            messages_processed = 0

            # Calculate deadline if timeout is specified
            deadline = time.monotonic() + stop_after if stop_after else None

            while True:
                # Exit if deadline reached
                if deadline and time.monotonic() >= deadline:
                    break

                # Create Kafka consumer
                consumer = Consumer({
                    **build_kafka_client_config(self.bootstrap),
                    "group.id": self.group_id,
                    "auto.offset.reset": "earliest",
                    "enable.auto.commit": True,
                })

                consumer.subscribe([self.src_topic])

                try:
                    while True:
                        if deadline and time.monotonic() >= deadline:
                            break

                        msg = consumer.poll(timeout=1.0)

                        # No message received
                        if msg is None:
                            continue

                        # Handle Kafka error messages
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

                        # Resolve, enrich and forward the message
                        self._forward(msg, ctx, step_log)

                except KafkaException as kafka_error:
                    span.set_attribute("error.type", "kafka_error")
                    span.record_exception(kafka_error)
                    self.logger.error(
                        "kafka error",
                        extra={
                            "error": str(kafka_error),
                            "retry": self.retry_delay,
                            "component.name": self.component_name,
                        }
                    )

                except Exception as unexpected_error:
                    span.set_attribute("error.type", "unexpected_error")
                    span.record_exception(unexpected_error)
                    self.logger.error(
                        "unexpected",
                        extra={
                            "error": str(unexpected_error),
                            "component.name": self.component_name,
                        },
                        exc_info=True
                    )

                finally:
                    # Ensure all buffered messages are sent
                    self.producer.flush()

                    # Close consumer cleanly
                    consumer.close()

                # Exit if deadline reached after cleanup
                if deadline and time.monotonic() >= deadline:
                    break

                # Retry delay before restarting consumer loop
                time.sleep(self.retry_delay)

            span.set_attribute("messages.processed", messages_processed)
            span.set_attribute("window.status", "completed")


@traced(span_name="consumer_topic_extractor_pipeline")
@timed_metric("pipeline_total_duration", "Total pipeline execution time")
def run(ctx: PipelineContext) -> None:
    """
    Entry point for the topic extractor pipeline stage.

    Args:
        ctx: Pipeline execution context.
    """
    tracer = OtelTracer.get_tracer(__name__)

    with tracer.start_as_current_span("extractor_pipeline") as span:
        span.set_attribute("pipeline.type", "consumer-topic-extractor")
        span.set_attribute("pipeline.triggered_by", ctx.triggered_by)
        span.set_attribute("pipeline.run_id", ctx.run_id)

        forwarder = TopicForwarder()
        step_log = StepLogger(forwarder.logger)

        # Log pipeline start banner
        step_log.pipeline_banner(
            ctx,
            service_name="consumer-topic-extractor",
            config_summary={
                "srcTopicName": forwarder.src_topic,
                "SCHEDULER_BACKEND": os.getenv("SCHEDULER_BACKEND", "standalone"),
                "PRODUCT_NAME": os.getenv("PRODUCT_NAME", "consumer-topic"),
            },
        )

        operation = "extractor_window"

        # Start main extraction step
        step_log.step_start(ctx, operation)

        # Apply timeout for every bounded trigger source — "manual" (the
        # self-service "Run Now" button, see utils/scheduler_backend.py's
        # run_once()) must stay bounded here too, or a manual run blocks
        # forever consuming instead of returning after one window.
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
    # Execute pipeline using configured scheduler backend

    temp_forwarder = TopicForwarder()

    if "--run-once" in sys.argv:
        # Self-service "Run Now" button (see service/podman_ops.py /
        # k8s_ops.py run_once()): execs this inside the already-running
        # container to force a single window on demand, bypassing the
        # scheduler entirely — no heartbeat needed for a process this
        # short-lived.
        run_once(
            run,
            pipeline_stage="extractor",
            pipeline_type="topic",
            pipeline_role="consumer",
            component_name=temp_forwarder.component_name,
        )
    else:
        heartbeat = HeartbeatLogger(
            logger=temp_forwarder.logger,
            component_name=temp_forwarder.component_name,
            metadata={
                "source_topic": temp_forwarder.src_topic,
                "scheduler_backend": os.getenv("SCHEDULER_BACKEND", "standalone"),
            },
        )
        heartbeat.start()

        # Pod deletion/rolling-update sends SIGTERM before the pod is killed —
        # catching it emits the SDK's own "heartbeat.stopped" event (see
        # dpn_observability_sdk.heartbeat.HeartbeatLogger.stop) within the grace
        # period, so the last-known-alive signal is followed by an explicit stop
        # rather than just silence.
        def _handle_sigterm(signum, frame):
            heartbeat.stop()
            sys.exit(0)

        signal.signal(signal.SIGTERM, _handle_sigterm)

        get_backend().execute(
            run,
            pipeline_stage="extractor",
            pipeline_type="topic",
            pipeline_role="consumer",
            component_name=temp_forwarder.component_name,
        )
