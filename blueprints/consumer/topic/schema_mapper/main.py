"""
Kafka Topic Schema Mapper (Consumer)

This service:
- Consumes messages from mapper topic
- Extracts metadata from headers
- Dynamically resolves target topic
- Ensures valid topics are created
- Skips invalid messages (no 'unknown' topics)

Behavior:
---------
✅ Only creates topics when ALL fields are valid
❌ Does NOT create dpn-unknown-* topics
✅ Supports camelCase + snake_case headers
"""

from __future__ import annotations

import os
import re
import time
from datetime import UTC, datetime

from confluent_kafka import Consumer, KafkaError, KafkaException, Producer
from opentelemetry import context as _otel_context, propagate as _otel_propagate
from dotenv import load_dotenv

from dpn_observability_sdk.otel_logger import OtelLogger as Logging
from dpn_observability_sdk.otel_tracer import OtelTracer
from dpn_observability_sdk.otel_metrics import OtelMetrics
from dpn_observability_sdk.otel_instrumentation import traced, timed_metric
from dpn_observability_sdk.heartbeat import HeartbeatLogger
from utils.topic_utils import TopicResolver, KafkaTopicManager
from utils.pipeline_context import PipelineContext
from utils.scheduler_backend import get_backend
from utils.step_logger import StepLogger


load_dotenv()

_TIMEOUT = int(os.getenv("TOPIC_TASK_TIMEOUT_SECS", "600"))


def _sanitize(value: str | None, default: str = "unknown") -> str:
    """
    Sanitize string for safe Kafka topic naming.
    """
    cleaned = re.sub(r"[^A-Za-z0-9]", "", value or default).lower()
    return cleaned or default


class TopicSchemaMapper:
    """
    Kafka Consumer Schema Mapper

    Responsibilities:
    - Consume messages
    - Extract and validate headers
    - Dynamically resolve target topic
    - Produce enriched messages
    """

    def __init__(self) -> None:
        # Same identifier used for the HeartbeatLogger and the scheduler
        # backend's component_name, so every log this class emits directly
        # (i.e. not via StepLogger/ctx) still carries a matching component.name.
        self.component_name = "consumer-topic-mapper"

        # Initialize OpenTelemetry
        self.tracer = OtelTracer.initialize(
            service_name="consumer-topic-mapper",
            service_version="1.0.0"
        )
        self.meter = OtelMetrics.initialize(
            service_name="consumer-topic-mapper",
            service_version="1.0.0"
        )

        # Create metrics
        self.messages_processed = self.meter.create_counter(
            name="messages_processed_total",
            description="Total messages processed by schema mapper",
            unit="1",
        )

        self.process_duration = self.meter.create_histogram(
            name="message_process_duration",
            description="Message processing duration",
            unit="ms",
        )

        self.messages_consumed = self.meter.create_counter(
            name="messages_consumed_total",
            description="Total messages consumed from mapper topic",
            unit="1",
        )

        # Initialize heartbeat logger (started in __main__)
        self.heartbeat: HeartbeatLogger | None = None

        self.logger = Logging().create_logger()

        # Kafka config
        self.bootstrap = os.getenv("bootstrapServer", "")
        self.src_topic = os.getenv("mapperTopicName", "")
        self.group_id = os.getenv("mapperGroupId", "consumer_topic_mapper")
        self.retry_delay = int(os.getenv("consumerRetryDelaySecs", "5"))

        # Default metadata (used only if headers missing, but NOT for topic creation)
        self.schema_type = _sanitize(os.getenv("schemaType"))
        self.org_name = _sanitize(os.getenv("orgName"))
        self.product_type = _sanitize(os.getenv("productType"))

        # Topic resolver
        self.tr = TopicResolver()

        # Resolve mapper topic
        self.mapper_topic = self.tr.resolve(
            os.getenv("mapperTopicName"),
            self.src_topic,
            "trfm",
        )

        self.target_topic = ""

        # Topic manager
        self.km = KafkaTopicManager(
            bootstrap_server=self.bootstrap,
            logger=self.logger,
        )

        # Ensure mapper topic exists
        self.km.ensure_exists(self.mapper_topic)

        # Kafka producer
        self.producer = Producer({"bootstrap.servers": self.bootstrap})


    def _resolve_target(self, headers: dict) -> None:
        """
        Resolve target topic ONLY if metadata is valid.
        Prevents creating 'unknown' topics.
        """

        headers = {k.lower(): v for k, v in headers.items()}

        # Support both naming styles
        schema_type = _sanitize(
            headers.get("schema_type") or headers.get("schematype")
        )
        org_name = _sanitize(
            headers.get("org_name") or headers.get("orgname")
        )
        product_type = _sanitize(
            headers.get("product_type") or headers.get("producttype")
        )

        # 🚨 CRITICAL: Skip invalid metadata
        if (
            schema_type == "unknown"
            or org_name == "unknown"
            or product_type == "unknown"
        ):
            self.logger.warning(
                "Skipping message due to invalid metadata",
                extra={
                    "schema_type": schema_type,
                    "org_name": org_name,
                    "product_type": product_type,
                    "component.name": self.component_name,
                },
            )
            self.target_topic = ""  # ensures no produce
            return

        topic_name = (
            f"dpn-consumer-{org_name}-{schema_type}-{product_type}-target"
        )

        resolved = self.tr.resolve(
            topic_name,
            self.src_topic,
            "target",
        )

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
        """Kafka delivery callback"""
        if err:
            self.logger.error(
                "delivery failed",
                extra={"error": str(err), "component.name": self.component_name},
            )


    @traced(span_name="process_message")
    def _process(self, msg, ctx: PipelineContext, step_log: StepLogger) -> None:
        """
        Process a single Kafka message.
        """
        # Restore the extractor's trace context from Kafka headers so this span
        # becomes a child of the extractor span — same trace_id end-to-end.
        _carrier = {
            k: v.decode() if isinstance(v, bytes) else v
            for k, v in (msg.headers() or [])
        }
        _remote_ctx = _otel_propagate.extract(_carrier)
        _token = _otel_context.attach(_remote_ctx)

        try:
            with self.tracer.start_as_current_span("process_message") as span:
                operation = "mapper_msg"
                start_time = datetime.now(UTC)

                span.set_attribute("kafka.source_topic", msg.topic())
                span.set_attribute("kafka.partition", msg.partition())
                span.set_attribute("kafka.offset", msg.offset())
                span.set_attribute("message.key", str(msg.key()) if msg.key() else "null")

                step_log.step_start(ctx, operation, extra={"partition": msg.partition()})

                try:
                    # Decode headers (reuse carrier already decoded above)
                    headers = _carrier

                    print("HEADERS:", headers)

                    # Resolve topic
                    self._resolve_target(headers)

                    # Skip if invalid
                    if not self.target_topic:
                        self.logger.warning(
                            "Skipping produce (no valid target topic)",
                            extra={"component.name": self.component_name},
                        )
                        span.set_attribute("process.status", "skipped")
                        return

                    print("TARGET:", self.target_topic)

                    span.set_attribute("kafka.destination_topic", self.target_topic)

                    enriched_headers = [
                        ("schemaType", _sanitize(headers.get("schema_type") or headers.get("schematype")).encode()),
                        ("orgName", _sanitize(headers.get("org_name") or headers.get("orgname")).encode()),
                        ("productType", _sanitize(headers.get("product_type") or headers.get("producttype")).encode()),
                        ("processedAt", datetime.now(UTC).isoformat().encode()),
                        ("offset", str(msg.offset())),
                    ]

                    self.producer.produce(
                        topic=self.target_topic,
                        value=msg.value(),
                        key=msg.key(),
                        headers=enriched_headers,
                        callback=self._on_delivery,
                    )

                    self.producer.poll(0)

                    duration_ms = int(
                        (datetime.now(UTC) - start_time).total_seconds() * 1000
                    )

                    # Record metrics
                    self.messages_processed.add(1, {
                        "source_topic": msg.topic(),
                        "destination_topic": self.target_topic,
                        "status": "success",
                    })

                    self.process_duration.record(duration_ms, {
                        "source_topic": msg.topic(),
                        "destination_topic": self.target_topic,
                    })

                    span.set_attribute("process.status", "success")
                    span.set_attribute("process.duration_ms", duration_ms)

                    step_log.step_end(
                        ctx,
                        operation,
                        extra={
                            "dst": self.target_topic,
                            "duration_ms": duration_ms,
                        },
                    )

                except Exception as exc:
                    self.messages_processed.add(1, {
                        "source_topic": msg.topic(),
                        "destination_topic": self.target_topic or "unknown",
                        "status": "error",
                    })

                    span.set_attribute("process.status", "error")
                    span.set_attribute("error.type", type(exc).__name__)
                    span.record_exception(exc)

                    step_log.step_failed(ctx, operation, exc=exc)
                    raise
        finally:
            _otel_context.detach(_token)


    @traced(span_name="consumer_window")
    @timed_metric("consumer_window_duration", "Duration of consumer window")
    def run_window(self, ctx, step_log, stop_after=None):
        """
        Main Kafka polling loop.
        """
        with self.tracer.start_as_current_span("consumer_window") as span:
            span.set_attribute("kafka.source_topic", self.mapper_topic)
            span.set_attribute("kafka.group_id", self.group_id)
            span.set_attribute("window.timeout_seconds", stop_after or 0)

            messages_processed = 0
            deadline = time.monotonic() + stop_after if stop_after else None

            while True:
                if deadline and time.monotonic() >= deadline:
                    break

                consumer = Consumer({
                    "bootstrap.servers": self.bootstrap,
                    "group.id": self.group_id,
                    "auto.offset.reset": "earliest",
                    "enable.auto.commit": True,
                })

                consumer.subscribe([self.mapper_topic])

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

                        self.messages_consumed.add(1, {
                            "source_topic": self.mapper_topic,
                            "partition": str(msg.partition()),
                        })

                        messages_processed += 1
                        self._process(msg, ctx, step_log)

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
                        exc_info=True,
                    )

                finally:
                    self.producer.flush()
                    consumer.close()

                if deadline and time.monotonic() >= deadline:
                    break

                time.sleep(self.retry_delay)

            span.set_attribute("messages.processed", messages_processed)
            span.set_attribute("window.status", "completed")


@traced(span_name="consumer_topic_mapper_pipeline")
@timed_metric("pipeline_total_duration", "Total pipeline execution time")
def run(ctx: PipelineContext) -> None:
    """Pipeline entry point."""
    tracer = OtelTracer.get_tracer(__name__)

    with tracer.start_as_current_span("mapper_pipeline") as span:
        span.set_attribute("pipeline.type", "consumer-topic-mapper")
        span.set_attribute("pipeline.triggered_by", ctx.triggered_by)
        span.set_attribute("pipeline.run_id", ctx.run_id)

        mapper = TopicSchemaMapper()
        step_log = StepLogger(mapper.logger)

        step_log.pipeline_banner(
            ctx,
            service_name="consumer-schema-mapper",
            config_summary={"topic": mapper.mapper_topic},
        )

        operation = "mapper_window"
        step_log.step_start(ctx, operation)

        timeout = _TIMEOUT if ctx.triggered_by in ["kafka-trigger", "interval"] else None
        span.set_attribute("pipeline.timeout_seconds", timeout or 0)

        try:
            mapper.run_window(ctx, step_log, stop_after=timeout)
            step_log.step_end(ctx, operation)
            span.set_attribute("pipeline.status", "success")

        except Exception as exc:
            span.set_attribute("pipeline.status", "error")
            span.set_attribute("error.type", type(exc).__name__)
            span.record_exception(exc)
            step_log.step_failed(ctx, operation, exc=exc)
            raise


if __name__ == "__main__":
    temp_mapper = TopicSchemaMapper()
    heartbeat = HeartbeatLogger(
        logger=temp_mapper.logger,
        component_name=temp_mapper.component_name,
        metadata={
            "source_topic": temp_mapper.src_topic,
            "mapper_topic": temp_mapper.mapper_topic,
            "scheduler_backend": os.getenv("SCHEDULER_BACKEND", "standalone"),
        },
    )
    heartbeat.start()
    get_backend().execute(
        run,
        pipeline_stage="schema_mapper",
        pipeline_type="topic",
        pipeline_role="consumer",
        component_name=temp_mapper.component_name,
    )

# {"product_type":"eqbdpggas","processed_at":"2026-06-02T14:04:06.603385+00:00","org_name":"neso","schema_type":"eqbd"}