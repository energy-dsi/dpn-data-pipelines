# Copyright DSI Project — Apache 2.0
# v1.0.0 Initial | v1.1.0 Kafka trigger + StepLogger 2026-05-26
# +---------+----------------------------------------------------------+---------------+-------------+
# | Version | Description                                              | Change Owner  | Change Date |
# +---------+----------------------------------------------------------+---------------+-------------+
# | 1.0.0   | Initial version                                          | DSI Team      | 2026-05-01  |
# | 1.1.0   | Kafka Trigger and StepLogger Integration                 | DSI Team      | 2026-05-26  |
# | 1.2.0   | OTEL Collector Integration                               | DSI Team      | 2026-06-26  |
# +---------+----------------------------------------------------------+---------------+-------------+

"""
/home/claude/dpn-complete/consumer/file/schema_mapper/main.py

Schema Mapper (file pipeline)

Modes:
- Drain mode (Kafka-trigger)
- Continuous consumer (standalone / interval)

K8s Configuration:
- SCHEDULER_BACKEND=kafka-trigger
- PRODUCT_NAME=consumer-file
"""

from __future__ import annotations

import base64
import json
import os
import time
from datetime import UTC, datetime

from confluent_kafka import Consumer, KafkaError, KafkaException
from azure.core.exceptions import AzureError, HttpResponseError
from botocore.exceptions import BotoCoreError, ClientError
from dotenv import load_dotenv
from google.api_core.exceptions import GoogleAPIError
from opentelemetry import context as _otel_context, propagate as _otel_propagate

from utils.config_validator import validate_cloud_config, validate_kafka_config
from utils.data_transection import DataTransection
from utils.exception_handler import HandleExceptions
from utils.kafka_transection import KafkaTransection
from dpn_observability_sdk.otel_logger import OtelLogger as Logging
from dpn_observability_sdk.otel_tracer import OtelTracer
from dpn_observability_sdk.otel_metrics import OtelMetrics
from dpn_observability_sdk.otel_instrumentation import traced, timed_metric
from dpn_observability_sdk.heartbeat import HeartbeatLogger
from utils.pipeline_context import PipelineContext
from utils.scheduler_backend import get_backend
from utils.step_logger import StepLogger
from utils.topic_utils import TopicResolver, KafkaTopicManager

# Load environment variables
load_dotenv()

# Idle timeout for drain mode
_DRAIN_IDLE = int(os.getenv("DRAIN_IDLE_SECS", "15"))


class SchemaMapper:
    """
    Handles schema mapping workflow:
    1. Reads file from source storage
    2. Validates schema (stub)
    3. Moves file to target storage
    4. Publishes event to Kafka
    """

    def __init__(self):
        # Same identifier used for the HeartbeatLogger and the scheduler
        # backend's component_name, so every log this class emits directly
        # (i.e. not via StepLogger/ctx) still carries a matching component.name.
        self.component_name = "consumer-file-schema-mapper"

        # Initialize OpenTelemetry
        self.tracer = OtelTracer.initialize(
            service_name="consumer-file-schema-mapper",
            service_version="1.0.0"
        )
        self.meter = OtelMetrics.initialize(
            service_name="consumer-file-schema-mapper",
            service_version="1.0.0"
        )

        # Create metrics
        self.messages_processed = self.meter.create_counter(
            name="messages_processed_total",
            description="Total messages processed by file schema mapper",
            unit="1",
        )

        self.process_duration = self.meter.create_histogram(
            name="message_process_duration",
            description="Message processing duration",
            unit="ms",
        )

        self.files_moved = self.meter.create_counter(
            name="files_moved_total",
            description="Total files moved by schema mapper",
            unit="1",
        )

        # Cloud & Kafka configuration
        self.cloud_provider = os.getenv("cloudProviderType", "azure")
        self.target_topic = os.getenv("targetTopicName", "")
        self.source_topic = os.getenv("mapperTopicName", "")
        self.bootstrap = os.getenv("bootstrapServer", "")

        # Storage configuration
        self.src_conn = base64.b64decode(os.getenv("mapperConnectionString", "")).decode()
        self.tgt_conn = base64.b64decode(os.getenv("targetConnectionString", "")).decode()
        self.src_container = os.getenv("mapperContainerName", "")
        self.tgt_container = os.getenv("targetContainerName", "")

        # Naming configuration
        self.org_name = os.getenv("orgName", "")
        self.schema_type = os.getenv("schemaType", "")
        self.file_name = None

        # AWS configuration (optional)
        self.aws_endpoint = os.getenv("AWS_ENDPOINT_URL") or None
        self.aws_key_id = base64.b64decode(os.getenv("AWS_ACCESS_KEY_ID", "")).decode()
        self.aws_secret = base64.b64decode(os.getenv("AWS_SECRET_ACCESS_KEY", "")).decode()
        self.aws_region = os.getenv("AWS_REGION", "us-east-1")

        # Initialize heartbeat logger (started in __main__)
        self.heartbeat: HeartbeatLogger | None = None

        # Logger
        self.logger = Logging().create_logger()

        # Validate configurations
        validate_cloud_config(
            cloud_provider=self.cloud_provider,
            azure_fields=["mapperConnectionString", "targetConnectionString"],
            logger=self.logger,
        )
        validate_kafka_config(logger=self.logger)

        # Resolve mapper topic name
        topic_resolver = TopicResolver()
        self.target_topic = topic_resolver.resolve(
            self.target_topic,
            self.target_topic,
            "trfm"
        )

        # Ensure mapper topic exists
        topic_manager = KafkaTopicManager(
            bootstrap_server=self.bootstrap,
            logger=self.logger
        )

        topic_manager.ensure_exists(self.target_topic)

        # Storage transaction handler
        self.data_trans = DataTransection(
            source_azure_conn_str=self.src_conn,
            source_container_name=self.src_container,
            target_container_name=self.tgt_container,
            source_blob_name=None,
            target_blob_name=None,
            target_azure_conn_str=self.tgt_conn,
            aws_endpoint_url=self.aws_endpoint,
            aws_access_key_id=self.aws_key_id,
            aws_secret_access_key=self.aws_secret,
            aws_region=self.aws_region,
            logger=self.logger,
            component_name=self.component_name,
        )

        # Kafka transaction handler
        self.kafka_trans = KafkaTransection(
            bootstrap_server=self.bootstrap,
            logger=self.logger,
            component_name=self.component_name,
        )

    def read_file(self, file):
        self.data_trans.source_blob_name = file
        return self.data_trans.data_read(cloud_vendor=self.cloud_provider)

    def validate(self, data):
        self.logger.info(
            "schema validation (stub)",
            extra={
                "schema_type": self.schema_type,
                "component.name": self.component_name,
            }
        )
        return True

    def move_file(self, file):
        self.file_name = file

        response = self.data_trans.file_copy(
            cloud_vendor=self.cloud_provider,
            file_name=file,
            dest_file_name=self.file_name
        )

        return response.copied

    def publish_event(self, moved):
        if moved:
            self.kafka_trans.send_message(
                target_topic=self.target_topic,
                message={
                    "sourceType": "S3"
                    if self.cloud_provider.upper() == "AWS"
                    else self.cloud_provider.upper(),
                    "storageContainer": self.tgt_container,
                    "path": self.file_name,
                },
            )

    @traced(span_name="process_message")
    def _process(self, msg, ctx: PipelineContext, step_log: StepLogger) -> None:
        # Restore trace context from Kafka headers so this span is a child of the extractor span
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

                payload = json.loads(msg.value().decode())
                file_name = payload.get("path", "")

                span.set_attribute("file.name", file_name or "unknown")

                if not file_name:
                    span.set_attribute("process.status", "skipped")
                    return

                step_log.step_start(ctx, operation, extra={"file": file_name})

                exc_h = HandleExceptions()

                try:
                    data = self.read_file(file_name)

                    if self.validate(data):
                        moved = self.move_file(file_name)
                        self.publish_event(moved)

                    duration_ms = int(
                        (datetime.now(UTC) - start_time).total_seconds() * 1000
                    )

                    self.messages_processed.add(1, {
                        "source_topic": msg.topic(),
                        "file": file_name,
                        "status": "success",
                    })

                    self.files_moved.add(1, {
                        "cloud_provider": self.cloud_provider,
                        "status": "success",
                    })

                    self.process_duration.record(duration_ms, {
                        "source_topic": msg.topic(),
                    })

                    span.set_attribute("process.status", "success")
                    span.set_attribute("process.duration_ms", duration_ms)

                    step_log.step_end(
                        ctx,
                        operation,
                        extra={"file": file_name, "duration_ms": duration_ms},
                    )

                except (HttpResponseError, AzureError) as e:
                    self.messages_processed.add(1, {"source_topic": msg.topic(), "status": "error"})
                    span.set_attribute("process.status", "error")
                    span.set_attribute("error.type", type(e).__name__)
                    span.record_exception(e)
                    step_log.step_failed(ctx, operation, exc=e)
                    exc_h.handle_storage_exception(e, "Azure")

                except (ClientError, BotoCoreError) as e:
                    self.messages_processed.add(1, {"source_topic": msg.topic(), "status": "error"})
                    span.set_attribute("process.status", "error")
                    span.set_attribute("error.type", type(e).__name__)
                    span.record_exception(e)
                    step_log.step_failed(ctx, operation, exc=e)
                    exc_h.handle_storage_exception(e, "AWS S3")

                except GoogleAPIError as e:
                    self.messages_processed.add(1, {"source_topic": msg.topic(), "status": "error"})
                    span.set_attribute("process.status", "error")
                    span.set_attribute("error.type", type(e).__name__)
                    span.record_exception(e)
                    step_log.step_failed(ctx, operation, exc=e)
                    exc_h.handle_storage_exception(e, "GCP")

                except Exception as e:
                    self.messages_processed.add(1, {"source_topic": msg.topic(), "status": "error"})
                    span.set_attribute("process.status", "error")
                    span.set_attribute("error.type", type(e).__name__)
                    span.record_exception(e)
                    step_log.step_failed(ctx, operation, exc=e)
                    exc_h.handle_storage_exception(e, "")

        finally:
            _otel_context.detach(_token)


@traced(span_name="consumer_file_mapper_pipeline")
@timed_metric("pipeline_total_duration", "Total pipeline execution time")
def run(ctx: PipelineContext) -> None:
    """
    Entry point for pipeline execution.
    Decides execution mode based on trigger.
    """
    tracer = OtelTracer.get_tracer(__name__)

    with tracer.start_as_current_span("mapper_pipeline") as span:
        span.set_attribute("pipeline.type", "consumer-file-mapper")
        span.set_attribute("pipeline.triggered_by", ctx.triggered_by)
        span.set_attribute("pipeline.run_id", ctx.run_id)

        mapper = SchemaMapper()
        step_log = StepLogger(mapper.logger)

        step_log.pipeline_banner(
            ctx,
            service_name="consumer-file-schema-mapper",
            config_summary={
                "sourceKafkaTopic": mapper.source_topic,
                "targetTopic": mapper.target_topic,
                "SCHEDULER_BACKEND": os.getenv("SCHEDULER_BACKEND", "standalone"),
                "PRODUCT_NAME": os.getenv("PRODUCT_NAME", "consumer-file"),
            },
        )

        operation = "mapper_window"
        step_log.step_start(ctx, operation)

        handler = lambda msg: mapper._process(msg, ctx, step_log)

        try:
            if ctx.triggered_by == "kafka-trigger":
                _drain(mapper, step_log, ctx, handler)
            else:
                _continuous(mapper, step_log, ctx, handler)

            step_log.step_end(ctx, operation)
            span.set_attribute("pipeline.status", "success")

        except Exception as exc:
            span.set_attribute("pipeline.status", "error")
            span.set_attribute("error.type", type(exc).__name__)
            span.record_exception(exc)
            step_log.step_failed(ctx, operation, exc=exc)
            raise


def _drain(mapper, step_log, ctx, handler):
    """
    Drain mode:
    Consumes messages until idle timeout is reached.
    """
    consumer = Consumer({
        "bootstrap.servers": mapper.bootstrap,
        "group.id": "consumer_file_mapper",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": True,
    })

    consumer.subscribe([mapper.source_topic])

    step_log.step_start(ctx, "drain_queue")

    processed = 0
    last_msg = time.monotonic()

    try:
        while True:
            msg = consumer.poll(timeout=1.0)

            if msg is None:
                if time.monotonic() - last_msg >= _DRAIN_IDLE:
                    break
                continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                raise KafkaException(msg.error())

            try:
                handler(msg)
                processed += 1
                last_msg = time.monotonic()
            except Exception:
                pass

    finally:
        consumer.close()

    step_log.step_end(ctx, "drain_queue", extra={"processed": processed})


def _continuous(mapper, step_log, ctx, handler):
    """
    Continuous consumption mode:
    Keeps consuming messages indefinitely with retry logic.
    """
    retry = int(os.getenv("consumerRetryDelaySecs", "5"))

    step_log.step_start(ctx, "consumer_loop")

    consumer = Consumer({
        "bootstrap.servers": mapper.bootstrap,
        "group.id": "consumer_file_mapper",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": True,
    })

    consumer.subscribe([mapper.source_topic])

    try:
        while True:
            try:
                msg = consumer.poll(1.0)

                if msg is None:
                    continue

                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    raise KafkaException(msg.error())

                handler(msg)

            except KafkaException as exc:
                step_log.step_failed(ctx, "consumer_loop", exc=exc)
                time.sleep(retry)
                step_log.step_start(ctx, "consumer_loop")

            except Exception as exc:
                step_log.step_failed(ctx, "consumer_loop", exc=exc)
                time.sleep(retry)
                step_log.step_start(ctx, "consumer_loop")

    finally:
        consumer.close()


if __name__ == "__main__":
    """
    Application entrypoint.
    Uses scheduler backend to execute pipeline.
    """
    temp_mapper = SchemaMapper()
    heartbeat = HeartbeatLogger(
        logger=temp_mapper.logger,
        component_name=temp_mapper.component_name,
        metadata={
            "source_topic": temp_mapper.source_topic,
            "target_topic": temp_mapper.target_topic,
            "scheduler_backend": os.getenv("SCHEDULER_BACKEND", "standalone"),
        },
    )
    heartbeat.start()
    get_backend().execute(
        run,
        pipeline_stage="schema_mapper",
        pipeline_type="file",
        pipeline_role="consumer",
        component_name=temp_mapper.component_name,
    )
