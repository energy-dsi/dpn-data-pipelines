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
# | 1.1.0   | Airflow Integration Release                              | DSI Team      | 2026-06-27  |
# | 1.2.0   | OTEL Collector Integration                               | DSI Team      | 2026-06-27  |
# +---------+----------------------------------------------------------+---------------+-------------+
"""
File Schema Mapper (Producer) - BP Natural Gas.

This module implements the producer file schema mapper responsible for
consuming file-ready events from a Kafka mapper topic, validating file
content, applying naming conventions, moving files to the target storage
container, and publishing downstream processing notifications.

The schema mapper supports scheduler-driven execution models,
OpenTelemetry observability, heartbeat monitoring, Kafka-triggered
execution, Airflow orchestration, and continuous event processing.

Features:
    * Kafka message consumption from mapper topics.
    * File validation and schema processing.
    * Deterministic file renaming strategy.
    * File transfer between mapper and target containers.
    * Kafka event publication for downstream processing.
    * OpenTelemetry tracing and metrics collection.
    * Heartbeat monitoring and operational logging.
    * Scheduler backend integration for orchestrated execution.
    * Multi-cloud support for Azure, AWS S3, and GCP.

Environment Variables:
    cloudProviderType: Cloud storage provider type.
    mapperTopicName: Source Kafka topic name.
    targetTopicName: Destination Kafka topic name.
    mapperConnectionString: Source storage connection string.
    mapperContainerName: Source storage container name.
    targetConnectionString: Target storage connection string.
    targetContainerName: Target storage container name.
    bootstrapServer: Kafka bootstrap server endpoint.
    orgName: Organization identifier.
    schemaType: Schema identifier.
    PRODUCT_NAME: Data product identifier.
    SCHEDULER_BACKEND: Execution backend configuration.
    DRAIN_IDLE_SECS: Idle timeout used by drain mode execution.

Pipeline Flow:
    Mapper Topic
        --> Schema Mapper
        --> File Validation
        --> File Rename
        --> Target Container
        --> Kafka Event
        --> Downstream Processing

Example:
    Source Topic:
        mapperTopicName

    Target Container:
        targetContainerName

    Target Kafka Topic:
        targetTopicName

File Location:
    Producer/file/<data_product>/schema_mapper/main.py
"""
from __future__ import annotations

import base64
import os
import time
from typing import Any
from datetime import UTC, datetime

from azure.core.exceptions import AzureError, HttpResponseError
from botocore.exceptions import BotoCoreError, ClientError
from dotenv import load_dotenv
from google.api_core.exceptions import GoogleAPIError
from confluent_kafka import Consumer, KafkaError, KafkaException
from opentelemetry import context as _otel_context, propagate as _otel_propagate

from utils.config_validator import validate_cloud_config, validate_kafka_config
from utils.data_transection import DataTransection
from utils.exception_handler import HandleExceptions
from utils.kafka_transection import KafkaTransection
from utils.otel_logger import OtelLogger as Logging
from utils.pipeline_context import PipelineContext
from utils.scheduler_backend import get_backend
from utils.step_logger import StepLogger
from dpn_observability_sdk.otel_logger import OtelLogger as Logging
from dpn_observability_sdk.otel_tracer import OtelTracer
from dpn_observability_sdk.otel_metrics import OtelMetrics
from dpn_observability_sdk.otel_instrumentation import traced, timed_metric
from dpn_observability_sdk.heartbeat import HeartbeatLogger

load_dotenv()

# How many consecutive seconds with no new Kafka messages before drain is done
_DRAIN_IDLE_SECS: int = int(os.getenv("DRAIN_IDLE_SECS", "15"))


# ---------------------------------------------------------------------------
# Business-logic class  (UNCHANGED from v1.0)
# ---------------------------------------------------------------------------

class SchemaMapper:
    """
    Producer-side schema mapper for BP Natural Gas.
    Consumes from mapperTopicName, validates, renames, routes to targetContainerName.
    """

    def __init__(self) -> None:

        # Initialize OpenTelemetry
        self.tracer = OtelTracer.initialize(
            service_name="producer-file-schema-mapper",
            service_version="1.0.0"
        )
        self.meter = OtelMetrics.initialize(
            service_name="producer-file-schema-mapper",
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

        # Initialize heartbeat logger (started in __main__)
        self.heartbeat: HeartbeatLogger | None = None

        self.cloud_provider: str      = os.getenv("cloudProviderType", "azure")
        self.target_kafka_topic: str  = os.getenv("targetTopicName", "")
        self.source_kafka_topic: str  = os.getenv("mapperTopicName", "")
        self.source_azure_conn_str: str = base64.b64decode(
            os.getenv("mapperConnectionString", "")
        ).decode("utf-8")
        self.source_container_name: str = os.getenv("mapperContainerName", "")
        self.target_container_name: str = os.getenv("targetContainerName", "")
        self.bootstrap_server: str    = os.getenv("bootstrapServer", "")
        self.target_azure_conn_str: str = base64.b64decode(
            os.getenv("targetConnectionString", "")
        ).decode("utf-8")
        self.org_name: str   = os.getenv("orgName", "")
        self.schema_type: str = os.getenv("schemaType", "")
        self.file_name: str | None = None

        self.aws_endpoint_url: str | None = os.getenv("AWS_ENDPOINT_URL") or None
        self.aws_access_key_id = base64.b64decode(
            os.getenv("AWS_ACCESS_KEY_ID", "")
        ).decode("utf-8")
        self.aws_secret_access_key = base64.b64decode(
            os.getenv("AWS_SECRET_ACCESS_KEY", "")
        ).decode("utf-8")
        self.aws_region: str = os.getenv("AWS_REGION", "us-east-1")

        self.logger = Logging().create_logger()

        validate_cloud_config(
            cloud_provider=self.cloud_provider,
            azure_fields=["mapperConnectionString", "targetConnectionString"],
            logger=self.logger,
        )
        validate_kafka_config(logger=self.logger)

        self.data_trans = DataTransection(
            source_azure_conn_str=self.source_azure_conn_str,
            source_container_name=self.source_container_name,
            target_container_name=self.target_container_name,
            source_blob_name=None,
            target_blob_name=None,
            target_azure_conn_str=self.target_azure_conn_str,
            aws_endpoint_url=self.aws_endpoint_url,
            aws_access_key_id=self.aws_access_key_id,
            aws_secret_access_key=self.aws_secret_access_key,
            aws_region=self.aws_region,
            logger=self.logger,
        )
        self.kafka_trans = KafkaTransection(
            bootstrap_server=self.bootstrap_server,
            logger=self.logger,
        )

    def read_records(self, file: str) -> str:
        self.data_trans.source_blob_name = file
        data = self.data_trans.data_read(cloud_vendor=self.cloud_provider)
        self.logger.info(
            "File content read",
            extra={"file": file, "provider": self.cloud_provider, "bytes": len(data)},
        )

        self.messages_processed.add(1, {
            "source_topic": self.source_kafka_topic,
            "file": file,
            "status": "success",
        })

        return data

    def schema_validation(self, data: str) -> bool:
        self.logger.info(
            "Schema validation (stub – PI3)",
            extra={"schema_type": self.schema_type, "data_length": len(data)},
        )
        return True

    def move_files(self, file: str) -> bool:
        self.file_name = (
            self.schema_type.lower() + "-"
            + self.org_name.lower() + "-"
            + file.replace(" ", "_").replace("-", "_").lower()
        )
        result = self.data_trans.file_copy(
            cloud_vendor=self.cloud_provider,
            file_name=file,
            dest_file_name=self.file_name,
        )
        self.logger.info(
            "Schema mapper file_copy result",
            extra={"source_file": file, "dest_file": self.file_name,
                   "copied": result.copied, "skipped": result.skipped},
        )
        self.files_moved.add(1, {
            "cloud_provider": self.cloud_provider,
            "status": "success",
        })
        return result.copied

    def send_to_kafka(self, is_file_move: bool) -> None:
        if is_file_move:


            self.kafka_trans.send_message(
                target_topic=self.target_kafka_topic,
                message={
                    "sourceType":     "S3"
                        if self.cloud_provider.upper() == "AWS"
                        else self.cloud_provider.upper(),
                    "storageContainer": self.target_container_name,
                    "path":             self.file_name,
                },
            )
            self.logger.info(
                "Target Kafka message published",
                extra={"topic": self.target_kafka_topic, "file": self.file_name},
            )


# ---------------------------------------------------------------------------
# Per-message processor  (shared by both drain and continuous modes)
# ---------------------------------------------------------------------------

@traced(span_name="process_message")
def _process_one_message(
    mapper: SchemaMapper,
    step_log: StepLogger,
    ctx: PipelineContext,
    payload: dict[str, Any],
) -> None:
    """
    Process one file through read → validate → move → publish.
    All log lines carry ctx.run_id so every message processed in one
    Airflow-triggered run is traceable together.
    """
    # Restore trace context from Kafka headers so this span is a child of the extractor span
    with mapper.tracer.start_as_current_span("process_message") as span:
        operation = "mapper_msg"
        start_time = datetime.now(UTC)
        
        file_name: str = payload.get("path", "")
        span.set_attribute("file.name", file_name or "unknown")

        if not file_name:
            span.set_attribute("process.status", "skipped")
            mapper.logger.warning(
                "[%s] Kafka payload missing 'path' — skipping (run_id=%s)",
                ctx.pipeline_stage, ctx.run_id[:8],
                extra={"event.name": "pipeline.payload.invalid", "payload": payload, **ctx.as_log_extra()},
            )
            return

        exc_handle = HandleExceptions()
        step_log.step_start(ctx, "schema_mapper_message", extra={"file": file_name})
        try:
            data     = mapper.read_records(file=file_name)
            is_valid = mapper.schema_validation(data)
            if is_valid:
                step_log.step_start(ctx, "move_files", extra={"file": file_name})
                is_moved = mapper.move_files(file=file_name)
                step_log.step_end(ctx, "move_files", extra={"moved": is_moved, "dest": mapper.file_name})
                mapper.send_to_kafka(is_file_move=is_moved)
                duration_ms = int(
                    (datetime.now(UTC) - start_time).total_seconds() * 1000
                )
                mapper.process_duration.record(duration_ms, {
                    "source_topic": mapper.source_kafka_topic,
                })
            step_log.step_end(ctx, "schema_mapper_message", extra={"file": file_name})

        except (HttpResponseError, AzureError) as exc:
            mapper.messages_processed.add(1, {"source_topic": mapper.source_kafka_topic, "status": "error"})
            span.set_attribute("process.status", "error")
            span.set_attribute("error.type", type(exc).__name__)
            span.record_exception(exc)
            step_log.step_failed(ctx, "schema_mapper_message", exc=exc, extra={"file": file_name})
            exc_handle.handle_storage_exception(exc, "Azure")
        except (ClientError, BotoCoreError) as exc:
            mapper.messages_processed.add(1, {"source_topic": mapper.source_kafka_topic, "status": "error"})
            span.set_attribute("process.status", "error")
            span.set_attribute("error.type", type(exc).__name__)
            span.record_exception(exc)
            step_log.step_failed(ctx, "schema_mapper_message", exc=exc, extra={"file": file_name})
            exc_handle.handle_storage_exception(exc, "AWS S3")
        except GoogleAPIError as exc:
            mapper.messages_processed.add(1, {"source_topic": mapper.source_kafka_topic, "status": "error"})
            span.set_attribute("process.status", "error")
            span.set_attribute("error.type", type(exc).__name__)
            span.record_exception(exc)
            step_log.step_failed(ctx, "schema_mapper_message", exc=exc, extra={"file": file_name})
            exc_handle.handle_storage_exception(exc, "GCP")
        except Exception as exc:  # noqa: BLE001
            mapper.messages_processed.add(1, {"source_topic": mapper.source_kafka_topic, "status": "error"})
            span.set_attribute("process.status", "error")
            span.set_attribute("error.type", type(exc).__name__)
            span.record_exception(exc)
            step_log.step_failed(ctx, "schema_mapper_message", exc=exc, extra={"file": file_name})
            exc_handle.handle_storage_exception(exc, "")


# ---------------------------------------------------------------------------
# run(ctx) — single entry point for all SchedulerBackends
# ---------------------------------------------------------------------------
@traced(span_name="producer_file_mapper_pipeline")
@timed_metric("pipeline_total_duration", "Total pipeline execution time")
def run(ctx: PipelineContext) -> None:
    """
    Run the schema mapper for one pipeline execution.

    DRAIN MODE  (ctx.triggered_by == "kafka-trigger")
    ──────────────────────────────────────────────────
    Processes all messages currently in the mapper Kafka topic.
    Considers the queue "empty" when no new messages arrive for
    DRAIN_IDLE_SECS consecutive seconds (default 15).
    Returns when drain is complete — KafkaTriggerBackend then publishes
    "completed" to dpn-pipeline-status so Airflow can proceed.

    Why drain mode?
    The adaptor just finished and published N file-ready events to the
    mapper topic. We know the batch is finite. By draining until idle
    we process exactly what the adaptor produced without running forever.

    CONTINUOUS MODE  (ctx.triggered_by == "interval" or "standalone")
    ──────────────────────────────────────────────────────────────────
    Long-lived consumer loop — the existing behaviour. Runs indefinitely,
    processing messages as they arrive. Used when Airflow is unavailable.
    """
    
    tracer = OtelTracer.get_tracer(__name__)

    with tracer.start_as_current_span("mapper_pipeline") as span:
        span.set_attribute("pipeline.type", "consumer-file-mapper")
        span.set_attribute("pipeline.triggered_by", ctx.triggered_by)
        span.set_attribute("pipeline.run_id", ctx.run_id)

        mapper    = SchemaMapper()
        step_log  = StepLogger(mapper.logger)

        step_log.pipeline_banner(
            ctx,
            service_name="producer-file-ssh-schema-mapper",
            config_summary={
                "cloudProviderType":   mapper.cloud_provider,
                "sourceKafkaTopic":    mapper.source_kafka_topic,
                "targetTopicName":     mapper.target_kafka_topic,
                "mapperContainerName": mapper.source_container_name,
                "targetContainerName": mapper.target_container_name,
                "PRODUCT_NAME":        os.getenv('PRODUCT_NAME', 'ssh'),
                "SCHEDULER_BACKEND":   os.getenv("SCHEDULER_BACKEND", "standalone"),
            },
        )

        operation = "mapper_window"
        step_log.step_start(ctx, operation)

        def _handler(payload: dict[str, Any]) -> None:
            _process_one_message(mapper, step_log, ctx, payload)

        try:
            if ctx.triggered_by == "kafka-trigger":
                _run_drain_mode(mapper, step_log, ctx, _handler)
            else:
                _run_continuous_mode(mapper, step_log, ctx, _handler)

            step_log.step_end(ctx, operation)
            span.set_attribute("pipeline.status", "success")

        except Exception as exc:
            span.set_attribute("pipeline.status", "error")
            span.set_attribute("error.type", type(exc).__name__)
            span.record_exception(exc)
            step_log.step_failed(ctx, operation, exc=exc)
            raise

@traced(span_name="producer_file_mapper_pipeline_run")
@timed_metric("pipeline_total_duration", "Total pipeline execution time")
def _run_drain_mode(
    mapper: SchemaMapper,
    step_log: StepLogger,
    ctx: PipelineContext,
    handler: Any,
) -> None:
    """
    Process all available messages then return.

    Creates its own consumer (not using kafka_trans.consume_messages) so we
    have explicit control over the idle-timeout exit condition.
    """
    with mapper.tracer.start_as_current_span("mapper_pipeline") as span:
        mapper.logger.info(
            "[%s] Drain mode — processing queue, idle timeout=%ds (run_id=%s)",
            ctx.pipeline_stage, _DRAIN_IDLE_SECS, ctx.run_id[:8],
            extra={
                "event.name":           "pipeline.mapper.drain_mode",
                "drain.idle_secs":      _DRAIN_IDLE_SECS,
                "source.kafka_topic":   mapper.source_kafka_topic,
                **ctx.as_log_extra(),
            },
        )

        consumer = Consumer({
            "bootstrap.servers":  mapper.bootstrap_server,
            "group.id":           "producer_schema_mapper",
            "auto.offset.reset":  "earliest",
            "enable.auto.commit": True,
        })
        consumer.subscribe([mapper.source_kafka_topic])

        step_log.step_start(ctx, "drain_queue")
        processed = 0
        last_message_at = time.monotonic()

        try:
            while True:
                msg = consumer.poll(timeout=1.0)

                if msg is None:
                    # No message this poll — check idle timeout
                    idle_secs = time.monotonic() - last_message_at
                    if idle_secs >= _DRAIN_IDLE_SECS:
                        mapper.logger.info(
                            "[%s] Queue drained — %d messages processed, idle %.0fs (run_id=%s)",
                            ctx.pipeline_stage, processed, idle_secs, ctx.run_id[:8],
                            extra={
                                "event.name":            "pipeline.mapper.drain_complete",
                                "messages.processed":    processed,
                                "drain.idle_elapsed_secs": idle_secs,
                                **ctx.as_log_extra(),
                            },
                        )
                        break   # drain complete — return to KafkaTriggerBackend poll loop
                    continue

                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        # Reached end of partition — but wait for idle timeout
                        # in case more messages arrive shortly after
                        continue
                    raise KafkaException(msg.error())

                # Process the message
                try:
                    import json
                    payload = json.loads(msg.value().decode("utf-8"))

                    # Restore the producer's trace context from Kafka headers
                    # *before* invoking the handler, so the handler's span
                    # (and any spans it creates) become children of the
                    # producer span — same trace_id end-to-end.
                    _carrier = {
                        k: v.decode() if isinstance(v, bytes) else v
                        for k, v in (msg.headers() or [])
                    }
                    _remote_ctx = _otel_propagate.extract(_carrier)
                    _token = _otel_context.attach(_remote_ctx)
                    try:
                        handler(payload)
                    finally:
                        _otel_context.detach(_token)

                    processed += 1
                    last_message_at = time.monotonic()
                    span.set_attribute("kafka.source_topic", msg.topic())
                    span.set_attribute("kafka.partition", msg.partition())
                    span.set_attribute("kafka.offset", msg.offset())
                except (json.JSONDecodeError, UnicodeDecodeError) as exc:
                    mapper.logger.warning(
                        "Malformed mapper message — skipping",
                        extra={"event.name": "pipeline.mapper.bad_message", "error": str(exc)},
                    )

        finally:
            consumer.close()

        step_log.step_end(ctx, "drain_queue", extra={"messages_processed": processed})


def _run_continuous_mode(
    mapper: SchemaMapper,
    step_log: StepLogger,
    ctx: PipelineContext,
    handler: Any,
) -> None:
    """
    Long-lived consumer loop — original behaviour for non-Airflow execution.
    Runs until the process is terminated.
    """
    retry_delay = int(os.getenv("consumerRetryDelaySecs", "5"))

    mapper.logger.info(
        "[%s] Continuous consumer mode (run_id=%s)",
        ctx.pipeline_stage, ctx.run_id[:8],
        extra={"event.name": "pipeline.mapper.continuous_mode", **ctx.as_log_extra()},
    )

    step_log.step_start(ctx, "mapper_consumer_loop")
    while True:
        try:
            mapper.kafka_trans.consume_messages(
                source_topic=mapper.source_kafka_topic,
                group_id="producer_schema_mapper",
                handler=handler,
            )
        except Exception as exc:  # noqa: BLE001
            step_log.step_failed(ctx, "mapper_consumer_loop", exc=exc)
            mapper.logger.error(
                "[%s] Consumer loop exited — retrying in %ds (run_id=%s)",
                ctx.pipeline_stage, retry_delay, ctx.run_id[:8],
                extra={"event.name": "pipeline.mapper.retry", "error": str(exc), **ctx.as_log_extra()},
                exc_info=True,
            )
            time.sleep(retry_delay)
            step_log.step_start(ctx, "mapper_consumer_loop")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":  # pragma: no cover
    temp_mapper = SchemaMapper()
    component_name=f"producer-file-schema-mapper-{os.getenv('PRODUCT_NAME', 'ssh')}"
    heartbeat = HeartbeatLogger(
        logger=temp_mapper.logger,
        component_name=component_name,
        metadata={
            "source_topic": temp_mapper.source_kafka_topic,
            "target_topic": temp_mapper.target_kafka_topic,
            "scheduler_backend": os.getenv("SCHEDULER_BACKEND", "standalone"),
        },
    )
    heartbeat.start()
    _backend = get_backend()
    _backend.execute(
        run,
        pipeline_stage="schema_mapper",
        pipeline_type="file",
        pipeline_role="producer",
        component_name=component_name
    )
