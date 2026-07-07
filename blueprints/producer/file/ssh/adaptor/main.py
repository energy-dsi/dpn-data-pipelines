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
File Adaptor (Producer) - BP Natural Gas.

This module implements the producer file adaptor responsible for
discovering files from a source storage container, copying them to a
mapper staging container, and publishing Kafka file-ready events for
downstream processing.

The adaptor supports scheduler-driven execution models, OpenTelemetry
observability, heartbeat monitoring, Kafka-triggered execution, and
Airflow-based orchestration.

Features:
    * File discovery from cloud storage providers.
    * File transfer between source and mapper containers.
    * Kafka event publication for downstream processing.
    * OpenTelemetry tracing and metrics collection.
    * Structured pipeline and step-level logging.
    * Heartbeat monitoring and operational visibility.
    * Scheduler backend integration for orchestrated execution.
    * Multi-cloud support for Azure, AWS S3, and GCP.

Environment Variables:
    cloudProviderType: Cloud storage provider type.
    srcConnectionString: Source storage connection string.
    srcContainerName: Source storage container name.
    mapperConnectionString: Mapper storage connection string.
    mapperContainerName: Mapper storage container name.
    mapperTopicName: Kafka topic for downstream notifications.
    bootstrapServer: Kafka bootstrap server endpoint.
    AWS_ENDPOINT_URL: AWS S3 endpoint URL.
    AWS_ACCESS_KEY_ID: AWS access key.
    AWS_SECRET_ACCESS_KEY: AWS secret key.
    AWS_REGION: AWS region.
    PRODUCT_NAME: Data product identifier.
    SCHEDULER_BACKEND: Execution backend configuration.

Pipeline Flow:
    Source Container
        --> File Adaptor
        --> Mapper Container
        --> Kafka Event
        --> Downstream Processing

Example:
    Source Container:
        ssh-stage

    Mapper Container:
        ssh-mapper

    Kafka Topic:
        mapperTopicName

File Location:
    Producer/file/<data_product>/adaptor/main.py
"""
from __future__ import annotations

import base64
import os
from datetime import UTC, datetime

from azure.core.exceptions import AzureError, HttpResponseError
from botocore.exceptions import BotoCoreError, ClientError
from dotenv import load_dotenv
from google.api_core.exceptions import GoogleAPIError
from opentelemetry import context as _otel_context, propagate as _otel_propagate

from utils.config_validator import validate_cloud_config, validate_kafka_config
from utils.data_transection import DataTransection
from utils.exception_handler import HandleExceptions
from utils.kafka_transection import KafkaTransection
from utils.pipeline_context import PipelineContext
from utils.scheduler_backend import get_backend
from utils.step_logger import StepLogger 
from dpn_observability_sdk.otel_logger import OtelLogger as Logging
from dpn_observability_sdk.otel_tracer import OtelTracer
from dpn_observability_sdk.otel_metrics import OtelMetrics
from dpn_observability_sdk.otel_instrumentation import traced, timed_metric
from dpn_observability_sdk.heartbeat import HeartbeatLogger

load_dotenv()


# ---------------------------------------------------------------------------
# Business-logic class  (UNCHANGED from v1.0)
# ---------------------------------------------------------------------------

class AdaptorFileProcess:
    """
    File-ingestion adaptor for BP Natural Gas.
    Reads from ssh-stage, copies to mapper staging, publishes Kafka events.
    """

    def __init__(self) -> None:

        # Initialize OpenTelemetry
        self.tracer = OtelTracer.initialize(
            service_name="producer-file-adaptor",
            service_version="1.0.0"
        )
        self.meter = OtelMetrics.initialize(
            service_name="producer-file-adaptor",
            service_version="1.0.0"
        )

        # Create metrics
        self.files_processed = self.meter.create_counter(
            name="files_processed_total",
            description="Total files processed by file extractor",
            unit="1",
        )

        self.file_copy_duration = self.meter.create_histogram(
            name="file_copy_duration",
            description="File copy duration",
            unit="ms",
        )

        self.files_discovered = self.meter.create_counter(
            name="files_discovered_total",
            description="Total files discovered from source container",
            unit="1",
        )

        # Initialize heartbeat logger (started in __main__)
        self.heartbeat: HeartbeatLogger | None = None

        # Captures the per-file "process_file" span context so send_to_kafka
        # can inject headers as a child of that span, not the pipeline span.
        self._file_span_context: _otel_context.Context | None = None

        self.cloud_provider: str = os.getenv("cloudProviderType", "azure")
        self.target_kafka_topic: str = os.getenv("mapperTopicName", "")
        self.source_azure_conn_str: str = base64.b64decode(
            os.getenv("srcConnectionString", "")
        ).decode("utf-8")
        self.source_container_name: str = os.getenv("srcContainerName", "")
        self.target_container_name: str = os.getenv("mapperContainerName", "")
        self.bootstrap_server: str = os.getenv("bootstrapServer", "")
        self.target_azure_conn_str: str = base64.b64decode(
            os.getenv("mapperConnectionString", "")
        ).decode("utf-8")
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
            azure_fields=["srcConnectionString", "mapperConnectionString"],
            logger=self.logger,
        )
        validate_kafka_config(logger=self.logger)

        self.logger.info(
            "Configuration validation successful",
            extra={"event.name": "config.validation.success", "cloud.provider": self.cloud_provider},
        )

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

    @traced(span_name="list_files")
    def read_source_file_info(self) -> list[str]:
        with self.tracer.start_as_current_span("list_files") as span:
            span.set_attribute("connectionstring", self.source_azure_conn_str)
            span.set_attribute("storage.container", self.source_container_name)
            span.set_attribute("storage.provider", self.cloud_provider)

            source_files = self.data_trans.source_file_info(cloud_provider=self.cloud_provider)

            self.files_discovered.add(len(source_files), {
                "cloud_provider": self.cloud_provider,
                "container": self.source_container_name,
            })

            span.set_attribute("files.discovered", len(source_files))

            self.logger.info(
                "Source files discovered",
                extra={"count": len(source_files), "files": source_files},
            )           
            return source_files

    @traced(span_name="process_file")
    def move_files(self, file: str) -> bool:

        with self.tracer.start_as_current_span("process_file") as span:
            span.set_attribute("file.name", file)
            span.set_attribute("storage.source_container", self.source_container_name)
            span.set_attribute("storage.target_container", self.target_container_name)

            start_time = datetime.now(UTC)

            try:
                result = self.data_trans.file_copy(cloud_vendor=self.cloud_provider, file_name=file)
                self.logger.info(
                    "Adaptor file_copy result",
                    extra={"file": file, "copied": result.copied, "skipped": result.skipped, "reason": result.reason},
                )

                duration_ms = int(
                    (datetime.now(UTC) - start_time).total_seconds() * 1000
                )

                self.files_processed.add(1, {
                    "cloud_provider": self.cloud_provider,
                    "status": "success" if result.copied else "skipped",
                })

                self.file_copy_duration.record(duration_ms, {
                    "cloud_provider": self.cloud_provider,
                })

                span.set_attribute("file.copied", result.copied)
                span.set_attribute("process.duration_ms", duration_ms)
                span.set_attribute("process.status", "success" if result.copied else "skipped")

                # Capture this span's context so send_to_kafka() below can
                # inject it as the parent trace context for the mapper span.
                self._file_span_context = _otel_context.get_current()

                return result.copied

            except Exception as exc:
                self.files_processed.add(1, {
                    "cloud_provider": self.cloud_provider,
                    "status": "error",
                })

                span.set_attribute("process.status", "error")
                span.set_attribute("error.type", type(exc).__name__)
                span.record_exception(exc)
                raise

    def send_to_kafka(self, file_name: str, is_file_move: bool) -> None:
        if is_file_move:

            message = {
                "sourceType":  "S3"
                        if self.cloud_provider.upper() == "AWS"
                        else self.cloud_provider.upper(),
                "storageContainer": self.target_container_name,
                "path":             file_name,
            }

            # Restore the per-file "process_file" span context (captured in
            # move_files) so the injected trace header is a child of that
            # span rather than the coarser pipeline-level span.
            token = None
            if self._file_span_context is not None:
                token = _otel_context.attach(self._file_span_context)
            try:
                self.kafka_trans.send_message(target_topic=self.target_kafka_topic, message=message)
            finally:
                if token is not None:
                    _otel_context.detach(token)

            self.logger.info(
                "Kafka message published",
                extra={"topic": self.target_kafka_topic, "file": file_name},
            )
        else:
            self.logger.info(
                "Kafka message suppressed — file not copied",
                extra={"file": file_name},
            )


# ---------------------------------------------------------------------------
# run(ctx) — single entry point for ALL scheduler backends
# ---------------------------------------------------------------------------
@traced(span_name="producer_file_adaptor_pipeline")
@timed_metric("pipeline_total_duration", "Total pipeline execution time")
def run(ctx: PipelineContext) -> None:
    """
    Execute one adaptor cycle.

    Called by:
      KafkaTriggerBackend  when a trigger message arrives on dpn-pipeline-control
      AirflowBackend       when Airflow fires the task (PythonOperator mode)
      IntervalBackend      on every poll tick (fallback without Airflow)
      StandaloneBackend    for a one-shot manual run

    The PipelineContext carries Airflow's run_id when triggered via Kafka or
    Airflow, so every log line from this cycle is correlated with the DAG run.
    """
    tracer = OtelTracer.get_tracer(__name__)

    with tracer.start_as_current_span("extractor_pipeline") as span:
        span.set_attribute("pipeline.type", "consumer-file-extractor")
        span.set_attribute("pipeline.triggered_by", ctx.triggered_by)
        span.set_attribute("pipeline.run_id", ctx.run_id)

        adaptor   = AdaptorFileProcess()
        step_log  = StepLogger(adaptor.logger)
        exc_handle = HandleExceptions()

        step_log.pipeline_banner(
            ctx,
            service_name="producer-file-ssh-adaptor",
            config_summary={
                "cloudProviderType":   adaptor.cloud_provider,
                "mapperTopicName":     adaptor.target_kafka_topic,
                "srcContainerName":    adaptor.source_container_name,
                "mapperContainerName": adaptor.target_container_name,
                "bootstrapServer":     adaptor.bootstrap_server,
                "PRODUCT_NAME":        os.getenv('PRODUCT_NAME', 'ssh'),
                "SCHEDULER_BACKEND":   os.getenv("SCHEDULER_BACKEND", "standalone"),
            },
        )

        # Step 1 — discover source files
        step_log.step_start(ctx, "read_source_file_info")
        try:
            source_files = adaptor.read_source_file_info()
            step_log.step_end(ctx, "read_source_file_info", extra={"files_found": len(source_files)})
        except (HttpResponseError, AzureError, ClientError, BotoCoreError, GoogleAPIError, Exception) as exc:
            span.set_attribute("pipeline.status", "error")
            span.set_attribute("error.type", type(exc).__name__)
            step_log.step_failed(ctx, "read_source_file_info", exc=exc)
            exc_handle.handle_storage_exception(exc, _provider_label(exc))
            return

        if not source_files:
            adaptor.logger.info(
                "[%s] No files found — nothing to do (run_id=%s)",
                ctx.pipeline_stage, ctx.run_id[:8],
                extra={"event.name": "pipeline.no_files", **ctx.as_log_extra()},
            )
            span.set_attribute("pipeline.status", "no_files")
            return

        # Step 2 — copy each file and publish Kafka events
        copied_count = skipped_count = 0

        for file in source_files:
            step_log.step_start(ctx, "move_and_publish", extra={"file": file})
            try:
                is_copied = adaptor.move_files(file=file)
                adaptor.send_to_kafka(file_name=file, is_file_move=is_copied)
                if is_copied:
                    copied_count += 1
                else:
                    skipped_count += 1
                step_log.step_end(ctx, "move_and_publish", extra={"file": file, "copied": is_copied})
            except (HttpResponseError, AzureError, ClientError, BotoCoreError, GoogleAPIError, Exception) as exc:
                step_log.step_failed(ctx, "move_and_publish", exc=exc, extra={"file": file})
                exc_handle.handle_storage_exception(exc, _provider_label(exc))
        
        span.set_attribute("pipeline.files.copied", copied_count)
        span.set_attribute("pipeline.files.skipped", skipped_count)
        span.set_attribute("pipeline.status", "success")

        adaptor.logger.info(
            "[%s] Adaptor cycle complete (run_id=%s)",
            ctx.pipeline_stage, ctx.run_id[:8],
            extra={
                "event.name":    "pipeline.cycle.complete",
                "files.total":   len(source_files),
                "files.copied":  copied_count,
                "files.skipped": skipped_count,
                **ctx.as_log_extra(),
            },
        )

def _provider_label(exc: Exception) -> str:
    if isinstance(exc, (HttpResponseError, AzureError)):
        return "Azure"
    if isinstance(exc, (ClientError, BotoCoreError)):
        return "AWS S3"
    if isinstance(exc, GoogleAPIError):
        return "GCP"
    return ""


# ---------------------------------------------------------------------------
# Entry point — scheduler-agnostic bootstrap
# ---------------------------------------------------------------------------

if __name__ == "__main__":  # pragma: no cover
    # Set SCHEDULER_BACKEND env var to choose mode:
    #
    #   SCHEDULER_BACKEND=kafka-trigger   ← recommended for Kubernetes
    #   PRODUCT_NAME=ssh
    #
    #   Pod subscribes to dpn-pipeline-control and runs one cycle each
    #   time Airflow publishes a matching trigger. No new pods created.
    #   Falls back gracefully if Airflow goes down (just stays idle).
    #
    #   SCHEDULER_BACKEND=interval         ← self-scheduling fallback
    #   SCHEDULER_BACKEND=standalone       ← one-shot (default)
    temp_proc = AdaptorFileProcess()
    component_name=f"producer-file-adaptor-{ os.getenv('PRODUCT_NAME', 'ssh')}"
    heartbeat = HeartbeatLogger(
        logger=temp_proc.logger,
        component_name=component_name,
        metadata={
            "kafka_topic": temp_proc.target_kafka_topic,
            "src_container": temp_proc.source_container_name,
            "cloud_provider": temp_proc.cloud_provider,
            "scheduler_backend": os.getenv("SCHEDULER_BACKEND", "standalone"),
        },
    )
    heartbeat.start()

    _backend = get_backend()
    _backend.execute(
        run,
        pipeline_stage="adaptor",
        pipeline_type="file",
        pipeline_role="producer",
        component_name=f"producer-file-adaptor-{ os.getenv('PRODUCT_NAME', 'ssh')}",
    )
