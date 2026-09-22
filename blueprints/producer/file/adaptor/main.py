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
# | 2.0.0   | Genericised — one image for every schema type/product    | DSI Team      | 2026-08-30  |
# | 3.0.0   | Schema mapper absorbed into adaptor — no mapper           | DSI Team      | 2026-09-14  |
# |         | container/topic; adaptor reads source and writes         |               |             |
# |         | straight to target in one hop.                           |               |             |
# | 3.1.0   | Removed the org-prefixed rename entirely — a file lands  | DSI Team      | 2026-09-15  |
# |         | in the target container under the exact name it arrived  |               |             |
# |         | with, no exceptions.                                     |               |             |
# +---------+----------------------------------------------------------+---------------+-------------+
"""
File Adaptor (Producer) - Generic Data Product Runtime.

This module implements the producer file adaptor responsible for
discovering files in a source storage container, copying each one
straight to the target storage container UNDER ITS ORIGINAL NAME (no
rename, no org prefix — see this file's v3.1.0 changelog entry), and
publishing a Kafka notification once it lands there. There is no
intermediate staging container or "mapper" hop any more — a data
product's adaptor is now the entire file-pathway pipeline by itself.

Unlike the per-schema-type blueprints under blueprints/producer/file/,
this module carries no product- or schema-specific defaults: every
identifier (product name, source/target locations, Kafka topic) is
supplied entirely via environment variables at container start. One
image serves every data product — a new data product is a new set of
environment variables passed to this same image, not a new
folder/Dockerfile/blueprint copy.

The adaptor supports scheduler-driven execution models, OpenTelemetry
observability, heartbeat monitoring, Kafka-triggered execution, and
Airflow-based orchestration.

Features:
    * File discovery from cloud storage providers.
    * Direct file transfer from the source container to the target
      container, unchanged filename, one copy — not two.
    * Kafka event publication for downstream processing.
    * OpenTelemetry tracing and metrics collection.
    * Structured pipeline and step-level logging.
    * Heartbeat monitoring and operational visibility.
    * Scheduler backend integration for orchestrated execution.
    * Multi-cloud support for Azure, AWS S3, and GCP.

Environment Variables:
    cloudProviderType: Cloud storage provider type.
    srcConnectionString: Source storage connection string (Azure, base64).
    srcContainerName: Source storage container name.
    targetConnectionString: Target storage connection string (Azure, base64).
    targetContainerName: Target storage container name.
    targetTopicName: Kafka topic for downstream notifications.
    bootstrapServer: Kafka bootstrap server endpoint.
    AWS_ENDPOINT_URL: AWS/S3-compatible endpoint URL.
    AWS_ACCESS_KEY_ID: AWS/S3 access key (base64).
    AWS_SECRET_ACCESS_KEY: AWS/S3 secret key (base64).
    AWS_REGION: AWS region.
    PRODUCT_NAME: Data product identifier — required, no default.
    SCHEDULER_BACKEND: Execution backend configuration.

Pipeline Flow:
    Source Container
        --> File Adaptor (copy, unchanged filename)
        --> Target Container
        --> Kafka Event
        --> Downstream Processing

File Location:
    blueprints/producer/file/adaptor/main.py
"""
from __future__ import annotations

import base64
import os
import signal
import sys
from datetime import UTC, datetime

from azure.core.exceptions import AzureError, HttpResponseError
from botocore.exceptions import BotoCoreError, ClientError
from dotenv import load_dotenv
from google.api_core.exceptions import GoogleAPIError
from opentelemetry import context as _otel_context, propagate as _otel_propagate

from utils.config_validator import validate_cloud_config, validate_kafka_config
from utils.data_transection import DataTransection, gcp_credentials_from_env
from utils.exception_handler import HandleExceptions
from utils.kafka_transection import KafkaTransection
from utils.pipeline_context import PipelineContext
from utils.scheduler_backend import get_backend, run_once
from utils.step_logger import StepLogger
from utils.topic_utils import KafkaTopicManager
from dpn_observability_sdk.otel_logger import OtelLogger as Logging
from dpn_observability_sdk.otel_tracer import OtelTracer
from dpn_observability_sdk.otel_metrics import OtelMetrics
from dpn_observability_sdk.otel_instrumentation import traced, timed_metric
from dpn_observability_sdk.heartbeat import HeartbeatLogger

load_dotenv()


# ---------------------------------------------------------------------------
# Business-logic class  (identical across every schema type — UNCHANGED)
# ---------------------------------------------------------------------------

class AdaptorFileProcess:
    """
    File-ingestion adaptor for any registered data product.
    Reads from srcContainerName and writes straight to
    targetContainerName under the same filename — then publishes a
    Kafka notification.
    """

    def __init__(self) -> None:

        # Same identifier used for the HeartbeatLogger and the scheduler
        # backend's component_name, so every log this class emits directly
        # (i.e. not via StepLogger/ctx) still carries a matching component.name.
        self.product_name: str = os.getenv("PRODUCT_NAME", "")
        self.component_name = f"producer-file-adaptor-{self.product_name or 'unknown'}"

        # Initialize OpenTelemetry
        self.tracer = OtelTracer.initialize(
            service_name="producer-file-adaptor",
            service_version="3.1.0"
        )
        self.meter = OtelMetrics.initialize(
            service_name="producer-file-adaptor",
            service_version="3.1.0"
        )

        # Create metrics
        self.files_processed = self.meter.create_counter(
            name="files_processed_total",
            description="Total files processed by file adaptor",
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

        self.file_name: str | None = None

        self.cloud_provider: str = os.getenv("cloudProviderType", "azure")
        self.target_kafka_topic: str = os.getenv("targetTopicName", "")
        self.source_azure_conn_str: str = base64.b64decode(
            os.getenv("srcConnectionString", "")
        ).decode("utf-8")
        self.source_container_name: str = os.getenv("srcContainerName", "")
        self.target_container_name: str = os.getenv("targetContainerName", "")
        self.bootstrap_server: str = os.getenv("bootstrapServer", "")
        self.target_azure_conn_str: str = base64.b64decode(
            os.getenv("targetConnectionString", "")
        ).decode("utf-8")
        self.aws_endpoint_url: str | None = os.getenv("AWS_ENDPOINT_URL") or None
        self.aws_access_key_id = base64.b64decode(
            os.getenv("AWS_ACCESS_KEY_ID", "")
        ).decode("utf-8")
        self.aws_secret_access_key = base64.b64decode(
            os.getenv("AWS_SECRET_ACCESS_KEY", "")
        ).decode("utf-8")
        self.aws_region: str = os.getenv("AWS_REGION", "us-east-1")
        self.gcp_service_account_info = gcp_credentials_from_env(
            os.getenv("GCP_SERVICE_ACCOUNT_KEY", "")
        )
        self.gcp_project_id: str | None = os.getenv("GCP_PROJECT_ID") or None

        self.logger = Logging().create_logger()

        validate_cloud_config(
            cloud_provider=self.cloud_provider,
            azure_fields=["srcConnectionString", "targetConnectionString"],
            logger=self.logger,
        )
        # No mapperTopicName any more — this adaptor absorbed its schema
        # mapper stage entirely (see this file's v3.0.0 changelog entry),
        # so there is no intermediate mapper topic to require.
        validate_kafka_config(logger=self.logger, require_mapper_topic=False)

        self.logger.info(
            "Configuration validation successful",
            extra={
                "event.name": "config.validation.success",
                "cloud.provider": self.cloud_provider,
                "component.name": self.component_name,
            },
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
            gcp_service_account_info=self.gcp_service_account_info,
            gcp_project_id=self.gcp_project_id,
            logger=self.logger,
            component_name=self.component_name,
        )
        # Ensure both containers exist before anything tries to read/write
        # them — a brand-new self-service product may name a container
        # that doesn't exist yet in the platform storage account.
        self.data_trans.ensure_container_exists(
            self.cloud_provider, self.source_container_name, side="source"
        )
        self.data_trans.ensure_container_exists(
            self.cloud_provider, self.target_container_name, side="target"
        )
        # Ensure the target topic exists before anything tries to produce to
        # it. A brand-new self-service product may name a topic that
        # doesn't exist yet on the broker.
        KafkaTopicManager(
            bootstrap_server=self.bootstrap_server,
            logger=self.logger,
        ).ensure_exists(self.target_kafka_topic)

        self.kafka_trans = KafkaTransection(
            bootstrap_server=self.bootstrap_server,
            logger=self.logger,
            component_name=self.component_name,
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
                extra={
                    "count": len(source_files),
                    "files": source_files,
                    "component.name": self.component_name,
                },
            )
            return source_files

    def schema_validation(self, file: str) -> bool:
        """Stub — kept from the removed schema mapper for PI3 schema-
        validation work; always True today (see that module's history)."""
        self.logger.info(
            "Schema validation (stub – PI3)",
            extra={"file": file, "component.name": self.component_name},
        )
        return True

    @traced(span_name="process_file")
    def move_files(self, file: str) -> bool:
        """
        Copy directly from source_container_name to target_container_name,
        under the SAME name it arrived with — no rename, no org prefix, no
        case/space/hyphen normalization. The removed schema mapper this
        adaptor absorbed used to rewrite the name (org-prefixed,
        spaces/hyphens to underscores); that behaviour was removed on
        request — a business user's file must reach the target container
        exactly as received. One hop either way: there is no mapper
        container in between any more.
        """
        with self.tracer.start_as_current_span("process_file") as span:
            span.set_attribute("file.name", file)
            span.set_attribute("storage.source_container", self.source_container_name)
            span.set_attribute("storage.target_container", self.target_container_name)

            start_time = datetime.now(UTC)

            self.file_name = file

            try:
                result = self.data_trans.file_copy(
                    cloud_vendor=self.cloud_provider,
                    file_name=file,
                    dest_file_name=self.file_name,
                )
                self.logger.info(
                    "Adaptor file_copy result",
                    extra={
                        "source_file": file,
                        "dest_file": self.file_name,
                        "copied": result.copied,
                        "skipped": result.skipped,
                        "reason": result.reason,
                        "component.name": self.component_name,
                    },
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
                # inject it as a child of this span rather than the coarser
                # pipeline-level span.
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

    def send_to_kafka(self, is_file_move: bool) -> None:
        if is_file_move:

            message = {
                "sourceType":  "S3"
                        if self.cloud_provider.upper() == "AWS"
                        else self.cloud_provider.upper(),
                "storageContainer": self.target_container_name,
                "path":             self.file_name,
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
                extra={
                    "topic": self.target_kafka_topic,
                    "file": self.file_name,
                    "component.name": self.component_name,
                },
            )
        else:
            self.logger.info(
                "Kafka message suppressed — file not copied",
                extra={"file": self.file_name, "component.name": self.component_name},
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
        span.set_attribute("pipeline.type", "producer-file-adaptor")
        span.set_attribute("pipeline.triggered_by", ctx.triggered_by)
        span.set_attribute("pipeline.run_id", ctx.run_id)

        adaptor   = AdaptorFileProcess()
        step_log  = StepLogger(adaptor.logger)
        exc_handle = HandleExceptions()

        step_log.pipeline_banner(
            ctx,
            service_name=f"producer-file-{adaptor.product_name or 'unknown'}-adaptor",
            config_summary={
                "cloudProviderType":   adaptor.cloud_provider,
                "targetTopicName":     adaptor.target_kafka_topic,
                "srcContainerName":    adaptor.source_container_name,
                "targetContainerName": adaptor.target_container_name,
                "bootstrapServer":     adaptor.bootstrap_server,
                "PRODUCT_NAME":        adaptor.product_name,
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

        # Step 2 — copy each file straight to target, unchanged, and publish
        copied_count = skipped_count = 0

        for file in source_files:
            step_log.step_start(ctx, "move_and_publish", extra={"file": file})
            try:
                adaptor.schema_validation(file)
                is_copied = adaptor.move_files(file=file)
                adaptor.send_to_kafka(is_file_move=is_copied)
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
    #
    #   Pod subscribes to dpn-pipeline-control and runs one cycle each
    #   time Airflow publishes a matching trigger. No new pods created.
    #   Falls back gracefully if Airflow goes down (just stays idle).
    #
    #   SCHEDULER_BACKEND=interval         ← self-scheduling fallback
    #   SCHEDULER_BACKEND=standalone       ← one-shot (default)
    #
    #   --run-once  ← self-service "Run Now" button (see
    #   service/podman_ops.py / k8s_ops.py run_once()): execs this
    #   inside the already-running container to force a single cycle on
    #   demand, bypassing the scheduler entirely — no heartbeat needed for
    #   a process this short-lived.
    temp_proc = AdaptorFileProcess()

    if "--run-once" in sys.argv:
        run_once(
            run,
            pipeline_stage="adaptor",
            pipeline_type="file",
            pipeline_role="producer",
            component_name=temp_proc.component_name,
        )
    else:
        heartbeat = HeartbeatLogger(
            logger=temp_proc.logger,
            component_name=temp_proc.component_name,
            metadata={
                "kafka_topic": temp_proc.target_kafka_topic,
                "src_container": temp_proc.source_container_name,
                "target_container": temp_proc.target_container_name,
                "cloud_provider": temp_proc.cloud_provider,
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

        _backend = get_backend()
        _backend.execute(
            run,
            pipeline_stage="adaptor",
            pipeline_type="file",
            pipeline_role="producer",
            component_name=temp_proc.component_name,
        )
