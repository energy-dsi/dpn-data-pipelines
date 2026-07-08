"""
Extractor — Consumer File Pipeline

This module implements the extractor stage of the file pipeline.

Responsibilities:
- Poll source container for files
- Copy files into mapper staging container
- Publish Kafka events for downstream processing

Version History
---------------
v1.0.0  Initial release
v1.1.0  Added Kafka trigger backend + StepLogger (2026-05-26)

Execution (Kubernetes Recommended)
---------------------------------
SCHEDULER_BACKEND=kafka-trigger
PRODUCT_NAME=consumer-file
"""

from __future__ import annotations

import base64
import os
import time
from datetime import UTC, datetime

# Cloud Exceptions
from azure.core.exceptions import AzureError, HttpResponseError
from botocore.exceptions import BotoCoreError, ClientError
from google.api_core.exceptions import GoogleAPIError

from dotenv import load_dotenv
from opentelemetry import context as otel_context

# Internal Utilities
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


# =============================================================================
# Business Logic: ExtractorFileProcess
# =============================================================================
class ExtractorFileProcess:
    """
    Handles file extraction workflow.

    Responsibilities:
    - Discover files from source storage
    - Copy files to staging container
    - Publish Kafka events
    """

    def __init__(self) -> None:
        """
        Initialize configuration, logger, and dependencies.
        """

        # Same identifier used for the HeartbeatLogger and the scheduler
        # backend's component_name, so every log this class emits directly
        # (i.e. not via StepLogger/ctx) still carries a matching component.name.
        self.component_name = "consumer-file-extractor"

        # Initialize OpenTelemetry
        self.tracer = OtelTracer.initialize(
            service_name="consumer-file-extractor",
            service_version="1.0.0"
        )
        self.meter = OtelMetrics.initialize(
            service_name="consumer-file-extractor",
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

        # Captures the per-file "process_file" span context so publish_event
        # can inject headers as a child of that span, not the pipeline span.
        self._file_span_context: otel_context.Context | None = None

        # ---------------------------------------------------------------------
        # Environment Configuration
        # ---------------------------------------------------------------------
        self.cloud_provider = os.getenv("cloudProviderType", "azure")
        self.kafka_topic = os.getenv("mapperTopicName", "")

        # Azure Configuration
        self.src_conn = base64.b64decode(
            os.getenv("srcConnectionString", "")
        ).decode()
        self.tgt_conn = base64.b64decode(
            os.getenv("mapperConnectionString", "")
        ).decode()

        self.src_container = os.getenv("srcContainerName", "")
        self.tgt_container = os.getenv("mapperContainerName", "")

        # Kafka Configuration
        self.bootstrap = os.getenv("bootstrapServer", "")

        # AWS Configuration (Optional)
        self.aws_endpoint = os.getenv("AWS_ENDPOINT_URL") or None
        self.aws_key_id = base64.b64decode(
            os.getenv("AWS_ACCESS_KEY_ID", "")
        ).decode()
        self.aws_secret = base64.b64decode(
            os.getenv("AWS_SECRET_ACCESS_KEY", "")
        ).decode()
        self.aws_region = os.getenv("AWS_REGION", "us-east-1")

        # ---------------------------------------------------------------------
        # Logger Initialization
        # ---------------------------------------------------------------------
        self.logger = Logging().create_logger()

        # ---------------------------------------------------------------------
        # Configuration Validation
        # ---------------------------------------------------------------------
        validate_cloud_config(
            cloud_provider=self.cloud_provider,
            azure_fields=["srcConnectionString", "mapperConnectionString"],
            logger=self.logger,
        )
        validate_kafka_config(logger=self.logger)

        # Resolve mapper topic name
        topic_resolver = TopicResolver()
        self.kafka_topic = topic_resolver.resolve(
            self.kafka_topic,
            self.kafka_topic,
            "trfm"
        )

        # Ensure mapper topic exists
        topic_manager = KafkaTopicManager(
            bootstrap_server=self.bootstrap,
            logger=self.logger
        )

        topic_manager.ensure_exists(self.kafka_topic)

        # ---------------------------------------------------------------------
        # Service Clients
        # ---------------------------------------------------------------------
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

        self.kafka_trans = KafkaTransection(
            bootstrap_server=self.bootstrap,
            logger=self.logger,
            component_name=self.component_name,
        )

    # -------------------------------------------------------------------------
    # File Operations
    # -------------------------------------------------------------------------
    @traced(span_name="list_files")
    def list_files(self):
        """
        Retrieve list of files from the source container.

        Returns:
            list: List of file names.
        """
        with self.tracer.start_as_current_span("list_files") as span:
            span.set_attribute("storage.container", self.src_container)
            span.set_attribute("storage.provider", self.cloud_provider)

            files = self.data_trans.source_file_info(
                cloud_provider=self.cloud_provider
            )

            self.files_discovered.add(len(files), {
                "cloud_provider": self.cloud_provider,
                "container": self.src_container,
            })

            span.set_attribute("files.discovered", len(files))

            self.logger.info(
                "files discovered",
                extra={"count": len(files), "component.name": self.component_name},
            )
            return files
        
    @traced(span_name="process_file")
    def process_file(self, file):
        """
        Copy a file from source to target staging.

        Args:
            file (str): File name.

        Returns:
            bool: True if file copied successfully, else False.
        """
        with self.tracer.start_as_current_span("process_file") as span:
            span.set_attribute("file.name", file)
            span.set_attribute("storage.source_container", self.src_container)
            span.set_attribute("storage.target_container", self.tgt_container)

            start_time = datetime.now(UTC)

            try:
                result = self.data_trans.file_copy(
                    cloud_vendor=self.cloud_provider,
                    file_name=file,
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

                self.logger.info(
                    "file_copy",
                    extra={
                        "file": file,
                        "copied": result.copied,
                        "component.name": self.component_name,
                    },
                )

                # Capture this span's context so publish_event() below can
                # inject it as the parent trace context for the mapper span.
                self._file_span_context = otel_context.get_current()

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

    def publish_event(self, file_name, ok):
        """
        Publish Kafka event for successfully processed files.

        Args:
            file_name (str): File name
            ok (bool): Indicates if processing succeeded
        """
        if ok:
            # Restore the per-file "process_file" span context (captured in
            # process_file) so the injected trace header is a child of that
            # span rather than the coarser pipeline-level span.
            token = None
            if self._file_span_context is not None:
                token = otel_context.attach(self._file_span_context)
            try:
                self.kafka_trans.send_message(
                    target_topic=self.kafka_topic,
                    message={
                        "sourceType": (
                            "S3"
                            if self.cloud_provider.upper() == "AWS"
                            else self.cloud_provider.upper()
                        ),
                        "storageContainer": self.tgt_container,
                        "path": file_name,
                    },
                )
            finally:
                if token is not None:
                    otel_context.detach(token)


# =============================================================================
# Helper Function
# =============================================================================
def _provider(exc):
    """
    Map exception types to cloud provider labels.

    Args:
        exc (Exception): Exception instance

    Returns:
        str: Provider name
    """
    if isinstance(exc, (HttpResponseError, AzureError)):
        return "Azure"
    if isinstance(exc, (ClientError, BotoCoreError)):
        return "AWS S3"
    if isinstance(exc, GoogleAPIError):
        return "GCP"
    return ""


# =============================================================================
# Pipeline Execution Entry Point
# =============================================================================
@traced(span_name="consumer_file_extractor_pipeline")
@timed_metric("pipeline_total_duration", "Total pipeline execution time")
def run(ctx: PipelineContext) -> None:
    """
    Execute one extractor pipeline cycle.

    Args:
        ctx (PipelineContext): Pipeline execution context
    """
    tracer = OtelTracer.get_tracer(__name__)

    with tracer.start_as_current_span("extractor_pipeline") as span:
        span.set_attribute("pipeline.type", "consumer-file-extractor")
        span.set_attribute("pipeline.triggered_by", ctx.triggered_by)
        span.set_attribute("pipeline.run_id", ctx.run_id)

        proc = ExtractorFileProcess()
        step_log = StepLogger(proc.logger)
        exc_h = HandleExceptions()

        # -------------------------------------------------------------------------
        # Pipeline Start Banner
        # -------------------------------------------------------------------------
        step_log.pipeline_banner(
            ctx,
            service_name="consumer-file-extractor",
            config_summary={
                "cloudProviderType": proc.cloud_provider,
                "mapperTopicName": proc.kafka_topic,
                "SCHEDULER_BACKEND": os.getenv("SCHEDULER_BACKEND", "standalone"),
                "PRODUCT_NAME": os.getenv("PRODUCT_NAME", "consumer-file"),
            },
        )

        # -------------------------------------------------------------------------
        # Step 1: File Discovery
        # -------------------------------------------------------------------------
        step_log.step_start(ctx, "list_files")
        try:
            files = proc.list_files()
            step_log.step_end(
                ctx,
                "list_files",
                extra={"files_found": len(files)},
            )
        except Exception as exc:
            span.set_attribute("pipeline.status", "error")
            span.set_attribute("error.type", type(exc).__name__)
            span.record_exception(exc)
            step_log.step_failed(ctx, "list_files", exc=exc)
            exc_h.handle_storage_exception(exc, _provider(exc))
            return

        # No files case
        if not files:
            proc.logger.info(
                "[%s] no files (run_id=%s)",
                ctx.pipeline_stage,
                ctx.run_id[:8],
                extra={"event.name": "pipeline.no_files", **ctx.as_log_extra()},
            )
            span.set_attribute("pipeline.status", "no_files")
            return

        # -------------------------------------------------------------------------
        # Step 2: File Processing
        # -------------------------------------------------------------------------
        copied = 0
        skipped = 0

        for f in files:
            step_log.step_start(ctx, "process_file", extra={"file": f})
            try:
                ok = proc.process_file(f)
                proc.publish_event(f, ok)

                if ok:
                    copied += 1
                else:
                    skipped += 1

                step_log.step_end(
                    ctx,
                    "process_file",
                    extra={"file": f, "ok": ok},
                )
            except Exception as exc:
                step_log.step_failed(
                    ctx,
                    "process_file",
                    exc=exc,
                    extra={"file": f},
                )
                exc_h.handle_storage_exception(exc, _provider(exc))

        # -------------------------------------------------------------------------
        # Pipeline Completion
        # -------------------------------------------------------------------------
        span.set_attribute("pipeline.files.copied", copied)
        span.set_attribute("pipeline.files.skipped", skipped)
        span.set_attribute("pipeline.status", "success")

        proc.logger.info(
            "[%s] cycle done copied=%d skipped=%d (run_id=%s)",
            ctx.pipeline_stage,
            copied,
            skipped,
            ctx.run_id[:8],
            extra={
                "event.name": "pipeline.cycle.complete",
                "files.copied": copied,
                "files.skipped": skipped,
                **ctx.as_log_extra(),
            },
        )


# =============================================================================
# Application Entry Point
# =============================================================================
if __name__ == "__main__":
    """
    Scheduler-agnostic execution entry.

    Uses `SCHEDULER_BACKEND` environment variable:
        kafka-trigger  → event-driven execution (recommended)
        interval       → polling mode
        standalone     → one-time execution
    """
    temp_proc = ExtractorFileProcess()
    heartbeat = HeartbeatLogger(
        logger=temp_proc.logger,
        component_name=temp_proc.component_name,
        metadata={
            "kafka_topic": temp_proc.kafka_topic,
            "src_container": temp_proc.src_container,
            "cloud_provider": temp_proc.cloud_provider,
            "scheduler_backend": os.getenv("SCHEDULER_BACKEND", "standalone"),
        },
    )
    heartbeat.start()
    get_backend().execute(
        run,
        pipeline_stage="extractor",
        pipeline_type="file",
        pipeline_role="consumer",
        component_name=temp_proc.component_name,
    )
