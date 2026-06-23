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

# Cloud Exceptions
from azure.core.exceptions import AzureError, HttpResponseError
from botocore.exceptions import BotoCoreError, ClientError
from google.api_core.exceptions import GoogleAPIError

from dotenv import load_dotenv

# Internal Utilities
from utils.config_validator import validate_cloud_config, validate_kafka_config
from utils.data_transection import DataTransection
from utils.exception_handler import HandleExceptions
from utils.kafka_transection import KafkaTransection
from utils.otel_logger import OtelLogger as Logging
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
        )

        self.kafka_trans = KafkaTransection(
            bootstrap_server=self.bootstrap,
            logger=self.logger,
        )

    # -------------------------------------------------------------------------
    # File Operations
    # -------------------------------------------------------------------------
    def list_files(self):
        """
        Retrieve list of files from the source container.

        Returns:
            list: List of file names.
        """
        files = self.data_trans.source_file_info(
            cloud_provider=self.cloud_provider
        )

        self.logger.info(
            "files discovered",
            extra={"count": len(files)},
        )
        return files

    def process_file(self, file):
        """
        Copy a file from source to target staging.

        Args:
            file (str): File name.

        Returns:
            bool: True if file copied successfully, else False.
        """
        result = self.data_trans.file_copy(
            cloud_vendor=self.cloud_provider,
            file_name=file,
        )

        self.logger.info(
            "file_copy",
            extra={"file": file, "copied": result.copied},
        )

        return result.copied

    def publish_event(self, file_name, ok):
        """
        Publish Kafka event for successfully processed files.

        Args:
            file_name (str): File name
            ok (bool): Indicates if processing succeeded
        """
        if ok:
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
def run(ctx: PipelineContext) -> None:
    """
    Execute one extractor pipeline cycle.

    Args:
        ctx (PipelineContext): Pipeline execution context
    """
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
    get_backend().execute(
        run,
        pipeline_stage="extractor",
        pipeline_type="file",
        pipeline_role="consumer",
    )