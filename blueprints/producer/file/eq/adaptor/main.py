# Copyright DSI Project
#
# Licensed under the Apache License, Version 2.0 (the "License");
# +---------+----------------------------------------------------------+---------------+-------------+
# | Version | Description                                              | Change Owner  | Change Date |
# +---------+----------------------------------------------------------+---------------+-------------+
# | 1.0.0   | Initial version                                          | DSI Team      | 2026-05-01  |
# | 1.1.0   | Add run(ctx), StepLogger, get_backend() — Kafka trigger  | DSI Team      | 2026-05-26  |
# +---------+----------------------------------------------------------+---------------+-------------+
"""
Adaptor — eq file ingestion.

Polls the eq-stage container for new files, copies each to
the mapper staging container, and publishes a Kafka file-ready event.

v1.1 changes
────────────
* run(ctx) added — the single entry point called by any SchedulerBackend.
* StepLogger wraps every significant operation for structured logs.
* __main__ now uses get_backend() — set SCHEDULER_BACKEND env var to
  control execution mode. Default is "standalone" (one-shot).

  For Kubernetes deployments:
    SCHEDULER_BACKEND=kafka-trigger
    PRODUCT_NAME=eq

  The pod then listens to dpn-pipeline-control and executes one cycle
  each time Airflow publishes a matching trigger message.

All AdaptorFileProcess business logic is UNCHANGED from v1.0.
"""
from __future__ import annotations

import base64
import os

from azure.core.exceptions import AzureError, HttpResponseError
from botocore.exceptions import BotoCoreError, ClientError
from dotenv import load_dotenv
from google.api_core.exceptions import GoogleAPIError

from utils.config_validator import validate_cloud_config, validate_kafka_config
from utils.data_transection import DataTransection
from utils.exception_handler import HandleExceptions
from utils.kafka_transection import KafkaTransection
from utils.otel_logger import OtelLogger as Logging
from utils.pipeline_context import PipelineContext
from utils.scheduler_backend import get_backend
from utils.step_logger import StepLogger

load_dotenv()


# ---------------------------------------------------------------------------
# Business-logic class  (UNCHANGED from v1.0)
# ---------------------------------------------------------------------------

class AdaptorFileProcess:
    """
    File-ingestion adaptor for eq.
    Reads from eq-stage, copies to mapper staging, publishes Kafka events.
    """

    def __init__(self) -> None:
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

    def read_source_file_info(self) -> list[str]:
        source_files = self.data_trans.source_file_info(cloud_provider=self.cloud_provider)
        self.logger.info(
            "Source files discovered",
            extra={"count": len(source_files), "files": source_files},
        )
        return source_files

    def move_files(self, file: str) -> bool:
        result = self.data_trans.file_copy(cloud_vendor=self.cloud_provider, file_name=file)
        self.logger.info(
            "Adaptor file_copy result",
            extra={"file": file, "copied": result.copied, "skipped": result.skipped, "reason": result.reason},
        )
        return result.copied

    def send_to_kafka(self, file_name: str, is_file_move: bool) -> None:
        if is_file_move:
            message = {
                "sourceType":  "S3"
                        if self.cloud_provider.upper() == "AWS"
                        else self.cloud_provider.upper(),
                "storageContainer": self.target_container_name,
                "path":             file_name,
            }
            self.kafka_trans.send_message(target_topic=self.target_kafka_topic, message=message)
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
    adaptor   = AdaptorFileProcess()
    step_log  = StepLogger(adaptor.logger)
    exc_handle = HandleExceptions()

    step_log.pipeline_banner(
        ctx,
        service_name="producer-file-eq-adaptor",
        config_summary={
            "cloudProviderType":   adaptor.cloud_provider,
            "mapperTopicName":     adaptor.target_kafka_topic,
            "srcContainerName":    adaptor.source_container_name,
            "mapperContainerName": adaptor.target_container_name,
            "bootstrapServer":     adaptor.bootstrap_server,
            "PRODUCT_NAME":        os.getenv("PRODUCT_NAME", "eq"),
            "SCHEDULER_BACKEND":   os.getenv("SCHEDULER_BACKEND", "standalone"),
        },
    )

    # Step 1 — discover source files
    step_log.step_start(ctx, "read_source_file_info")
    try:
        source_files = adaptor.read_source_file_info()
        step_log.step_end(ctx, "read_source_file_info", extra={"files_found": len(source_files)})
    except (HttpResponseError, AzureError, ClientError, BotoCoreError, GoogleAPIError, Exception) as exc:
        step_log.step_failed(ctx, "read_source_file_info", exc=exc)
        exc_handle.handle_storage_exception(exc, _provider_label(exc))
        return

    if not source_files:
        adaptor.logger.info(
            "[%s] No files found — nothing to do (run_id=%s)",
            ctx.pipeline_stage, ctx.run_id[:8],
            extra={"event.name": "pipeline.no_files", **ctx.as_log_extra()},
        )
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
    #   PRODUCT_NAME=eq
    #
    #   Pod subscribes to dpn-pipeline-control and runs one cycle each
    #   time Airflow publishes a matching trigger. No new pods created.
    #   Falls back gracefully if Airflow goes down (just stays idle).
    #
    #   SCHEDULER_BACKEND=interval         ← self-scheduling fallback
    #   SCHEDULER_BACKEND=standalone       ← one-shot (default)
    _backend = get_backend()
    _backend.execute(
        run,
        pipeline_stage="adaptor",
        pipeline_type="file",
        pipeline_role="producer",
    )
