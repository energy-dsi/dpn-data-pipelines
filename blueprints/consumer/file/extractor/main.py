"""
Extractor — Consumer File Pipeline

This module implements the file pipeline's single container: discovers
files in the source container and copies them straight to the target
container, publishing one Kafka event per file to targetTopicName.

There is no separate schema_mapper stage any more — the extractor absorbed
it entirely (staging container + mapperTopicName relay removed), the same
way blueprints/producer/{file,topic}/adaptor/main.py absorbed their own
schema mappers. See that changelog for the rationale; the consumer side
never renamed files either (schema_mapper.move_file always wrote back
under the exact incoming name), so merging the two steps changes no
observable file-naming behavior, only removes the intermediate hop.

Responsibilities:
- Poll source container for files
- Copy files directly into the target container
- Publish one Kafka event per copied file to targetTopicName

Version History
---------------
v1.0.0  Initial release
v1.1.0  Added Kafka trigger backend + StepLogger (2026-05-26)
v1.2.0  OTEL Collector Integration (2026-06-26)
v2.0.0  Absorbed schema_mapper — direct source -> target copy, single
        publish to targetTopicName, no staging container/mapper topic
        (2026-09-15)
v2.1.0  Added --run-once support so the self-service UI's Run Now button
        (service/k8s_ops.py's run_once()) actually forces a single cycle
        instead of silently re-entering the kafka-trigger wait loop
        (2026-09-24)

Execution (Kubernetes Recommended)
---------------------------------
SCHEDULER_BACKEND=kafka-trigger
PRODUCT_NAME=consumer-file
"""
# +---------+----------------------------------------------------------+---------------+-------------+
# | Version | Description                                              | Change Owner  | Change Date |
# +---------+----------------------------------------------------------+---------------+-------------+
# | 1.0.0   | Initial version                                          | DSI Team      | 2026-05-01  |
# | 1.1.0   | Kafka Trigger and StepLogger Integration                 | DSI Team      | 2026-05-26  |
# | 1.2.0   | OTEL Collector Integration                               | DSI Team      | 2026-06-26  |
# | 2.0.0   | Simplified data pipeline implementation                  | DSI Team      | 2026-10-11  |
# +---------+----------------------------------------------------------+---------------+-------------+

from __future__ import annotations

import base64
import os
import signal
import sys
import threading
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime

# Cloud Exceptions
from azure.core.exceptions import AzureError, HttpResponseError
from botocore.exceptions import BotoCoreError, ClientError
from google.api_core.exceptions import GoogleAPIError

from dotenv import load_dotenv
from opentelemetry import context as otel_context

# Internal Utilities
from utils.config_validator import validate_cloud_config, validate_kafka_config
from utils.data_transection import DataTransection, gcp_credentials_from_env
from utils.exception_handler import HandleExceptions
from utils.kafka_transection import KafkaTransection
from dpn_observability_sdk.otel_logger import OtelLogger as Logging
from dpn_observability_sdk.otel_tracer import OtelTracer
from dpn_observability_sdk.otel_metrics import OtelMetrics
from dpn_observability_sdk.otel_instrumentation import traced, timed_metric
from dpn_observability_sdk.heartbeat import HeartbeatLogger
from utils.pipeline_context import PipelineContext
from utils.scheduler_backend import get_backend, run_once
from utils.step_logger import StepLogger
from utils.topic_utils import TopicResolver, KafkaTopicManager

# Load environment variables
load_dotenv()


# =============================================================================
# Business Logic: ExtractorFileProcess
# =============================================================================
class ExtractorFileProcess:
    """
    Handles the whole file pipeline in one pass.

    Responsibilities:
    - Discover files from source storage
    - Copy files directly to the target container
    - Publish one Kafka event per file to targetTopicName
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
            service_version="2.0.0"
        )
        self.meter = OtelMetrics.initialize(
            service_name="consumer-file-extractor",
            service_version="2.0.0"
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
        # threading.local(), not a plain attribute: run() now processes
        # multiple files concurrently (see MAX_FILE_COPY_WORKERS below), and
        # a shared instance attribute here would let one thread's context
        # leak into another's in-flight publish_event() call, stamping the
        # wrong file's Kafka message with the wrong trace parent. Each
        # thread gets its own isolated .value instead.
        self._file_span_context = threading.local()

        # ---------------------------------------------------------------------
        # Environment Configuration
        # ---------------------------------------------------------------------
        self.cloud_provider = os.getenv("cloudProviderType", "azure")
        self.kafka_topic = os.getenv("targetTopicName", "")

        # Azure Configuration
        self.src_conn = base64.b64decode(
            os.getenv("srcConnectionString", "")
        ).decode()
        self.tgt_conn = base64.b64decode(
            os.getenv("targetConnectionString", "")
        ).decode()

        self.src_container = os.getenv("srcContainerName", "")
        self.tgt_container = os.getenv("targetContainerName", "")

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
        self.aws_region = os.getenv("AWS_REGION", "eu-west-2")
        self.gcp_service_account_info = gcp_credentials_from_env(
            os.getenv("GCP_SERVICE_ACCOUNT_KEY", "")
        )
        self.gcp_project_id = os.getenv("GCP_PROJECT_ID") or None

        # ---------------------------------------------------------------------
        # Logger Initialization
        # ---------------------------------------------------------------------
        self.logger = Logging().create_logger()

        # ---------------------------------------------------------------------
        # Configuration Validation
        # ---------------------------------------------------------------------
        validate_cloud_config(
            cloud_provider=self.cloud_provider,
            azure_fields=["srcConnectionString", "targetConnectionString"],
            logger=self.logger,
        )
        validate_kafka_config(logger=self.logger, require_mapper_topic=False)

        # Resolve target topic name
        topic_resolver = TopicResolver()
        self.kafka_topic = topic_resolver.resolve(
            self.kafka_topic,
            self.kafka_topic,
            "target"
        )

        # Ensure target topic exists
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
            gcp_service_account_info=self.gcp_service_account_info,
            gcp_project_id=self.gcp_project_id,
            logger=self.logger,
            component_name=self.component_name,
            # Off by default. Flip DEBUG_STORAGE_SDK=true on the Deployment
            # and trigger one Run Now to capture the cloud SDK's own
            # per-request HTTP logging — the way to actually see why a
            # single-file list/copy is taking tens of seconds (default SDK
            # retry/backoff absorbing a transient failure) rather than
            # guessing from duration_ms alone.
            debug_logging=os.getenv("DEBUG_STORAGE_SDK", "").strip().lower() in ("1", "true", "yes"),
        )

        # Ensure both containers exist before anything tries to read/write
        # them — a brand-new self-service product may name a container
        # that doesn't exist yet in the platform storage account.
        self.data_trans.ensure_container_exists(
            self.cloud_provider, self.src_container, side="source"
        )
        self.data_trans.ensure_container_exists(
            self.cloud_provider, self.tgt_container, side="target"
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
        Discover files in the source container and which of them actually
        need copying.

        Fetches each side's full listing (name + last-modified) in ONE call
        per side via DataTransection.list_files_with_timestamps() instead of
        source_file_info() (names only) plus a separate per-file timestamp
        lookup for every single file regardless of whether it changed —
        confirmed live to cost 25-68s PER FILE on nothing more than a
        single-blob container (see this file's v2.2.0 changelog entry and
        DEBUG_STORAGE_SDK). That per-file cost now only applies to files
        that actually turn out to need copying.

        Returns:
            tuple[list[str], list[str]]: (every file name in the source
            container, the subset of those that are new or updated since
            whatever's already in the target).
        """
        with self.tracer.start_as_current_span("list_files") as span:
            span.set_attribute("storage.container", self.src_container)
            span.set_attribute("storage.provider", self.cloud_provider)

            src_meta = self.data_trans.list_files_with_timestamps(
                cloud_provider=self.cloud_provider, side="source"
            )
            tgt_meta = self.data_trans.list_files_with_timestamps(
                cloud_provider=self.cloud_provider, side="target"
            )

            files = list(src_meta.keys())
            pending = [
                name
                for name, src_modified in src_meta.items()
                if self._needs_copy(src_modified, tgt_meta.get(name))
            ]

            self.files_discovered.add(len(files), {
                "cloud_provider": self.cloud_provider,
                "container": self.src_container,
            })

            span.set_attribute("files.discovered", len(files))
            span.set_attribute("files.pending", len(pending))

            self.logger.info(
                "files discovered",
                extra={
                    "count": len(files),
                    "pending": len(pending),
                    "component.name": self.component_name,
                },
            )
            return files, pending

    @staticmethod
    def _needs_copy(src_modified, tgt_modified) -> bool:
        """Mirrors DataTransection._azure_copy_file()'s (and its S3/GCS
        siblings') own gating rule exactly, so this pre-filter's decision
        always agrees with what the real per-file copy would decide:
        target already current only when it exists and is not older than
        source. A missing src_modified defers to the real file_copy() call
        rather than guessing — it already handles "source not found"."""
        if src_modified is None:
            return True
        return tgt_modified is None or src_modified > tgt_modified

    @traced(span_name="process_file")
    def process_file(self, file):
        """
        Copy a file directly from source to the target container.

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
                    dest_file_name=file,
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
                # inject it as the parent trace context for the target span.
                self._file_span_context.value = otel_context.get_current()

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
            span_context = getattr(self._file_span_context, "value", None)
            if span_context is not None:
                token = otel_context.attach(span_context)
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


# Bounds run()'s per-cycle ThreadPoolExecutor — processing more files at once
# than this just queues them instead of speeding anything further up, and
# caps how many concurrent connections this pod opens against the storage
# backend and Kafka at once.
_MAX_FILE_COPY_WORKERS = int(os.getenv("FILE_COPY_MAX_WORKERS", "8"))


def _copy_one(proc: "ExtractorFileProcess", file_name: str, parent_otel_context):
    """
    Run in a worker thread by run()'s ThreadPoolExecutor — does the actual
    per-file work (copy + publish) only. Deliberately does NOT touch
    step_log/exc_h itself: every StepLogger/HandleExceptions call for this
    file happens back in run()'s own thread, in executor.map()'s submission
    order, once this returns — so logging stays single-threaded and in the
    same order it was in before this file's processing was parallelized,
    even though the actual I/O now happens concurrently.

    parent_otel_context is attached explicitly (the same attach()/detach()
    pattern ExtractorFileProcess.publish_event() already uses, not
    contextvars.copy_context().run()) so process_file()'s own span nests
    under run()'s span instead of starting unparented. A plain
    contextvars.Context snapshot cannot be reused this way: entering the
    same Context object from two threads at once raises "cannot enter
    context: ... is already entered" — confirmed by actually running this
    concurrently, not just assumed. OTel's Context, unlike a raw
    contextvars.Context, is an immutable value every thread can safely
    attach() from at the same time.
    """
    token = otel_context.attach(parent_otel_context)
    try:
        ok = proc.process_file(file_name)
        proc.publish_event(file_name, ok)
        return file_name, ok, None
    except Exception as exc:  # noqa: BLE001 - reported to the caller, not raised here
        return file_name, None, exc
    finally:
        otel_context.detach(token)


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
                "targetTopicName": proc.kafka_topic,
                "SCHEDULER_BACKEND": os.getenv("SCHEDULER_BACKEND", "standalone"),
                "PRODUCT_NAME": os.getenv("PRODUCT_NAME", "consumer-file"),
            },
        )

        # -------------------------------------------------------------------------
        # Step 1: File Discovery
        # -------------------------------------------------------------------------
        step_log.step_start(ctx, "list_files")
        try:
            files, pending = proc.list_files()
            step_log.step_end(
                ctx,
                "list_files",
                extra={"files_found": len(files), "files_pending": len(pending)},
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
        # Step 2: File Processing — only files list_files() flagged as
        # pending (new/updated) ever touch process_file(); everything else
        # was already confirmed up-to-date from the single bulk listing
        # above, with zero further per-file network calls.
        # -------------------------------------------------------------------------
        copied = 0
        skipped = len(files) - len(pending)

        if pending:
            for f in pending:
                step_log.step_start(ctx, "process_file", extra={"file": f})

            # Each process_file()/publish_event() call is I/O-bound (network
            # round-trips to the cloud storage/Kafka APIs), so running them
            # concurrently instead of one-at-a-time turns a cycle's total
            # time from "sum of every file's latency" into "roughly the
            # slowest single file's latency" — confirmed live: a 4-file
            # cycle took ~4 minutes sequentially. executor.map() (not
            # as_completed()) deliberately keeps results in the same order
            # pending was submitted in, so step_end/step_failed logging
            # below reads the same as the old sequential loop did.
            # Captured once, in this thread, and reused (via attach()/
            # detach() inside _copy_one, not contextvars.Context.run() —
            # see that function's own docstring for why) so each worker's
            # process_file() span still nests under this function's own
            # span instead of starting an unparented one.
            parent_otel_context = otel_context.get_current()
            max_workers = min(len(pending), _MAX_FILE_COPY_WORKERS)
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                results = executor.map(
                    lambda f: _copy_one(proc, f, parent_otel_context), pending
                )
                for f, ok, exc in results:
                    if exc is not None:
                        step_log.step_failed(
                            ctx, "process_file", exc=exc, extra={"file": f}
                        )
                        exc_h.handle_storage_exception(exc, _provider(exc))
                        continue

                    if ok:
                        copied += 1
                    else:
                        skipped += 1

                    step_log.step_end(
                        ctx,
                        "process_file",
                        extra={"file": f, "ok": ok},
                    )

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

    if "--run-once" in sys.argv:
        # Self-service "Run Now" button (see service/podman_ops.py /
        # k8s_ops.py run_once()): execs this inside the already-running
        # container to force a single cycle on demand, bypassing the
        # scheduler entirely — no heartbeat needed for a process this
        # short-lived.
        run_once(
            run,
            pipeline_stage="extractor",
            pipeline_type="file",
            pipeline_role="consumer",
            component_name=temp_proc.component_name,
        )
    else:
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
            pipeline_type="file",
            pipeline_role="consumer",
            component_name=temp_proc.component_name,
        )
