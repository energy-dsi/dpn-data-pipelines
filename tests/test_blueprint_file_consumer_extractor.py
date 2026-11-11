import base64
import importlib.util
import os
import runpy
import sys
import time
import types
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import Mock

import pytest


SOURCE_PATH = Path(
    os.getenv(
        "CONSUMER_FILE_EXTRACTOR_MAIN_PATH",
        "blueprints/consumer/file/extractor/main.py",
    )
)


def b64(value: str) -> str:
    return base64.b64encode(value.encode("utf-8")).decode("utf-8")


class FakeAzureError(Exception):
    pass


class FakeHttpResponseError(FakeAzureError):
    pass


class FakeBotoCoreError(Exception):
    pass


class FakeClientError(Exception):
    pass


class FakeGoogleAPIError(Exception):
    pass


class FakeCopyResult:
    def __init__(self, copied=True, skipped=False, reason="ok"):
        self.copied = copied
        self.skipped = skipped
        self.reason = reason


class FakeMetric:
    def __init__(self):
        self.calls = []

    def add(self, value, attributes=None):
        self.calls.append(("add", value, attributes))

    def record(self, value, attributes=None):
        self.calls.append(("record", value, attributes))


class FakeMeter:
    def create_counter(self, **kwargs):
        return FakeMetric()

    def create_histogram(self, **kwargs):
        return FakeMetric()


class FakeSpan:
    def __init__(self):
        self.attributes = {}
        self.exceptions = []

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def set_attribute(self, key, value):
        self.attributes[key] = value

    def record_exception(self, exc):
        self.exceptions.append(exc)


class FakeTracer:
    def start_as_current_span(self, name, *args, **kwargs):
        return FakeSpan()


class CapturingTracer:
    """Records every span so tests can assert on attributes and exceptions."""
    def __init__(self):
        self.spans: list = []

    def start_as_current_span(self, name, *args, **kwargs):
        span = FakeSpan()
        self.spans.append(span)
        return span


class FakeLogger:
    def __init__(self):
        self.info_calls = []
        self.error_calls = []
        self.warning_calls = []

    def info(self, message, *args, extra=None, **kwargs):
        self.info_calls.append((message, args, extra, kwargs))

    def error(self, message, *args, extra=None, **kwargs):
        self.error_calls.append((message, args, extra, kwargs))

    def warning(self, message, *args, extra=None, **kwargs):
        self.warning_calls.append((message, args, extra, kwargs))


class FakeDataTransection:
    instances = []
    source_files = ["file1.csv", "file2.csv"]
    source_exception = None
    copy_results = {}
    copy_exception = None
    # list_files_with_timestamps() test knobs — source_timestamps/
    # target_timestamps are optional explicit {name: datetime} overrides;
    # left empty, source defaults to synthesizing a fixed timestamp per
    # name in source_files (so existing tests that only set source_files
    # keep working), and target defaults to {} (nothing in target yet),
    # which makes every source file look "pending" — i.e. every existing
    # test's files all still reach process_file() exactly as before,
    # since none of them populate target_timestamps.
    source_timestamps = {}
    target_timestamps = {}
    list_timestamps_exception = None

    def __init__(self, **kwargs):
        self.kwargs = kwargs
        self.source_file_info_calls = []
        self.file_copy_calls = []
        self.ensure_container_calls = []
        self.list_files_with_timestamps_calls = []
        FakeDataTransection.instances.append(self)

    def ensure_container_exists(self, cloud_provider, container_name, side="source"):
        self.ensure_container_calls.append((cloud_provider, container_name, side))
        return True

    def source_file_info(self, cloud_provider):
        self.source_file_info_calls.append(cloud_provider)

        if FakeDataTransection.source_exception:
            raise FakeDataTransection.source_exception

        return list(FakeDataTransection.source_files)

    def list_files_with_timestamps(self, cloud_provider, side="source"):
        self.list_files_with_timestamps_calls.append((cloud_provider, side))

        if FakeDataTransection.list_timestamps_exception:
            raise FakeDataTransection.list_timestamps_exception

        if side == "target":
            return dict(FakeDataTransection.target_timestamps)

        if FakeDataTransection.source_timestamps:
            return dict(FakeDataTransection.source_timestamps)

        default_ts = datetime(2026, 1, 1, tzinfo=timezone.utc)
        return {name: default_ts for name in FakeDataTransection.source_files}

    def file_copy(self, cloud_vendor, file_name, dest_file_name=None):
        self.file_copy_calls.append((cloud_vendor, file_name, dest_file_name))

        if FakeDataTransection.copy_exception:
            raise FakeDataTransection.copy_exception

        return FakeDataTransection.copy_results.get(
            file_name,
            FakeCopyResult(copied=True, skipped=False, reason="copied"),
        )


class FakeKafkaTransection:
    instances = []

    def __init__(self, bootstrap_server, logger, component_name=None):
        self.bootstrap_server = bootstrap_server
        self.logger = logger
        self.component_name = component_name
        self.sent_messages = []
        FakeKafkaTransection.instances.append(self)

    def send_message(self, target_topic, message):
        self.sent_messages.append((target_topic, message))


class FakeHandleExceptions:
    instances = []

    def __init__(self):
        self.handled = []
        FakeHandleExceptions.instances.append(self)

    def handle_storage_exception(self, exc, provider):
        self.handled.append((exc, provider))


class FakeTopicResolver:
    instances = []

    def __init__(self):
        FakeTopicResolver.instances.append(self)

    def resolve(self, topic_name, src_topic, suffix):
        return topic_name or f"{src_topic}.{suffix}"


class FakeKafkaTopicManager:
    instances = []

    def __init__(self, bootstrap_server, logger):
        self.bootstrap_server = bootstrap_server
        self.logger = logger
        self.ensured_topics = []
        FakeKafkaTopicManager.instances.append(self)

    def ensure_exists(self, topic):
        self.ensured_topics.append(topic)


class FakeStepLogger:
    instances = []

    def __init__(self, logger=None):
        self.logger = logger
        self.pipeline_banner_calls = []
        self.step_start_calls = []
        self.step_end_calls = []
        self.step_failed_calls = []
        FakeStepLogger.instances.append(self)

    def pipeline_banner(self, ctx, service_name, config_summary):
        self.pipeline_banner_calls.append((ctx, service_name, config_summary))

    def step_start(self, ctx, operation, extra=None):
        self.step_start_calls.append((ctx, operation, extra))

    def step_end(self, ctx, operation, extra=None):
        self.step_end_calls.append((ctx, operation, extra))

    def step_failed(self, ctx, operation, exc=None, extra=None):
        self.step_failed_calls.append((ctx, operation, exc, extra))


class FakeHeartbeatLogger:
    instances = []

    def __init__(self, logger, component_name, metadata):
        self.logger = logger
        self.component_name = component_name
        self.metadata = metadata
        self.started = False
        self.stopped = False
        FakeHeartbeatLogger.instances.append(self)

    def start(self):
        self.started = True

    def stop(self):
        self.stopped = True


class FakeBackend:
    instances = []

    def __init__(self):
        self.execute_calls = []
        FakeBackend.instances.append(self)

    def execute(self, func, **kwargs):
        self.execute_calls.append((func, kwargs))


class FakeContext:
    def __init__(
        self,
        triggered_by="manual",
        run_id="run-123456789",
        pipeline_stage="extractor",
    ):
        self.triggered_by = triggered_by
        self.run_id = run_id
        self.pipeline_stage = pipeline_stage

    def as_log_extra(self):
        return {
            "triggered_by": self.triggered_by,
            "run_id": self.run_id,
            "pipeline_stage": self.pipeline_stage,
        }


def identity_decorator(*decorator_args, **decorator_kwargs):
    def wrapper(func):
        return func

    return wrapper


@pytest.fixture(autouse=True)
def fake_external_modules(monkeypatch):
    FakeDataTransection.instances.clear()
    FakeDataTransection.source_files = ["file1.csv", "file2.csv"]
    FakeDataTransection.source_exception = None
    FakeDataTransection.copy_results = {}
    FakeDataTransection.copy_exception = None
    FakeDataTransection.source_timestamps = {}
    FakeDataTransection.target_timestamps = {}
    FakeDataTransection.list_timestamps_exception = None

    FakeKafkaTransection.instances.clear()
    FakeHandleExceptions.instances.clear()
    FakeTopicResolver.instances.clear()
    FakeKafkaTopicManager.instances.clear()
    FakeStepLogger.instances.clear()
    FakeHeartbeatLogger.instances.clear()
    FakeBackend.instances.clear()

    monkeypatch.setenv("cloudProviderType", "azure")
    monkeypatch.setenv("targetTopicName", "target-topic")
    monkeypatch.setenv("srcConnectionString", b64("source-connection"))
    monkeypatch.setenv("targetConnectionString", b64("target-connection"))
    monkeypatch.setenv("srcContainerName", "source-container")
    monkeypatch.setenv("targetContainerName", "target-container")
    monkeypatch.setenv("bootstrapServer", "localhost:9092")
    monkeypatch.setenv("AWS_ENDPOINT_URL", "http://localhost:4566")
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", b64("aws-key"))
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", b64("aws-secret"))
    monkeypatch.setenv("AWS_REGION", "eu-west-2")
    monkeypatch.setenv("PRODUCT_NAME", "consumer-file")
    monkeypatch.setenv("SCHEDULER_BACKEND", "standalone")

    azure_pkg = types.ModuleType("azure")
    azure_core = types.ModuleType("azure.core")
    azure_exceptions = types.ModuleType("azure.core.exceptions")
    azure_exceptions.AzureError = FakeAzureError
    azure_exceptions.HttpResponseError = FakeHttpResponseError

    monkeypatch.setitem(sys.modules, "azure", azure_pkg)
    monkeypatch.setitem(sys.modules, "azure.core", azure_core)
    monkeypatch.setitem(sys.modules, "azure.core.exceptions", azure_exceptions)

    botocore_pkg = types.ModuleType("botocore")
    botocore_exceptions = types.ModuleType("botocore.exceptions")
    botocore_exceptions.BotoCoreError = FakeBotoCoreError
    botocore_exceptions.ClientError = FakeClientError

    monkeypatch.setitem(sys.modules, "botocore", botocore_pkg)
    monkeypatch.setitem(sys.modules, "botocore.exceptions", botocore_exceptions)

    google_pkg = types.ModuleType("google")
    google_api_core = types.ModuleType("google.api_core")
    google_api_core_exceptions = types.ModuleType("google.api_core.exceptions")
    google_api_core_exceptions.GoogleAPIError = FakeGoogleAPIError

    monkeypatch.setitem(sys.modules, "google", google_pkg)
    monkeypatch.setitem(sys.modules, "google.api_core", google_api_core)
    monkeypatch.setitem(
        sys.modules,
        "google.api_core.exceptions",
        google_api_core_exceptions,
    )

    dotenv = types.ModuleType("dotenv")
    dotenv.load_dotenv = Mock()
    monkeypatch.setitem(sys.modules, "dotenv", dotenv)

    utils_pkg = types.ModuleType("utils")

    config_validator = types.ModuleType("utils.config_validator")
    config_validator.validate_cloud_config = Mock()
    config_validator.validate_kafka_config = Mock()

    data_transection = types.ModuleType("utils.data_transection")
    data_transection.DataTransection = FakeDataTransection
    data_transection.gcp_credentials_from_env = lambda *_a, **_k: None

    exception_handler = types.ModuleType("utils.exception_handler")
    exception_handler.HandleExceptions = FakeHandleExceptions

    kafka_transection = types.ModuleType("utils.kafka_transection")
    kafka_transection.KafkaTransection = FakeKafkaTransection

    pipeline_context = types.ModuleType("utils.pipeline_context")
    pipeline_context.PipelineContext = FakeContext

    scheduler_backend = types.ModuleType("utils.scheduler_backend")
    scheduler_backend.get_backend = Mock(return_value=FakeBackend())
    scheduler_backend.run_once = Mock()

    step_logger = types.ModuleType("utils.step_logger")
    step_logger.StepLogger = FakeStepLogger

    topic_utils = types.ModuleType("utils.topic_utils")
    topic_utils.TopicResolver = FakeTopicResolver
    topic_utils.KafkaTopicManager = FakeKafkaTopicManager

    monkeypatch.setitem(sys.modules, "utils", utils_pkg)
    monkeypatch.setitem(sys.modules, "utils.config_validator", config_validator)
    monkeypatch.setitem(sys.modules, "utils.data_transection", data_transection)
    monkeypatch.setitem(sys.modules, "utils.exception_handler", exception_handler)
    monkeypatch.setitem(sys.modules, "utils.kafka_transection", kafka_transection)
    monkeypatch.setitem(sys.modules, "utils.pipeline_context", pipeline_context)
    monkeypatch.setitem(sys.modules, "utils.scheduler_backend", scheduler_backend)
    monkeypatch.setitem(sys.modules, "utils.step_logger", step_logger)
    monkeypatch.setitem(sys.modules, "utils.topic_utils", topic_utils)

    sdk_pkg = types.ModuleType("dpn_observability_sdk")

    otel_logger = types.ModuleType("dpn_observability_sdk.otel_logger")
    otel_logger.OtelLogger = Mock(
        return_value=types.SimpleNamespace(
            create_logger=Mock(return_value=FakeLogger())
        )
    )

    otel_tracer = types.ModuleType("dpn_observability_sdk.otel_tracer")
    fake_tracer = FakeTracer()
    otel_tracer.OtelTracer = types.SimpleNamespace(
        initialize=Mock(return_value=fake_tracer),
        get_tracer=Mock(return_value=fake_tracer),
    )

    otel_metrics = types.ModuleType("dpn_observability_sdk.otel_metrics")
    otel_metrics.OtelMetrics = types.SimpleNamespace(
        initialize=Mock(return_value=FakeMeter())
    )

    otel_instrumentation = types.ModuleType(
        "dpn_observability_sdk.otel_instrumentation"
    )
    otel_instrumentation.traced = identity_decorator
    otel_instrumentation.timed_metric = identity_decorator

    heartbeat = types.ModuleType("dpn_observability_sdk.heartbeat")
    heartbeat.HeartbeatLogger = FakeHeartbeatLogger

    monkeypatch.setitem(sys.modules, "dpn_observability_sdk", sdk_pkg)
    monkeypatch.setitem(sys.modules, "dpn_observability_sdk.otel_logger", otel_logger)
    monkeypatch.setitem(sys.modules, "dpn_observability_sdk.otel_tracer", otel_tracer)
    monkeypatch.setitem(sys.modules, "dpn_observability_sdk.otel_metrics", otel_metrics)
    monkeypatch.setitem(
        sys.modules,
        "dpn_observability_sdk.otel_instrumentation",
        otel_instrumentation,
    )
    monkeypatch.setitem(sys.modules, "dpn_observability_sdk.heartbeat", heartbeat)


@pytest.fixture
def load_module():
    def _load():
        if not SOURCE_PATH.exists():
            pytest.skip(
                f"Source file not found: {SOURCE_PATH}. "
                "Run pytest from repo root or set CONSUMER_FILE_EXTRACTOR_MAIN_PATH."
            )

        module_name = "consumer_file_extractor_main_under_test"

        if module_name in sys.modules:
            del sys.modules[module_name]

        spec = importlib.util.spec_from_file_location(module_name, SOURCE_PATH)
        module = importlib.util.module_from_spec(spec)
        sys.modules[module_name] = module
        spec.loader.exec_module(module)
        return module

    return _load


def test_extractor_initialises_config_validates_topic_and_clients(load_module):
    module = load_module()

    proc = module.ExtractorFileProcess()

    assert proc.cloud_provider == "azure"
    assert proc.kafka_topic == "target-topic"
    assert proc.src_conn == "source-connection"
    assert proc.tgt_conn == "target-connection"
    assert proc.src_container == "source-container"
    assert proc.tgt_container == "target-container"
    assert proc.bootstrap == "localhost:9092"
    assert proc.aws_endpoint == "http://localhost:4566"
    assert proc.aws_key_id == "aws-key"
    assert proc.aws_secret == "aws-secret"
    assert proc.aws_region == "eu-west-2"
    assert proc.heartbeat is None

    module.validate_cloud_config.assert_called_once()
    _, cloud_kwargs = module.validate_cloud_config.call_args
    assert cloud_kwargs["azure_fields"] == ["srcConnectionString", "targetConnectionString"]

    module.validate_kafka_config.assert_called_once()
    _, kafka_kwargs = module.validate_kafka_config.call_args
    assert kafka_kwargs["require_mapper_topic"] is False

    topic_manager = FakeKafkaTopicManager.instances[-1]
    assert topic_manager.bootstrap_server == "localhost:9092"
    assert topic_manager.ensured_topics == ["target-topic"]

    data_trans = FakeDataTransection.instances[-1]
    assert data_trans.kwargs["source_azure_conn_str"] == "source-connection"
    assert data_trans.kwargs["target_azure_conn_str"] == "target-connection"
    assert data_trans.kwargs["source_container_name"] == "source-container"
    assert data_trans.kwargs["target_container_name"] == "target-container"

    kafka_trans = FakeKafkaTransection.instances[-1]
    assert kafka_trans.bootstrap_server == "localhost:9092"


def test_extractor_initialises_defaults_for_optional_env(load_module, monkeypatch):
    monkeypatch.delenv("cloudProviderType", raising=False)
    monkeypatch.delenv("AWS_ENDPOINT_URL", raising=False)
    monkeypatch.delenv("AWS_REGION", raising=False)

    module = load_module()

    proc = module.ExtractorFileProcess()

    assert proc.cloud_provider == "azure"
    assert proc.aws_endpoint is None
    # Default changed from us-east-1 to eu-west-2 in the v2.0.0 merge.
    assert proc.aws_region == "eu-west-2"


def test_extractor_resolves_empty_target_topic_with_suffix(load_module, monkeypatch):
    monkeypatch.setenv("targetTopicName", "")

    module = load_module()

    proc = module.ExtractorFileProcess()

    assert proc.kafka_topic == ".target"
    assert FakeKafkaTopicManager.instances[-1].ensured_topics == [".target"]


def test_list_files_returns_files_records_metric_and_logs(load_module):
    module = load_module()
    proc = module.ExtractorFileProcess()

    FakeDataTransection.source_files = ["a.csv", "b.csv", "c.csv"]

    files, pending = proc.list_files()

    assert files == ["a.csv", "b.csv", "c.csv"]
    # target_timestamps defaults to {} (nothing in target yet) -> every
    # source file is pending.
    assert pending == ["a.csv", "b.csv", "c.csv"]
    assert proc.data_trans.list_files_with_timestamps_calls == [
        ("azure", "source"),
        ("azure", "target"),
    ]

    assert proc.files_discovered.calls[-1] == (
        "add",
        3,
        {
            "cloud_provider": "azure",
            "container": "source-container",
        },
    )

    assert proc.logger.info_calls[-1][0] == "files discovered"
    assert proc.logger.info_calls[-1][2]["count"] == 3


def test_needs_copy_mirrors_data_transection_gating_rule(load_module):
    module = load_module()
    older = datetime(2026, 1, 1, tzinfo=timezone.utc)
    newer = datetime(2026, 1, 2, tzinfo=timezone.utc)

    needs_copy = module.ExtractorFileProcess._needs_copy

    assert needs_copy(None, None) is True  # unknown source timestamp -> defer to file_copy()
    assert needs_copy(None, newer) is True
    assert needs_copy(newer, None) is True  # target missing entirely -> new file
    assert needs_copy(newer, older) is True  # source newer -> updated
    assert needs_copy(older, newer) is False  # source older -> already up-to-date
    assert needs_copy(older, older) is False  # equal -> already up-to-date


def test_list_files_skips_already_up_to_date_target_files(load_module):
    module = load_module()
    proc = module.ExtractorFileProcess()

    older = datetime(2026, 1, 1, tzinfo=timezone.utc)
    newer = datetime(2026, 1, 2, tzinfo=timezone.utc)

    FakeDataTransection.source_timestamps = {
        "stale.csv": older,       # target has a newer/equal copy -> skip
        "updated.csv": newer,     # target has an older copy -> pending
        "new.csv": newer,         # target has nothing -> pending
    }
    FakeDataTransection.target_timestamps = {
        "stale.csv": newer,
        "updated.csv": older,
    }

    files, pending = proc.list_files()

    assert sorted(files) == ["new.csv", "stale.csv", "updated.csv"]
    assert sorted(pending) == ["new.csv", "updated.csv"]


def test_process_file_success_records_success_metric_and_returns_true(load_module):
    module = load_module()
    proc = module.ExtractorFileProcess()

    FakeDataTransection.copy_results = {
        "file1.csv": FakeCopyResult(copied=True, skipped=False)
    }

    result = proc.process_file("file1.csv")

    assert result is True
    # Direct source -> target copy, filename unchanged (dest == source name).
    assert proc.data_trans.file_copy_calls == [("azure", "file1.csv", "file1.csv")]

    assert proc.files_processed.calls[-1][0] == "add"
    assert proc.files_processed.calls[-1][2]["status"] == "success"

    assert proc.file_copy_duration.calls[-1][0] == "record"

    assert proc.logger.info_calls[-1][0] == "file_copy"
    assert proc.logger.info_calls[-1][2]["copied"] is True


def test_process_file_skipped_records_skipped_metric_and_returns_false(load_module):
    module = load_module()
    proc = module.ExtractorFileProcess()

    FakeDataTransection.copy_results = {
        "file1.csv": FakeCopyResult(copied=False, skipped=True)
    }

    result = proc.process_file("file1.csv")

    assert result is False
    assert proc.files_processed.calls[-1][2]["status"] == "skipped"
    assert proc.logger.info_calls[-1][2]["copied"] is False


def test_process_file_exception_records_error_metric_and_reraises(load_module):
    module = load_module()
    proc = module.ExtractorFileProcess()

    FakeDataTransection.copy_exception = RuntimeError("copy failed")

    with pytest.raises(RuntimeError, match="copy failed"):
        proc.process_file("bad.csv")

    assert proc.files_processed.calls[-1][2]["status"] == "error"


def test_publish_event_when_ok_for_azure(load_module):
    module = load_module()
    proc = module.ExtractorFileProcess()

    proc.publish_event("file1.csv", True)

    kafka_trans = FakeKafkaTransection.instances[-1]

    assert kafka_trans.sent_messages == [
        (
            "target-topic",
            {
                "sourceType": "AZURE",
                "storageContainer": "target-container",
                "path": "file1.csv",
            },
        )
    ]


def test_publish_event_when_aws_maps_source_type_to_s3(load_module, monkeypatch):
    monkeypatch.setenv("cloudProviderType", "AWS")

    module = load_module()
    proc = module.ExtractorFileProcess()

    proc.publish_event("file1.csv", True)

    kafka_trans = FakeKafkaTransection.instances[-1]
    assert kafka_trans.sent_messages[-1][1]["sourceType"] == "S3"


def test_publish_event_when_not_ok_does_nothing(load_module):
    module = load_module()
    proc = module.ExtractorFileProcess()

    proc.publish_event("file1.csv", False)

    kafka_trans = FakeKafkaTransection.instances[-1]
    assert kafka_trans.sent_messages == []


def test_provider_returns_expected_values(load_module):
    module = load_module()

    assert module._provider(FakeHttpResponseError("http")) == "Azure"
    assert module._provider(FakeAzureError("azure")) == "Azure"
    assert module._provider(FakeClientError("client")) == "AWS S3"
    assert module._provider(FakeBotoCoreError("boto")) == "AWS S3"
    assert module._provider(FakeGoogleAPIError("google")) == "GCP"
    assert module._provider(RuntimeError("other")) == ""


def test_run_no_files_sets_no_files_and_returns(load_module):
    module = load_module()
    FakeDataTransection.source_files = []

    ctx = FakeContext(run_id="run-empty-123456")

    module.run(ctx)

    step_log = FakeStepLogger.instances[-1]

    assert step_log.pipeline_banner_calls
    assert step_log.step_start_calls[-1][1] == "list_files"
    assert step_log.step_end_calls[-1][1] == "list_files"

    data_trans = FakeDataTransection.instances[-1]
    assert data_trans.list_files_with_timestamps_calls == [
        ("azure", "source"),
        ("azure", "target"),
    ]


def test_run_list_files_exception_is_handled_and_returns(load_module):
    module = load_module()

    FakeDataTransection.list_timestamps_exception = FakeHttpResponseError("azure failed")
    ctx = FakeContext()

    module.run(ctx)

    step_log = FakeStepLogger.instances[-1]
    exc_handler = FakeHandleExceptions.instances[-1]

    assert step_log.step_failed_calls[-1][1] == "list_files"
    assert isinstance(exc_handler.handled[-1][0], FakeHttpResponseError)
    assert exc_handler.handled[-1][1] == "Azure"


def test_run_processes_copied_and_skipped_files(load_module):
    module = load_module()

    FakeDataTransection.source_files = ["copied.csv", "skipped.csv"]
    FakeDataTransection.copy_results = {
        "copied.csv": FakeCopyResult(copied=True, skipped=False),
        "skipped.csv": FakeCopyResult(copied=False, skipped=True),
    }

    ctx = FakeContext(run_id="run-success-123456")

    module.run(ctx)

    step_log = FakeStepLogger.instances[-1]
    kafka_trans = FakeKafkaTransection.instances[-1]

    process_ends = [
        call for call in step_log.step_end_calls if call[1] == "process_file"
    ]

    assert len(process_ends) == 2
    assert process_ends[0][2]["ok"] is True
    assert process_ends[1][2]["ok"] is False

    assert kafka_trans.sent_messages == [
        (
            "target-topic",
            {
                "sourceType": "AZURE",
                "storageContainer": "target-container",
                "path": "copied.csv",
            },
        )
    ]


def test_run_processes_pending_files_concurrently_not_sequentially(load_module):
    """The whole point of this change: proves run() actually overlaps
    multiple files' I/O instead of merely returning correct results that
    happen to also be achievable sequentially."""
    module = load_module()

    FakeDataTransection.source_files = ["a.csv", "b.csv", "c.csv", "d.csv"]
    sleep_seconds = 0.15

    original_file_copy = FakeDataTransection.file_copy

    def slow_file_copy(self, cloud_vendor, file_name, dest_file_name=None):
        time.sleep(sleep_seconds)
        return original_file_copy(self, cloud_vendor, file_name, dest_file_name)

    FakeDataTransection.file_copy = slow_file_copy
    try:
        start = time.monotonic()
        module.run(FakeContext())
        elapsed = time.monotonic() - start
    finally:
        FakeDataTransection.file_copy = original_file_copy

    # Sequential would take >= 4 * 0.15s = 0.6s. Concurrent (default
    # FILE_COPY_MAX_WORKERS=8, so all 4 run at once) should land close to
    # one sleep_seconds plus scheduling overhead — comfortably under half
    # the sequential total even with generous CI jitter margin.
    assert elapsed < sleep_seconds * 2.5

    step_log = FakeStepLogger.instances[-1]
    process_ends = [c for c in step_log.step_end_calls if c[1] == "process_file"]
    assert len(process_ends) == 4


def test_run_process_file_exception_is_handled_and_continues(load_module):
    module = load_module()

    FakeDataTransection.source_files = ["bad.csv"]
    FakeDataTransection.copy_exception = FakeClientError("aws failed")

    ctx = FakeContext()

    module.run(ctx)

    step_log = FakeStepLogger.instances[-1]
    exc_handler = FakeHandleExceptions.instances[-1]

    assert step_log.step_failed_calls[-1][1] == "process_file"
    assert step_log.step_failed_calls[-1][3] == {"file": "bad.csv"}
    assert isinstance(exc_handler.handled[-1][0], FakeClientError)
    assert exc_handler.handled[-1][1] == "AWS S3"


def test_main_block_creates_heartbeat_and_executes_backend():
    if not SOURCE_PATH.exists():
        pytest.skip(
            f"Source file not found: {SOURCE_PATH}. "
            "Run pytest from repo root or set CONSUMER_FILE_EXTRACTOR_MAIN_PATH."
        )

    runpy.run_path(str(SOURCE_PATH), run_name="__main__")

    assert FakeHeartbeatLogger.instances
    heartbeat = FakeHeartbeatLogger.instances[-1]

    assert heartbeat.started is True
    assert heartbeat.component_name == "consumer-file-extractor"
    assert heartbeat.metadata == {
        "kafka_topic": "target-topic",
        "src_container": "source-container",
        "cloud_provider": "azure",
        "scheduler_backend": "standalone",
    }

    assert FakeBackend.instances
    backend = FakeBackend.instances[-1]

    assert backend.execute_calls
    func, kwargs = backend.execute_calls[-1]

    assert callable(func)
    assert kwargs == {
        "pipeline_stage": "extractor",
        "pipeline_type": "file",
        "pipeline_role": "consumer",
        "component_name": "consumer-file-extractor",
    }


def test_main_block_run_once_flag_calls_run_once_and_skips_heartbeat(monkeypatch):
    if not SOURCE_PATH.exists():
        pytest.skip(
            f"Source file not found: {SOURCE_PATH}. "
            "Run pytest from repo root or set CONSUMER_FILE_EXTRACTOR_MAIN_PATH."
        )

    monkeypatch.setattr(sys, "argv", [sys.argv[0], "--run-once"])

    runpy.run_path(str(SOURCE_PATH), run_name="__main__")

    scheduler_backend_mod = sys.modules["utils.scheduler_backend"]
    assert scheduler_backend_mod.run_once.called

    func = scheduler_backend_mod.run_once.call_args[0][0]
    kwargs = scheduler_backend_mod.run_once.call_args[1]

    assert callable(func)
    assert kwargs == {
        "pipeline_stage": "extractor",
        "pipeline_type": "file",
        "pipeline_role": "consumer",
        "component_name": "consumer-file-extractor",
    }

    # The --run-once CLI path never spins up the heartbeat/backend loop.
    assert not FakeHeartbeatLogger.instances
    assert not FakeBackend.instances[-1].execute_calls


def test_main_block_sigterm_handler_stops_heartbeat_and_exits(monkeypatch):
    if not SOURCE_PATH.exists():
        pytest.skip(
            f"Source file not found: {SOURCE_PATH}. "
            "Run pytest from repo root or set CONSUMER_FILE_EXTRACTOR_MAIN_PATH."
        )

    import signal as signal_module

    captured_handlers = {}

    def fake_signal(signum, handler):
        captured_handlers[signum] = handler

    monkeypatch.setattr(signal_module, "signal", fake_signal)

    runpy.run_path(str(SOURCE_PATH), run_name="__main__")

    handler = captured_handlers[signal_module.SIGTERM]
    heartbeat = FakeHeartbeatLogger.instances[-1]
    assert heartbeat.started is True
    assert heartbeat.stopped is False

    with pytest.raises(SystemExit) as exc_info:
        handler(signal_module.SIGTERM, None)

    assert exc_info.value.code == 0
    assert heartbeat.stopped is True


# =============================================================================
# list_files — empty result path
# =============================================================================

def test_list_files_returns_empty_list_and_records_zero_count(load_module):
    module = load_module()
    proc = module.ExtractorFileProcess()

    FakeDataTransection.source_files = []

    files, pending = proc.list_files()

    assert files == []
    assert pending == []
    assert proc.files_discovered.calls[-1] == (
        "add",
        0,
        {"cloud_provider": "azure", "container": "source-container"},
    )
    assert proc.logger.info_calls[-1][2]["count"] == 0


# =============================================================================
# process_file — span attribute assertions
# =============================================================================

def test_process_file_sets_span_attributes_on_success(load_module):
    module = load_module()
    proc = module.ExtractorFileProcess()

    capturing = CapturingTracer()
    proc.tracer = capturing

    FakeDataTransection.copy_results = {"x.csv": FakeCopyResult(copied=True)}
    proc.process_file("x.csv")

    span = capturing.spans[-1]
    assert span.attributes["file.name"] == "x.csv"
    assert span.attributes["storage.source_container"] == "source-container"
    assert span.attributes["storage.target_container"] == "target-container"
    assert span.attributes["file.copied"] is True
    assert span.attributes["process.status"] == "success"
    assert "process.duration_ms" in span.attributes


def test_process_file_sets_span_attributes_on_exception(load_module):
    module = load_module()
    proc = module.ExtractorFileProcess()

    capturing = CapturingTracer()
    proc.tracer = capturing

    FakeDataTransection.copy_exception = ValueError("disk full")

    with pytest.raises(ValueError):
        proc.process_file("bad.csv")

    span = capturing.spans[-1]
    assert span.attributes["process.status"] == "error"
    assert span.attributes["error.type"] == "ValueError"
    assert len(span.exceptions) == 1


# =============================================================================
# publish_event — GCP provider uses provider.upper() (else branch of ternary)
# =============================================================================

def test_publish_event_ok_true_gcp_uses_provider_upper(load_module, monkeypatch):
    monkeypatch.setenv("cloudProviderType", "gcp")

    module = load_module()
    proc = module.ExtractorFileProcess()

    proc.publish_event("report.csv", True)

    kafka_trans = FakeKafkaTransection.instances[-1]
    assert kafka_trans.sent_messages[-1][1]["sourceType"] == "GCP"


# =============================================================================
# run() — span attribute assertions for all exit paths
# =============================================================================

def test_run_sets_success_span_attributes(load_module):
    module = load_module()

    FakeDataTransection.source_files = ["a.csv", "b.csv"]
    FakeDataTransection.copy_results = {
        "a.csv": FakeCopyResult(copied=True),
        "b.csv": FakeCopyResult(copied=False, skipped=True),
    }

    capturing = CapturingTracer()
    module.OtelTracer.get_tracer = Mock(return_value=capturing)

    ctx = FakeContext(triggered_by="kafka-trigger", run_id="run-span-123456")
    module.run(ctx)

    span = capturing.spans[-1]
    assert span.attributes["pipeline.type"] == "consumer-file-extractor"
    assert span.attributes["pipeline.triggered_by"] == "kafka-trigger"
    assert span.attributes["pipeline.run_id"] == "run-span-123456"
    assert span.attributes["pipeline.status"] == "success"
    assert span.attributes["pipeline.files.copied"] == 1
    assert span.attributes["pipeline.files.skipped"] == 1


def test_run_sets_error_span_attributes_on_list_files_failure(load_module):
    module = load_module()

    FakeDataTransection.list_timestamps_exception = FakeBotoCoreError("s3 down")

    capturing = CapturingTracer()
    module.OtelTracer.get_tracer = Mock(return_value=capturing)

    module.run(FakeContext())

    span = capturing.spans[-1]
    assert span.attributes["pipeline.status"] == "error"
    assert span.attributes["error.type"] == "FakeBotoCoreError"
    assert len(span.exceptions) == 1


def test_run_sets_no_files_span_attribute(load_module):
    module = load_module()

    FakeDataTransection.source_files = []

    capturing = CapturingTracer()
    module.OtelTracer.get_tracer = Mock(return_value=capturing)

    module.run(FakeContext())

    span = capturing.spans[-1]
    assert span.attributes["pipeline.status"] == "no_files"


# =============================================================================
# run() — mixed outcome cycle (success + skip + exception in same loop)
# =============================================================================

def test_run_mixed_success_skip_and_exception_in_same_cycle(load_module):
    module = load_module()

    FakeDataTransection.source_files = ["good.csv", "skip.csv", "bad.csv"]

    original_copy = FakeDataTransection.file_copy

    def mixed_copy(self, cloud_vendor, file_name, dest_file_name=None):
        if file_name == "good.csv":
            return FakeCopyResult(copied=True)
        if file_name == "skip.csv":
            return FakeCopyResult(copied=False, skipped=True)
        raise FakeGoogleAPIError("gcp error")

    FakeDataTransection.file_copy = mixed_copy

    ctx = FakeContext(run_id="run-mixed-12345678")
    module.run(ctx)

    step_log = FakeStepLogger.instances[-1]
    exc_handler = FakeHandleExceptions.instances[-1]
    kafka_trans = FakeKafkaTransection.instances[-1]

    ended = [c for c in step_log.step_end_calls if c[1] == "process_file"]
    assert len(ended) == 2
    assert ended[0][2]["ok"] is True
    assert ended[1][2]["ok"] is False

    failed = [c for c in step_log.step_failed_calls if c[1] == "process_file"]
    assert len(failed) == 1
    assert failed[0][3] == {"file": "bad.csv"}
    assert exc_handler.handled[-1][1] == "GCP"

    assert len(kafka_trans.sent_messages) == 1
    assert kafka_trans.sent_messages[0][1]["path"] == "good.csv"

    FakeDataTransection.file_copy = original_copy


# =============================================================================
# run() — pipeline banner config_summary values
# =============================================================================

def test_run_pipeline_banner_includes_correct_config_summary(load_module, monkeypatch):
    monkeypatch.setenv("SCHEDULER_BACKEND", "kafka-trigger")
    monkeypatch.setenv("PRODUCT_NAME", "consumer-file-prod")

    module = load_module()
    FakeDataTransection.source_files = []

    ctx = FakeContext()
    module.run(ctx)

    step_log = FakeStepLogger.instances[-1]
    _, service_name, config = step_log.pipeline_banner_calls[0]

    assert service_name == "consumer-file-extractor"
    assert config["cloudProviderType"] == "azure"
    assert config["targetTopicName"] == "target-topic"
    assert config["SCHEDULER_BACKEND"] == "kafka-trigger"
    assert config["PRODUCT_NAME"] == "consumer-file-prod"


# =============================================================================
# run() — list_files unknown exception maps to empty provider string
# =============================================================================

def test_run_list_files_unknown_exception_maps_to_empty_provider(load_module):
    module = load_module()

    FakeDataTransection.list_timestamps_exception = RuntimeError("unexpected")

    module.run(FakeContext())

    exc_handler = FakeHandleExceptions.instances[-1]
    assert exc_handler.handled[-1][1] == ""