import base64
import importlib.util
import os
import sys
import types
from pathlib import Path
from unittest.mock import Mock

import pytest


SOURCE_PATH = Path(
    os.getenv(
        "FILE_ADAPTOR_MAIN_PATH",
        "blueprints/producer/file/ssh/adaptor/main.py",
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
    def start_as_current_span(self, name):
        return FakeSpan()


class FakeLogger:
    def __init__(self):
        self.info_calls = []
        self.error_calls = []

    def info(self, message, *args, extra=None, **kwargs):
        self.info_calls.append((message, args, extra, kwargs))

    def error(self, message, *args, extra=None, **kwargs):
        self.error_calls.append((message, args, extra, kwargs))


class FakeDataTransection:
    instances = []
    source_files = ["file1.csv", "file2.csv"]
    source_exception = None
    copy_results = {}
    copy_exception = None

    def __init__(self, **kwargs):
        self.kwargs = kwargs
        self.source_file_info_calls = []
        self.file_copy_calls = []
        FakeDataTransection.instances.append(self)

    def source_file_info(self, cloud_provider):
        self.source_file_info_calls.append(cloud_provider)

        if FakeDataTransection.source_exception:
            raise FakeDataTransection.source_exception

        return list(FakeDataTransection.source_files)

    def file_copy(self, cloud_vendor, file_name):
        self.file_copy_calls.append((cloud_vendor, file_name))

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
        FakeHeartbeatLogger.instances.append(self)

    def start(self):
        self.started = True


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
        pipeline_stage="adaptor",
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

    FakeKafkaTransection.instances.clear()
    FakeHandleExceptions.instances.clear()
    FakeStepLogger.instances.clear()
    FakeHeartbeatLogger.instances.clear()
    FakeBackend.instances.clear()

    monkeypatch.setenv("cloudProviderType", "azure")
    monkeypatch.setenv("mapperTopicName", "mapper-topic")
    monkeypatch.setenv("srcConnectionString", b64("source-connection"))
    monkeypatch.setenv("srcContainerName", "source-container")
    monkeypatch.setenv("mapperConnectionString", b64("target-connection"))
    monkeypatch.setenv("mapperContainerName", "target-container")
    monkeypatch.setenv("bootstrapServer", "localhost:9092")
    monkeypatch.setenv("AWS_ENDPOINT_URL", "http://localhost:4566")
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", b64("aws-key"))
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", b64("aws-secret"))
    monkeypatch.setenv("AWS_REGION", "eu-west-2")
    monkeypatch.setenv("PRODUCT_NAME", "bp-natural-gas")
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

    otel = types.ModuleType("opentelemetry")
    otel_context = types.ModuleType("opentelemetry.context")
    otel_propagate = types.ModuleType("opentelemetry.propagate")

    otel_context.attach = Mock(return_value="fake-token")
    otel_context.get_current = Mock(return_value="fake-current-context")
    otel_context.detach = Mock()
    otel_propagate.extract = Mock(return_value={"remote": "context"})
    otel_propagate.inject = Mock()

    otel.context = otel_context
    otel.propagate = otel_propagate

    monkeypatch.setitem(sys.modules, "opentelemetry", otel)
    monkeypatch.setitem(sys.modules, "opentelemetry.context", otel_context)
    monkeypatch.setitem(sys.modules, "opentelemetry.propagate", otel_propagate)

    utils_pkg = types.ModuleType("utils")

    config_validator = types.ModuleType("utils.config_validator")
    config_validator.validate_cloud_config = Mock()
    config_validator.validate_kafka_config = Mock()

    data_transection = types.ModuleType("utils.data_transection")
    data_transection.DataTransection = FakeDataTransection

    exception_handler = types.ModuleType("utils.exception_handler")
    exception_handler.HandleExceptions = FakeHandleExceptions

    kafka_transection = types.ModuleType("utils.kafka_transection")
    kafka_transection.KafkaTransection = FakeKafkaTransection

    pipeline_context = types.ModuleType("utils.pipeline_context")
    pipeline_context.PipelineContext = FakeContext

    scheduler_backend = types.ModuleType("utils.scheduler_backend")
    scheduler_backend.get_backend = Mock(return_value=FakeBackend())

    step_logger = types.ModuleType("utils.step_logger")
    step_logger.StepLogger = FakeStepLogger

    monkeypatch.setitem(sys.modules, "utils", utils_pkg)
    monkeypatch.setitem(sys.modules, "utils.config_validator", config_validator)
    monkeypatch.setitem(sys.modules, "utils.data_transection", data_transection)
    monkeypatch.setitem(sys.modules, "utils.exception_handler", exception_handler)
    monkeypatch.setitem(sys.modules, "utils.kafka_transection", kafka_transection)
    monkeypatch.setitem(sys.modules, "utils.pipeline_context", pipeline_context)
    monkeypatch.setitem(sys.modules, "utils.scheduler_backend", scheduler_backend)
    monkeypatch.setitem(sys.modules, "utils.step_logger", step_logger)

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
                "Run pytest from repo root or set FILE_ADAPTOR_MAIN_PATH."
            )

        module_name = "file_adaptor_main_under_test"

        if module_name in sys.modules:
            del sys.modules[module_name]

        spec = importlib.util.spec_from_file_location(module_name, SOURCE_PATH)
        module = importlib.util.module_from_spec(spec)
        sys.modules[module_name] = module
        spec.loader.exec_module(module)
        return module

    return _load


def test_adaptor_initialises_config_validates_and_creates_transactions(load_module):
    module = load_module()

    adaptor = module.AdaptorFileProcess()

    assert adaptor.cloud_provider == "azure"
    assert adaptor.target_kafka_topic == "mapper-topic"
    assert adaptor.source_azure_conn_str == "source-connection"
    assert adaptor.source_container_name == "source-container"
    assert adaptor.target_container_name == "target-container"
    assert adaptor.bootstrap_server == "localhost:9092"
    assert adaptor.target_azure_conn_str == "target-connection"
    assert adaptor.aws_endpoint_url == "http://localhost:4566"
    assert adaptor.aws_access_key_id == "aws-key"
    assert adaptor.aws_secret_access_key == "aws-secret"
    assert adaptor.aws_region == "eu-west-2"
    assert adaptor.heartbeat is None

    module.validate_cloud_config.assert_called_once()
    module.validate_kafka_config.assert_called_once()

    data_trans = FakeDataTransection.instances[-1]
    assert data_trans.kwargs["source_azure_conn_str"] == "source-connection"
    assert data_trans.kwargs["target_azure_conn_str"] == "target-connection"
    assert data_trans.kwargs["source_container_name"] == "source-container"
    assert data_trans.kwargs["target_container_name"] == "target-container"
    assert data_trans.kwargs["aws_endpoint_url"] == "http://localhost:4566"

    kafka_trans = FakeKafkaTransection.instances[-1]
    assert kafka_trans.bootstrap_server == "localhost:9092"

    assert adaptor.logger.info_calls[-1][0] == "Configuration validation successful"


def test_adaptor_initialises_defaults_for_optional_env(load_module, monkeypatch):
    monkeypatch.delenv("cloudProviderType", raising=False)
    monkeypatch.delenv("AWS_ENDPOINT_URL", raising=False)
    monkeypatch.delenv("AWS_REGION", raising=False)

    module = load_module()

    adaptor = module.AdaptorFileProcess()

    assert adaptor.cloud_provider == "azure"
    assert adaptor.aws_endpoint_url is None
    assert adaptor.aws_region == "us-east-1"


def test_read_source_file_info_returns_files_records_metric_and_logs(load_module):
    module = load_module()
    adaptor = module.AdaptorFileProcess()

    FakeDataTransection.source_files = ["a.csv", "b.csv", "c.csv"]

    result = adaptor.read_source_file_info()

    assert result == ["a.csv", "b.csv", "c.csv"]
    assert adaptor.data_trans.source_file_info_calls == ["azure"]

    assert adaptor.files_discovered.calls[-1] == (
        "add",
        3,
        {
            "cloud_provider": "azure",
            "container": "source-container",
        },
    )

    assert adaptor.logger.info_calls[-1][0] == "Source files discovered"
    assert adaptor.logger.info_calls[-1][2]["count"] == 3


def test_move_files_success_copied_records_success_metric_and_returns_true(load_module):
    module = load_module()
    adaptor = module.AdaptorFileProcess()

    FakeDataTransection.copy_results = {
        "file1.csv": FakeCopyResult(copied=True, skipped=False, reason="copied")
    }

    result = adaptor.move_files("file1.csv")

    assert result is True
    assert adaptor.data_trans.file_copy_calls == [("azure", "file1.csv")]

    assert adaptor.files_processed.calls[-1][0] == "add"
    assert adaptor.files_processed.calls[-1][2]["status"] == "success"

    assert adaptor.file_copy_duration.calls[-1][0] == "record"

    assert adaptor.logger.info_calls[-1][0] == "Adaptor file_copy result"
    assert adaptor.logger.info_calls[-1][2]["copied"] is True


def test_move_files_skipped_records_skipped_metric_and_returns_false(load_module):
    module = load_module()
    adaptor = module.AdaptorFileProcess()

    FakeDataTransection.copy_results = {
        "file1.csv": FakeCopyResult(copied=False, skipped=True, reason="exists")
    }

    result = adaptor.move_files("file1.csv")

    assert result is False
    assert adaptor.files_processed.calls[-1][2]["status"] == "skipped"
    assert adaptor.logger.info_calls[-1][2]["skipped"] is True


def test_move_files_exception_records_error_metric_and_reraises(load_module):
    module = load_module()
    adaptor = module.AdaptorFileProcess()

    FakeDataTransection.copy_exception = RuntimeError("copy failed")

    with pytest.raises(RuntimeError, match="copy failed"):
        adaptor.move_files("bad.csv")

    assert adaptor.files_processed.calls[-1][2]["status"] == "error"


def test_send_to_kafka_when_file_moved_uses_cloud_provider_uppercase(load_module):
    module = load_module()
    adaptor = module.AdaptorFileProcess()
    adaptor.cloud_provider = "azure"

    adaptor.send_to_kafka("file1.csv", True)

    kafka_trans = FakeKafkaTransection.instances[-1]

    assert kafka_trans.sent_messages == [
        (
            "mapper-topic",
            {
                "sourceType": "AZURE",
                "storageContainer": "target-container",
                "path": "file1.csv",
            },
        )
    ]

    assert adaptor.logger.info_calls[-1][0] == "Kafka message published"


def test_send_to_kafka_when_aws_maps_source_type_to_s3(load_module, monkeypatch):
    monkeypatch.setenv("cloudProviderType", "AWS")

    module = load_module()
    adaptor = module.AdaptorFileProcess()

    adaptor.send_to_kafka("file1.csv", True)

    kafka_trans = FakeKafkaTransection.instances[-1]

    assert kafka_trans.sent_messages[-1][1]["sourceType"] == "S3"


def test_send_to_kafka_when_file_not_moved_suppresses_message(load_module):
    module = load_module()
    adaptor = module.AdaptorFileProcess()

    adaptor.send_to_kafka("file1.csv", False)

    kafka_trans = FakeKafkaTransection.instances[-1]

    assert kafka_trans.sent_messages == []
    assert adaptor.logger.info_calls[-1][0] == "Kafka message suppressed — file not copied"


def test_run_no_files_sets_no_files_and_returns(load_module):
    module = load_module()
    FakeDataTransection.source_files = []

    ctx = FakeContext(run_id="run-empty-123456")

    module.run(ctx)

    step_log = FakeStepLogger.instances[-1]

    assert step_log.pipeline_banner_calls
    assert step_log.step_start_calls[-1][1] == "read_source_file_info"
    assert step_log.step_end_calls[-1][1] == "read_source_file_info"

    adaptor = FakeDataTransection.instances[-1]
    assert adaptor.source_file_info_calls == ["azure"]


def test_run_read_source_exception_is_handled_and_returns(load_module):
    module = load_module()

    FakeDataTransection.source_exception = FakeHttpResponseError("azure failed")
    ctx = FakeContext()

    module.run(ctx)

    step_log = FakeStepLogger.instances[-1]
    exc_handler = FakeHandleExceptions.instances[-1]

    assert step_log.step_failed_calls[-1][1] == "read_source_file_info"
    assert isinstance(exc_handler.handled[-1][0], FakeHttpResponseError)
    assert exc_handler.handled[-1][1] == "Azure"


def test_run_processes_copied_and_skipped_files(load_module):
    module = load_module()

    FakeDataTransection.source_files = ["copied.csv", "skipped.csv"]
    FakeDataTransection.copy_results = {
        "copied.csv": FakeCopyResult(copied=True, skipped=False, reason="copied"),
        "skipped.csv": FakeCopyResult(copied=False, skipped=True, reason="exists"),
    }

    ctx = FakeContext(run_id="run-success-123456")

    module.run(ctx)

    step_log = FakeStepLogger.instances[-1]
    kafka_trans = FakeKafkaTransection.instances[-1]

    move_and_publish_ends = [
        call for call in step_log.step_end_calls if call[1] == "move_and_publish"
    ]

    assert len(move_and_publish_ends) == 2
    assert move_and_publish_ends[0][2]["copied"] is True
    assert move_and_publish_ends[1][2]["copied"] is False

    assert kafka_trans.sent_messages == [
        (
            "mapper-topic",
            {
                "sourceType": "AZURE",
                "storageContainer": "target-container",
                "path": "copied.csv",
            },
        )
    ]


def test_run_move_file_exception_is_handled_and_continues(load_module):
    module = load_module()

    FakeDataTransection.source_files = ["bad.csv"]
    FakeDataTransection.copy_exception = FakeClientError("aws failed")

    ctx = FakeContext()

    module.run(ctx)

    step_log = FakeStepLogger.instances[-1]
    exc_handler = FakeHandleExceptions.instances[-1]

    assert step_log.step_failed_calls[-1][1] == "move_and_publish"
    assert step_log.step_failed_calls[-1][3] == {"file": "bad.csv"}
    assert isinstance(exc_handler.handled[-1][0], FakeClientError)
    assert exc_handler.handled[-1][1] == "AWS S3"


def test_provider_label_returns_expected_values(load_module):
    module = load_module()

    assert module._provider_label(FakeHttpResponseError("http")) == "Azure"
    assert module._provider_label(FakeAzureError("azure")) == "Azure"
    assert module._provider_label(FakeClientError("client")) == "AWS S3"
    assert module._provider_label(FakeBotoCoreError("boto")) == "AWS S3"
    assert module._provider_label(FakeGoogleAPIError("google")) == "GCP"
    assert module._provider_label(RuntimeError("other")) == ""
