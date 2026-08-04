import base64
import importlib.util
import json
import os
import sys
import types
from pathlib import Path
from unittest.mock import Mock

import pytest


SOURCE_PATH = Path(
    os.getenv(
        "FILE_SCHEMA_MAPPER_MAIN_PATH",
        "blueprints/producer/file/eq/schema_mapper/main.py",
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


class FakeKafkaError:
    _PARTITION_EOF = -191

    def __init__(self, code_value=None):
        self._code_value = code_value

    def code(self):
        return self._code_value

    def __str__(self):
        return f"FakeKafkaError({self._code_value})"


class FakeKafkaException(Exception):
    pass


class FakeKafkaMessage:
    def __init__(
        self,
        value=b'{"path": "file1.csv"}',
        headers=None,
        error_obj=None,
        topic="mapper-topic",
        partition=0,
        offset=10,
    ):
        self._value = value
        self._headers = headers
        self._error_obj = error_obj
        self._topic = topic
        self._partition = partition
        self._offset = offset

    def value(self):
        return self._value

    def headers(self):
        return self._headers

    def error(self):
        return self._error_obj

    def topic(self):
        return self._topic

    def partition(self):
        return self._partition

    def offset(self):
        return self._offset


class FakeConsumer:
    instances = []
    poll_side_effects = []

    def __init__(self, config):
        self.config = config
        self.subscriptions = []
        self.closed = False
        FakeConsumer.instances.append(self)

    def subscribe(self, topics):
        self.subscriptions.append(topics)

    def poll(self, timeout=1.0):
        if not FakeConsumer.poll_side_effects:
            return None

        effect = FakeConsumer.poll_side_effects.pop(0)

        if isinstance(effect, BaseException):
            raise effect

        if callable(effect):
            return effect()

        return effect

    def close(self):
        self.closed = True


class FakeCopyResult:
    def __init__(self, copied=True, skipped=False):
        self.copied = copied
        self.skipped = skipped


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
        self.warning_calls = []
        self.error_calls = []

    def info(self, message, *args, extra=None, **kwargs):
        self.info_calls.append((message, args, extra, kwargs))

    def warning(self, message, *args, extra=None, **kwargs):
        self.warning_calls.append((message, args, extra, kwargs))

    def error(self, message, *args, extra=None, **kwargs):
        self.error_calls.append((message, args, extra, kwargs))


class FakeDataTransection:
    instances = []
    data_read_value = "file-content"
    data_read_exception = None
    file_copy_result = FakeCopyResult(copied=True, skipped=False)
    file_copy_exception = None

    def __init__(self, **kwargs):
        self.kwargs = kwargs
        self.source_blob_name = kwargs.get("source_blob_name")
        self.target_blob_name = kwargs.get("target_blob_name")
        self.data_read_calls = []
        self.file_copy_calls = []
        FakeDataTransection.instances.append(self)

    def data_read(self, cloud_vendor):
        self.data_read_calls.append(cloud_vendor)

        if FakeDataTransection.data_read_exception:
            raise FakeDataTransection.data_read_exception

        return FakeDataTransection.data_read_value

    def file_copy(self, cloud_vendor, file_name, dest_file_name=None):
        self.file_copy_calls.append((cloud_vendor, file_name, dest_file_name))

        if FakeDataTransection.file_copy_exception:
            raise FakeDataTransection.file_copy_exception

        return FakeDataTransection.file_copy_result


class FakeKafkaTransection:
    instances = []
    consume_side_effects = []

    def __init__(self, bootstrap_server, logger, component_name=None):
        self.bootstrap_server = bootstrap_server
        self.logger = logger
        self.component_name = component_name
        self.sent_messages = []
        self.consume_messages_calls = []
        FakeKafkaTransection.instances.append(self)

    def send_message(self, target_topic, message):
        self.sent_messages.append((target_topic, message))

    def consume_messages(self, source_topic, group_id, handler):
        self.consume_messages_calls.append(
            {
                "source_topic": source_topic,
                "group_id": group_id,
                "handler": handler,
            }
        )

        if FakeKafkaTransection.consume_side_effects:
            effect = FakeKafkaTransection.consume_side_effects.pop(0)

            if isinstance(effect, BaseException):
                raise effect

            if callable(effect):
                return effect()

        return None


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
        pipeline_stage="schema_mapper",
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
    FakeConsumer.instances.clear()
    FakeConsumer.poll_side_effects.clear()

    FakeDataTransection.instances.clear()
    FakeDataTransection.data_read_value = "file-content"
    FakeDataTransection.data_read_exception = None
    FakeDataTransection.file_copy_result = FakeCopyResult(copied=True, skipped=False)
    FakeDataTransection.file_copy_exception = None

    FakeKafkaTransection.instances.clear()
    FakeKafkaTransection.consume_side_effects.clear()

    FakeHandleExceptions.instances.clear()
    FakeStepLogger.instances.clear()
    FakeHeartbeatLogger.instances.clear()
    FakeBackend.instances.clear()

    monkeypatch.setenv("cloudProviderType", "azure")
    monkeypatch.setenv("targetTopicName", "target-topic")
    monkeypatch.setenv("mapperTopicName", "mapper-topic")
    monkeypatch.setenv("mapperConnectionString", b64("mapper-connection"))
    monkeypatch.setenv("mapperContainerName", "mapper-container")
    monkeypatch.setenv("targetConnectionString", b64("target-connection"))
    monkeypatch.setenv("targetContainerName", "target-container")
    monkeypatch.setenv("bootstrapServer", "localhost:9092")
    monkeypatch.setenv("orgName", "BP")
    monkeypatch.setenv("schemaType", "NaturalGas")
    monkeypatch.setenv("AWS_ENDPOINT_URL", "http://localhost:4566")
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", b64("aws-key"))
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", b64("aws-secret"))
    monkeypatch.setenv("AWS_REGION", "eu-west-2")
    monkeypatch.setenv("PRODUCT_NAME", "bp-natural-gas")
    monkeypatch.setenv("SCHEDULER_BACKEND", "standalone")
    monkeypatch.setenv("DRAIN_IDLE_SECS", "2")
    monkeypatch.setenv("consumerRetryDelaySecs", "1")

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

    confluent_kafka = types.ModuleType("confluent_kafka")
    confluent_kafka.Consumer = FakeConsumer
    confluent_kafka.KafkaError = FakeKafkaError
    confluent_kafka.KafkaException = FakeKafkaException

    monkeypatch.setitem(sys.modules, "confluent_kafka", confluent_kafka)

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

    utils_otel_logger = types.ModuleType("utils.otel_logger")
    utils_otel_logger.OtelLogger = Mock(
        return_value=types.SimpleNamespace(
            create_logger=Mock(return_value=FakeLogger())
        )
    )

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
    monkeypatch.setitem(sys.modules, "utils.otel_logger", utils_otel_logger)
    monkeypatch.setitem(sys.modules, "utils.pipeline_context", pipeline_context)
    monkeypatch.setitem(sys.modules, "utils.scheduler_backend", scheduler_backend)
    monkeypatch.setitem(sys.modules, "utils.step_logger", step_logger)

    sdk_pkg = types.ModuleType("dpn_observability_sdk")

    sdk_otel_logger = types.ModuleType("dpn_observability_sdk.otel_logger")
    sdk_otel_logger.OtelLogger = Mock(
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
    monkeypatch.setitem(
        sys.modules,
        "dpn_observability_sdk.otel_logger",
        sdk_otel_logger,
    )
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
                "Run pytest from repo root or set FILE_SCHEMA_MAPPER_MAIN_PATH."
            )

        module_name = "file_schema_mapper_main_under_test"

        if module_name in sys.modules:
            del sys.modules[module_name]

        spec = importlib.util.spec_from_file_location(module_name, SOURCE_PATH)
        module = importlib.util.module_from_spec(spec)
        sys.modules[module_name] = module
        spec.loader.exec_module(module)
        return module

    return _load


def test_schema_mapper_initialises_config_validates_and_creates_transactions(load_module):
    module = load_module()

    mapper = module.SchemaMapper()

    assert mapper.cloud_provider == "azure"
    assert mapper.target_kafka_topic == "target-topic"
    assert mapper.source_kafka_topic == "mapper-topic"
    assert mapper.source_azure_conn_str == "mapper-connection"
    assert mapper.source_container_name == "mapper-container"
    assert mapper.target_container_name == "target-container"
    assert mapper.bootstrap_server == "localhost:9092"
    assert mapper.target_azure_conn_str == "target-connection"
    assert mapper.org_name == "BP"
    assert mapper.schema_type == "NaturalGas"
    assert mapper.file_name is None
    assert mapper.aws_endpoint_url == "http://localhost:4566"
    assert mapper.aws_access_key_id == "aws-key"
    assert mapper.aws_secret_access_key == "aws-secret"
    assert mapper.aws_region == "eu-west-2"
    assert mapper.heartbeat is None

    module.validate_cloud_config.assert_called_once()
    module.validate_kafka_config.assert_called_once()

    data_trans = FakeDataTransection.instances[-1]
    assert data_trans.kwargs["source_azure_conn_str"] == "mapper-connection"
    assert data_trans.kwargs["target_azure_conn_str"] == "target-connection"
    assert data_trans.kwargs["source_container_name"] == "mapper-container"
    assert data_trans.kwargs["target_container_name"] == "target-container"

    kafka_trans = FakeKafkaTransection.instances[-1]
    assert kafka_trans.bootstrap_server == "localhost:9092"


def test_schema_mapper_initialises_defaults_for_optional_env(load_module, monkeypatch):
    monkeypatch.delenv("cloudProviderType", raising=False)
    monkeypatch.delenv("AWS_ENDPOINT_URL", raising=False)
    monkeypatch.delenv("AWS_REGION", raising=False)

    module = load_module()

    mapper = module.SchemaMapper()

    assert mapper.cloud_provider == "azure"
    assert mapper.aws_endpoint_url is None
    assert mapper.aws_region == "us-east-1"


def test_read_records_sets_source_blob_reads_data_records_metric_and_logs(load_module):
    module = load_module()
    mapper = module.SchemaMapper()
    mapper.file_name = "dest.csv"

    FakeDataTransection.data_read_value = "abc123"

    result = mapper.read_records("input.csv")

    assert result == "abc123"
    assert mapper.data_trans.source_blob_name == "input.csv"
    assert mapper.data_trans.data_read_calls == ["azure"]
    assert mapper.messages_processed.calls[-1] == (
        "add",
        1,
        {
            "source_topic": "mapper-topic",
            "file": "input.csv",
            "status": "success",
        },
    )
    assert mapper.logger.info_calls[-1][0] == "File content read"
    assert mapper.logger.info_calls[-1][2]["bytes"] == 6


def test_schema_validation_logs_and_returns_true(load_module):
    module = load_module()
    mapper = module.SchemaMapper()

    assert mapper.schema_validation("hello") is True
    assert mapper.logger.info_calls[-1][0] == "Schema validation (stub – PI3)"
    assert mapper.logger.info_calls[-1][2]["data_length"] == 5


def test_move_files_renames_file_copies_records_metric_and_returns_true(load_module):
    module = load_module()
    mapper = module.SchemaMapper()

    FakeDataTransection.file_copy_result = FakeCopyResult(copied=True, skipped=False)

    result = mapper.move_files("My-File Name.csv")

    expected_name = "naturalgas-bp-my_file_name.csv"

    assert result is True
    assert mapper.file_name == expected_name
    assert mapper.data_trans.file_copy_calls == [
        ("azure", "My-File Name.csv", expected_name)
    ]
    assert mapper.files_moved.calls[-1] == (
        "add",
        1,
        {
            "cloud_provider": "azure",
            "status": "success",
        },
    )
    assert mapper.logger.info_calls[-1][0] == "Schema mapper file_copy result"
    assert mapper.logger.info_calls[-1][2]["dest_file"] == expected_name


def test_move_files_returns_false_when_copy_skipped(load_module):
    module = load_module()
    mapper = module.SchemaMapper()

    FakeDataTransection.file_copy_result = FakeCopyResult(copied=False, skipped=True)

    result = mapper.move_files("exists.csv")

    assert result is False
    assert mapper.logger.info_calls[-1][2]["copied"] is False
    assert mapper.logger.info_calls[-1][2]["skipped"] is True


def test_send_to_kafka_when_file_moved_for_azure(load_module):
    module = load_module()
    mapper = module.SchemaMapper()
    mapper.file_name = "naturalgas-bp-file.csv"

    mapper.send_to_kafka(True)

    kafka_trans = FakeKafkaTransection.instances[-1]
    assert kafka_trans.sent_messages == [
        (
            "target-topic",
            {
                "sourceType": "AZURE",
                "storageContainer": "target-container",
                "path": "naturalgas-bp-file.csv",
            },
        )
    ]
    assert mapper.logger.info_calls[-1][0] == "Target Kafka message published"


def test_send_to_kafka_when_aws_maps_source_type_to_s3(load_module, monkeypatch):
    monkeypatch.setenv("cloudProviderType", "AWS")

    module = load_module()
    mapper = module.SchemaMapper()
    mapper.file_name = "naturalgas-bp-file.csv"

    mapper.send_to_kafka(True)

    kafka_trans = FakeKafkaTransection.instances[-1]
    assert kafka_trans.sent_messages[-1][1]["sourceType"] == "S3"


def test_send_to_kafka_when_file_not_moved_does_nothing(load_module):
    module = load_module()
    mapper = module.SchemaMapper()
    mapper.file_name = "naturalgas-bp-file.csv"

    mapper.send_to_kafka(False)

    kafka_trans = FakeKafkaTransection.instances[-1]
    assert kafka_trans.sent_messages == []


def test_process_one_message_missing_path_logs_warning_and_returns(load_module):
    module = load_module()
    mapper = module.SchemaMapper()
    step_log = FakeStepLogger(mapper.logger)
    ctx = FakeContext()

    module._process_one_message(mapper, step_log, ctx, payload={})

    assert mapper.logger.warning_calls[-1][0] == (
        "[%s] Kafka payload missing 'path' — skipping (run_id=%s)"
    )
    assert step_log.step_start_calls == []


def test_process_one_message_success_reads_validates_moves_publishes(load_module):
    module = load_module()
    mapper = module.SchemaMapper()
    step_log = FakeStepLogger(mapper.logger)
    ctx = FakeContext()

    module._process_one_message(
        mapper,
        step_log,
        ctx,
        payload={"path": "File One.csv"},
    )

    assert mapper.file_name == "naturalgas-bp-file_one.csv"

    assert step_log.step_start_calls[0][1] == "schema_mapper_message"
    assert step_log.step_start_calls[1][1] == "move_files"

    assert step_log.step_end_calls[0][1] == "move_files"
    assert step_log.step_end_calls[1][1] == "schema_mapper_message"

    kafka_trans = FakeKafkaTransection.instances[-1]
    assert kafka_trans.sent_messages[-1][1]["path"] == "naturalgas-bp-file_one.csv"

    assert mapper.process_duration.calls[-1][0] == "record"


def test_process_one_message_validation_false_skips_move_and_publish(load_module):
    module = load_module()
    mapper = module.SchemaMapper()
    mapper.schema_validation = Mock(return_value=False)
    mapper.move_files = Mock()
    mapper.send_to_kafka = Mock()

    step_log = FakeStepLogger(mapper.logger)
    ctx = FakeContext()

    module._process_one_message(
        mapper,
        step_log,
        ctx,
        payload={"path": "invalid.csv"},
    )

    mapper.move_files.assert_not_called()
    mapper.send_to_kafka.assert_not_called()
    assert step_log.step_end_calls[-1][1] == "schema_mapper_message"


@pytest.mark.parametrize(
    "exception, provider",
    [
        (FakeHttpResponseError("azure http failed"), "Azure"),
        (FakeAzureError("azure failed"), "Azure"),
        (FakeClientError("aws client failed"), "AWS S3"),
        (FakeBotoCoreError("aws boto failed"), "AWS S3"),
        (FakeGoogleAPIError("gcp failed"), "GCP"),
        (RuntimeError("generic failed"), ""),
    ],
)
def test_process_one_message_handles_provider_exceptions(load_module, exception, provider):
    module = load_module()
    mapper = module.SchemaMapper()
    mapper.read_records = Mock(side_effect=exception)

    step_log = FakeStepLogger(mapper.logger)
    ctx = FakeContext()

    module._process_one_message(
        mapper,
        step_log,
        ctx,
        payload={"path": "bad.csv"},
    )

    assert mapper.messages_processed.calls[-1][2]["status"] == "error"
    assert step_log.step_failed_calls[-1][1] == "schema_mapper_message"
    assert step_log.step_failed_calls[-1][3] == {"file": "bad.csv"}

    exc_handler = FakeHandleExceptions.instances[-1]
    assert exc_handler.handled[-1][0] is exception
    assert exc_handler.handled[-1][1] == provider


def test_run_uses_drain_mode_for_kafka_trigger_and_marks_success(load_module, monkeypatch):
    module = load_module()

    drain = Mock()
    continuous = Mock()

    monkeypatch.setattr(module, "_run_drain_mode", drain)
    monkeypatch.setattr(module, "_run_continuous_mode", continuous)

    ctx = FakeContext(triggered_by="kafka-trigger", run_id="run-kafka-123456")

    module.run(ctx)

    drain.assert_called_once()
    continuous.assert_not_called()

    step_log = FakeStepLogger.instances[-1]
    assert step_log.pipeline_banner_calls
    assert step_log.step_start_calls[-1][1] == "mapper_window"
    assert step_log.step_end_calls[-1][1] == "mapper_window"


def test_run_uses_continuous_mode_for_manual_trigger(load_module, monkeypatch):
    module = load_module()

    drain = Mock()
    continuous = Mock()

    monkeypatch.setattr(module, "_run_drain_mode", drain)
    monkeypatch.setattr(module, "_run_continuous_mode", continuous)

    ctx = FakeContext(triggered_by="manual", run_id="run-manual-123456")

    module.run(ctx)

    drain.assert_not_called()
    continuous.assert_called_once()

    step_log = FakeStepLogger.instances[-1]
    assert step_log.step_end_calls[-1][1] == "mapper_window"


def test_run_failure_marks_failed_and_reraises(load_module, monkeypatch):
    module = load_module()

    monkeypatch.setattr(
        module,
        "_run_continuous_mode",
        Mock(side_effect=RuntimeError("window failed")),
    )

    ctx = FakeContext(triggered_by="manual")

    with pytest.raises(RuntimeError, match="window failed"):
        module.run(ctx)

    step_log = FakeStepLogger.instances[-1]
    assert step_log.step_failed_calls[-1][1] == "mapper_window"


def test_run_drain_mode_processes_valid_message_eof_bad_message_and_idle_break(
    load_module,
    monkeypatch,
):
    module = load_module()
    mapper = module.SchemaMapper()
    step_log = FakeStepLogger(mapper.logger)
    ctx = FakeContext(triggered_by="kafka-trigger")

    handler = Mock()

    valid_message = FakeKafkaMessage(
        value=json.dumps({"path": "file1.csv"}).encode("utf-8"),
        headers=[("traceparent", b"00-test-trace"), ("plain", "value")],
        topic="mapper-topic",
        partition=2,
        offset=99,
    )
    eof_message = FakeKafkaMessage(
        error_obj=FakeKafkaError(FakeKafkaError._PARTITION_EOF)
    )
    bad_message = FakeKafkaMessage(value=b"{bad-json")

    FakeConsumer.poll_side_effects[:] = [
        eof_message,
        valid_message,
        bad_message,
        None,
    ]

    monotonic_values = iter(
        [
            0.0,  # last_message_at init
            0.1,  # after valid message
            3.0,  # idle timeout check
        ]
    )
    monkeypatch.setattr(module.time, "monotonic", lambda: next(monotonic_values))

    module._run_drain_mode(mapper, step_log, ctx, handler)

    consumer = FakeConsumer.instances[-1]

    assert consumer.config == {
        "bootstrap.servers": "localhost:9092",
        "group.id": "producer_schema_mapper",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": True,
    }
    assert consumer.subscriptions == [["mapper-topic"]]
    assert consumer.closed is True

    handler.assert_called_once_with({"path": "file1.csv"})

    assert mapper.logger.warning_calls[-1][0] == "Malformed mapper message — skipping"
    assert mapper.logger.info_calls[-1][0] == (
        "[%s] Queue drained — %d messages processed, idle %.0fs (run_id=%s)"
    )

    assert step_log.step_start_calls[-1][1] == "drain_queue"
    assert step_log.step_end_calls[-1][1] == "drain_queue"
    assert step_log.step_end_calls[-1][2]["messages_processed"] == 1

    module._otel_propagate.extract.assert_called_once()
    module._otel_context.attach.assert_called_once_with({"remote": "context"})


def test_run_drain_mode_raises_for_kafka_error_and_closes_consumer(
    load_module,
    monkeypatch,
):
    module = load_module()
    mapper = module.SchemaMapper()
    step_log = FakeStepLogger(mapper.logger)
    ctx = FakeContext(triggered_by="kafka-trigger")

    FakeConsumer.poll_side_effects[:] = [
        FakeKafkaMessage(error_obj=FakeKafkaError(123))
    ]

    monkeypatch.setattr(module.time, "monotonic", Mock(return_value=0.0))

    with pytest.raises(FakeKafkaException):
        module._run_drain_mode(mapper, step_log, ctx, Mock())

    assert FakeConsumer.instances[-1].closed is True


def test_run_drain_mode_handles_unicode_decode_error_and_then_idle_break(
    load_module,
    monkeypatch,
):
    module = load_module()
    mapper = module.SchemaMapper()
    step_log = FakeStepLogger(mapper.logger)
    ctx = FakeContext(triggered_by="kafka-trigger")

    bad_unicode_message = FakeKafkaMessage(value=b"\xff\xfe\xfa")
    FakeConsumer.poll_side_effects[:] = [
        bad_unicode_message,
        None,
    ]

    monotonic_values = iter(
        [
            0.0,  # last_message_at init
            3.0,  # idle timeout check
        ]
    )
    monkeypatch.setattr(module.time, "monotonic", lambda: next(monotonic_values))

    module._run_drain_mode(mapper, step_log, ctx, Mock())

    assert mapper.logger.warning_calls[-1][0] == "Malformed mapper message — skipping"
    assert FakeConsumer.instances[-1].closed is True
    assert step_log.step_end_calls[-1][2]["messages_processed"] == 0


def test_run_continuous_mode_retries_then_keyboard_interrupt_exits(
    load_module,
    monkeypatch,
):
    module = load_module()
    mapper = module.SchemaMapper()
    step_log = FakeStepLogger(mapper.logger)
    ctx = FakeContext(triggered_by="manual")

    FakeKafkaTransection.consume_side_effects[:] = [
        RuntimeError("consumer failed"),
        KeyboardInterrupt(),
    ]

    sleep = Mock()
    monkeypatch.setattr(module.time, "sleep", sleep)

    with pytest.raises(KeyboardInterrupt):
        module._run_continuous_mode(mapper, step_log, ctx, Mock())

    kafka_trans = FakeKafkaTransection.instances[-1]

    assert len(kafka_trans.consume_messages_calls) == 2
    assert kafka_trans.consume_messages_calls[0]["source_topic"] == "mapper-topic"
    assert kafka_trans.consume_messages_calls[0]["group_id"] == "producer_schema_mapper"

    assert step_log.step_start_calls[0][1] == "mapper_consumer_loop"
    assert step_log.step_failed_calls[-1][1] == "mapper_consumer_loop"
    assert step_log.step_start_calls[-1][1] == "mapper_consumer_loop"

    assert mapper.logger.error_calls[-1][0] == (
        "[%s] Consumer loop exited — retrying in %ds (run_id=%s)"
    )
    sleep.assert_called_once_with(1)