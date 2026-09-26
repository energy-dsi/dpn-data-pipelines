import base64
import importlib.util
import json
import os
import runpy
import sys
import types
from pathlib import Path
from unittest.mock import Mock

import pytest


SOURCE_PATH = Path(
    os.getenv(
        "CONSUMER_FILE_SCHEMA_MAPPER_MAIN_PATH",
        "blueprints/consumer/file/schema_mapper/main.py",
    )
)


def b64(value: str) -> str:
    return base64.b64encode(value.encode()).decode()


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


class FakeKafkaException(Exception):
    pass


class FakeKafkaError:
    _PARTITION_EOF = -191

    def __init__(self, code_value=None):
        self._code_value = code_value

    def code(self):
        return self._code_value

    def __str__(self):
        return f"FakeKafkaError({self._code_value})"


class FakeKafkaMessage:
    def __init__(
        self,
        value=b'{"path": "file1.csv"}',
        headers=None,
        error_obj=None,
        topic="mapper-topic",
        partition=0,
        offset=1,
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

        item = FakeConsumer.poll_side_effects.pop(0)

        if isinstance(item, BaseException):
            raise item

        if callable(item):
            return item()

        return item

    def close(self):
        self.closed = True


class FakeCopyResult:
    def __init__(self, copied=True):
        self.copied = copied


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

    def info(self, msg, *args, extra=None, **kwargs):
        self.info_calls.append((msg, args, extra, kwargs))

    def warning(self, msg, *args, extra=None, **kwargs):
        self.warning_calls.append((msg, args, extra, kwargs))

    def error(self, msg, *args, extra=None, **kwargs):
        self.error_calls.append((msg, args, extra, kwargs))


class FakeDataTransection:
    instances = []
    data_read_value = "file-content"
    data_read_exception = None
    file_copy_result = FakeCopyResult(True)
    file_copy_exception = None

    def __init__(self, **kwargs):
        self.kwargs = kwargs
        self.source_blob_name = kwargs.get("source_blob_name")
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


def identity_decorator(*args, **kwargs):
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
    FakeDataTransection.file_copy_result = FakeCopyResult(True)
    FakeDataTransection.file_copy_exception = None

    FakeKafkaTransection.instances.clear()
    FakeHandleExceptions.instances.clear()
    FakeKafkaTopicManager.instances.clear()
    FakeStepLogger.instances.clear()
    FakeHeartbeatLogger.instances.clear()
    FakeBackend.instances.clear()

    monkeypatch.setenv("cloudProviderType", "azure")
    monkeypatch.setenv("targetTopicName", "target-topic")
    monkeypatch.setenv("mapperTopicName", "mapper-topic")
    monkeypatch.setenv("bootstrapServer", "localhost:9092")
    monkeypatch.setenv("mapperConnectionString", b64("mapper-connection"))
    monkeypatch.setenv("targetConnectionString", b64("target-connection"))
    monkeypatch.setenv("mapperContainerName", "mapper-container")
    monkeypatch.setenv("targetContainerName", "target-container")
    monkeypatch.setenv("orgName", "NESO")
    monkeypatch.setenv("schemaType", "EQBD")
    monkeypatch.setenv("AWS_ENDPOINT_URL", "http://localhost:4566")
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", b64("aws-key"))
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", b64("aws-secret"))
    monkeypatch.setenv("AWS_REGION", "eu-west-2")
    monkeypatch.setenv("PRODUCT_NAME", "consumer-file")
    monkeypatch.setenv("SCHEDULER_BACKEND", "standalone")
    monkeypatch.setenv("DRAIN_IDLE_SECS", "2")
    monkeypatch.setenv("consumerRetryDelaySecs", "1")

    azure_exceptions = types.ModuleType("azure.core.exceptions")
    azure_exceptions.AzureError = FakeAzureError
    azure_exceptions.HttpResponseError = FakeHttpResponseError
    monkeypatch.setitem(sys.modules, "azure", types.ModuleType("azure"))
    monkeypatch.setitem(sys.modules, "azure.core", types.ModuleType("azure.core"))
    monkeypatch.setitem(sys.modules, "azure.core.exceptions", azure_exceptions)

    botocore_exceptions = types.ModuleType("botocore.exceptions")
    botocore_exceptions.BotoCoreError = FakeBotoCoreError
    botocore_exceptions.ClientError = FakeClientError
    monkeypatch.setitem(sys.modules, "botocore", types.ModuleType("botocore"))
    monkeypatch.setitem(sys.modules, "botocore.exceptions", botocore_exceptions)

    google_exceptions = types.ModuleType("google.api_core.exceptions")
    google_exceptions.GoogleAPIError = FakeGoogleAPIError
    monkeypatch.setitem(sys.modules, "google", types.ModuleType("google"))
    monkeypatch.setitem(sys.modules, "google.api_core", types.ModuleType("google.api_core"))
    monkeypatch.setitem(sys.modules, "google.api_core.exceptions", google_exceptions)

    confluent = types.ModuleType("confluent_kafka")
    confluent.Consumer = FakeConsumer
    confluent.KafkaError = FakeKafkaError
    confluent.KafkaException = FakeKafkaException
    monkeypatch.setitem(sys.modules, "confluent_kafka", confluent)

    dotenv = types.ModuleType("dotenv")
    dotenv.load_dotenv = Mock()
    monkeypatch.setitem(sys.modules, "dotenv", dotenv)

    otel_context = types.ModuleType("opentelemetry.context")
    otel_context.attach = Mock(return_value="fake-token")
    otel_context.detach = Mock()

    otel_propagate = types.ModuleType("opentelemetry.propagate")
    otel_propagate.extract = Mock(return_value={"remote": "context"})

    otel = types.ModuleType("opentelemetry")
    otel.context = otel_context
    otel.propagate = otel_propagate

    monkeypatch.setitem(sys.modules, "opentelemetry", otel)
    monkeypatch.setitem(sys.modules, "opentelemetry.context", otel_context)
    monkeypatch.setitem(sys.modules, "opentelemetry.propagate", otel_propagate)

    monkeypatch.setitem(sys.modules, "utils", types.ModuleType("utils"))

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

    topic_utils = types.ModuleType("utils.topic_utils")
    topic_utils.TopicResolver = FakeTopicResolver
    topic_utils.KafkaTopicManager = FakeKafkaTopicManager

    monkeypatch.setitem(sys.modules, "utils.config_validator", config_validator)
    monkeypatch.setitem(sys.modules, "utils.data_transection", data_transection)
    monkeypatch.setitem(sys.modules, "utils.exception_handler", exception_handler)
    monkeypatch.setitem(sys.modules, "utils.kafka_transection", kafka_transection)
    monkeypatch.setitem(sys.modules, "utils.pipeline_context", pipeline_context)
    monkeypatch.setitem(sys.modules, "utils.scheduler_backend", scheduler_backend)
    monkeypatch.setitem(sys.modules, "utils.step_logger", step_logger)
    monkeypatch.setitem(sys.modules, "utils.topic_utils", topic_utils)

    monkeypatch.setitem(
        sys.modules,
        "dpn_observability_sdk",
        types.ModuleType("dpn_observability_sdk"),
    )

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

    instr = types.ModuleType("dpn_observability_sdk.otel_instrumentation")
    instr.traced = identity_decorator
    instr.timed_metric = identity_decorator

    heartbeat = types.ModuleType("dpn_observability_sdk.heartbeat")
    heartbeat.HeartbeatLogger = FakeHeartbeatLogger

    monkeypatch.setitem(sys.modules, "dpn_observability_sdk.otel_logger", otel_logger)
    monkeypatch.setitem(sys.modules, "dpn_observability_sdk.otel_tracer", otel_tracer)
    monkeypatch.setitem(sys.modules, "dpn_observability_sdk.otel_metrics", otel_metrics)
    monkeypatch.setitem(
        sys.modules,
        "dpn_observability_sdk.otel_instrumentation",
        instr,
    )
    monkeypatch.setitem(sys.modules, "dpn_observability_sdk.heartbeat", heartbeat)


@pytest.fixture
def load_module():
    def _load():
        if not SOURCE_PATH.exists():
            pytest.skip(f"Source file not found: {SOURCE_PATH}")

        module_name = "consumer_file_schema_mapper_main_under_test"
        sys.modules.pop(module_name, None)

        spec = importlib.util.spec_from_file_location(module_name, SOURCE_PATH)
        module = importlib.util.module_from_spec(spec)
        sys.modules[module_name] = module
        spec.loader.exec_module(module)
        return module

    return _load


def test_init_config_clients_and_topic(load_module):
    module = load_module()
    mapper = module.SchemaMapper()

    assert mapper.cloud_provider == "azure"
    assert mapper.target_topic == "target-topic"
    assert mapper.source_topic == "mapper-topic"
    assert mapper.bootstrap == "localhost:9092"
    assert mapper.src_conn == "mapper-connection"
    assert mapper.tgt_conn == "target-connection"
    assert mapper.src_container == "mapper-container"
    assert mapper.tgt_container == "target-container"
    assert mapper.org_name == "NESO"
    assert mapper.schema_type == "EQBD"
    assert mapper.aws_endpoint == "http://localhost:4566"
    assert mapper.aws_key_id == "aws-key"
    assert mapper.aws_secret == "aws-secret"
    assert mapper.aws_region == "eu-west-2"
    assert mapper.heartbeat is None

    assert FakeKafkaTopicManager.instances[-1].ensured_topics == ["target-topic"]
    module.validate_cloud_config.assert_called_once()
    module.validate_kafka_config.assert_called_once()


def test_init_defaults_and_empty_target_topic(load_module, monkeypatch):
    monkeypatch.delenv("cloudProviderType", raising=False)
    monkeypatch.delenv("AWS_ENDPOINT_URL", raising=False)
    monkeypatch.delenv("AWS_REGION", raising=False)
    monkeypatch.setenv("targetTopicName", "")

    module = load_module()
    mapper = module.SchemaMapper()

    assert mapper.cloud_provider == "azure"
    assert mapper.aws_endpoint is None
    assert mapper.aws_region == "us-east-1"
    assert mapper.target_topic == ".trfm"
    assert FakeKafkaTopicManager.instances[-1].ensured_topics == [".trfm"]


def test_read_validate_move_publish(load_module, monkeypatch):
    module = load_module()
    mapper = module.SchemaMapper()

    assert mapper.read_file("a.csv") == "file-content"
    assert mapper.data_trans.source_blob_name == "a.csv"
    assert mapper.data_trans.data_read_calls == ["azure"]

    assert mapper.validate("abc") is True
    assert mapper.logger.info_calls[-1][0] == "schema validation (stub)"

    assert mapper.move_file("a.csv") is True
    assert mapper.file_name == "a.csv"
    assert mapper.data_trans.file_copy_calls == [("azure", "a.csv", "a.csv")]

    mapper.publish_event(True)
    assert FakeKafkaTransection.instances[-1].sent_messages[-1][1] == {
        "sourceType": "AZURE",
        "storageContainer": "target-container",
        "path": "a.csv",
    }

    before = len(FakeKafkaTransection.instances[-1].sent_messages)
    mapper.publish_event(False)
    assert len(FakeKafkaTransection.instances[-1].sent_messages) == before

    FakeDataTransection.file_copy_result = FakeCopyResult(False)
    assert mapper.move_file("skipped.csv") is False

    monkeypatch.setenv("cloudProviderType", "AWS")
    module = load_module()
    aws_mapper = module.SchemaMapper()
    aws_mapper.file_name = "b.csv"
    aws_mapper.publish_event(True)

    assert FakeKafkaTransection.instances[-1].sent_messages[-1][1]["sourceType"] == "S3"


def test_process_success_missing_path_validation_false_and_invalid_json(load_module):
    module = load_module()
    ctx = FakeContext()

    mapper = module.SchemaMapper()
    step = FakeStepLogger(mapper.logger)
    msg = FakeKafkaMessage(headers=[("traceparent", b"00-test")])

    mapper._process(msg, ctx, step)

    assert mapper.messages_processed.calls[-1][2]["status"] == "success"
    assert mapper.files_moved.calls[-1][2]["status"] == "success"
    assert mapper.process_duration.calls[-1][0] == "record"
    assert step.step_start_calls[-1][1] == "mapper_msg"
    assert step.step_end_calls[-1][1] == "mapper_msg"

    module._otel_propagate.extract.assert_called()
    module._otel_context.attach.assert_called_with({"remote": "context"})
    module._otel_context.detach.assert_called_with("fake-token")

    mapper2 = module.SchemaMapper()
    step2 = FakeStepLogger(mapper2.logger)

    mapper2._process(FakeKafkaMessage(value=b"{}"), ctx, step2)

    assert step2.step_start_calls == []
    assert module._otel_context.detach.call_count >= 2

    mapper3 = module.SchemaMapper()
    mapper3.validate = Mock(return_value=False)
    mapper3.move_file = Mock()
    mapper3.publish_event = Mock()
    step3 = FakeStepLogger(mapper3.logger)

    mapper3._process(FakeKafkaMessage(value=b'{"path":"x.csv"}'), ctx, step3)

    mapper3.move_file.assert_not_called()
    mapper3.publish_event.assert_not_called()
    assert mapper3.messages_processed.calls[-1][2]["status"] == "success"

    mapper4 = module.SchemaMapper()
    step4 = FakeStepLogger(mapper4.logger)

    with pytest.raises(json.JSONDecodeError):
        mapper4._process(FakeKafkaMessage(value=b"{bad-json"), ctx, step4)

    assert module._otel_context.detach.call_count >= 4


@pytest.mark.parametrize(
    "exc, provider",
    [
        (FakeHttpResponseError("x"), "Azure"),
        (FakeAzureError("x"), "Azure"),
        (FakeClientError("x"), "AWS S3"),
        (FakeBotoCoreError("x"), "AWS S3"),
        (FakeGoogleAPIError("x"), "GCP"),
        (RuntimeError("x"), ""),
    ],
)
def test_process_handles_all_exception_groups(load_module, exc, provider):
    module = load_module()
    mapper = module.SchemaMapper()
    mapper.read_file = Mock(side_effect=exc)

    step = FakeStepLogger(mapper.logger)

    mapper._process(
        FakeKafkaMessage(value=b'{"path":"bad.csv"}'),
        FakeContext(),
        step,
    )

    assert mapper.messages_processed.calls[-1][2]["status"] == "error"
    assert step.step_failed_calls[-1][1] == "mapper_msg"
    assert FakeHandleExceptions.instances[-1].handled[-1] == (exc, provider)
    module._otel_context.detach.assert_called_with("fake-token")


def test_run_selects_modes_and_handles_failure(load_module, monkeypatch):
    module = load_module()

    drain = Mock()
    continuous = Mock()

    monkeypatch.setattr(module, "_drain", drain)
    monkeypatch.setattr(module, "_continuous", continuous)

    module.run(FakeContext(triggered_by="kafka-trigger"))

    drain.assert_called_once()
    continuous.assert_not_called()
    assert FakeStepLogger.instances[-1].step_end_calls[-1][1] == "mapper_window"

    drain.reset_mock()
    continuous.reset_mock()

    module.run(FakeContext(triggered_by="manual"))

    continuous.assert_called_once()
    drain.assert_not_called()

    monkeypatch.setattr(
        module,
        "_continuous",
        Mock(side_effect=RuntimeError("boom")),
    )

    with pytest.raises(RuntimeError, match="boom"):
        module.run(FakeContext(triggered_by="manual"))

    assert FakeStepLogger.instances[-1].step_failed_calls[-1][1] == "mapper_window"


def test_drain_success_eof_handler_error_idle_continue_and_kafka_error(
    load_module,
    monkeypatch,
):
    module = load_module()
    mapper = module.SchemaMapper()
    step = FakeStepLogger(mapper.logger)
    ctx = FakeContext()

    handler = Mock(side_effect=[None, RuntimeError("handler fail")])

    FakeConsumer.poll_side_effects[:] = [
        None,
        FakeKafkaMessage(error_obj=FakeKafkaError(FakeKafkaError._PARTITION_EOF)),
        FakeKafkaMessage(value=b'{"path":"one.csv"}'),
        FakeKafkaMessage(value=b'{"path":"two.csv"}'),
        None,
    ]

    values = iter(
        [
            0.0,  # last_msg initial
            1.0,  # first None: not idle yet, continue
            1.5,  # after valid message
            4.0,  # final None: idle break
        ]
    )
    monkeypatch.setattr(module.time, "monotonic", lambda: next(values))

    module._drain(mapper, step, ctx, handler)

    assert FakeConsumer.instances[-1].closed is True
    assert handler.call_count == 2
    assert step.step_start_calls[-1][1] == "drain_queue"
    assert step.step_end_calls[-1][1] == "drain_queue"
    assert step.step_end_calls[-1][2]["processed"] == 1

    FakeConsumer.poll_side_effects[:] = [
        FakeKafkaMessage(error_obj=FakeKafkaError(777))
    ]
    monkeypatch.setattr(module.time, "monotonic", Mock(return_value=0.0))

    with pytest.raises(FakeKafkaException):
        module._drain(mapper, step, ctx, Mock())

    assert FakeConsumer.instances[-1].closed is True


def test_continuous_paths_then_keyboardinterrupt(load_module, monkeypatch):
    module = load_module()
    mapper = module.SchemaMapper()
    step = FakeStepLogger(mapper.logger)
    ctx = FakeContext()
    handler = Mock()

    FakeConsumer.poll_side_effects[:] = [
        None,
        FakeKafkaMessage(error_obj=FakeKafkaError(FakeKafkaError._PARTITION_EOF)),
        FakeKafkaMessage(value=b'{"path":"ok.csv"}'),
        FakeKafkaMessage(error_obj=FakeKafkaError(999)),
        RuntimeError("poll exploded"),
        KeyboardInterrupt(),
    ]

    sleep = Mock()
    monkeypatch.setattr(module.time, "sleep", sleep)

    with pytest.raises(KeyboardInterrupt):
        module._continuous(mapper, step, ctx, handler)

    assert FakeConsumer.instances[-1].closed is True
    handler.assert_called_once()
    assert sleep.call_count == 2
    assert len(step.step_failed_calls) == 2
    assert step.step_start_calls[0][1] == "consumer_loop"


def test_main_block():
    if not SOURCE_PATH.exists():
        pytest.skip(f"Source file not found: {SOURCE_PATH}")

    runpy.run_path(str(SOURCE_PATH), run_name="__main__")

    heartbeat = FakeHeartbeatLogger.instances[-1]

    assert heartbeat.started is True
    assert heartbeat.component_name == "consumer-file-schema-mapper"
    assert heartbeat.metadata == {
        "source_topic": "mapper-topic",
        "target_topic": "target-topic",
        "scheduler_backend": "standalone",
    }

    _, kwargs = FakeBackend.instances[-1].execute_calls[-1]

    assert kwargs == {
        "pipeline_stage": "schema_mapper",
        "pipeline_type": "file",
        "pipeline_role": "consumer",
        "component_name": "consumer-file-schema-mapper",
    }