import importlib.util
import os
import runpy
import sys
import types
from pathlib import Path
from unittest.mock import Mock

import pytest


SOURCE_PATH = Path(
    os.getenv(
        "CONSUMER_TOPIC_SCHEMA_MAPPER_MAIN_PATH",
        "blueprints/consumer/topic/schema_mapper/main.py",
    )
)


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
        value=b"value",
        key=b"key",
        headers=None,
        error_obj=None,
        topic="mapper-topic",
        partition=0,
        offset=10,
    ):
        self._value = value
        self._key = key
        self._headers = headers
        self._error_obj = error_obj
        self._topic = topic
        self._partition = partition
        self._offset = offset

    def value(self):
        return self._value

    def key(self):
        return self._key

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


class FakeProducer:
    instances = []

    def __init__(self, config):
        self.config = config
        self.produced = []
        self.poll_calls = []
        self.flush_calls = 0
        FakeProducer.instances.append(self)

    def produce(self, **kwargs):
        self.produced.append(kwargs)

    def poll(self, timeout):
        self.poll_calls.append(timeout)

    def flush(self):
        self.flush_calls += 1


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


class FakeTopicResolver:
    def resolve(self, topic_name, src_topic, suffix):
        return topic_name or f"{src_topic}.{suffix}"


class FakeKafkaTopicManager:
    instances = []
    raise_on_ensure = False

    def __init__(self, bootstrap_server, logger):
        self.bootstrap_server = bootstrap_server
        self.logger = logger
        self.ensured_topics = []
        FakeKafkaTopicManager.instances.append(self)

    def ensure_exists(self, topic):
        self.ensured_topics.append(topic)
        if FakeKafkaTopicManager.raise_on_ensure:
            raise RuntimeError("topic create failed")


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
    FakeProducer.instances.clear()
    FakeKafkaTopicManager.instances.clear()
    FakeKafkaTopicManager.raise_on_ensure = False
    FakeStepLogger.instances.clear()
    FakeHeartbeatLogger.instances.clear()
    FakeBackend.instances.clear()

    monkeypatch.setenv("bootstrapServer", "localhost:9092")
    monkeypatch.setenv("mapperTopicName", "mapper-topic")
    monkeypatch.setenv("mapperGroupId", "mapper-group")
    monkeypatch.setenv("consumerRetryDelaySecs", "1")
    monkeypatch.setenv("schemaType", "EQBD")
    monkeypatch.setenv("orgName", "NESO")
    monkeypatch.setenv("productType", "EQBD PG Gas")
    monkeypatch.setenv("PRODUCT_NAME", "consumer-topic")
    monkeypatch.setenv("SCHEDULER_BACKEND", "standalone")
    monkeypatch.setenv("TOPIC_TASK_TIMEOUT_SECS", "7")

    confluent = types.ModuleType("confluent_kafka")
    confluent.Consumer = FakeConsumer
    confluent.Producer = FakeProducer
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

    utils_pkg = types.ModuleType("utils")

    topic_utils = types.ModuleType("utils.topic_utils")
    topic_utils.TopicResolver = FakeTopicResolver
    topic_utils.KafkaTopicManager = FakeKafkaTopicManager

    pipeline_context = types.ModuleType("utils.pipeline_context")
    pipeline_context.PipelineContext = FakeContext

    scheduler_backend = types.ModuleType("utils.scheduler_backend")
    scheduler_backend.get_backend = Mock(return_value=FakeBackend())

    step_logger = types.ModuleType("utils.step_logger")
    step_logger.StepLogger = FakeStepLogger

    monkeypatch.setitem(sys.modules, "utils", utils_pkg)
    monkeypatch.setitem(sys.modules, "utils.topic_utils", topic_utils)
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

    instrumentation = types.ModuleType("dpn_observability_sdk.otel_instrumentation")
    instrumentation.traced = identity_decorator
    instrumentation.timed_metric = identity_decorator

    heartbeat = types.ModuleType("dpn_observability_sdk.heartbeat")
    heartbeat.HeartbeatLogger = FakeHeartbeatLogger

    monkeypatch.setitem(sys.modules, "dpn_observability_sdk", sdk_pkg)
    monkeypatch.setitem(sys.modules, "dpn_observability_sdk.otel_logger", otel_logger)
    monkeypatch.setitem(sys.modules, "dpn_observability_sdk.otel_tracer", otel_tracer)
    monkeypatch.setitem(sys.modules, "dpn_observability_sdk.otel_metrics", otel_metrics)
    monkeypatch.setitem(
        sys.modules,
        "dpn_observability_sdk.otel_instrumentation",
        instrumentation,
    )
    monkeypatch.setitem(sys.modules, "dpn_observability_sdk.heartbeat", heartbeat)


@pytest.fixture
def load_module():
    def _load():
        if not SOURCE_PATH.exists():
            pytest.skip(
                f"Source file not found: {SOURCE_PATH}. "
                "Run pytest from repo root or set CONSUMER_TOPIC_SCHEMA_MAPPER_MAIN_PATH."
            )

        module_name = "consumer_topic_schema_mapper_main_under_test"
        sys.modules.pop(module_name, None)

        spec = importlib.util.spec_from_file_location(module_name, SOURCE_PATH)
        module = importlib.util.module_from_spec(spec)
        sys.modules[module_name] = module
        spec.loader.exec_module(module)
        return module

    return _load


def valid_headers_snake():
    return [
        ("schema_type", b"EQBD"),
        ("org_name", b"NESO"),
        ("product_type", b"EQBD PG Gas"),
        ("traceparent", b"00-test-trace"),
    ]


def valid_headers_camel():
    return [
        ("schemaType", "EQBD"),
        ("orgName", "NESO"),
        ("productType", "EQBD PG Gas"),
    ]


def test_sanitize(load_module):
    module = load_module()

    assert module._sanitize("EQBD PG Gas") == "eqbdpggas"
    assert module._sanitize(None) == "unknown"
    assert module._sanitize("!!!", default="fallback") == "fallback"
    assert module._sanitize("A-B_C 123") == "abc123"


def test_initialises_config_topics_and_producer(load_module):
    module = load_module()
    mapper = module.TopicSchemaMapper()

    assert mapper.bootstrap == "localhost:9092"
    assert mapper.src_topic == "mapper-topic"
    assert mapper.group_id == "mapper-group"
    assert mapper.retry_delay == 1
    assert mapper.schema_type == "eqbd"
    assert mapper.org_name == "neso"
    assert mapper.product_type == "eqbdpggas"
    assert mapper.mapper_topic == "mapper-topic"
    assert mapper.target_topic == ""
    assert mapper.heartbeat is None

    assert FakeKafkaTopicManager.instances[-1].ensured_topics == ["mapper-topic"]
    assert FakeProducer.instances[-1].config == {"bootstrap.servers": "localhost:9092"}


def test_initialises_defaults(load_module, monkeypatch):
    monkeypatch.delenv("mapperGroupId", raising=False)
    monkeypatch.delenv("schemaType", raising=False)
    monkeypatch.delenv("orgName", raising=False)
    monkeypatch.delenv("productType", raising=False)
    monkeypatch.delenv("mapperTopicName", raising=False)

    module = load_module()
    mapper = module.TopicSchemaMapper()

    assert mapper.group_id == "consumer_topic_mapper"
    assert mapper.schema_type == "unknown"
    assert mapper.org_name == "unknown"
    assert mapper.product_type == "unknown"
    assert mapper.mapper_topic == ".trfm"


def test_resolve_target_valid_snake_camel_invalid_and_creation_failure(load_module):
    module = load_module()
    mapper = module.TopicSchemaMapper()

    mapper._resolve_target(
        {
            "schema_type": "EQBD",
            "org_name": "NESO",
            "product_type": "EQBD PG Gas",
        }
    )

    expected = "dpn-consumer-neso-eqbd-eqbdpggas-target"

    assert mapper.target_topic == expected
    assert FakeKafkaTopicManager.instances[-1].ensured_topics[-1] == expected
    assert mapper.logger.info_calls[-1][0] == f"Ensuring topic exists: {expected}"

    ensure_count = len(FakeKafkaTopicManager.instances[-1].ensured_topics)

    mapper._resolve_target(
        {
            "schemaType": "EQBD",
            "orgName": "NESO",
            "productType": "EQBD PG Gas",
        }
    )

    assert len(FakeKafkaTopicManager.instances[-1].ensured_topics) == ensure_count

    mapper._resolve_target(
        {
            "schema_type": "",
            "org_name": "NESO",
            "product_type": "EQBD PG Gas",
        }
    )

    assert mapper.target_topic == ""
    assert mapper.logger.warning_calls[-1][0] == "Skipping message due to invalid metadata"

    FakeKafkaTopicManager.raise_on_ensure = True

    mapper._resolve_target(
        {
            "schema_type": "ABC",
            "org_name": "NESO",
            "product_type": "Gas",
        }
    )

    assert mapper.target_topic == "dpn-consumer-neso-abc-gas-target"
    assert mapper.logger.error_calls[-1][0].startswith("Topic creation failed:")


def test_on_delivery(load_module):
    module = load_module()
    mapper = module.TopicSchemaMapper()

    mapper._on_delivery(Exception("delivery failed"), Mock())

    assert mapper.logger.error_calls[-1][0] == "delivery failed"

    before = len(mapper.logger.error_calls)
    mapper._on_delivery(None, Mock())

    assert len(mapper.logger.error_calls) == before


def test_process_success_snake_headers_produces_and_records_metrics(load_module):
    module = load_module()
    mapper = module.TopicSchemaMapper()
    step_log = FakeStepLogger(mapper.logger)
    ctx = FakeContext()
    msg = FakeKafkaMessage(headers=valid_headers_snake(), partition=3, offset=42)

    mapper._process(msg, ctx, step_log)

    produced = mapper.producer.produced[-1]
    expected_topic = "dpn-consumer-neso-eqbd-eqbdpggas-target"

    assert produced["topic"] == expected_topic
    assert produced["value"] == b"value"
    assert produced["key"] == b"key"
    assert produced["callback"] == mapper._on_delivery

    headers = dict(produced["headers"])

    assert headers["schemaType"] == b"eqbd"
    assert headers["orgName"] == b"neso"
    assert headers["productType"] == b"eqbdpggas"
    assert headers["offset"] == "42"
    assert b"T" in headers["processedAt"]

    assert mapper.producer.poll_calls == [0]
    assert mapper.messages_processed.calls[-1][2]["status"] == "success"
    assert mapper.process_duration.calls[-1][0] == "record"

    assert step_log.step_start_calls[-1][1] == "mapper_msg"
    assert step_log.step_end_calls[-1][1] == "mapper_msg"

    module._otel_propagate.extract.assert_called()
    module._otel_context.attach.assert_called_with({"remote": "context"})
    module._otel_context.detach.assert_called_with("fake-token")


def test_process_success_camel_headers_with_no_key(load_module):
    module = load_module()
    mapper = module.TopicSchemaMapper()
    msg = FakeKafkaMessage(headers=valid_headers_camel(), key=None)

    mapper._process(msg, FakeContext(), FakeStepLogger(mapper.logger))

    produced = mapper.producer.produced[-1]

    assert produced["key"] is None
    assert produced["topic"] == "dpn-consumer-neso-eqbd-eqbdpggas-target"


def test_process_invalid_headers_skips_produce_and_detaches(load_module):
    module = load_module()
    mapper = module.TopicSchemaMapper()
    step_log = FakeStepLogger(mapper.logger)
    msg = FakeKafkaMessage(headers=None)

    mapper._process(msg, FakeContext(), step_log)

    assert mapper.producer.produced == []
    assert mapper.target_topic == ""
    assert mapper.logger.warning_calls[-1][0] == "Skipping produce (no valid target topic)"
    assert step_log.step_start_calls[-1][1] == "mapper_msg"
    assert step_log.step_end_calls == []
    assert module._otel_context.detach.called


def test_process_failure_records_error_and_reraises(load_module):
    module = load_module()
    mapper = module.TopicSchemaMapper()
    step_log = FakeStepLogger(mapper.logger)

    def boom(**kwargs):
        raise RuntimeError("produce failed")

    mapper.producer.produce = boom

    with pytest.raises(RuntimeError, match="produce failed"):
        mapper._process(
            FakeKafkaMessage(headers=valid_headers_snake()),
            FakeContext(),
            step_log,
        )

    assert mapper.messages_processed.calls[-1][2]["status"] == "error"
    assert step_log.step_failed_calls[-1][1] == "mapper_msg"
    assert module._otel_context.detach.called


def test_run_window_processes_none_eof_valid_and_breaks(load_module, monkeypatch):
    module = load_module()
    mapper = module.TopicSchemaMapper()
    step_log = FakeStepLogger(mapper.logger)
    ctx = FakeContext()

    eof_msg = FakeKafkaMessage(error_obj=FakeKafkaError(FakeKafkaError._PARTITION_EOF))
    valid_msg = FakeKafkaMessage(partition=4)

    FakeConsumer.poll_side_effects[:] = [
        None,
        eof_msg,
        valid_msg,
    ]

    monotonic_values = iter([0, 0.1, 0.2, 0.3, 0.4, 1.1, 1.2])
    monkeypatch.setattr(module.time, "monotonic", lambda: next(monotonic_values))
    monkeypatch.setattr(module.time, "sleep", Mock())

    mapper._process = Mock()

    mapper.run_window(ctx, step_log, stop_after=1)

    assert FakeConsumer.instances[-1].subscriptions == [["mapper-topic"]]
    assert FakeConsumer.instances[-1].closed is True
    assert mapper.producer.flush_calls == 1

    mapper._process.assert_called_once_with(valid_msg, ctx, step_log)
    assert mapper.messages_consumed.calls[-1][2]["partition"] == "4"


def test_run_window_handles_kafka_exception(load_module, monkeypatch):
    module = load_module()
    mapper = module.TopicSchemaMapper()

    FakeConsumer.poll_side_effects[:] = [
        FakeKafkaMessage(error_obj=FakeKafkaError(999))
    ]

    monotonic_values = iter([0, 0.1, 0.2, 1.1])
    monkeypatch.setattr(module.time, "monotonic", lambda: next(monotonic_values))
    monkeypatch.setattr(module.time, "sleep", Mock())

    mapper.run_window(FakeContext(), FakeStepLogger(mapper.logger), stop_after=1)

    assert mapper.logger.error_calls[-1][0] == "kafka error"
    assert FakeConsumer.instances[-1].closed is True
    assert mapper.producer.flush_calls == 1


def test_run_window_handles_unexpected_exception(load_module, monkeypatch):
    module = load_module()
    mapper = module.TopicSchemaMapper()

    FakeConsumer.poll_side_effects[:] = [
        RuntimeError("poll exploded")
    ]

    monotonic_values = iter([0, 0.1, 0.2, 1.1])
    monkeypatch.setattr(module.time, "monotonic", lambda: next(monotonic_values))
    monkeypatch.setattr(module.time, "sleep", Mock())

    mapper.run_window(FakeContext(), FakeStepLogger(mapper.logger), stop_after=1)

    assert mapper.logger.error_calls[-1][0] == "unexpected"
    assert "poll exploded" in mapper.logger.error_calls[-1][2]["error"]
    assert FakeConsumer.instances[-1].closed is True
    assert mapper.producer.flush_calls == 1


def test_run_window_exits_immediately_when_deadline_already_passed(load_module, monkeypatch):
    module = load_module()
    mapper = module.TopicSchemaMapper()

    # deadline = 0 + 1 = 1; first outer check returns 1.0 >= 1 → break before consumer is created
    monotonic_values = iter([0, 1.0])
    monkeypatch.setattr(module.time, "monotonic", lambda: next(monotonic_values))

    mapper.run_window(FakeContext(), FakeStepLogger(mapper.logger), stop_after=1)

    assert not FakeConsumer.instances


def test_run_window_without_deadline_sleeps_between_retries(load_module, monkeypatch):
    module = load_module()
    mapper = module.TopicSchemaMapper()

    FakeConsumer.poll_side_effects[:] = [
        FakeKafkaException("temporary kafka issue")
    ]

    class StopLoop(Exception):
        pass

    def fake_sleep(seconds):
        assert seconds == mapper.retry_delay
        raise StopLoop()

    monkeypatch.setattr(module.time, "sleep", fake_sleep)

    with pytest.raises(StopLoop):
        mapper.run_window(FakeContext(), FakeStepLogger(mapper.logger), stop_after=None)

    assert mapper.logger.error_calls[-1][0] == "kafka error"
    assert FakeConsumer.instances[-1].closed is True


def test_run_timeouts_success_and_failure(load_module, monkeypatch):
    module = load_module()

    fake_mapper = Mock()
    fake_mapper.logger = FakeLogger()
    fake_mapper.mapper_topic = "mapper-topic"
    fake_mapper.run_window = Mock()

    monkeypatch.setattr(module, "TopicSchemaMapper", Mock(return_value=fake_mapper))
    monkeypatch.setattr(module, "StepLogger", FakeStepLogger)

    module.run(FakeContext(triggered_by="kafka-trigger"))

    _, _, kwargs = fake_mapper.run_window.mock_calls[-1]
    assert kwargs["stop_after"] == 7
    assert FakeStepLogger.instances[-1].step_end_calls[-1][1] == "mapper_window"

    fake_mapper.run_window.reset_mock()

    module.run(FakeContext(triggered_by="interval"))

    _, _, kwargs = fake_mapper.run_window.mock_calls[-1]
    assert kwargs["stop_after"] == 7

    fake_mapper.run_window.reset_mock()

    module.run(FakeContext(triggered_by="manual"))

    _, _, kwargs = fake_mapper.run_window.mock_calls[-1]
    assert kwargs["stop_after"] is None

    fake_mapper.run_window = Mock(side_effect=RuntimeError("window failed"))

    with pytest.raises(RuntimeError, match="window failed"):
        module.run(FakeContext(triggered_by="manual"))

    assert FakeStepLogger.instances[-1].step_failed_calls[-1][1] == "mapper_window"


def test_main_block():
    if not SOURCE_PATH.exists():
        pytest.skip(
            f"Source file not found: {SOURCE_PATH}. "
            "Run pytest from repo root or set CONSUMER_TOPIC_SCHEMA_MAPPER_MAIN_PATH."
        )

    runpy.run_path(str(SOURCE_PATH), run_name="__main__")

    heartbeat = FakeHeartbeatLogger.instances[-1]

    assert heartbeat.started is True
    assert heartbeat.component_name == "consumer-topic-mapper"
    assert heartbeat.metadata == {
        "source_topic": "mapper-topic",
        "mapper_topic": "mapper-topic",
        "scheduler_backend": "standalone",
    }

    _, kwargs = FakeBackend.instances[-1].execute_calls[-1]

    assert kwargs == {
        "pipeline_stage": "schema_mapper",
        "pipeline_type": "topic",
        "pipeline_role": "consumer",
        "component_name": "consumer-topic-mapper",
    }