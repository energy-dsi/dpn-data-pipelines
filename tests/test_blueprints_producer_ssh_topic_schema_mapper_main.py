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
        "SCHEMA_MAPPER_MAIN_PATH",
        "blueprints/producer/topic/ssh/schema_mapper/main.py",
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
    def __init__(self):
        self.span_calls = []

    def start_as_current_span(self, name, context=None):
        self.span_calls.append((name, context))
        return FakeSpan()


class FakeLogger:
    def __init__(self):
        self.info_calls = []
        self.error_calls = []

    def info(self, message, extra=None, **kwargs):
        self.info_calls.append((message, extra, kwargs))

    def error(self, message, extra=None, **kwargs):
        self.error_calls.append((message, extra, kwargs))


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

    def poll(self, timeout):
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

    def step_failed(self, ctx, operation, exc=None):
        self.step_failed_calls.append((ctx, operation, exc))


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


class FakeMessage:
    def __init__(
        self,
        value=b"value",
        key=b"key",
        topic="raw-topic",
        partition=0,
        offset=10,
        headers=None,
        error_obj=None,
    ):
        self._value = value
        self._key = key
        self._topic = topic
        self._partition = partition
        self._offset = offset
        self._headers = headers
        self._error_obj = error_obj

    def value(self):
        return self._value

    def key(self):
        return self._key

    def topic(self):
        return self._topic

    def partition(self):
        return self._partition

    def offset(self):
        return self._offset

    def headers(self):
        return self._headers

    def error(self):
        return self._error_obj


class FakeContext:
    def __init__(self, triggered_by="manual", run_id="run-123"):
        self.triggered_by = triggered_by
        self.run_id = run_id

    def as_log_extra(self):
        return {"pipeline.run_id": self.run_id, "pipeline.triggered_by": self.triggered_by}


def identity_decorator(*decorator_args, **decorator_kwargs):
    def wrapper(func):
        return func

    return wrapper


@pytest.fixture(autouse=True)
def fake_external_modules(monkeypatch):
    FakeProducer.instances.clear()
    FakeConsumer.instances.clear()
    FakeConsumer.poll_side_effects.clear()
    FakeKafkaTopicManager.instances.clear()
    FakeStepLogger.instances.clear()
    FakeHeartbeatLogger.instances.clear()
    FakeBackend.instances.clear()

    monkeypatch.setenv("bootstrapServer", "localhost:9092")
    monkeypatch.setenv("mapperTopicName", "raw-topic")
    monkeypatch.setenv("mapperGroupId", "mapper-group")
    monkeypatch.setenv("consumerRetryDelaySecs", "1")
    monkeypatch.setenv("schemaType", "EQBD")
    monkeypatch.setenv("orgName", "NESO")
    monkeypatch.setenv("productType", "EQBD PG Gas")
    monkeypatch.setenv("SCHEDULER_BACKEND", "standalone")
    monkeypatch.setenv("TOPIC_TASK_TIMEOUT_SECS", "7")

    confluent_kafka = types.ModuleType("confluent_kafka")
    confluent_kafka.Consumer = FakeConsumer
    confluent_kafka.Producer = FakeProducer
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
    otel_propagate.extract = Mock(return_value={"remote": "context"})

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

    otel_logger = types.ModuleType("dpn_observability_sdk.otel_logger")
    otel_logger.OtelLogger = Mock(
        return_value=types.SimpleNamespace(
            create_logger=Mock(return_value=FakeLogger())
        )
    )

    monkeypatch.setitem(sys.modules, "dpn_observability_sdk", sdk_pkg)
    monkeypatch.setitem(sys.modules, "dpn_observability_sdk.otel_tracer", otel_tracer)
    monkeypatch.setitem(sys.modules, "dpn_observability_sdk.otel_metrics", otel_metrics)
    monkeypatch.setitem(
        sys.modules,
        "dpn_observability_sdk.otel_instrumentation",
        otel_instrumentation,
    )
    monkeypatch.setitem(sys.modules, "dpn_observability_sdk.heartbeat", heartbeat)
    monkeypatch.setitem(sys.modules, "dpn_observability_sdk.otel_logger", otel_logger)


@pytest.fixture
def load_module():
    def _load():
        if not SOURCE_PATH.exists():
            pytest.skip(
                f"Source file not found: {SOURCE_PATH}. "
                "Run pytest from repo root or set SCHEMA_MAPPER_MAIN_PATH."
            )

        module_name = "schema_mapper_main_under_test"

        if module_name in sys.modules:
            del sys.modules[module_name]

        spec = importlib.util.spec_from_file_location(module_name, SOURCE_PATH)
        module = importlib.util.module_from_spec(spec)
        sys.modules[module_name] = module
        spec.loader.exec_module(module)
        return module

    return _load


def test_sanitize_handles_normal_none_symbols_and_empty_output(load_module):
    module = load_module()

    assert module._sanitize("EQBD PG Gas") == "eqbdpggas"
    assert module._sanitize(None) == "unknown"
    assert module._sanitize("!!!", default="fallback") == "fallback"
    assert module._sanitize("A-B_C 123") == "abc123"


def test_topic_schema_mapper_initialises_topics_metrics_and_producer(load_module):
    module = load_module()

    mapper = module.TopicSchemaMapper()

    assert mapper.bootstrap == "localhost:9092"
    assert mapper.src_topic == "raw-topic"
    assert mapper.group_id == "mapper-group"
    assert mapper.retry_delay == 1
    assert mapper.schema_type == "eqbd"
    assert mapper.org_name == "neso"
    assert mapper.product_type == "eqbdpggas"
    assert mapper.heartbeat is None

    expected_target = "dpn-producer-neso-eqbd-eqbdpggas-target"
    assert mapper.target_topic == expected_target

    topic_manager = FakeKafkaTopicManager.instances[0]
    assert topic_manager.bootstrap_server == "localhost:9092"
    assert topic_manager.ensured_topics == ["raw-topic", expected_target]

    assert mapper.logger.info_calls[-1][0] == (
        f"Ensuring target topic exists: {expected_target}"
    )

    assert FakeProducer.instances[0].config == {
        "bootstrap.servers": "localhost:9092"
    }


def test_topic_schema_mapper_defaults_env_values(load_module, monkeypatch):
    monkeypatch.delenv("mapperGroupId", raising=False)
    monkeypatch.delenv("schemaType", raising=False)
    monkeypatch.delenv("orgName", raising=False)
    monkeypatch.delenv("productType", raising=False)

    module = load_module()
    mapper = module.TopicSchemaMapper()

    assert mapper.group_id == "producer_mapper"
    assert mapper.schema_type == "unknown"
    assert mapper.org_name == "unknown"
    assert mapper.product_type == "unknown"
    assert mapper.target_topic == "dpn-producer-unknown-unknown-unknown-target"


def test_on_delivery_logs_error(load_module):
    module = load_module()
    mapper = module.TopicSchemaMapper()

    mapper._on_delivery(Exception("delivery failed"), Mock())

    assert mapper.logger.error_calls[-1][0] == "Delivery failed"
    assert "delivery failed" in mapper.logger.error_calls[-1][1]["error"]


def test_on_delivery_success_does_nothing(load_module):
    module = load_module()
    mapper = module.TopicSchemaMapper()

    before = len(mapper.logger.error_calls)

    mapper._on_delivery(None, Mock())

    assert len(mapper.logger.error_calls) == before


def test_process_success_enriches_headers_produces_polls_flushes_and_logs(load_module):
    module = load_module()
    mapper = module.TopicSchemaMapper()
    ctx = FakeContext()
    step_log = FakeStepLogger()

    msg = FakeMessage(
        headers=[
            ("traceparent", b"00-test-trace"),
            ("plain", "value"),
        ],
        offset=42,
    )

    mapper._process(msg, ctx, step_log)

    produced = mapper.producer.produced[0]

    assert produced["topic"] == mapper.target_topic
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
    assert mapper.producer.flush_calls == 1

    assert step_log.step_start_calls[-1][1] == "mapper_msg"
    assert step_log.step_end_calls[-1][1] == "mapper_msg"
    assert not step_log.step_failed_calls

    module._otel_propagate.extract.assert_called_once()
    assert mapper.tracer.span_calls[-1] == ("process_message", {"remote": "context"})


def test_process_success_with_no_headers(load_module):
    module = load_module()
    mapper = module.TopicSchemaMapper()
    ctx = FakeContext()
    step_log = FakeStepLogger()

    msg = FakeMessage(headers=None)

    mapper._process(msg, ctx, step_log)

    assert mapper.producer.produced
    module._otel_propagate.extract.assert_called_once_with({})


def test_process_failure_records_exception_step_failed_and_reraises(load_module):
    module = load_module()
    mapper = module.TopicSchemaMapper()
    ctx = FakeContext()
    step_log = FakeStepLogger()
    msg = FakeMessage()

    def boom(**kwargs):
        raise RuntimeError("produce failed")

    mapper.producer.produce = boom

    with pytest.raises(RuntimeError, match="produce failed"):
        mapper._process(msg, ctx, step_log)

    assert step_log.step_failed_calls[-1][1] == "mapper_msg"


def test_run_window_processes_none_eof_and_valid_message(load_module, monkeypatch):
    module = load_module()
    mapper = module.TopicSchemaMapper()
    ctx = FakeContext()
    step_log = FakeStepLogger()

    eof_message = FakeMessage(error_obj=FakeKafkaError(FakeKafkaError._PARTITION_EOF))
    valid_message = FakeMessage(partition=3)

    FakeConsumer.poll_side_effects[:] = [
        None,
        eof_message,
        valid_message,
    ]

    monotonic_values = iter([0, 0.1, 0.2, 0.3, 0.4, 1.1, 1.2])
    monkeypatch.setattr(module.time, "monotonic", lambda: next(monotonic_values))
    monkeypatch.setattr(module.time, "sleep", Mock())

    mapper._process = Mock()

    mapper.run_window(ctx, step_log, stop_after=1)

    consumer = FakeConsumer.instances[0]

    assert consumer.subscriptions == [["raw-topic"]]
    assert consumer.closed is True
    assert mapper.producer.flush_calls == 1
    mapper._process.assert_called_once_with(valid_message, ctx, step_log)


def test_run_window_handles_kafka_error_as_unexpected(load_module, monkeypatch):
    module = load_module()
    mapper = module.TopicSchemaMapper()
    ctx = FakeContext()
    step_log = FakeStepLogger()

    bad_message = FakeMessage(error_obj=FakeKafkaError(999))
    FakeConsumer.poll_side_effects[:] = [bad_message]

    monotonic_values = iter([0, 0.1, 0.2, 1.1, 1.2])
    monkeypatch.setattr(module.time, "monotonic", lambda: next(monotonic_values))
    monkeypatch.setattr(module.time, "sleep", Mock())

    mapper.run_window(ctx, step_log, stop_after=1)

    assert mapper.logger.error_calls[-1][0] == "unexpected"
    assert FakeConsumer.instances[0].closed is True
    assert mapper.producer.flush_calls == 1


def test_run_window_handles_unexpected_poll_exception(load_module, monkeypatch):
    module = load_module()
    mapper = module.TopicSchemaMapper()
    ctx = FakeContext()
    step_log = FakeStepLogger()

    FakeConsumer.poll_side_effects[:] = [RuntimeError("poll exploded")]

    monotonic_values = iter([0, 0.1, 0.2, 1.1, 1.2])
    monkeypatch.setattr(module.time, "monotonic", lambda: next(monotonic_values))
    monkeypatch.setattr(module.time, "sleep", Mock())

    mapper.run_window(ctx, step_log, stop_after=1)

    assert mapper.logger.error_calls[-1][0] == "unexpected"
    assert "poll exploded" in mapper.logger.error_calls[-1][1]["error"]
    assert FakeConsumer.instances[0].closed is True
    assert mapper.producer.flush_calls == 1


def test_run_window_counts_skipped_when_target_topic_is_empty(load_module, monkeypatch):
    module = load_module()
    mapper = module.TopicSchemaMapper()
    mapper.target_topic = ""

    ctx = FakeContext()
    step_log = FakeStepLogger()
    valid_message = FakeMessage()

    FakeConsumer.poll_side_effects[:] = [valid_message]

    # Required monotonic calls:
    # 1. deadline calculation
    # 2. outer while deadline check
    # 3. inner while deadline check
    # 4. inner while deadline check after message processing
    # 5. outer while deadline check after sleep
    monotonic_values = iter([0, 0.1, 0.2, 1.1, 1.2])

    monkeypatch.setattr(module.time, "monotonic", lambda: next(monotonic_values))
    monkeypatch.setattr(module.time, "sleep", Mock())

    mapper._process = Mock()

    mapper.run_window(ctx, step_log, stop_after=1)

    mapper._process.assert_called_once_with(valid_message, ctx, step_log)
    assert FakeConsumer.instances[0].closed is True


def test_run_window_without_deadline_sleeps_between_retries(load_module, monkeypatch):
    module = load_module()
    mapper = module.TopicSchemaMapper()
    ctx = FakeContext()
    step_log = FakeStepLogger()

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
        mapper.run_window(ctx, step_log, stop_after=None)

    assert mapper.logger.error_calls[-1][0] == "unexpected"
    assert FakeConsumer.instances[0].closed is True


def test_run_uses_timeout_for_kafka_trigger(load_module, monkeypatch):
    module = load_module()

    fake_mapper = Mock()
    fake_mapper.logger = FakeLogger()
    fake_mapper.src_topic = "raw-topic"
    fake_mapper.target_topic = "target-topic"
    fake_mapper.run_window = Mock()

    monkeypatch.setattr(module, "TopicSchemaMapper", Mock(return_value=fake_mapper))
    monkeypatch.setattr(module, "StepLogger", FakeStepLogger)

    ctx = FakeContext(triggered_by="kafka-trigger", run_id="run-kafka")

    module.run(ctx)

    fake_mapper.run_window.assert_called_once()
    _, _, kwargs = fake_mapper.run_window.mock_calls[0]
    assert kwargs["stop_after"] == 7

    step_log = FakeStepLogger.instances[-1]
    assert step_log.pipeline_banner_calls
    assert step_log.pipeline_banner_calls[-1][1] == "producer-schema-mapper"


def test_run_uses_timeout_for_interval_trigger(load_module, monkeypatch):
    module = load_module()

    fake_mapper = Mock()
    fake_mapper.logger = FakeLogger()
    fake_mapper.src_topic = "raw-topic"
    fake_mapper.target_topic = "target-topic"
    fake_mapper.run_window = Mock()

    monkeypatch.setattr(module, "TopicSchemaMapper", Mock(return_value=fake_mapper))
    monkeypatch.setattr(module, "StepLogger", FakeStepLogger)

    ctx = FakeContext(triggered_by="interval", run_id="run-interval")

    module.run(ctx)

    _, _, kwargs = fake_mapper.run_window.mock_calls[0]
    assert kwargs["stop_after"] == 7


def test_run_uses_no_timeout_for_manual_trigger(load_module, monkeypatch):
    module = load_module()

    fake_mapper = Mock()
    fake_mapper.logger = FakeLogger()
    fake_mapper.src_topic = "raw-topic"
    fake_mapper.target_topic = "target-topic"
    fake_mapper.run_window = Mock()

    monkeypatch.setattr(module, "TopicSchemaMapper", Mock(return_value=fake_mapper))
    monkeypatch.setattr(module, "StepLogger", FakeStepLogger)

    ctx = FakeContext(triggered_by="manual", run_id="run-manual")

    module.run(ctx)

    _, _, kwargs = fake_mapper.run_window.mock_calls[0]
    assert kwargs["stop_after"] is None


def test_run_failure_reraises(load_module, monkeypatch):
    module = load_module()

    fake_mapper = Mock()
    fake_mapper.logger = FakeLogger()
    fake_mapper.src_topic = "raw-topic"
    fake_mapper.target_topic = "target-topic"
    fake_mapper.run_window = Mock(side_effect=RuntimeError("window failed"))

    monkeypatch.setattr(module, "TopicSchemaMapper", Mock(return_value=fake_mapper))
    monkeypatch.setattr(module, "StepLogger", FakeStepLogger)

    ctx = FakeContext(triggered_by="manual", run_id="run-error")

    with pytest.raises(RuntimeError, match="window failed"):
        module.run(ctx)


def test_main_block_creates_backend_heartbeat_and_executes_run():
    if not SOURCE_PATH.exists():
        pytest.skip(
            f"Source file not found: {SOURCE_PATH}. "
            "Run pytest from repo root or set SCHEMA_MAPPER_MAIN_PATH."
        )

    runpy.run_path(str(SOURCE_PATH), run_name="__main__")

    assert FakeBackend.instances
    backend = FakeBackend.instances[-1]

    assert backend.execute_calls
    func, kwargs = backend.execute_calls[-1]

    assert callable(func)
    assert kwargs == {
        "pipeline_stage": "schema_mapper",
        "pipeline_type": "topic",
        "pipeline_role": "producer",
        "component_name": "producer-topic-schema-mapper-ssh",
    }

    assert FakeHeartbeatLogger.instances
    heartbeat = FakeHeartbeatLogger.instances[-1]

    assert heartbeat.started is True
    assert heartbeat.component_name == "producer-topic-schema-mapper-ssh"
    assert heartbeat.metadata["source_topic"] == "raw-topic"
    assert heartbeat.metadata["target_topic"] == (
        "dpn-producer-neso-eqbd-eqbdpggas-target"
    )
    assert heartbeat.metadata["scheduler_backend"] == "standalone"