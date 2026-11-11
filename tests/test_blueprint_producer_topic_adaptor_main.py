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
        "ADAPTOR_MAIN_PATH",
        "blueprints/producer/topic/adaptor/main.py",
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
    def start_as_current_span(self, name):
        return FakeSpan()


class FakeLogger:
    def __init__(self):
        self.info_calls = []
        self.error_calls = []
        self.warning_calls = []

    def info(self, message, extra=None, **kwargs):
        self.info_calls.append((message, extra, kwargs))

    def error(self, message, extra=None, **kwargs):
        self.error_calls.append((message, extra, kwargs))

    def warning(self, message, extra=None, **kwargs):
        self.warning_calls.append((message, extra, kwargs))


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

    def flush(self, timeout=None):
        self.flush_calls += 1
        self.flush_timeout = timeout
        return 0


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
    def resolve(self, provided, src_topic, suffix):
        return provided or f"{src_topic}.{suffix}"


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


class FakeMessage:
    def __init__(
        self,
        value=b"value",
        key=b"key",
        topic="source-topic",
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
    monkeypatch.setenv("KAFKA_SECURITY_PROTOCOL", "PLAINTEXT")
    for _var in ("KAFKA_SASL_MECHANISM", "KAFKA_OAUTH_CLIENT_ID", "KAFKA_OAUTH_CLIENT_SECRET", "KAFKA_OAUTH_TOKEN_ENDPOINT_URL", "KAFKA_OAUTH_SCOPE", "KAFKA_SASL_USERNAME", "KAFKA_SASL_PASSWORD", "KAFKA_SSL_CA_LOCATION", "KAFKA_SSL_CERTIFICATE_LOCATION", "KAFKA_SSL_KEY_LOCATION", "KAFKA_SSL_KEY_PASSWORD", "KAFKA_SSL_ENDPOINT_IDENTIFICATION"):
        monkeypatch.delenv(_var, raising=False)
    monkeypatch.setenv("srcTopicName", "input-topic")
    # No mapperTopicName any more — the adaptor absorbed the (now-removed)
    # schema mapper stage entirely, so it computes the target topic itself
    # from orgName/productType. See
    # blueprints/producer/topic/adaptor/main.py's v3.0.0 changelog entry.
    monkeypatch.setenv("orgName", "Acme Corp")
    monkeypatch.setenv("productType", "widget-01")
    monkeypatch.setenv("srcGroupId", "group-1")
    monkeypatch.setenv("consumerRetryDelaySecs", "1")
    monkeypatch.setenv("PRODUCT_NAME", "test-product")
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

    def fake_inject(carrier):
        carrier["traceparent"] = "00-test-trace"

    otel_propagate.inject = fake_inject
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
    scheduler_backend.run_once = Mock()

    step_logger = types.ModuleType("utils.step_logger")
    step_logger.StepLogger = FakeStepLogger

    kafka_security = types.ModuleType("utils.kafka_security")
    kafka_security.build_kafka_client_config = lambda bootstrap_server: {
        "bootstrap.servers": bootstrap_server,
        "security.protocol": "plaintext",
    }

    monkeypatch.setitem(sys.modules, "utils", utils_pkg)
    monkeypatch.setitem(sys.modules, "utils.topic_utils", topic_utils)
    monkeypatch.setitem(sys.modules, "utils.pipeline_context", pipeline_context)
    monkeypatch.setitem(sys.modules, "utils.scheduler_backend", scheduler_backend)
    monkeypatch.setitem(sys.modules, "utils.step_logger", step_logger)
    monkeypatch.setitem(sys.modules, "utils.kafka_security", kafka_security)

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
                "Run pytest from repo root or set ADAPTOR_MAIN_PATH."
            )

        module_name = "adaptor_main_under_test"

        if module_name in sys.modules:
            del sys.modules[module_name]

        spec = importlib.util.spec_from_file_location(module_name, SOURCE_PATH)
        module = importlib.util.module_from_spec(spec)
        sys.modules[module_name] = module
        spec.loader.exec_module(module)
        return module

    return _load


def test_topic_forwarder_initialises(load_module):
    module = load_module()

    forwarder = module.TopicForwarder()

    assert forwarder.bootstrap == "localhost:9092"
    assert forwarder.src_topic == "input-topic"
    assert forwarder.org_name == "acmecorp"
    assert forwarder.product_type == "widget01"
    assert forwarder.target_topic == "dpn-producer-acmecorp-widget01-target"
    assert forwarder.group_id == "group-1"
    assert forwarder.retry_delay == 1
    assert forwarder.heartbeat is None

    assert FakeKafkaTopicManager.instances[0].bootstrap_server == "localhost:9092"
    assert FakeKafkaTopicManager.instances[0].ensured_topics == [
        "input-topic",
        "dpn-producer-acmecorp-widget01-target",
    ]

    assert FakeProducer.instances[0].config == {
        "bootstrap.servers": "localhost:9092",
        "security.protocol": "plaintext",
    }

    assert forwarder.logger.info_calls[0][0] == "Adaptor initialised"


def test_topic_forwarder_honors_target_topic_name_override(load_module, monkeypatch):
    """targetTopicName is a real user-provided value from the self-service
    review screen (see main.py's v3.1.0 changelog entry) — when set, it
    must be used verbatim instead of the computed
    dpn-producer-<org>-<product>-target formula."""
    monkeypatch.setenv("targetTopicName", "my-custom-target-topic")

    module = load_module()
    forwarder = module.TopicForwarder()

    assert forwarder.target_topic == "my-custom-target-topic"
    assert FakeKafkaTopicManager.instances[0].ensured_topics == [
        "input-topic",
        "my-custom-target-topic",
    ]


def test_topic_forwarder_defaults_group_and_sanitizes_missing_org_product(load_module, monkeypatch):
    monkeypatch.delenv("srcGroupId", raising=False)
    monkeypatch.delenv("orgName", raising=False)
    monkeypatch.delenv("productType", raising=False)

    module = load_module()
    forwarder = module.TopicForwarder()

    assert forwarder.group_id == forwarder.component_name
    assert forwarder.org_name == "unknown"
    assert forwarder.product_type == "unknown"
    assert forwarder.target_topic == "dpn-producer-unknown-unknown-target"


def test_on_delivery_logs_error(load_module):
    module = load_module()
    forwarder = module.TopicForwarder()

    forwarder._on_delivery(Exception("delivery failed"), Mock())

    assert forwarder.logger.error_calls[-1][0] == "Delivery failed"
    assert "delivery failed" in forwarder.logger.error_calls[-1][1]["error"]


def test_on_delivery_success_does_nothing(load_module):
    module = load_module()
    forwarder = module.TopicForwarder()

    before = len(forwarder.logger.error_calls)

    forwarder._on_delivery(None, Mock())

    assert len(forwarder.logger.error_calls) == before


def test_forward_success_produces_straight_to_target_with_enriched_headers(load_module):
    module = load_module()
    forwarder = module.TopicForwarder()
    ctx = FakeContext()
    step_log = FakeStepLogger()
    msg = FakeMessage(headers=[("existing", b"header")])

    forwarder._forward(msg, ctx, step_log)

    produced = forwarder.producer.produced[0]

    # One hop, straight to the target topic — no mapper topic in between.
    assert produced["topic"] == "dpn-producer-acmecorp-widget01-target"
    assert produced["value"] == b"value"
    assert produced["key"] == b"key"
    assert produced["callback"] == forwarder._on_delivery
    assert ("existing", b"header") in produced["headers"]
    assert ("traceparent", b"00-test-trace") in produced["headers"]
    # The same enrichment headers the removed schema mapper always added.
    assert ("orgName", b"acmecorp") in produced["headers"]
    assert ("productType", b"widget01") in produced["headers"]
    assert any(k == "offset" for k, _v in produced["headers"])
    assert any(k == "processedAt" for k, _v in produced["headers"])

    assert forwarder.producer.poll_calls == [0]
    assert step_log.step_start_calls
    assert step_log.step_end_calls
    assert not step_log.step_failed_calls

    assert forwarder.messages_forwarded.calls[-1][0] == "add"
    assert forwarder.messages_forwarded.calls[-1][2]["status"] == "success"
    assert forwarder.forward_duration.calls[-1][0] == "record"


def test_forward_success_with_no_key_and_no_headers(load_module):
    module = load_module()
    forwarder = module.TopicForwarder()
    ctx = FakeContext()
    step_log = FakeStepLogger()
    msg = FakeMessage(key=None, headers=None)

    forwarder._forward(msg, ctx, step_log)

    produced = forwarder.producer.produced[0]

    assert produced["key"] is None
    assert ("traceparent", b"00-test-trace") in produced["headers"]
    assert ("orgName", b"acmecorp") in produced["headers"]


def test_forward_failure(load_module):
    module = load_module()
    forwarder = module.TopicForwarder()
    ctx = FakeContext()
    step_log = FakeStepLogger()
    msg = FakeMessage()

    def boom(**kwargs):
        raise RuntimeError("produce failed")

    forwarder.producer.produce = boom

    with pytest.raises(RuntimeError, match="produce failed"):
        forwarder._forward(msg, ctx, step_log)

    assert step_log.step_failed_calls
    assert forwarder.messages_forwarded.calls[-1][2]["status"] == "error"


def test_run_window_processes_none_eof_and_valid_message(load_module, monkeypatch):
    module = load_module()
    forwarder = module.TopicForwarder()
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

    forwarder._forward = Mock()

    forwarder.run_window(ctx, step_log, stop_after=1)

    consumer = FakeConsumer.instances[0]

    assert consumer.subscriptions == [["input-topic"]]
    assert consumer.closed is True
    assert forwarder.producer.flush_calls == 1
    forwarder._forward.assert_called_once_with(valid_message, ctx, step_log)
    assert forwarder.messages_consumed.calls[-1][2]["partition"] == "3"


def test_run_window_outer_deadline_exits_before_consumer_is_created(
    load_module, monkeypatch
):
    """Covers the outer while-True deadline break firing on the very first
    loop iteration, before any Consumer is instantiated."""
    module = load_module()
    forwarder = module.TopicForwarder()
    ctx = FakeContext()
    step_log = FakeStepLogger()

    values = iter([0, 999])
    monkeypatch.setattr(module.time, "monotonic", lambda: next(values))

    forwarder.run_window(ctx, step_log, stop_after=1)

    assert len(FakeConsumer.instances) == 0
    assert forwarder.producer.flush_calls == 0


def test_run_window_logs_warning_when_flush_leaves_pending_messages(
    load_module, monkeypatch
):
    """Covers the `if pending:` warning branch when the producer's flush()
    times out with undelivered messages still buffered."""
    module = load_module()
    forwarder = module.TopicForwarder()
    ctx = FakeContext()
    step_log = FakeStepLogger()

    valid_message = FakeMessage(partition=3)
    FakeConsumer.poll_side_effects[:] = [valid_message]

    monotonic_values = iter([0, 0.1, 1.1, 1.2])
    monkeypatch.setattr(module.time, "monotonic", lambda: next(monotonic_values))

    forwarder._forward = Mock()
    forwarder.producer.flush = Mock(return_value=3)

    forwarder.run_window(ctx, step_log, stop_after=1)

    assert forwarder.logger.warning_calls
    message, extra, _ = forwarder.logger.warning_calls[-1]
    assert "undelivered" in message.lower()
    assert extra["pending_messages"] == 3
    assert extra["flush_timeout_secs"] == forwarder.flush_timeout


def test_run_window_handles_kafka_exception(load_module, monkeypatch):
    module = load_module()
    forwarder = module.TopicForwarder()
    ctx = FakeContext()
    step_log = FakeStepLogger()

    bad_message = FakeMessage(error_obj=FakeKafkaError(999))
    FakeConsumer.poll_side_effects[:] = [bad_message]

    monotonic_values = iter([0, 0.1, 0.2, 1.1])
    monkeypatch.setattr(module.time, "monotonic", lambda: next(monotonic_values))

    forwarder.run_window(ctx, step_log, stop_after=1)

    assert forwarder.logger.error_calls[-1][0] == "Kafka error"
    assert FakeConsumer.instances[0].closed is True
    assert forwarder.producer.flush_calls == 1


def test_run_window_handles_unexpected_exception(load_module, monkeypatch):
    module = load_module()
    forwarder = module.TopicForwarder()
    ctx = FakeContext()
    step_log = FakeStepLogger()

    FakeConsumer.poll_side_effects[:] = [RuntimeError("poll exploded")]

    monotonic_values = iter([0, 0.1, 0.2, 1.1])
    monkeypatch.setattr(module.time, "monotonic", lambda: next(monotonic_values))

    forwarder.run_window(ctx, step_log, stop_after=1)

    assert forwarder.logger.error_calls[-1][0] == "Unexpected error"
    assert FakeConsumer.instances[0].closed is True
    assert forwarder.producer.flush_calls == 1


def test_run_window_without_deadline_sleeps_between_retries(load_module, monkeypatch):
    module = load_module()
    forwarder = module.TopicForwarder()
    ctx = FakeContext()
    step_log = FakeStepLogger()

    FakeConsumer.poll_side_effects[:] = [
        FakeKafkaException("temporary kafka issue")
    ]

    class StopLoop(Exception):
        pass

    def fake_sleep(seconds):
        assert seconds == forwarder.retry_delay
        raise StopLoop()

    monkeypatch.setattr(module.time, "sleep", fake_sleep)

    with pytest.raises(StopLoop):
        forwarder.run_window(ctx, step_log, stop_after=None)

    assert forwarder.logger.error_calls[-1][0] == "Kafka error"
    assert FakeConsumer.instances[0].closed is True


def test_run_uses_timeout_for_kafka_trigger(load_module, monkeypatch):
    module = load_module()

    fake_forwarder = Mock()
    fake_forwarder.logger = FakeLogger()
    fake_forwarder.src_topic = "input-topic"
    fake_forwarder.target_topic = "dpn-producer-acmecorp-widget01-target"
    fake_forwarder.run_window = Mock()

    monkeypatch.setattr(module, "TopicForwarder", Mock(return_value=fake_forwarder))
    monkeypatch.setattr(module, "StepLogger", FakeStepLogger)

    ctx = FakeContext(triggered_by="kafka-trigger", run_id="run-kafka")

    module.run(ctx)

    fake_forwarder.run_window.assert_called_once()
    _, _, kwargs = fake_forwarder.run_window.mock_calls[0]
    assert kwargs["stop_after"] == 7

    step_log = FakeStepLogger.instances[-1]
    assert step_log.pipeline_banner_calls
    assert step_log.step_start_calls[-1][1] == "adaptor_window"
    assert step_log.step_end_calls[-1][1] == "adaptor_window"


def test_run_uses_timeout_for_interval_trigger(load_module, monkeypatch):
    module = load_module()

    fake_forwarder = Mock()
    fake_forwarder.logger = FakeLogger()
    fake_forwarder.src_topic = "input-topic"
    fake_forwarder.target_topic = "dpn-producer-acmecorp-widget01-target"
    fake_forwarder.run_window = Mock()

    monkeypatch.setattr(module, "TopicForwarder", Mock(return_value=fake_forwarder))
    monkeypatch.setattr(module, "StepLogger", FakeStepLogger)

    ctx = FakeContext(triggered_by="interval", run_id="run-interval")

    module.run(ctx)

    _, _, kwargs = fake_forwarder.run_window.mock_calls[0]
    assert kwargs["stop_after"] == 7


def test_run_uses_no_timeout_for_manual_trigger(load_module, monkeypatch):
    module = load_module()

    fake_forwarder = Mock()
    fake_forwarder.logger = FakeLogger()
    fake_forwarder.src_topic = "input-topic"
    fake_forwarder.target_topic = "dpn-producer-acmecorp-widget01-target"
    fake_forwarder.run_window = Mock()

    monkeypatch.setattr(module, "TopicForwarder", Mock(return_value=fake_forwarder))
    monkeypatch.setattr(module, "StepLogger", FakeStepLogger)

    ctx = FakeContext(triggered_by="standalone", run_id="run-manual")

    module.run(ctx)

    _, _, kwargs = fake_forwarder.run_window.mock_calls[0]
    assert kwargs["stop_after"] is None


def test_run_failure_marks_step_failed_and_reraises(load_module, monkeypatch):
    module = load_module()

    fake_forwarder = Mock()
    fake_forwarder.logger = FakeLogger()
    fake_forwarder.src_topic = "input-topic"
    fake_forwarder.target_topic = "dpn-producer-acmecorp-widget01-target"
    fake_forwarder.run_window = Mock(side_effect=RuntimeError("window failed"))

    monkeypatch.setattr(module, "TopicForwarder", Mock(return_value=fake_forwarder))
    monkeypatch.setattr(module, "StepLogger", FakeStepLogger)

    ctx = FakeContext(triggered_by="manual", run_id="run-error")

    with pytest.raises(RuntimeError, match="window failed"):
        module.run(ctx)

    step_log = FakeStepLogger.instances[-1]
    assert step_log.step_failed_calls[-1][1] == "adaptor_window"


def test_main_block_creates_backend_heartbeat_and_executes_run():
    if not SOURCE_PATH.exists():
        pytest.skip(
            f"Source file not found: {SOURCE_PATH}. "
            "Run pytest from repo root or set ADAPTOR_MAIN_PATH."
        )

    runpy.run_path(str(SOURCE_PATH), run_name="__main__")

    assert FakeBackend.instances
    backend = FakeBackend.instances[-1]

    assert backend.execute_calls
    func, kwargs = backend.execute_calls[-1]

    assert callable(func)
    assert kwargs == {
        "pipeline_stage": "adaptor",
        "pipeline_type": "topic",
        "pipeline_role": "producer",
        "component_name": "producer-topic-adaptor-test-product",
    }

    assert FakeHeartbeatLogger.instances
    heartbeat = FakeHeartbeatLogger.instances[-1]

    assert heartbeat.started is True
    assert heartbeat.component_name == "producer-topic-adaptor-test-product"
    assert heartbeat.metadata["source_topic"] == "input-topic"
    assert heartbeat.metadata["target_topic"] == "dpn-producer-acmecorp-widget01-target"
    assert heartbeat.metadata["scheduler_backend"] == "standalone"


def test_main_block_run_once_flag_calls_run_once_and_skips_heartbeat(monkeypatch):
    if not SOURCE_PATH.exists():
        pytest.skip(
            f"Source file not found: {SOURCE_PATH}. "
            "Run pytest from repo root or set ADAPTOR_MAIN_PATH."
        )

    monkeypatch.setattr(sys, "argv", [sys.argv[0], "--run-once"])

    runpy.run_path(str(SOURCE_PATH), run_name="__main__")

    scheduler_backend_mod = sys.modules["utils.scheduler_backend"]
    assert scheduler_backend_mod.run_once.called

    func = scheduler_backend_mod.run_once.call_args[0][0]
    kwargs = scheduler_backend_mod.run_once.call_args[1]

    assert callable(func)
    assert kwargs == {
        "pipeline_stage": "adaptor",
        "pipeline_type": "topic",
        "pipeline_role": "producer",
        "component_name": "producer-topic-adaptor-test-product",
    }

    # The --run-once CLI path never spins up the heartbeat/backend loop.
    assert not FakeHeartbeatLogger.instances


def test_main_block_sigterm_handler_stops_heartbeat_and_exits(monkeypatch):
    if not SOURCE_PATH.exists():
        pytest.skip(
            f"Source file not found: {SOURCE_PATH}. "
            "Run pytest from repo root or set ADAPTOR_MAIN_PATH."
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
