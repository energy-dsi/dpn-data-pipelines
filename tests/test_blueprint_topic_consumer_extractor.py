# Copyright DSI Project — Apache 2.0
"""
100% line + branch coverage for blueprints/consumer/topic/extractor/main.py

Coverage targets
----------------
TopicForwarder.__init__         - all env var paths (set / default)
TopicForwarder._on_delivery     - err truthy and falsy
TopicForwarder._forward         - success (with/without headers, key/no-key,
                                  str/bytes in carrier), exception path
TopicForwarder.run_window       - every loop branch: outer-deadline True on
                                  first entry, inner-deadline True, None msg,
                                  EOF, non-EOF KafkaException, unexpected
                                  exception, post-cleanup False path + retry,
                                  no-deadline retry
run()                           - kafka-trigger / interval / manual, failure
__main__ block                  - heartbeat + backend execute
module-level _TIMEOUT           - explicit env set and default (600)
"""
from __future__ import annotations

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
        "CONSUMER_TOPIC_EXTRACTOR_MAIN_PATH",
        "blueprints/consumer/topic/extractor/main.py",
    )
)


# ---------------------------------------------------------------------------
# Lightweight Kafka fakes
# ---------------------------------------------------------------------------

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
        topic="source-topic",
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
    instances: list = []
    poll_side_effects: list = []

    def __init__(self, config):
        self.config = config
        self.subscriptions: list = []
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
    instances: list = []

    def __init__(self, config):
        self.config = config
        self.produced: list = []
        self.poll_calls: list = []
        self.flush_calls = 0
        FakeProducer.instances.append(self)

    def produce(self, **kwargs):
        self.produced.append(kwargs)

    def poll(self, timeout):
        self.poll_calls.append(timeout)

    def flush(self):
        self.flush_calls += 1


# ---------------------------------------------------------------------------
# OTEL / SDK fakes
# ---------------------------------------------------------------------------

class FakeMetric:
    def __init__(self):
        self.calls: list = []

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
        self.attributes: dict = {}
        self.exceptions: list = []

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
        self.info_calls: list = []
        self.error_calls: list = []

    def info(self, message, *args, extra=None, **kwargs):
        self.info_calls.append((message, args, extra, kwargs))

    def error(self, message, *args, extra=None, **kwargs):
        self.error_calls.append((message, args, extra, kwargs))


# ---------------------------------------------------------------------------
# Utils fakes
# ---------------------------------------------------------------------------

class FakeTopicResolver:
    def resolve(self, mapper_topic, src_topic, suffix):
        return mapper_topic or f"{src_topic}.{suffix}"


class FakeKafkaTopicManager:
    instances: list = []

    def __init__(self, bootstrap_server, logger):
        self.bootstrap_server = bootstrap_server
        self.logger = logger
        self.ensured_topics: list = []
        FakeKafkaTopicManager.instances.append(self)

    def ensure_exists(self, topic):
        self.ensured_topics.append(topic)


class FakeStepLogger:
    instances: list = []

    def __init__(self, logger=None):
        self.logger = logger
        self.pipeline_banner_calls: list = []
        self.step_start_calls: list = []
        self.step_end_calls: list = []
        self.step_failed_calls: list = []
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
    instances: list = []

    def __init__(self, logger, component_name, metadata):
        self.logger = logger
        self.component_name = component_name
        self.metadata = metadata
        self.started = False
        FakeHeartbeatLogger.instances.append(self)

    def start(self):
        self.started = True


class FakeBackend:
    instances: list = []

    def __init__(self):
        self.execute_calls: list = []
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


def identity_decorator(*args, **kwargs):
    def wrapper(func):
        return func
    return wrapper


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture(autouse=True)
def fake_external_modules(monkeypatch):
    """
    Patch every external dependency before each test and reset all
    class-level instance registries so tests are fully isolated.
    """
    FakeConsumer.instances.clear()
    FakeConsumer.poll_side_effects.clear()
    FakeProducer.instances.clear()
    FakeKafkaTopicManager.instances.clear()
    FakeStepLogger.instances.clear()
    FakeHeartbeatLogger.instances.clear()
    FakeBackend.instances.clear()

    # Environment
    monkeypatch.setenv("bootstrapServer", "localhost:9092")
    monkeypatch.setenv("srcTopicName", "source-topic")
    monkeypatch.setenv("srcGroupId", "group-1")
    monkeypatch.setenv("mapperTopicName", "mapper-topic")
    monkeypatch.setenv("consumerRetryDelaySecs", "1")
    monkeypatch.setenv("PRODUCT_NAME", "consumer-topic")
    monkeypatch.setenv("SCHEDULER_BACKEND", "standalone")
    monkeypatch.setenv("TOPIC_TASK_TIMEOUT_SECS", "7")

    # confluent_kafka
    confluent = types.ModuleType("confluent_kafka")
    confluent.Consumer = FakeConsumer
    confluent.Producer = FakeProducer
    confluent.KafkaError = FakeKafkaError
    confluent.KafkaException = FakeKafkaException
    monkeypatch.setitem(sys.modules, "confluent_kafka", confluent)

    # dotenv
    dotenv = types.ModuleType("dotenv")
    dotenv.load_dotenv = Mock()
    monkeypatch.setitem(sys.modules, "dotenv", dotenv)

    # opentelemetry
    otel_context = types.ModuleType("opentelemetry.context")
    otel_context.Context = Mock(return_value="empty-context")

    otel_propagate = types.ModuleType("opentelemetry.propagate")

    def inject_str(carrier):
        carrier["traceparent"] = "00-test-trace"

    otel_propagate.inject = Mock(side_effect=inject_str)

    otel = types.ModuleType("opentelemetry")
    otel.context = otel_context
    otel.propagate = otel_propagate

    monkeypatch.setitem(sys.modules, "opentelemetry", otel)
    monkeypatch.setitem(sys.modules, "opentelemetry.context", otel_context)
    monkeypatch.setitem(sys.modules, "opentelemetry.propagate", otel_propagate)

    # utils
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

    # dpn_observability_sdk
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
    """Return a callable that loads (or reloads) the module under test."""
    def _load():
        if not SOURCE_PATH.exists():
            pytest.skip(
                f"Source file not found: {SOURCE_PATH}. "
                "Run pytest from repo root or set CONSUMER_TOPIC_EXTRACTOR_MAIN_PATH."
            )
        module_name = "consumer_topic_extractor_main_under_test"
        sys.modules.pop(module_name, None)
        spec = importlib.util.spec_from_file_location(module_name, SOURCE_PATH)
        module = importlib.util.module_from_spec(spec)
        sys.modules[module_name] = module
        spec.loader.exec_module(module)
        return module
    return _load


# ---------------------------------------------------------------------------
# TopicForwarder.__init__
# ---------------------------------------------------------------------------

def test_initialises_config_topics_metrics_and_producer(load_module):
    module = load_module()
    forwarder = module.TopicForwarder()

    assert forwarder.bootstrap == "localhost:9092"
    assert forwarder.src_topic == "source-topic"
    assert forwarder.group_id == "group-1"
    assert forwarder.retry_delay == 1
    assert forwarder.mapper_topic == "mapper-topic"
    assert forwarder.heartbeat is None

    mgr = FakeKafkaTopicManager.instances[-1]
    assert mgr.ensured_topics == ["source-topic", "mapper-topic"]

    assert FakeProducer.instances[-1].config == {"bootstrap.servers": "localhost:9092"}
    assert forwarder.logger.info_calls[-1][0] == "extractor initialised"

    extra = forwarder.logger.info_calls[-1][2]
    assert extra["srcTopicName"] == "source-topic"
    assert extra["mapperTopicName"] == "mapper-topic"
    assert extra["srcGroupId"] == "group-1"


def test_initialises_defaults_when_optional_envs_absent(load_module, monkeypatch):
    monkeypatch.delenv("srcGroupId", raising=False)
    monkeypatch.delenv("mapperTopicName", raising=False)

    module = load_module()
    forwarder = module.TopicForwarder()

    assert forwarder.group_id == forwarder.SERVICE_NAME
    assert forwarder.mapper_topic == "source-topic.trfm"


def test_timeout_constant_reads_from_env(load_module):
    module = load_module()
    assert module._TIMEOUT == 7


def test_timeout_constant_defaults_to_600_when_env_not_set(load_module, monkeypatch):
    monkeypatch.delenv("TOPIC_TASK_TIMEOUT_SECS", raising=False)
    module = load_module()
    assert module._TIMEOUT == 600


# ---------------------------------------------------------------------------
# TopicForwarder._on_delivery
# ---------------------------------------------------------------------------

def test_on_delivery_logs_on_error(load_module):
    module = load_module()
    forwarder = module.TopicForwarder()

    forwarder._on_delivery(Exception("delivery failed"), Mock())

    assert forwarder.logger.error_calls[-1][0] == "delivery failed"
    assert "delivery failed" in forwarder.logger.error_calls[-1][2]["error"]


def test_on_delivery_silent_on_success(load_module):
    module = load_module()
    forwarder = module.TopicForwarder()

    before = len(forwarder.logger.error_calls)
    forwarder._on_delivery(None, Mock())

    assert len(forwarder.logger.error_calls) == before


# ---------------------------------------------------------------------------
# TopicForwarder._forward
# ---------------------------------------------------------------------------

def test_forward_success_with_existing_headers_and_no_key(load_module):
    module = load_module()
    forwarder = module.TopicForwarder()
    step_log = FakeStepLogger(forwarder.logger)
    msg = FakeKafkaMessage(headers=[("x-custom", b"hdr")], key=None)

    forwarder._forward(msg, FakeContext(), step_log)

    produced = forwarder.producer.produced[-1]
    assert produced["topic"] == "mapper-topic"
    assert produced["value"] == b"value"
    assert produced["key"] is None
    assert produced["callback"] == forwarder._on_delivery

    headers = produced["headers"]
    assert ("x-custom", b"hdr") in headers
    assert ("traceparent", b"00-test-trace") in headers

    assert forwarder.producer.poll_calls == [0]
    assert forwarder.messages_forwarded.calls[-1][2]["status"] == "success"
    assert forwarder.forward_duration.calls[-1][0] == "record"

    assert step_log.step_start_calls[-1][1] == "forward"
    assert step_log.step_end_calls[-1][1] == "forward"

    # span attributes
    module._otel_context.Context.assert_called()
    module._otel_propagate.inject.assert_called()


def test_forward_success_with_no_headers_and_key_present(load_module):
    module = load_module()
    forwarder = module.TopicForwarder()
    step_log = FakeStepLogger(forwarder.logger)
    msg = FakeKafkaMessage(headers=None, key=b"my-key")

    forwarder._forward(msg, FakeContext(), step_log)

    produced = forwarder.producer.produced[-1]
    assert produced["headers"] == [("traceparent", b"00-test-trace")]
    assert produced["key"] == b"my-key"


def test_forward_carrier_bytes_value_passes_through_unencoded(load_module):
    """Covers the ``else v`` branch in the header-encoding list comprehension
    when the OTEL inject call places raw bytes (not str) into the carrier."""
    module = load_module()
    forwarder = module.TopicForwarder()
    step_log = FakeStepLogger(forwarder.logger)

    def inject_bytes(carrier):
        carrier["traceparent"] = b"bytes-trace-id"

    module._otel_propagate.inject = Mock(side_effect=inject_bytes)

    msg = FakeKafkaMessage(headers=None, key=None)
    forwarder._forward(msg, FakeContext(), step_log)

    produced = forwarder.producer.produced[-1]
    assert ("traceparent", b"bytes-trace-id") in produced["headers"]


def test_forward_failure_records_error_metric_and_reraises(load_module):
    module = load_module()
    forwarder = module.TopicForwarder()
    step_log = FakeStepLogger(forwarder.logger)

    def boom(**kwargs):
        raise RuntimeError("produce failed")

    forwarder.producer.produce = boom

    with pytest.raises(RuntimeError, match="produce failed"):
        forwarder._forward(FakeKafkaMessage(), FakeContext(), step_log)

    assert forwarder.messages_forwarded.calls[-1][2]["status"] == "error"
    assert step_log.step_failed_calls[-1][1] == "forward"


# ---------------------------------------------------------------------------
# TopicForwarder.run_window
# ---------------------------------------------------------------------------

def test_run_window_processes_none_eof_and_valid_messages_then_breaks(
    load_module, monkeypatch
):
    """Covers: msg=None (continue), EOF (continue), valid msg forwarded,
    inner deadline check True (breaks inner loop), post-cleanup deadline True
    (breaks outer loop)."""
    module = load_module()
    forwarder = module.TopicForwarder()
    step_log = FakeStepLogger(forwarder.logger)
    ctx = FakeContext()

    eof_msg = FakeKafkaMessage(error_obj=FakeKafkaError(FakeKafkaError._PARTITION_EOF))
    valid_msg = FakeKafkaMessage(partition=5)
    FakeConsumer.poll_side_effects[:] = [None, eof_msg, valid_msg]

    # deadline = 0+1 = 1; checks at 0.1, 0.2, 0.3, 0.4, 1.1(break inner), 1.2(break outer)
    monotonic_values = iter([0, 0.1, 0.2, 0.3, 0.4, 1.1, 1.2])
    monkeypatch.setattr(module.time, "monotonic", lambda: next(monotonic_values))
    monkeypatch.setattr(module.time, "sleep", Mock())

    forwarder._forward = Mock()
    forwarder.run_window(ctx, step_log, stop_after=1)

    consumer = FakeConsumer.instances[-1]
    assert consumer.subscriptions == [["source-topic"]]
    assert consumer.closed is True
    assert forwarder.producer.flush_calls == 1

    forwarder._forward.assert_called_once_with(valid_msg, ctx, step_log)
    assert forwarder.messages_consumed.calls[-1][2]["partition"] == "5"


def test_run_window_outer_deadline_exits_before_consumer_is_created(
    load_module, monkeypatch
):
    """Covers the outer while-True deadline break (line 274-275) firing on
    the very first loop iteration before any Consumer is instantiated."""
    module = load_module()
    forwarder = module.TopicForwarder()
    step_log = FakeStepLogger(forwarder.logger)

    # deadline = 0+1 = 1; outer check immediately sees 999 >= 1 → break
    values = iter([0, 999])
    monkeypatch.setattr(module.time, "monotonic", lambda: next(values))

    forwarder.run_window(FakeContext(), step_log, stop_after=1)

    assert len(FakeConsumer.instances) == 0
    assert forwarder.producer.flush_calls == 0


def test_run_window_handles_non_eof_kafka_exception(load_module, monkeypatch):
    """Covers the KafkaException handler and post-cleanup deadline True path."""
    module = load_module()
    forwarder = module.TopicForwarder()
    step_log = FakeStepLogger(forwarder.logger)

    bad_msg = FakeKafkaMessage(error_obj=FakeKafkaError(999))
    FakeConsumer.poll_side_effects[:] = [bad_msg]

    # deadline=1; outer False(0.1), inner False(0.2), post-cleanup True(1.1)
    monotonic_values = iter([0, 0.1, 0.2, 1.1])
    monkeypatch.setattr(module.time, "monotonic", lambda: next(monotonic_values))
    monkeypatch.setattr(module.time, "sleep", Mock())

    forwarder.run_window(FakeContext(), step_log, stop_after=1)

    assert forwarder.logger.error_calls[-1][0] == "kafka error"
    assert FakeConsumer.instances[-1].closed is True
    assert forwarder.producer.flush_calls == 1


def test_run_window_retries_after_kafka_error_then_exits_at_outer_deadline(
    load_module, monkeypatch
):
    """Covers the post-cleanup deadline check being False (retry path when
    deadline is set but not yet reached) then True on the next outer iteration.

    Specifically exercises:
    - post-cleanup ``if deadline and time.monotonic() >= deadline`` → False
    - ``time.sleep`` called while a deadline is active
    - outer while-True deadline check → True on re-entry
    """
    module = load_module()
    forwarder = module.TopicForwarder()
    step_log = FakeStepLogger(forwarder.logger)

    bad_msg = FakeKafkaMessage(error_obj=FakeKafkaError(999))
    FakeConsumer.poll_side_effects[:] = [bad_msg]

    # deadline = 0 + 10 = 10
    # iteration 1: outer False(0.5), inner False(1.0), KafkaException caught
    #              post-cleanup False(2.0) → sleep
    # iteration 2: outer True(11) → break
    values = iter([0, 0.5, 1.0, 2.0, 11])
    monkeypatch.setattr(module.time, "monotonic", lambda: next(values))

    sleep_calls: list = []
    monkeypatch.setattr(module.time, "sleep", lambda s: sleep_calls.append(s))

    forwarder.run_window(FakeContext(), step_log, stop_after=10)

    assert sleep_calls == [forwarder.retry_delay]
    # iteration 2 breaks at the outer deadline check before Consumer is created
    assert len(FakeConsumer.instances) == 1


def test_run_window_handles_unexpected_exception(load_module, monkeypatch):
    """Covers the generic Exception handler branch."""
    module = load_module()
    forwarder = module.TopicForwarder()
    step_log = FakeStepLogger(forwarder.logger)

    FakeConsumer.poll_side_effects[:] = [RuntimeError("poll exploded")]

    monotonic_values = iter([0, 0.1, 0.2, 1.1])
    monkeypatch.setattr(module.time, "monotonic", lambda: next(monotonic_values))
    monkeypatch.setattr(module.time, "sleep", Mock())

    forwarder.run_window(FakeContext(), step_log, stop_after=1)

    assert forwarder.logger.error_calls[-1][0] == "unexpected"
    assert "poll exploded" in forwarder.logger.error_calls[-1][2]["error"]
    assert FakeConsumer.instances[-1].closed is True
    assert forwarder.producer.flush_calls == 1


def test_run_window_no_deadline_retries_after_kafka_error(load_module, monkeypatch):
    """Covers the no-deadline (stop_after=None) path where time.sleep is
    called between consumer loop restarts."""
    module = load_module()
    forwarder = module.TopicForwarder()
    step_log = FakeStepLogger(forwarder.logger)

    FakeConsumer.poll_side_effects[:] = [FakeKafkaException("transient")]

    class _StopLoop(Exception):
        pass

    def fake_sleep(seconds):
        assert seconds == forwarder.retry_delay
        raise _StopLoop()

    monkeypatch.setattr(module.time, "sleep", fake_sleep)

    with pytest.raises(_StopLoop):
        forwarder.run_window(FakeContext(), step_log, stop_after=None)

    assert forwarder.logger.error_calls[-1][0] == "kafka error"
    assert FakeConsumer.instances[-1].closed is True
    assert forwarder.producer.flush_calls == 1


# ---------------------------------------------------------------------------
# run()
# ---------------------------------------------------------------------------

def _make_fake_forwarder():
    f = Mock()
    f.logger = FakeLogger()
    f.src_topic = "source-topic"
    f.mapper_topic = "mapper-topic"
    return f


def test_run_applies_timeout_for_kafka_trigger(load_module, monkeypatch):
    module = load_module()
    fake_forwarder = _make_fake_forwarder()
    monkeypatch.setattr(module, "TopicForwarder", Mock(return_value=fake_forwarder))
    monkeypatch.setattr(module, "StepLogger", FakeStepLogger)

    ctx = FakeContext(triggered_by="kafka-trigger", run_id="run-kafka")
    module.run(ctx)

    _, _, kwargs = fake_forwarder.run_window.mock_calls[0]
    assert kwargs["stop_after"] == 7

    step_log = FakeStepLogger.instances[-1]
    assert step_log.pipeline_banner_calls
    assert step_log.step_start_calls[-1][1] == "extractor_window"
    assert step_log.step_end_calls[-1][1] == "extractor_window"


def test_run_applies_timeout_for_interval_trigger(load_module, monkeypatch):
    module = load_module()
    fake_forwarder = _make_fake_forwarder()
    monkeypatch.setattr(module, "TopicForwarder", Mock(return_value=fake_forwarder))
    monkeypatch.setattr(module, "StepLogger", FakeStepLogger)

    module.run(FakeContext(triggered_by="interval"))

    _, _, kwargs = fake_forwarder.run_window.mock_calls[0]
    assert kwargs["stop_after"] == 7


def test_run_uses_no_timeout_for_manual_trigger(load_module, monkeypatch):
    module = load_module()
    fake_forwarder = _make_fake_forwarder()
    monkeypatch.setattr(module, "TopicForwarder", Mock(return_value=fake_forwarder))
    monkeypatch.setattr(module, "StepLogger", FakeStepLogger)

    module.run(FakeContext(triggered_by="manual"))

    _, _, kwargs = fake_forwarder.run_window.mock_calls[0]
    assert kwargs["stop_after"] is None


def test_run_failure_records_step_failed_and_reraises(load_module, monkeypatch):
    module = load_module()
    fake_forwarder = _make_fake_forwarder()
    fake_forwarder.run_window = Mock(side_effect=RuntimeError("window failed"))
    monkeypatch.setattr(module, "TopicForwarder", Mock(return_value=fake_forwarder))
    monkeypatch.setattr(module, "StepLogger", FakeStepLogger)

    with pytest.raises(RuntimeError, match="window failed"):
        module.run(FakeContext(triggered_by="manual"))

    step_log = FakeStepLogger.instances[-1]
    assert step_log.step_failed_calls[-1][1] == "extractor_window"


def test_run_sets_span_attributes_for_success(load_module, monkeypatch):
    module = load_module()
    fake_forwarder = _make_fake_forwarder()
    monkeypatch.setattr(module, "TopicForwarder", Mock(return_value=fake_forwarder))
    monkeypatch.setattr(module, "StepLogger", FakeStepLogger)

    span_captures: list = []

    class CapturingTracer:
        def start_as_current_span(self, name, *args, **kwargs):
            span = FakeSpan()
            span_captures.append(span)
            return span

    module.OtelTracer.get_tracer = Mock(return_value=CapturingTracer())

    ctx = FakeContext(triggered_by="kafka-trigger", run_id="run-span-test")
    module.run(ctx)

    span = span_captures[-1]
    assert span.attributes["pipeline.type"] == "consumer-topic-extractor"
    assert span.attributes["pipeline.triggered_by"] == "kafka-trigger"
    assert span.attributes["pipeline.run_id"] == "run-span-test"
    assert span.attributes["pipeline.status"] == "success"
    assert span.attributes["pipeline.timeout_seconds"] == 7


def test_run_sets_span_attributes_on_error(load_module, monkeypatch):
    module = load_module()
    fake_forwarder = _make_fake_forwarder()
    fake_forwarder.run_window = Mock(side_effect=ValueError("oops"))
    monkeypatch.setattr(module, "TopicForwarder", Mock(return_value=fake_forwarder))
    monkeypatch.setattr(module, "StepLogger", FakeStepLogger)

    span_captures: list = []

    class CapturingTracer:
        def start_as_current_span(self, name, *args, **kwargs):
            span = FakeSpan()
            span_captures.append(span)
            return span

    module.OtelTracer.get_tracer = Mock(return_value=CapturingTracer())

    with pytest.raises(ValueError):
        module.run(FakeContext())

    span = span_captures[-1]
    assert span.attributes["pipeline.status"] == "error"
    assert span.attributes["error.type"] == "ValueError"
    assert len(span.exceptions) == 1


# ---------------------------------------------------------------------------
# __main__ block
# ---------------------------------------------------------------------------

def test_main_block_starts_heartbeat_and_calls_backend_execute():
    if not SOURCE_PATH.exists():
        pytest.skip(
            f"Source file not found: {SOURCE_PATH}. "
            "Run pytest from repo root or set CONSUMER_TOPIC_EXTRACTOR_MAIN_PATH."
        )

    runpy.run_path(str(SOURCE_PATH), run_name="__main__")

    assert FakeHeartbeatLogger.instances, "HeartbeatLogger was never instantiated"

    heartbeat = FakeHeartbeatLogger.instances[-1]
    assert heartbeat.started is True
    assert heartbeat.component_name == "consumer-topic-extractor"
    assert heartbeat.metadata == {
        "source_topic": "source-topic",
        "mapper_topic": "mapper-topic",
        "scheduler_backend": "standalone",
    }

    assert FakeBackend.instances, "backend was never created"
    _, kwargs = FakeBackend.instances[-1].execute_calls[-1]
    assert kwargs == {
        "pipeline_stage": "extractor",
        "pipeline_type": "topic",
        "pipeline_role": "consumer",
        "component_name": "consumer-topic-extractor",
    }
