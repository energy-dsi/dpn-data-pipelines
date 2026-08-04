# tests/test_otel_logger.py

import json
import logging
import sys
from unittest.mock import Mock

import pytest

from dpn_observability_sdk import otel_logger


@pytest.fixture(autouse=True)
def reset_class_state():
    otel_logger.OtelLogger._initialized = False
    yield
    otel_logger.OtelLogger._initialized = False


def test_formatter_basic_info():
    formatter = otel_logger.OTelJsonFormatter(
        service_name="svc",
        service_version="1.0.0",
    )

    record = logging.LogRecord(
        name="test_logger",
        level=logging.INFO,
        pathname="/tmp/test.py",
        lineno=10,
        msg="hello",
        args=(),
        exc_info=None,
    )

    result = formatter.format(record)
    payload = json.loads(result)

    assert payload["body"] == "hello"
    assert payload["severity_number"] == 9
    assert payload["severity_text"] == "INFO"
    assert payload["resource"]["service.name"] == "svc"
    assert payload["resource"]["service.version"] == "1.0.0"
    assert payload["attributes"]["code.lineno"] == 10


def test_formatter_unknown_level():
    formatter = otel_logger.OTelJsonFormatter("svc", "1.0")

    record = logging.LogRecord(
        name="logger",
        level=999,
        pathname="file.py",
        lineno=1,
        msg="message",
        args=(),
        exc_info=None,
    )

    payload = json.loads(formatter.format(record))

    assert payload["severity_number"] == 9
    assert payload["severity_text"] == "INFO"


def test_formatter_adds_extra_attributes():
    formatter = otel_logger.OTelJsonFormatter("svc", "1.0")

    record = logging.LogRecord(
        name="logger",
        level=logging.INFO,
        pathname="file.py",
        lineno=1,
        msg="message",
        args=(),
        exc_info=None,
    )

    record.component = "consumer"
    record.event_name = "heartbeat"

    payload = json.loads(formatter.format(record))

    assert payload["attributes"]["component"] == "consumer"
    assert payload["attributes"]["event_name"] == "heartbeat"


def test_formatter_with_exception():
    formatter = otel_logger.OTelJsonFormatter("svc", "1.0")

    try:
        raise ValueError("bad value")
    except ValueError:
        exc_info = sys.exc_info()

    record = logging.LogRecord(
        name="logger",
        level=logging.ERROR,
        pathname="file.py",
        lineno=100,
        msg="failure",
        args=(),
        exc_info=exc_info,
    )

    payload = json.loads(formatter.format(record))

    assert payload["attributes"]["exception.type"] == "ValueError"
    assert payload["attributes"]["exception.message"] == "bad value"
    assert "exception.stacktrace" in payload["attributes"]


def test_formatter_valid_otel_context(monkeypatch):
    monkeypatch.setattr(otel_logger, "_OTEL_AVAILABLE", True)

    formatter = otel_logger.OTelJsonFormatter("svc", "1.0")

    span_context = Mock()
    span_context.is_valid = True
    span_context.trace_id = 123
    span_context.span_id = 456
    span_context.trace_flags = 1

    span = Mock()
    span.get_span_context.return_value = span_context

    trace_mock = Mock()
    trace_mock.get_current_span.return_value = span

    monkeypatch.setattr(
        otel_logger,
        "_otel_trace",
        trace_mock,
        raising=False,
    )

    record = logging.LogRecord(
        "logger",
        logging.INFO,
        "file.py",
        1,
        "message",
        (),
        None,
    )

    payload = json.loads(formatter.format(record))

    assert payload["trace_id"] == format(123, "032x")
    assert payload["span_id"] == format(456, "016x")
    assert payload["trace_flags"] == format(1, "02x")


def test_formatter_invalid_otel_context(monkeypatch):
    monkeypatch.setattr(otel_logger, "_OTEL_AVAILABLE", True)

    formatter = otel_logger.OTelJsonFormatter("svc", "1.0")

    span_context = Mock()
    span_context.is_valid = False

    span = Mock()
    span.get_span_context.return_value = span_context

    trace_mock = Mock()
    trace_mock.get_current_span.return_value = span

    monkeypatch.setattr(
        otel_logger,
        "_otel_trace",
        trace_mock,
        raising=False,
    )

    record = logging.LogRecord(
        "logger",
        logging.INFO,
        "file.py",
        1,
        "message",
        (),
        None,
    )

    payload = json.loads(formatter.format(record))

    assert "trace_id" not in payload
    assert "span_id" not in payload
    assert "trace_flags" not in payload


def test_create_logger_already_initialized(monkeypatch):
    otel_logger.OtelLogger._initialized = True

    logger = logging.Logger("existing")

    monkeypatch.setattr(
        otel_logger.logging,
        "getLogger",
        lambda name=None: logger,
    )

    result = otel_logger.OtelLogger().create_logger("existing")

    assert result is logger
    assert logger.propagate is True


def test_create_logger_no_duplicate_stream_handler(monkeypatch):
    otel_logger.OtelLogger._initialized = True

    logger = logging.Logger("svc")
    logger.handlers = [logging.StreamHandler()]

    monkeypatch.setattr(
        otel_logger.logging,
        "getLogger",
        lambda name=None: logger,
    )

    otel_logger.OtelLogger().create_logger("svc")

    stream_handlers = [
        h for h in logger.handlers
        if type(h) is logging.StreamHandler
    ]

    assert len(stream_handlers) == 1


def test_create_logger_initialization(monkeypatch):
    provider = Mock()
    exporter = Mock()
    processor = Mock()
    logging_handler = Mock()

    monkeypatch.setenv("SERVICE_NAME", "service-a")
    monkeypatch.setenv("SERVICE_VERSION", "2.0")
    monkeypatch.setenv(
        "OTEL_EXPORTER_OTLP_ENDPOINT",
        "http://collector:4317",
    )

    monkeypatch.setattr(
        otel_logger,
        "LoggerProvider",
        Mock(return_value=provider),
        raising=False,
    )

    monkeypatch.setattr(
        otel_logger,
        "OTLPLogExporter",
        Mock(return_value=exporter),
        raising=False,
    )

    monkeypatch.setattr(
        otel_logger,
        "SimpleLogRecordProcessor",
        Mock(return_value=processor),
        raising=False,
    )

    monkeypatch.setattr(
        otel_logger,
        "LoggingHandler",
        Mock(return_value=logging_handler),
        raising=False,
    )

    monkeypatch.setattr(
        otel_logger,
        "set_logger_provider",
        Mock(),
        raising=False,
    )

    monkeypatch.setattr(
        otel_logger.Resource,
        "create",
        Mock(return_value="resource"),
        raising=False,
    )

    root_logger = logging.Logger("root")
    app_logger = logging.Logger("app")

    call_count = {"count": 0}

    def fake_get_logger(name=None):
        call_count["count"] += 1
        return root_logger if call_count["count"] == 1 else app_logger

    monkeypatch.setattr(
        otel_logger.logging,
        "getLogger",
        fake_get_logger,
    )

    result = otel_logger.OtelLogger().create_logger("app")

    assert result is app_logger
    assert otel_logger.OtelLogger._initialized is True
    assert app_logger.propagate is True

    provider.add_log_record_processor.assert_called_once_with(processor)


def test_logging_alias():
    assert otel_logger.Logging is otel_logger.OtelLogger