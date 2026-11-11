# tests/test_otel_instrumentation.py

from unittest.mock import Mock

import pytest

from dpn_observability_sdk import otel_instrumentation


def test_setup_telemetry():
    setup_tracing_mock = Mock()
    setup_metrics_mock = Mock()

    otel_instrumentation.setup_tracing = setup_tracing_mock
    otel_instrumentation.setup_metrics = setup_metrics_mock

    otel_instrumentation.setup_telemetry(
        service_name="svc",
        service_version="1.0",
        otlp_endpoint="http://collector",
        environment="dev",
    )

    setup_tracing_mock.assert_called_once_with(
        service_name="svc",
        service_version="1.0",
        otlp_endpoint="http://collector",
        environment="dev",
    )

    setup_metrics_mock.assert_called_once_with(
        service_name="svc",
        service_version="1.0",
        otlp_endpoint="http://collector",
        environment="dev",
    )


def test_shutdown_telemetry():
    shutdown_tracing_mock = Mock()
    shutdown_metrics_mock = Mock()

    otel_instrumentation.shutdown_tracing = shutdown_tracing_mock
    otel_instrumentation.shutdown_metrics = shutdown_metrics_mock

    otel_instrumentation.shutdown_telemetry()

    shutdown_tracing_mock.assert_called_once()
    shutdown_metrics_mock.assert_called_once()


def test_traced_success_with_attributes():
    span = Mock()

    class SpanContext:
        def __enter__(self):
            return span

        def __exit__(self, exc_type, exc_val, exc_tb):
            pass

    tracer = Mock()
    tracer.start_as_current_span.return_value = SpanContext()

    otel_instrumentation.get_tracer = Mock(return_value=tracer)

    @otel_instrumentation.traced(
        span_name="custom-span",
        attributes={"component": "test"},
    )
    def sample():
        return "success"

    result = sample()

    assert result == "success"

    tracer.start_as_current_span.assert_called_once_with("custom-span")

    span.set_attribute.assert_any_call("component", "test")
    span.set_attribute.assert_any_call("code.function", "sample")
    span.set_attribute.assert_any_call("code.namespace", sample.__module__)

    span.set_status.assert_called_once()
    span.record_exception.assert_not_called()


def test_traced_success_without_span_name():
    span = Mock()

    class SpanContext:
        def __enter__(self):
            return span

        def __exit__(self, exc_type, exc_val, exc_tb):
            pass

    tracer = Mock()
    tracer.start_as_current_span.return_value = SpanContext()

    otel_instrumentation.get_tracer = Mock(return_value=tracer)

    @otel_instrumentation.traced()
    def my_function():
        return 123

    assert my_function() == 123

    tracer.start_as_current_span.assert_called_once_with("my_function")


def test_traced_exception():
    span = Mock()

    class SpanContext:
        def __enter__(self):
            return span

        def __exit__(self, exc_type, exc_val, exc_tb):
            pass

    tracer = Mock()
    tracer.start_as_current_span.return_value = SpanContext()

    otel_instrumentation.get_tracer = Mock(return_value=tracer)

    @otel_instrumentation.traced()
    def failing():
        raise ValueError("boom")

    with pytest.raises(ValueError, match="boom"):
        failing()

    span.record_exception.assert_called_once()

    status = span.set_status.call_args.args[0]
    assert status.status_code.name == "ERROR"


def test_timed_metric_success_with_attributes():
    histogram = Mock()

    otel_instrumentation.create_histogram = Mock(return_value=histogram)

    decorator = otel_instrumentation.timed_metric(
        "duration_metric",
        description="desc",
        unit="ms",
        attributes={"env": "test"},
    )

    @decorator
    def process():
        return "done"

    result = process()

    assert result == "done"

    histogram.record.assert_called_once()

    attrs = histogram.record.call_args.args[1]

    assert attrs["env"] == "test"
    assert attrs["function"] == "process"
    assert attrs["status"] == "success"


def test_timed_metric_success_without_attributes():
    histogram = Mock()

    otel_instrumentation.create_histogram = Mock(return_value=histogram)

    @otel_instrumentation.timed_metric("metric")
    def process():
        return True

    assert process() is True

    attrs = histogram.record.call_args.args[1]

    assert attrs["function"] == "process"
    assert attrs["status"] == "success"


def test_timed_metric_exception():
    histogram = Mock()

    otel_instrumentation.create_histogram = Mock(return_value=histogram)

    @otel_instrumentation.timed_metric(
        "duration_metric",
        attributes={"service": "api"},
    )
    def failing():
        raise RuntimeError("failure")

    with pytest.raises(RuntimeError):
        failing()

    histogram.record.assert_called_once()

    attrs = histogram.record.call_args.args[1]

    assert attrs["service"] == "api"
    assert attrs["function"] == "failing"
    assert attrs["status"] == "error"
    assert attrs["error_type"] == "RuntimeError"


def test_counter_metric_success():
    counter = Mock()

    otel_instrumentation.create_counter = Mock(return_value=counter)

    @otel_instrumentation.counter_metric(
        "calls_total",
        attributes={"source": "unit-test"},
        increment=5,
    )
    def process():
        return "ok"

    result = process()

    assert result == "ok"

    counter.add.assert_called_once()

    increment = counter.add.call_args.args[0]
    attrs = counter.add.call_args.args[1]

    assert increment == 5
    assert attrs["source"] == "unit-test"
    assert attrs["function"] == "process"
    assert attrs["status"] == "success"


def test_counter_metric_success_without_attributes():
    counter = Mock()

    otel_instrumentation.create_counter = Mock(return_value=counter)

    @otel_instrumentation.counter_metric("calls_total")
    def process():
        return 10

    assert process() == 10

    attrs = counter.add.call_args.args[1]

    assert attrs["function"] == "process"
    assert attrs["status"] == "success"


def test_counter_metric_exception():
    counter = Mock()

    otel_instrumentation.create_counter = Mock(return_value=counter)

    @otel_instrumentation.counter_metric(
        "errors_total",
        attributes={"component": "validator"},
        increment=2,
    )
    def failing():
        raise KeyError("missing")

    with pytest.raises(KeyError):
        failing()

    counter.add.assert_called_once()

    increment = counter.add.call_args.args[0]
    attrs = counter.add.call_args.args[1]

    assert increment == 2
    assert attrs["component"] == "validator"
    assert attrs["function"] == "failing"
    assert attrs["status"] == "error"
    assert attrs["error_type"] == "KeyError"


def test_record_exception_metric_with_attributes():
    counter = Mock()

    otel_instrumentation.create_counter = Mock(return_value=counter)

    exc = ValueError("validation failed")

    otel_instrumentation.record_exception_metric(
        exc,
        {"operation": "validation"},
    )

    otel_instrumentation.create_counter.assert_called_once_with(
        "errors_total",
        "Total number of errors",
        "1",
    )

    counter.add.assert_called_once()

    increment = counter.add.call_args.args[0]
    attrs = counter.add.call_args.args[1]

    assert increment == 1
    assert attrs["operation"] == "validation"
    assert attrs["error_type"] == "ValueError"
    assert attrs["error_message"] == "validation failed"


def test_record_exception_metric_without_attributes():
    counter = Mock()

    otel_instrumentation.create_counter = Mock(return_value=counter)

    exc = RuntimeError("runtime issue")

    otel_instrumentation.record_exception_metric(exc)

    counter.add.assert_called_once()

    attrs = counter.add.call_args.args[1]

    assert attrs["error_type"] == "RuntimeError"
    assert attrs["error_message"] == "runtime issue"