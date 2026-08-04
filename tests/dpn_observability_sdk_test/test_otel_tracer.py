from unittest.mock import Mock

import pytest

from dpn_observability_sdk import otel_tracer


@pytest.fixture(autouse=True)
def reset_state():
    otel_tracer._tracer_provider = None
    otel_tracer.OtelTracer._initialized = False
    yield
    otel_tracer._tracer_provider = None
    otel_tracer.OtelTracer._initialized = False


def test_setup_tracing_with_parameters(monkeypatch):
    provider = Mock()

    monkeypatch.setattr(
        otel_tracer.Resource,
        "create",
        Mock(return_value="resource"),
    )

    monkeypatch.setattr(
        otel_tracer,
        "TracerProvider",
        Mock(return_value=provider),
    )

    exporter_mock = Mock(return_value="exporter")
    monkeypatch.setattr(
        otel_tracer,
        "OTLPSpanExporter",
        exporter_mock,
    )

    processor_mock = Mock(return_value="processor")
    monkeypatch.setattr(
        otel_tracer,
        "BatchSpanProcessor",
        processor_mock,
    )

    set_provider_mock = Mock()
    monkeypatch.setattr(
        otel_tracer.trace,
        "set_tracer_provider",
        set_provider_mock,
    )

    result = otel_tracer.setup_tracing(
        service_name="svc",
        service_version="1.0",
        otlp_endpoint="http://collector",
        environment="dev",
    )

    assert result is provider
    assert otel_tracer._tracer_provider is provider

    provider.add_span_processor.assert_called_once_with("processor")
    set_provider_mock.assert_called_once_with(provider)


def test_setup_tracing_uses_environment_variables(monkeypatch):
    provider = Mock()

    monkeypatch.setenv("SERVICE_NAME", "env-service")
    monkeypatch.setenv("SERVICE_VERSION", "2.0")
    monkeypatch.setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://env-endpoint")
    monkeypatch.setenv("ENVIRONMENT", "test")
    monkeypatch.setenv("OTEL_EXPORTER_OTLP_INSECURE", "false")

    monkeypatch.setattr(
        otel_tracer.Resource,
        "create",
        Mock(return_value="resource"),
    )

    monkeypatch.setattr(
        otel_tracer,
        "TracerProvider",
        Mock(return_value=provider),
    )

    exporter_mock = Mock(return_value="exporter")
    monkeypatch.setattr(
        otel_tracer,
        "OTLPSpanExporter",
        exporter_mock,
    )

    monkeypatch.setattr(
        otel_tracer,
        "BatchSpanProcessor",
        Mock(return_value="processor"),
    )

    monkeypatch.setattr(
        otel_tracer.trace,
        "set_tracer_provider",
        Mock(),
    )

    otel_tracer.setup_tracing()

    exporter_mock.assert_called_once_with(
        endpoint="http://env-endpoint",
        insecure=False,
    )


def test_get_tracer_initializes_when_provider_missing(monkeypatch):
    setup_mock = Mock()

    monkeypatch.setattr(
        otel_tracer,
        "setup_tracing",
        setup_mock,
    )

    tracer = Mock()

    get_tracer_mock = Mock(return_value=tracer)

    monkeypatch.setattr(
        otel_tracer.trace,
        "get_tracer",
        get_tracer_mock,
    )

    result = otel_tracer.get_tracer("custom")

    assert result is tracer

    setup_mock.assert_called_once()
    get_tracer_mock.assert_called_once_with("custom")


def test_get_tracer_existing_provider(monkeypatch):
    otel_tracer._tracer_provider = Mock()

    tracer = Mock()

    get_tracer_mock = Mock(return_value=tracer)

    monkeypatch.setattr(
        otel_tracer.trace,
        "get_tracer",
        get_tracer_mock,
    )

    result = otel_tracer.get_tracer("service")

    assert result is tracer

    get_tracer_mock.assert_called_once_with("service")


def test_get_tracer_default_name(monkeypatch):
    otel_tracer._tracer_provider = Mock()

    tracer = Mock()

    get_tracer_mock = Mock(return_value=tracer)

    monkeypatch.setattr(
        otel_tracer.trace,
        "get_tracer",
        get_tracer_mock,
    )

    result = otel_tracer.get_tracer()

    assert result is tracer

    get_tracer_mock.assert_called_once_with(
        otel_tracer.__name__
    )


def test_shutdown_tracing_with_provider():
    provider = Mock()

    otel_tracer._tracer_provider = provider

    otel_tracer.shutdown_tracing()

    provider.shutdown.assert_called_once()
    assert otel_tracer._tracer_provider is None


def test_shutdown_tracing_without_provider():
    otel_tracer._tracer_provider = None

    otel_tracer.shutdown_tracing()

    assert otel_tracer._tracer_provider is None


def test_class_initialize_first_time(monkeypatch):
    setup_mock = Mock()

    monkeypatch.setattr(
        otel_tracer,
        "setup_tracing",
        setup_mock,
    )

    tracer = Mock()

    monkeypatch.setattr(
        otel_tracer.trace,
        "get_tracer",
        Mock(return_value=tracer),
    )

    result = otel_tracer.OtelTracer.initialize(
        service_name="svc",
        service_version="1.0",
        otlp_endpoint="endpoint",
        environment="dev",
    )

    assert result is tracer
    assert otel_tracer.OtelTracer._initialized is True

    setup_mock.assert_called_once_with(
        service_name="svc",
        service_version="1.0",
        otlp_endpoint="endpoint",
        environment="dev",
    )


def test_class_initialize_already_initialized(monkeypatch):
    otel_tracer.OtelTracer._initialized = True

    tracer = Mock()

    monkeypatch.setattr(
        otel_tracer.trace,
        "get_tracer",
        Mock(return_value=tracer),
    )

    result = otel_tracer.OtelTracer.initialize()

    assert result is tracer


def test_class_get_tracer_initializes(monkeypatch):
    otel_tracer.OtelTracer._initialized = False

    initialize_mock = Mock()

    monkeypatch.setattr(
        otel_tracer.OtelTracer,
        "initialize",
        initialize_mock,
    )

    get_tracer_mock = Mock(return_value="tracer")

    monkeypatch.setattr(
        otel_tracer,
        "get_tracer",
        get_tracer_mock,
    )

    result = otel_tracer.OtelTracer.get_tracer("abc")

    assert result == "tracer"

    initialize_mock.assert_called_once()
    get_tracer_mock.assert_called_once_with("abc")


def test_class_get_tracer_without_initialization(monkeypatch):
    otel_tracer.OtelTracer._initialized = True

    get_tracer_mock = Mock(return_value="tracer")

    monkeypatch.setattr(
        otel_tracer,
        "get_tracer",
        get_tracer_mock,
    )

    result = otel_tracer.OtelTracer.get_tracer("abc")

    assert result == "tracer"

    get_tracer_mock.assert_called_once_with("abc")


def test_class_shutdown(monkeypatch):
    shutdown_mock = Mock()

    monkeypatch.setattr(
        otel_tracer,
        "shutdown_tracing",
        shutdown_mock,
    )

    otel_tracer.OtelTracer._initialized = True

    otel_tracer.OtelTracer.shutdown()

    shutdown_mock.assert_called_once()
    assert otel_tracer.OtelTracer._initialized is False