# tests/test_otel_metrics.py

from unittest.mock import Mock

import pytest

from dpn_observability_sdk import otel_metrics


@pytest.fixture(autouse=True)
def reset_globals():
    otel_metrics._meter_provider = None
    otel_metrics.OtelMetrics._initialized = False
    yield
    otel_metrics._meter_provider = None
    otel_metrics.OtelMetrics._initialized = False


def test_setup_metrics_with_parameters(monkeypatch):
    provider = Mock()

    monkeypatch.setattr(
        otel_metrics.Resource,
        "create",
        Mock(return_value="resource"),
    )

    monkeypatch.setattr(
        otel_metrics,
        "OTLPMetricExporter",
        Mock(return_value="exporter"),
    )

    monkeypatch.setattr(
        otel_metrics,
        "PeriodicExportingMetricReader",
        Mock(return_value="reader"),
    )

    monkeypatch.setattr(
        otel_metrics,
        "MeterProvider",
        Mock(return_value=provider),
    )

    set_meter_provider_mock = Mock()

    monkeypatch.setattr(
        otel_metrics.metrics,
        "set_meter_provider",
        set_meter_provider_mock,
    )

    result = otel_metrics.setup_metrics(
        service_name="svc",
        service_version="1.0",
        otlp_endpoint="http://collector",
        environment="dev",
        export_interval_millis=5000,
    )

    assert result is provider
    assert otel_metrics._meter_provider is provider

    set_meter_provider_mock.assert_called_once_with(provider)


def test_setup_metrics_with_environment_variables(monkeypatch):
    provider = Mock()

    monkeypatch.setenv("SERVICE_NAME", "env-service")
    monkeypatch.setenv("SERVICE_VERSION", "2.0")
    monkeypatch.setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://env-endpoint")
    monkeypatch.setenv("ENVIRONMENT", "test")
    monkeypatch.setenv("OTEL_EXPORTER_OTLP_INSECURE", "false")

    monkeypatch.setattr(
        otel_metrics.Resource,
        "create",
        Mock(return_value="resource"),
    )

    exporter_mock = Mock(return_value="exporter")

    monkeypatch.setattr(
        otel_metrics,
        "OTLPMetricExporter",
        exporter_mock,
    )

    monkeypatch.setattr(
        otel_metrics,
        "PeriodicExportingMetricReader",
        Mock(return_value="reader"),
    )

    monkeypatch.setattr(
        otel_metrics,
        "MeterProvider",
        Mock(return_value=provider),
    )

    monkeypatch.setattr(
        otel_metrics.metrics,
        "set_meter_provider",
        Mock(),
    )

    otel_metrics.setup_metrics()

    exporter_mock.assert_called_once_with(
        endpoint="http://env-endpoint",
        insecure=False,
    )


def test_get_meter_initializes_when_provider_missing(monkeypatch):
    setup_metrics_mock = Mock()

    monkeypatch.setattr(
        otel_metrics,
        "setup_metrics",
        setup_metrics_mock,
    )

    expected_meter = Mock()

    monkeypatch.setattr(
        otel_metrics.metrics,
        "get_meter",
        Mock(return_value=expected_meter),
    )

    result = otel_metrics.get_meter("my-meter")

    assert result is expected_meter
    setup_metrics_mock.assert_called_once()


def test_get_meter_with_existing_provider(monkeypatch):
    otel_metrics._meter_provider = Mock()

    expected_meter = Mock()

    get_meter_mock = Mock(return_value=expected_meter)

    monkeypatch.setattr(
        otel_metrics.metrics,
        "get_meter",
        get_meter_mock,
    )

    result = otel_metrics.get_meter("custom")

    assert result is expected_meter
    get_meter_mock.assert_called_once_with("custom")


def test_get_meter_uses_module_name(monkeypatch):
    otel_metrics._meter_provider = Mock()

    expected_meter = Mock()

    get_meter_mock = Mock(return_value=expected_meter)

    monkeypatch.setattr(
        otel_metrics.metrics,
        "get_meter",
        get_meter_mock,
    )

    otel_metrics.get_meter()

    get_meter_mock.assert_called_once_with(
        otel_metrics.__name__
    )


def test_create_counter():
    meter = Mock()
    counter = Mock()

    meter.create_counter.return_value = counter

    otel_metrics.get_meter = Mock(return_value=meter)

    result = otel_metrics.create_counter(
        "requests_total",
        "desc",
        "1",
    )

    assert result is counter

    meter.create_counter.assert_called_once_with(
        name="requests_total",
        description="desc",
        unit="1",
    )


def test_create_histogram():
    meter = Mock()
    histogram = Mock()

    meter.create_histogram.return_value = histogram

    otel_metrics.get_meter = Mock(return_value=meter)

    result = otel_metrics.create_histogram(
        "duration",
        "desc",
        "ms",
    )

    assert result is histogram

    meter.create_histogram.assert_called_once_with(
        name="duration",
        description="desc",
        unit="ms",
    )


def test_create_up_down_counter():
    meter = Mock()
    updown = Mock()

    meter.create_up_down_counter.return_value = updown

    otel_metrics.get_meter = Mock(return_value=meter)

    result = otel_metrics.create_up_down_counter(
        "active_tasks",
        "desc",
        "1",
    )

    assert result is updown

    meter.create_up_down_counter.assert_called_once_with(
        name="active_tasks",
        description="desc",
        unit="1",
    )


def test_shutdown_metrics_with_provider():
    provider = Mock()

    otel_metrics._meter_provider = provider

    otel_metrics.shutdown_metrics()

    provider.shutdown.assert_called_once()
    assert otel_metrics._meter_provider is None


def test_shutdown_metrics_without_provider():
    otel_metrics._meter_provider = None

    otel_metrics.shutdown_metrics()

    assert otel_metrics._meter_provider is None


def test_otelmetrics_initialize_first_time(monkeypatch):
    setup_metrics_mock = Mock()

    monkeypatch.setattr(
        otel_metrics,
        "setup_metrics",
        setup_metrics_mock,
    )

    meter = Mock()

    monkeypatch.setattr(
        otel_metrics.metrics,
        "get_meter",
        Mock(return_value=meter),
    )

    result = otel_metrics.OtelMetrics.initialize(
        service_name="svc",
        service_version="1.0",
        otlp_endpoint="endpoint",
        environment="dev",
    )

    assert result is meter
    assert otel_metrics.OtelMetrics._initialized is True

    setup_metrics_mock.assert_called_once()


def test_otelmetrics_initialize_when_already_initialized(monkeypatch):
    otel_metrics.OtelMetrics._initialized = True

    meter = Mock()

    monkeypatch.setattr(
        otel_metrics.metrics,
        "get_meter",
        Mock(return_value=meter),
    )

    result = otel_metrics.OtelMetrics.initialize()

    assert result is meter


def test_otelmetrics_get_meter_initializes(monkeypatch):
    otel_metrics.OtelMetrics._initialized = False

    initialize_mock = Mock()

    monkeypatch.setattr(
        otel_metrics.OtelMetrics,
        "initialize",
        initialize_mock,
    )

    get_meter_mock = Mock(return_value="meter")

    monkeypatch.setattr(
        otel_metrics,
        "get_meter",
        get_meter_mock,
    )

    result = otel_metrics.OtelMetrics.get_meter("abc")

    assert result == "meter"

    initialize_mock.assert_called_once()
    get_meter_mock.assert_called_once_with("abc")


def test_otelmetrics_create_counter_initializes(monkeypatch):
    otel_metrics.OtelMetrics._initialized = False

    monkeypatch.setattr(
        otel_metrics.OtelMetrics,
        "initialize",
        Mock(),
    )

    create_counter_mock = Mock(return_value="counter")

    monkeypatch.setattr(
        otel_metrics,
        "create_counter",
        create_counter_mock,
    )

    result = otel_metrics.OtelMetrics.create_counter(
        "metric",
        "desc",
        "1",
    )

    assert result == "counter"

    create_counter_mock.assert_called_once_with(
        "metric",
        "desc",
        "1",
    )


def test_otelmetrics_create_histogram_initializes(monkeypatch):
    otel_metrics.OtelMetrics._initialized = False

    monkeypatch.setattr(
        otel_metrics.OtelMetrics,
        "initialize",
        Mock(),
    )

    create_histogram_mock = Mock(return_value="hist")

    monkeypatch.setattr(
        otel_metrics,
        "create_histogram",
        create_histogram_mock,
    )

    result = otel_metrics.OtelMetrics.create_histogram(
        "metric",
        "desc",
        "ms",
    )

    assert result == "hist"

    create_histogram_mock.assert_called_once_with(
        "metric",
        "desc",
        "ms",
    )


def test_otelmetrics_create_up_down_counter_initializes(monkeypatch):
    otel_metrics.OtelMetrics._initialized = False

    monkeypatch.setattr(
        otel_metrics.OtelMetrics,
        "initialize",
        Mock(),
    )

    create_updown_mock = Mock(return_value="updown")

    monkeypatch.setattr(
        otel_metrics,
        "create_up_down_counter",
        create_updown_mock,
    )

    result = otel_metrics.OtelMetrics.create_up_down_counter(
        "metric",
        "desc",
        "1",
    )

    assert result == "updown"

    create_updown_mock.assert_called_once_with(
        "metric",
        "desc",
        "1",
    )


def test_otelmetrics_shutdown():
    shutdown_mock = Mock()

    monkeypatch = pytest.MonkeyPatch()

    monkeypatch.setattr(
        otel_metrics,
        "shutdown_metrics",
        shutdown_mock,
    )

    otel_metrics.OtelMetrics._initialized = True

    otel_metrics.OtelMetrics.shutdown()

    shutdown_mock.assert_called_once()
    assert otel_metrics.OtelMetrics._initialized is False

    monkeypatch.undo()