# tests/dpn_observability_sdk_test/test_otlp_transport.py

import sys
import types
from unittest.mock import Mock

import pytest

from dpn_observability_sdk import otlp_transport


@pytest.fixture(autouse=True)
def reset_env(monkeypatch):
    monkeypatch.delenv("OTEL_EXPORTER_OTLP_INSECURE", raising=False)
    yield


def _fake_module(module_path, class_name, mock_class, monkeypatch):
    """Inject a fake module at *module_path* exposing *class_name* = mock_class.

    The real import statements inside build_*_exporter are local, so patching
    sys.modules before the function runs intercepts them without needing the
    real opentelemetry exporter packages to behave a particular way.
    """
    fake = types.ModuleType(module_path)
    setattr(fake, class_name, mock_class)
    monkeypatch.setitem(sys.modules, module_path, fake)


# ---------------------------------------------------------------------------
# insecure()
# ---------------------------------------------------------------------------
def test_insecure_defaults_to_true(monkeypatch):
    monkeypatch.delenv("OTEL_EXPORTER_OTLP_INSECURE", raising=False)
    assert otlp_transport.insecure() is True


def test_insecure_false(monkeypatch):
    monkeypatch.setenv("OTEL_EXPORTER_OTLP_INSECURE", "false")
    assert otlp_transport.insecure() is False


def test_insecure_strips_and_lowercases(monkeypatch):
    monkeypatch.setenv("OTEL_EXPORTER_OTLP_INSECURE", "  FALSE  ")
    assert otlp_transport.insecure() is False


# ---------------------------------------------------------------------------
# _http_endpoint()
# ---------------------------------------------------------------------------
def test_http_endpoint_none_for_empty_endpoint():
    assert otlp_transport._http_endpoint(None, "logs") is None
    assert otlp_transport._http_endpoint("  ", "traces") is None


def test_http_endpoint_adds_scheme_for_bare_host_port_insecure(monkeypatch):
    monkeypatch.setenv("OTEL_EXPORTER_OTLP_INSECURE", "true")
    assert otlp_transport._http_endpoint("collector:4318", "logs") == "http://collector:4318/v1/logs"


def test_http_endpoint_adds_https_scheme_for_bare_host_port_secure(monkeypatch):
    monkeypatch.setenv("OTEL_EXPORTER_OTLP_INSECURE", "false")
    assert otlp_transport._http_endpoint("collector:4318", "traces") == "https://collector:4318/v1/traces"


def test_http_endpoint_keeps_existing_scheme():
    result = otlp_transport._http_endpoint("https://collector:4318", "metrics")
    assert result == "https://collector:4318/v1/metrics"


def test_http_endpoint_strips_trailing_slash_before_appending_path():
    result = otlp_transport._http_endpoint("https://collector:4318/", "logs")
    assert result == "https://collector:4318/v1/logs"


def test_http_endpoint_does_not_duplicate_existing_path():
    result = otlp_transport._http_endpoint("https://collector:4318/v1/logs", "logs")
    assert result == "https://collector:4318/v1/logs"


# ---------------------------------------------------------------------------
# _prepare_trust()
# ---------------------------------------------------------------------------
def test_prepare_trust_skips_when_no_tls_and_oauth_disabled(monkeypatch):
    monkeypatch.setattr(otlp_transport, "oauth_enabled", lambda: False)
    configure_mock = Mock()
    monkeypatch.setattr(otlp_transport.otlp_truststore, "configure", configure_mock)

    otlp_transport._prepare_trust("endpoint", False)

    configure_mock.assert_not_called()


def test_prepare_trust_runs_when_tls_true(monkeypatch):
    monkeypatch.setattr(otlp_transport, "oauth_enabled", lambda: False)
    configure_mock = Mock()
    monkeypatch.setattr(otlp_transport.otlp_truststore, "configure", configure_mock)

    otlp_transport._prepare_trust("endpoint", True)

    configure_mock.assert_called_once()


def test_prepare_trust_runs_when_oauth_enabled_even_if_not_tls(monkeypatch):
    monkeypatch.setattr(otlp_transport, "oauth_enabled", lambda: True)
    configure_mock = Mock()
    monkeypatch.setattr(otlp_transport.otlp_truststore, "configure", configure_mock)

    otlp_transport._prepare_trust("endpoint", False)

    configure_mock.assert_called_once()


# ---------------------------------------------------------------------------
# build_log_exporter / build_span_exporter / build_metric_exporter
#
# _prepare_trust is stubbed out in these tests (it has its own direct unit
# tests above) so building an exporter never touches the real truststore.
# Not autouse: the _prepare_trust unit tests above exercise the real function.
# ---------------------------------------------------------------------------
@pytest.fixture()
def stub_prepare_trust(monkeypatch):
    mock = Mock()
    monkeypatch.setattr(otlp_transport, "_prepare_trust", mock)
    return mock


@pytest.mark.parametrize(
    "builder, grpc_path, grpc_class, http_path, http_class, signal",
    [
        (
            "build_log_exporter",
            "opentelemetry.exporter.otlp.proto.grpc._log_exporter",
            "OTLPLogExporter",
            "opentelemetry.exporter.otlp.proto.http._log_exporter",
            "OTLPLogExporter",
            "logs",
        ),
        (
            "build_span_exporter",
            "opentelemetry.exporter.otlp.proto.grpc.trace_exporter",
            "OTLPSpanExporter",
            "opentelemetry.exporter.otlp.proto.http.trace_exporter",
            "OTLPSpanExporter",
            "traces",
        ),
        (
            "build_metric_exporter",
            "opentelemetry.exporter.otlp.proto.grpc.metric_exporter",
            "OTLPMetricExporter",
            "opentelemetry.exporter.otlp.proto.http.metric_exporter",
            "OTLPMetricExporter",
            "metrics",
        ),
    ],
)
def test_build_exporter_grpc_branch(
    monkeypatch, stub_prepare_trust, builder, grpc_path, grpc_class, http_path, http_class, signal
):
    monkeypatch.setenv("OTEL_EXPORTER_OTLP_INSECURE", "true")
    monkeypatch.setattr(otlp_transport, "auth_headers", lambda: {"authorization": "Bearer tok"})

    sentinel = object()
    mock_cls = Mock(return_value=sentinel)
    _fake_module(grpc_path, grpc_class, mock_cls, monkeypatch)

    result = getattr(otlp_transport, builder)("collector:4317")

    assert result is sentinel
    mock_cls.assert_called_once_with(
        endpoint="collector:4317", insecure=True, headers={"authorization": "Bearer tok"}
    )
    stub_prepare_trust.assert_called_once_with("collector:4317", False)


def test_build_exporter_grpc_branch_all_signals_use_distinct_classes(monkeypatch, stub_prepare_trust):
    # Sanity check that each builder imports its own exporter class, not a
    # shared one left behind by test ordering/module caching.
    monkeypatch.setenv("OTEL_EXPORTER_OTLP_INSECURE", "true")
    monkeypatch.setattr(otlp_transport, "auth_headers", lambda: {})

    log_mock = Mock(return_value="log-exporter")
    span_mock = Mock(return_value="span-exporter")
    metric_mock = Mock(return_value="metric-exporter")

    _fake_module(
        "opentelemetry.exporter.otlp.proto.grpc._log_exporter", "OTLPLogExporter", log_mock, monkeypatch
    )
    _fake_module(
        "opentelemetry.exporter.otlp.proto.grpc.trace_exporter", "OTLPSpanExporter", span_mock, monkeypatch
    )
    _fake_module(
        "opentelemetry.exporter.otlp.proto.grpc.metric_exporter",
        "OTLPMetricExporter",
        metric_mock,
        monkeypatch,
    )

    assert otlp_transport.build_log_exporter("c:4317") == "log-exporter"
    assert otlp_transport.build_span_exporter("c:4317") == "span-exporter"
    assert otlp_transport.build_metric_exporter("c:4317") == "metric-exporter"


@pytest.mark.parametrize(
    "builder, grpc_path, grpc_class, http_path, http_class, signal",
    [
        (
            "build_log_exporter",
            "opentelemetry.exporter.otlp.proto.grpc._log_exporter",
            "OTLPLogExporter",
            "opentelemetry.exporter.otlp.proto.http._log_exporter",
            "OTLPLogExporter",
            "logs",
        ),
        (
            "build_span_exporter",
            "opentelemetry.exporter.otlp.proto.grpc.trace_exporter",
            "OTLPSpanExporter",
            "opentelemetry.exporter.otlp.proto.http.trace_exporter",
            "OTLPSpanExporter",
            "traces",
        ),
        (
            "build_metric_exporter",
            "opentelemetry.exporter.otlp.proto.grpc.metric_exporter",
            "OTLPMetricExporter",
            "opentelemetry.exporter.otlp.proto.http.metric_exporter",
            "OTLPMetricExporter",
            "metrics",
        ),
    ],
)
def test_build_exporter_http_branch(
    monkeypatch, stub_prepare_trust, builder, grpc_path, grpc_class, http_path, http_class, signal
):
    monkeypatch.setenv("OTEL_EXPORTER_OTLP_INSECURE", "false")
    monkeypatch.setattr(otlp_transport, "make_http_session", lambda: "oauth-session")

    sentinel = object()
    mock_cls = Mock(return_value=sentinel)
    _fake_module(http_path, http_class, mock_cls, monkeypatch)

    result = getattr(otlp_transport, builder)("collector:4318")

    expected_url = f"https://collector:4318/v1/{signal}"
    assert result is sentinel
    mock_cls.assert_called_once_with(endpoint=expected_url, session="oauth-session")
    stub_prepare_trust.assert_called_once_with(expected_url, True)
