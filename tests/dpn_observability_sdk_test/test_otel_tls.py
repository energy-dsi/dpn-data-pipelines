# tests/dpn_observability_sdk_test/test_otel_tls.py

from dpn_observability_sdk import otel_tls


def test_otlp_insecure_defaults_to_true(monkeypatch):
    monkeypatch.delenv("OTEL_EXPORTER_OTLP_INSECURE", raising=False)
    assert otel_tls.otlp_insecure() is True


def test_otlp_insecure_false(monkeypatch):
    monkeypatch.setenv("OTEL_EXPORTER_OTLP_INSECURE", "false")
    assert otel_tls.otlp_insecure() is False


def test_otlp_insecure_is_case_insensitive(monkeypatch):
    monkeypatch.setenv("OTEL_EXPORTER_OTLP_INSECURE", "FALSE")
    assert otel_tls.otlp_insecure() is False

    monkeypatch.setenv("OTEL_EXPORTER_OTLP_INSECURE", "TRUE")
    assert otel_tls.otlp_insecure() is True


def test_otlp_credentials_none_when_insecure(monkeypatch):
    monkeypatch.setenv("OTEL_EXPORTER_OTLP_INSECURE", "true")
    # Even with a CA configured, insecure mode short-circuits before reading it.
    monkeypatch.setenv("OTEL_EXPORTER_OTLP_CERTIFICATE", "/some/path.pem")

    assert otel_tls.otlp_credentials() is None


def test_otlp_credentials_none_when_no_ca_configured(monkeypatch):
    monkeypatch.setenv("OTEL_EXPORTER_OTLP_INSECURE", "false")
    monkeypatch.delenv("OTEL_EXPORTER_OTLP_CERTIFICATE", raising=False)

    assert otel_tls.otlp_credentials() is None


def test_otlp_credentials_builds_channel_credentials_from_ca_file(tmp_path, monkeypatch):
    ca_file = tmp_path / "ca.pem"
    ca_file.write_bytes(b"FAKE-CERT-BYTES")

    monkeypatch.setenv("OTEL_EXPORTER_OTLP_INSECURE", "false")
    monkeypatch.setenv("OTEL_EXPORTER_OTLP_CERTIFICATE", str(ca_file))

    sentinel = object()
    captured = {}

    def fake_ssl_channel_credentials(root_certificates=None):
        captured["root_certificates"] = root_certificates
        return sentinel

    monkeypatch.setattr(otel_tls.grpc, "ssl_channel_credentials", fake_ssl_channel_credentials)

    result = otel_tls.otlp_credentials()

    assert result is sentinel
    assert captured["root_certificates"] == b"FAKE-CERT-BYTES"
