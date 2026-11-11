# tests/dpn_observability_sdk_test/test_otlp_auth.py

import sys
from unittest.mock import Mock

import pytest
import requests as real_requests

from dpn_observability_sdk import otlp_auth

_ENV_VARS = (
    "OTEL_OAUTH_DISABLED",
    "OTEL_OAUTH_TOKEN_ENDPOINT_URL",
    "KAFKA_OAUTH_TOKEN_ENDPOINT_URL",
    "OTEL_OAUTH_CLIENT_ID",
    "KAFKA_OAUTH_CLIENT_ID",
    "OTEL_OAUTH_CLIENT_SECRET",
    "KAFKA_OAUTH_CLIENT_SECRET",
    "OTEL_OAUTH_SCOPE",
    "KAFKA_OAUTH_SCOPE",
    "OTEL_EXPORTER_OTLP_CERTIFICATE",
)


@pytest.fixture(autouse=True)
def reset_module_state(monkeypatch):
    for var in _ENV_VARS:
        monkeypatch.delenv(var, raising=False)
    monkeypatch.setattr(otlp_auth, "_token", None)
    monkeypatch.setattr(otlp_auth, "_token_expires_at", 0.0)
    yield
    monkeypatch.setattr(otlp_auth, "_token", None)
    monkeypatch.setattr(otlp_auth, "_token_expires_at", 0.0)


# ---------------------------------------------------------------------------
# _oauth_disabled / oauth_enabled
# ---------------------------------------------------------------------------
def test_oauth_disabled_default_is_false():
    assert otlp_auth._oauth_disabled() is False
    assert otlp_auth.oauth_enabled() is True


def test_oauth_disabled_true(monkeypatch):
    monkeypatch.setenv("OTEL_OAUTH_DISABLED", "true")
    assert otlp_auth._oauth_disabled() is True
    assert otlp_auth.oauth_enabled() is False


def test_oauth_disabled_is_case_insensitive_and_strips(monkeypatch):
    monkeypatch.setenv("OTEL_OAUTH_DISABLED", "  TRUE  ")
    assert otlp_auth._oauth_disabled() is True


# ---------------------------------------------------------------------------
# _env
# ---------------------------------------------------------------------------
def test_env_prefers_otel_key(monkeypatch):
    monkeypatch.setenv("OTEL_OAUTH_CLIENT_ID", "otel-id")
    monkeypatch.setenv("KAFKA_OAUTH_CLIENT_ID", "kafka-id")
    assert otlp_auth._env("OTEL_OAUTH_CLIENT_ID", "KAFKA_OAUTH_CLIENT_ID") == "otel-id"


def test_env_falls_back_to_kafka_key(monkeypatch):
    monkeypatch.delenv("OTEL_OAUTH_CLIENT_ID", raising=False)
    monkeypatch.setenv("KAFKA_OAUTH_CLIENT_ID", "kafka-id")
    assert otlp_auth._env("OTEL_OAUTH_CLIENT_ID", "KAFKA_OAUTH_CLIENT_ID") == "kafka-id"


def test_env_returns_empty_string_when_neither_set():
    assert otlp_auth._env("OTEL_OAUTH_CLIENT_ID", "KAFKA_OAUTH_CLIENT_ID") == ""


# ---------------------------------------------------------------------------
# _fetch_token
# ---------------------------------------------------------------------------
def test_fetch_token_raises_when_requests_not_installed(monkeypatch):
    monkeypatch.setitem(sys.modules, "requests", None)
    with pytest.raises(otlp_auth.OtlpAuthError, match="the 'requests' package is required"):
        otlp_auth._fetch_token()


def test_fetch_token_missing_token_url(monkeypatch):
    with pytest.raises(otlp_auth.OtlpAuthError, match="OTEL_OAUTH_TOKEN_ENDPOINT_URL"):
        otlp_auth._fetch_token()


def test_fetch_token_missing_client_credentials(monkeypatch):
    monkeypatch.setenv("OTEL_OAUTH_TOKEN_ENDPOINT_URL", "https://example.test/token")
    with pytest.raises(otlp_auth.OtlpAuthError, match="OTEL_OAUTH_CLIENT_ID"):
        otlp_auth._fetch_token()


def _set_valid_creds(monkeypatch):
    monkeypatch.setenv("OTEL_OAUTH_TOKEN_ENDPOINT_URL", "https://example.test/token")
    monkeypatch.setenv("OTEL_OAUTH_CLIENT_ID", "cid")
    monkeypatch.setenv("OTEL_OAUTH_CLIENT_SECRET", "secret")


class _FakeResponse:
    def __init__(self, payload, raise_exc=None):
        self._payload = payload
        self._raise_exc = raise_exc

    def raise_for_status(self):
        if self._raise_exc:
            raise self._raise_exc

    def json(self):
        return self._payload


def test_fetch_token_success_no_scope_no_ca(monkeypatch):
    _set_valid_creds(monkeypatch)
    captured = {}

    def fake_post(url, data=None, verify=None, timeout=None):
        captured.update(url=url, data=data, verify=verify, timeout=timeout)
        return _FakeResponse({"access_token": "tok-abc", "expires_in": 100})

    monkeypatch.setattr(real_requests, "post", fake_post)
    monkeypatch.setattr(otlp_auth.time, "monotonic", lambda: 1000.0)

    token, expires_at = otlp_auth._fetch_token()

    assert token == "tok-abc"
    assert expires_at == 1000.0 + 100 - otlp_auth._EXPIRY_MARGIN
    assert captured["url"] == "https://example.test/token"
    assert captured["data"] == {
        "grant_type": "client_credentials",
        "client_id": "cid",
        "client_secret": "secret",
    }
    assert captured["verify"] is True
    assert captured["timeout"] == 10


def test_fetch_token_includes_scope_when_set(monkeypatch):
    _set_valid_creds(monkeypatch)
    monkeypatch.setenv("OTEL_OAUTH_SCOPE", "myscope")
    captured = {}

    def fake_post(url, data=None, verify=None, timeout=None):
        captured.update(data=data)
        return _FakeResponse({"access_token": "tok", "expires_in": 60})

    monkeypatch.setattr(real_requests, "post", fake_post)

    otlp_auth._fetch_token()

    assert captured["data"]["scope"] == "myscope"


def test_fetch_token_uses_ca_bundle_when_configured(monkeypatch, tmp_path):
    _set_valid_creds(monkeypatch)
    ca_file = tmp_path / "ca.pem"
    monkeypatch.setenv("OTEL_EXPORTER_OTLP_CERTIFICATE", str(ca_file))
    captured = {}

    def fake_post(url, data=None, verify=None, timeout=None):
        captured["verify"] = verify
        return _FakeResponse({"access_token": "tok", "expires_in": 60})

    monkeypatch.setattr(real_requests, "post", fake_post)

    otlp_auth._fetch_token()

    assert captured["verify"] == str(ca_file)


def test_fetch_token_wraps_request_exception(monkeypatch):
    _set_valid_creds(monkeypatch)

    def fake_post(*a, **kw):
        raise ConnectionError("boom")

    monkeypatch.setattr(real_requests, "post", fake_post)

    with pytest.raises(otlp_auth.OtlpAuthError, match="failed"):
        otlp_auth._fetch_token()


def test_fetch_token_wraps_raise_for_status_failure(monkeypatch):
    _set_valid_creds(monkeypatch)

    def fake_post(*a, **kw):
        return _FakeResponse({}, raise_exc=RuntimeError("401"))

    monkeypatch.setattr(real_requests, "post", fake_post)

    with pytest.raises(otlp_auth.OtlpAuthError, match="failed"):
        otlp_auth._fetch_token()


def test_fetch_token_missing_access_token_in_response(monkeypatch):
    _set_valid_creds(monkeypatch)

    def fake_post(*a, **kw):
        return _FakeResponse({"foo": "bar"})

    monkeypatch.setattr(real_requests, "post", fake_post)

    with pytest.raises(otlp_auth.OtlpAuthError, match="did not contain access_token"):
        otlp_auth._fetch_token()


def test_fetch_token_defaults_expires_in_when_absent(monkeypatch):
    _set_valid_creds(monkeypatch)

    def fake_post(*a, **kw):
        return _FakeResponse({"access_token": "tok"})

    monkeypatch.setattr(real_requests, "post", fake_post)
    monkeypatch.setattr(otlp_auth.time, "monotonic", lambda: 0.0)

    _, expires_at = otlp_auth._fetch_token()

    assert expires_at == 300 - otlp_auth._EXPIRY_MARGIN


# ---------------------------------------------------------------------------
# get_token
# ---------------------------------------------------------------------------
def test_get_token_fetches_once_and_caches(monkeypatch):
    fetch_mock = Mock(return_value=("tok1", 10_000.0))
    monkeypatch.setattr(otlp_auth, "_fetch_token", fetch_mock)
    monkeypatch.setattr(otlp_auth.time, "monotonic", lambda: 100.0)

    assert otlp_auth.get_token() == "tok1"
    assert otlp_auth.get_token() == "tok1"
    fetch_mock.assert_called_once()


def test_get_token_refreshes_once_expired(monkeypatch):
    monkeypatch.setattr(otlp_auth, "_token", "old")
    monkeypatch.setattr(otlp_auth, "_token_expires_at", 50.0)
    monkeypatch.setattr(otlp_auth.time, "monotonic", lambda: 100.0)

    fetch_mock = Mock(return_value=("new", 500.0))
    monkeypatch.setattr(otlp_auth, "_fetch_token", fetch_mock)

    assert otlp_auth.get_token() == "new"
    fetch_mock.assert_called_once()


# ---------------------------------------------------------------------------
# auth_headers
# ---------------------------------------------------------------------------
def test_auth_headers_empty_when_disabled(monkeypatch):
    monkeypatch.setenv("OTEL_OAUTH_DISABLED", "true")
    assert otlp_auth.auth_headers() == {}


def test_auth_headers_bearer_when_enabled(monkeypatch):
    monkeypatch.setattr(otlp_auth, "get_token", lambda: "zzz")
    assert otlp_auth.auth_headers() == {"authorization": "Bearer zzz"}


# ---------------------------------------------------------------------------
# _OAuthSession
# ---------------------------------------------------------------------------
def test_oauth_session_injects_bearer_header_when_enabled(monkeypatch):
    monkeypatch.setattr(otlp_auth, "get_token", lambda: "tok123")

    captured = {}

    def fake_super_post(self, url, data=None, json=None, **kwargs):
        captured["headers"] = dict(self.headers)
        captured["url"] = url
        return "response"

    monkeypatch.setattr(otlp_auth._BaseSession, "post", fake_super_post)

    session = otlp_auth._OAuthSession()
    result = session.post("http://example.test")

    assert result == "response"
    assert captured["headers"]["authorization"] == "Bearer tok123"
    assert captured["url"] == "http://example.test"


def test_oauth_session_skips_header_when_disabled(monkeypatch):
    monkeypatch.setenv("OTEL_OAUTH_DISABLED", "true")

    captured = {}

    def fake_super_post(self, url, data=None, json=None, **kwargs):
        captured["headers"] = dict(self.headers)
        return "response"

    monkeypatch.setattr(otlp_auth._BaseSession, "post", fake_super_post)

    session = otlp_auth._OAuthSession()
    session.post("http://example.test")

    assert "authorization" not in captured["headers"]


# ---------------------------------------------------------------------------
# make_http_session
# ---------------------------------------------------------------------------
def test_make_http_session_returns_oauth_session_when_requests_available():
    session = otlp_auth.make_http_session()
    assert isinstance(session, otlp_auth._OAuthSession)


def test_make_http_session_returns_none_when_requests_missing(monkeypatch):
    monkeypatch.setattr(otlp_auth, "_requests_module", None)
    assert otlp_auth.make_http_session() is None
