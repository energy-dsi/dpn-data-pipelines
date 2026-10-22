# Copyright DSI Project
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# +---------+----------------------------------------------------------+---------------+-------------+
# | Version | Description                                              | Change Owner  | Change Date |
# +---------+----------------------------------------------------------+---------------+-------------+
# | 1.0.0   | Initial version                                           | DSI Team      | 2026-09-21  |
# +---------+----------------------------------------------------------+---------------+-------------+
"""
otlp_auth - fetches a Keycloak OAuth2 client-credentials token and supplies it
as an ``Authorization: Bearer <token>`` header for OTLP exporters.

Why this exists
---------------
The OTEL collector is configured to authenticate inbound telemetry via Keycloak
(oidc_auth extension). Exporters that do not present a valid Bearer token will
be rejected with 401. This module:

    1. reads the Keycloak token endpoint and client credentials from env vars
       (the same vars and Secret already used for Kafka OAuth, so no new
       Secrets or volumes are needed),
    2. performs a client-credentials grant and caches the token until it
       expires (with a configurable safety margin),
    3. exposes ``auth_headers()`` so ``otlp_transport`` can inject the header
       at exporter-construction time for HTTP, or as gRPC call metadata for
       gRPC.

Environment variables
---------------------
``OTEL_OAUTH_TOKEN_ENDPOINT_URL``
    Keycloak token URL, e.g.
    ``https://dpn-keycloak.ns-dpn-01.svc.cluster.local:8443/realms/dpn-realm/protocol/openid-connect/token``
    Defaults to ``KAFKA_OAUTH_TOKEN_ENDPOINT_URL`` when not set, so charts that
    already carry the Kafka OAuth env block need no new variable.

``OTEL_OAUTH_CLIENT_ID``
    OAuth client id. Defaults to ``KAFKA_OAUTH_CLIENT_ID``.

``OTEL_OAUTH_CLIENT_SECRET``
    OAuth client secret. Defaults to ``KAFKA_OAUTH_CLIENT_SECRET``.

``OTEL_OAUTH_SCOPE``
    Optional scope to request. Defaults to ``KAFKA_OAUTH_SCOPE`` when set.

``OTEL_OAUTH_TOKEN_EXPIRY_MARGIN_SECS``
    How many seconds before expiry to treat the token as stale (default: 30).

``OTEL_OAUTH_DISABLED``
    Set to ``true`` to skip OAuth entirely (useful in dev/test environments
    where the collector has no auth extension). Default: ``false``.
"""

from __future__ import annotations

import logging
import os
import threading
import time
from typing import Optional

try:
    import requests as _requests_module
    from requests import Session as _BaseSession
except ImportError:  # pragma: no cover
    _requests_module = None  # type: ignore[assignment]
    _BaseSession = object  # type: ignore[assignment,misc]

_LOG = logging.getLogger(__name__)

_EXPIRY_MARGIN = int(os.getenv("OTEL_OAUTH_TOKEN_EXPIRY_MARGIN_SECS", "30"))

# ---------------------------------------------------------------------------
# Cached token state — one token shared across all three signal exporters.
# ---------------------------------------------------------------------------
_token: Optional[str] = None
_token_expires_at: float = 0.0
_lock = threading.Lock()


class OtlpAuthError(RuntimeError):
    """Raised when a token cannot be obtained and OAuth is not disabled."""


def _oauth_disabled() -> bool:
    return os.getenv("OTEL_OAUTH_DISABLED", "false").strip().lower() == "true"


def oauth_enabled() -> bool:
    """Whether auth_headers()/get_token() will hit Keycloak (OAuth not disabled).

    Keycloak's token endpoint is always HTTPS regardless of whether the OTLP
    collector transport itself is TLS - callers that gate CA trust setup on
    the collector's transport alone (e.g. otlp_transport._prepare_trust) also
    need this to know the Keycloak leg needs trust configured too.
    """
    return not _oauth_disabled()


def _env(otel_key: str, kafka_fallback: str) -> str:
    """Read *otel_key*, falling back to *kafka_fallback*."""
    return (os.getenv(otel_key) or os.getenv(kafka_fallback) or "").strip()


def _fetch_token() -> tuple[str, float]:
    """Perform a client-credentials grant and return (access_token, expires_at)."""
    try:
        import requests  # type: ignore[import]
    except ImportError as exc:
        raise OtlpAuthError(
            "the 'requests' package is required for OTLP OAuth but is not "
            "installed; add it to requirements.txt or set OTEL_OAUTH_DISABLED=true"
        ) from exc

    token_url = _env("OTEL_OAUTH_TOKEN_ENDPOINT_URL", "KAFKA_OAUTH_TOKEN_ENDPOINT_URL")
    client_id = _env("OTEL_OAUTH_CLIENT_ID", "KAFKA_OAUTH_CLIENT_ID")
    client_secret = _env("OTEL_OAUTH_CLIENT_SECRET", "KAFKA_OAUTH_CLIENT_SECRET")

    if not token_url:
        raise OtlpAuthError(
            "OTEL_OAUTH_TOKEN_ENDPOINT_URL (or KAFKA_OAUTH_TOKEN_ENDPOINT_URL) "
            "is required for OTLP OAuth but is not set"
        )
    if not client_id or not client_secret:
        raise OtlpAuthError(
            "OTEL_OAUTH_CLIENT_ID and OTEL_OAUTH_CLIENT_SECRET (or their "
            "KAFKA_OAUTH_* equivalents) are required for OTLP OAuth but are not set"
        )

    data: dict[str, str] = {
        "grant_type": "client_credentials",
        "client_id": client_id,
        "client_secret": client_secret,
    }
    scope = _env("OTEL_OAUTH_SCOPE", "KAFKA_OAUTH_SCOPE")
    if scope:
        data["scope"] = scope

    # Use the same CA the OTLP exporters trust so the token endpoint and the
    # collector share one verified chain. Falls back to system CAs when
    # OTEL_EXPORTER_OTLP_CERTIFICATE is not set (e.g. insecure/dev).
    ca_bundle: str | bool = os.getenv("OTEL_EXPORTER_OTLP_CERTIFICATE", "") or True

    try:
        resp = requests.post(token_url, data=data, verify=ca_bundle, timeout=10)
        resp.raise_for_status()
    except Exception as exc:
        raise OtlpAuthError(f"OTLP OAuth token request to {token_url} failed: {exc}") from exc

    payload = resp.json()
    access_token: str = payload.get("access_token", "")
    if not access_token:
        raise OtlpAuthError(
            f"OTLP OAuth token response from {token_url} did not contain "
            f"access_token; response keys: {list(payload)}"
        )

    expires_in: int = int(payload.get("expires_in", 300))
    expires_at = time.monotonic() + expires_in - _EXPIRY_MARGIN

    _LOG.debug("OTLP OAuth token acquired, expires_in=%ds", expires_in)
    return access_token, expires_at


def get_token() -> str:
    """Return a valid Bearer token, refreshing if expired. Thread-safe."""
    global _token, _token_expires_at

    with _lock:
        if _token is None or time.monotonic() >= _token_expires_at:
            _token, _token_expires_at = _fetch_token()
        return _token


def auth_headers() -> dict[str, str]:
    """Return ``{"authorization": "Bearer <token>"}`` or ``{}`` when disabled.

    Called by ``otlp_transport`` before constructing each exporter so that
    every export carries a valid token. Returns an empty dict when
    ``OTEL_OAUTH_DISABLED=true`` so that dev/test environments with no auth
    extension on the collector work without any env changes.
    """
    if _oauth_disabled():
        return {}
    return {"authorization": f"Bearer {get_token()}"}


class _OAuthSession(_BaseSession):
    """A ``requests.Session`` that injects a fresh Bearer token before every POST.

    The OTLP HTTP exporters bake ``headers`` into ``self._session.headers`` at
    construction time and never touch them again.  Passing this session instead
    means the ``Authorization`` header is always current, even after the token
    that was live at pod-start expires hours later.

    When ``OTEL_OAUTH_DISABLED=true`` the session is a plain ``requests.Session``
    with no auth header added — same behaviour as before.
    """

    def post(self, url, data=None, json=None, **kwargs):  # type: ignore[override]
        if not _oauth_disabled():
            self.headers.update({"authorization": f"Bearer {get_token()}"})
        return super().post(url, data=data, json=json, **kwargs)


def make_http_session() -> object:
    """Return an ``_OAuthSession`` for the HTTP OTLP exporters.

    Falls back to a plain ``requests.Session`` when the ``requests`` package is
    not installed *and* OAuth is disabled — the exporter will raise its own
    ImportError at export time in that case, not here.

    Usage in ``otlp_transport``::

        exporter = OTLPLogExporter(endpoint=url, session=make_http_session())
    """
    if _requests_module is None:
        # requests not installed; return None so the exporter creates its own
        # session and raises ImportError at the point of use — clearer than
        # failing here with an attribute error on None.
        return None  # type: ignore[return-value]
    return _OAuthSession()
