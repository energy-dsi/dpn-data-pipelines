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
"""
otlp_transport - picks the OTLP exporter transport from
``OTEL_EXPORTER_OTLP_INSECURE``, and builds the exporter for each signal.

Why this exists
---------------
``OTEL_EXPORTER_OTLP_INSECURE`` is the single source of truth for both the
transport and TLS:

- ``INSECURE=true``  -> gRPC, plaintext (collector's :4317 receiver)
- ``INSECURE=false`` -> HTTP/protobuf, TLS/HTTPS (collector's :4318 receiver)

The port itself is NOT chosen by this module - it comes verbatim from
``OTEL_EXPORTER_OTLP_ENDPOINT`` in values.yaml (e.g. ``host:4318``). Whoever
sets INSECURE is responsible for pairing it with the matching port; this
module only decides which exporter class and scheme to use.

The two transports are NOT interchangeable at the API level, which is the whole
reason this indirection is worth having:

===================  ===========================  =============================
                     gRPC (:4317)                 HTTP (:4318)
===================  ===========================  =============================
TLS selected by      ``insecure=`` argument       the endpoint URL scheme
                                                  (there is no ``insecure``
                                                  parameter - passing one is a
                                                  ``TypeError``)
endpoint form        ``host:port``, no scheme     full URL *including* the
                                                  per-signal path, e.g.
                                                  ``https://host:4318/v1/logs``
CA trust             ``OTEL_EXPORTER_OTLP_CERTIFICATE`` (both read it themselves)
===================  ===========================  =============================

Note on the endpoint path: the HTTP exporters only append ``/v1/<signal>``
themselves when they resolve the endpoint from the environment. An endpoint
passed as a constructor argument is used VERBATIM, so this module appends the
path - otherwise every export would POST to the collector's ``/`` and 404.
"""

from __future__ import annotations

import os
from typing import Any

from dpn_observability_sdk import otlp_truststore

_SIGNAL_PATHS = {"logs": "v1/logs", "traces": "v1/traces", "metrics": "v1/metrics"}


def insecure() -> bool:
    """Shared reading of OTEL_EXPORTER_OTLP_INSECURE (defaults to true)."""
    return os.getenv("OTEL_EXPORTER_OTLP_INSECURE", "true").strip().lower() == "true"


def _http_endpoint(endpoint: str | None, signal: str) -> str | None:
    """Normalise *endpoint* into a full per-signal OTLP/HTTP URL.

    Returns None for an empty endpoint so the exporter falls back to its own
    environment handling (which appends the signal path itself).
    """
    path = _SIGNAL_PATHS[signal]
    ep = (endpoint or "").strip()
    if not ep:
        return None
    if "://" not in ep:
        # A bare host:port (the gRPC form, and what the deployed values.yaml
        # files carry) still has to become a URL here. Honour INSECURE for the
        # scheme so a plaintext collector remains reachable over HTTP.
        ep = ("http://" if insecure() else "https://") + ep
    ep = ep.rstrip("/")
    if ep.endswith("/" + path) or ep.endswith(path):
        return ep
    return f"{ep}/{path}"


def _prepare_trust(endpoint: str | None, tls: bool) -> None:
    """Resolve OTLP CA trust from the common truststore before building an exporter.

    Must run BEFORE the exporter is constructed: both transports read
    ``OTEL_EXPORTER_OTLP_CERTIFICATE`` themselves at construction time, and
    ``otlp_truststore.configure()`` is what sets it.

    Only called when TLS is in play. ``configure()`` has no silent no-op any
    more: it always resolves to ``DEFAULT_TRUSTSTORE_PATH`` (or the
    ``OTEL_TRUSTSTORE_PATH`` override) and raises ``TruststoreError`` if that
    path does not exist - the same fail-closed posture as
    utils/kafka_security.py, where an unset CA fails the handshake instead of
    degrading to an unverified connection. Skipping the call entirely for
    plaintext/gRPC keeps a producer with no truststore mounted working exactly
    as before for that transport, since it never needed one.
    """
    if not tls:
        return
    otlp_truststore.configure()


def build_log_exporter(endpoint: str | None) -> Any:
    if insecure():
        _prepare_trust(endpoint, False)
        from opentelemetry.exporter.otlp.proto.grpc._log_exporter import (
            OTLPLogExporter as _GrpcLogExporter,
        )
        return _GrpcLogExporter(endpoint=endpoint, insecure=True)
    else:
        url = _http_endpoint(endpoint, "logs")
        _prepare_trust(url, (url or "").lower().startswith("https://"))
        from opentelemetry.exporter.otlp.proto.http._log_exporter import (
            OTLPLogExporter as _HttpLogExporter,
        )
        return _HttpLogExporter(endpoint=url)


def build_span_exporter(endpoint: str | None) -> Any:
    if insecure():
        _prepare_trust(endpoint, False)
        from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import (
            OTLPSpanExporter as _GrpcSpanExporter,
        )
        return _GrpcSpanExporter(endpoint=endpoint, insecure=True)
    else:
        url = _http_endpoint(endpoint, "traces")
        _prepare_trust(url, (url or "").lower().startswith("https://"))
        from opentelemetry.exporter.otlp.proto.http.trace_exporter import (
            OTLPSpanExporter as _HttpSpanExporter,
        )
        return _HttpSpanExporter(endpoint=url)


def build_metric_exporter(endpoint: str | None) -> Any:
    if insecure():
        _prepare_trust(endpoint, False)
        from opentelemetry.exporter.otlp.proto.grpc.metric_exporter import (
            OTLPMetricExporter as _GrpcMetricExporter,
        )
        return _GrpcMetricExporter(endpoint=endpoint, insecure=True)
    else:
        url = _http_endpoint(endpoint, "metrics")
        _prepare_trust(url, (url or "").lower().startswith("https://"))
        from opentelemetry.exporter.otlp.proto.http.metric_exporter import (
            OTLPMetricExporter as _HttpMetricExporter,
        )
        return _HttpMetricExporter(endpoint=url)
