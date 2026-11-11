# Copyright 2026 DSI Project
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
# | 1.0.0   | Initial version                                           | DSI Team      | 2026-09-04  |
# +---------+----------------------------------------------------------+---------------+-------------+
"""
Shared TLS config for the OTLP gRPC exporters (otel_logger/otel_tracer/
otel_metrics) so all three agree on how OTEL_EXPORTER_OTLP_INSECURE and
OTEL_EXPORTER_OTLP_CERTIFICATE are interpreted.

Before this existed, otel_logger.py hardcoded insecure=True regardless of
env, while otel_tracer.py/otel_metrics.py read OTEL_EXPORTER_OTLP_INSECURE
from env — so a collector that requires TLS (OTEL_EXPORTER_OTLP_INSECURE
set to "false") rejected the logs exporter's plaintext attempt with
StatusCode.UNAVAILABLE while traces/metrics attempted TLS but had no CA to
verify the collector's cert against (CERTIFICATE_VERIFY_FAILED), since the
collector's cert is signed by an internal CA, not a public one.

OTEL_EXPORTER_OTLP_CERTIFICATE points at a PEM CA bundle (mounted from a
Secret, e.g. dpn-health-ca — see charts/tls-ca-sync/health-manifests.yaml)
that verifies that internal CA. Only meaningful when insecure is False.
"""
import os
from typing import Optional

import grpc  # dependency of every opentelemetry-exporter-otlp-proto-grpc package


def otlp_insecure() -> bool:
    return os.getenv("OTEL_EXPORTER_OTLP_INSECURE", "true").lower() == "true"


def otlp_credentials() -> Optional[grpc.ChannelCredentials]:
    """
    None when running insecure (the flag governs the channel type, so
    credentials are irrelevant) or when no CA path is configured — in the
    latter case the exporter falls back to its own default (the system
    trust store), matching prior behavior when OTEL_EXPORTER_OTLP_CERTIFICATE
    was never wired up anywhere.
    """
    if otlp_insecure():
        return None

    ca_path = os.getenv("OTEL_EXPORTER_OTLP_CERTIFICATE")
    if not ca_path:
        return None

    with open(ca_path, "rb") as f:
        return grpc.ssl_channel_credentials(root_certificates=f.read())
