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
# | 1.0.0   | Initial version                                           | DSI Team      | 2026-09-23  |
# +---------+----------------------------------------------------------+---------------+-------------+
"""
Extract the plain PEM files oauth2-proxy needs from the same
password-protected PKCS12 dpn-tls Secret every other container in this
repo already reads via utils/kafka_truststore.py / utils/kafka_security.py.

Why this exists
----------------
oauth2-proxy is a real Go binary, not one of this repo's own Python
processes — it has no PKCS12 support of its own, and dpn-tls deliberately
carries no loose PEM files (see utils/kafka_truststore.py's module
docstring for why: a tamper-evident PKCS12 archive instead of a plain
ca.crt). So oauth2-proxy's --provider-ca-file and TLS cert/key can't point
at dpn-tls directly the way the chart's ConfigMap otherwise assumes.

Run once as an initContainer before oauth2-proxy starts (see
service/charts/templates/oauth2-proxy-deployment.yaml, which invokes this
as `python -m service.scripts.extract_oauth2_proxy_certs` from the image's
/app WORKDIR — a real module import, not a sys.path hack), writing into an
emptyDir the main container then mounts read-only at the same path the
ConfigMap already references.

Reads (mounted from dpn-tls, read-only):
    TRUSTSTORE_PATH (default /etc/secrets/truststore.jks), TRUSTSTORE_PASSWORD
    KEYSTORE_PATH   (default /etc/secrets/keystore.jks),   KEYSTORE_PASSWORD
Writes, into OUT_DIR (default /etc/kafka/secrets), each 0600:
    ca.crt   - every CA certificate in the truststore
    tls.crt  - the keystore's server certificate, followed by every CA
               certificate in the truststore (see below for why)
    tls.key  - the keystore's private key, unencrypted PKCS8 PEM

Why tls.crt includes the truststore's CA certs too
---------------------------------------------------
keystore.jks was built for Kafka client-identity mTLS, not for serving
HTTPS to a browser — it typically bundles the leaf certificate alone, with
no intermediate/root alongside it (a Kafka broker verifies the leaf against
its own separately-configured trust anchor, so the client never needs to
present a chain). A browser has no such out-of-band trust anchor for this
leaf; it needs the full leaf -> intermediate -> root chain in one place to
build a path to whatever root CA is installed locally, or it fails with
NET::ERR_CERT_AUTHORITY_INVALID even when that root CA is genuinely
trusted. Appending the truststore's CA certificates (which is exactly what
a Kafka client already trusts this leaf through) gives the browser that
same path.
"""
from __future__ import annotations

import os
import sys

from utils.kafka_truststore import (
    TruststoreError,
    load_ca_pem_from_path,
    load_server_cert_and_key_from_path,
)


def _write(path: str, data: bytes) -> None:
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    with os.fdopen(fd, "wb") as handle:
        handle.write(data)


def main() -> int:
    truststore_path = os.getenv("TRUSTSTORE_PATH", "/etc/secrets/truststore.jks")
    truststore_password = os.getenv("TRUSTSTORE_PASSWORD") or None
    keystore_path = os.getenv("KEYSTORE_PATH", "/etc/secrets/keystore.jks")
    keystore_password = os.getenv("KEYSTORE_PASSWORD") or None
    out_dir = os.getenv("OUT_DIR", "/etc/kafka/secrets")

    os.makedirs(out_dir, exist_ok=True)

    try:
        ca_pem = load_ca_pem_from_path(truststore_path, truststore_password)
        _write(os.path.join(out_dir, "ca.crt"), ca_pem)

        cert_pem, key_pem = load_server_cert_and_key_from_path(keystore_path, keystore_password)
        # Full chain for the browser — see module docstring for why the
        # leaf alone (what keystore.jks provides on its own) isn't enough.
        _write(os.path.join(out_dir, "tls.crt"), cert_pem + ca_pem)
        _write(os.path.join(out_dir, "tls.key"), key_pem)
    except TruststoreError as exc:
        print(f"extract_oauth2_proxy_certs: {exc}", file=sys.stderr)
        return 1

    print(f"Wrote ca.crt/tls.crt/tls.key to {out_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
