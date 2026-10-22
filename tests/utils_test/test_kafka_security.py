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
"""
Pins down utils/kafka_security.py's workaround for confluent-kafka's PyPI
wheel hardcoding /etc/pki/tls/certs/ca-bundle.crt for its OAUTHBEARER/OIDC
token-endpoint HTTP client (ignoring both ssl.ca.location and SSL_CERT_FILE)
— confirmed directly against a deployed self-service pod, where this is why
topic deletion (self-service's own Kafka admin client) failed while topic
creation (the blueprint adaptor's differently-built confluent-kafka) did not.
"""
from __future__ import annotations

from pathlib import Path

import pytest

from utils import kafka_security


_KAFKA_ENV_VARS = [
    "KAFKA_SECURITY_PROTOCOL",
    "KAFKA_SASL_MECHANISM",
    "KAFKA_OAUTH_CLIENT_ID",
    "KAFKA_OAUTH_CLIENT_SECRET",
    "KAFKA_OAUTH_TOKEN_ENDPOINT_URL",
    "KAFKA_OAUTH_SCOPE",
    "KAFKA_SASL_USERNAME",
    "KAFKA_SASL_PASSWORD",
    "KAFKA_SSL_CA_LOCATION",
    "KAFKA_SSL_CERTIFICATE_LOCATION",
    "KAFKA_SSL_KEY_LOCATION",
    "KAFKA_SSL_KEY_PASSWORD",
    "KAFKA_SSL_KEYSTORE_LOCATION",
    "KAFKA_SSL_KEYSTORE_PASSWORD",
    "KEYSTORE_PASSWORD",
    "KAFKA_SSL_ENDPOINT_IDENTIFICATION",
    "SSL_CERT_FILE",
]


@pytest.fixture(autouse=True)
def _no_truststore(monkeypatch: pytest.MonkeyPatch):
    """Force _resolve_ca_trust_location() down the KAFKA_SSL_CA_LOCATION
    fallback path, same as a deployment with no truststore.jks mounted."""
    monkeypatch.setattr(kafka_security.kafka_truststore, "resolve_ca_pem_path", lambda: None)
    # A clean slate for every test in this file — build_kafka_client_config()
    # reads these directly from the real process environment (not injected),
    # and os.environ.setdefault("SSL_CERT_FILE", ...) mutates that same real
    # environment, so a leftover value from another test/module could
    # otherwise make a test here pass or fail depending on run order.
    for name in _KAFKA_ENV_VARS:
        monkeypatch.delenv(name, raising=False)
    # No keystore mounted at the real default path unless a test opts in.
    monkeypatch.setattr(kafka_security, "DEFAULT_KEYSTORE_PATH", "/does/not/exist/keystore.jks")


def test_build_config_mirrors_ca_to_bundled_libcurl_default_path(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    ca_file = tmp_path / "ca.pem"
    ca_file.write_bytes(b"-----BEGIN CERTIFICATE-----\nfake\n-----END CERTIFICATE-----\n")
    mirrored_path = tmp_path / "mirrored-ca-bundle.crt"

    monkeypatch.setenv("KAFKA_SSL_CA_LOCATION", str(ca_file))
    monkeypatch.setattr(kafka_security, "_BUNDLED_LIBCURL_OIDC_CA_PATH", str(mirrored_path))

    config = kafka_security.build_kafka_client_config("broker:9092")

    assert config["ssl.ca.location"] == str(ca_file)
    assert mirrored_path.read_bytes() == ca_file.read_bytes()


def test_build_config_does_not_raise_when_mirror_path_unwritable(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    """A build/runtime environment where this particular confluent-kafka bug
    doesn't apply (e.g. no such directory, or not writable) must not break
    Kafka client construction — ssl.ca.location alone still works for every
    non-OIDC-HTTP-client use of the connection."""
    ca_file = tmp_path / "ca.pem"
    ca_file.write_bytes(b"fake-ca")
    unwritable_path = tmp_path / "does-not-exist" / "ca-bundle.crt"

    monkeypatch.setenv("KAFKA_SSL_CA_LOCATION", str(ca_file))
    monkeypatch.setattr(kafka_security, "_BUNDLED_LIBCURL_OIDC_CA_PATH", str(unwritable_path))

    config = kafka_security.build_kafka_client_config("broker:9092")

    assert config["ssl.ca.location"] == str(ca_file)
    assert not unwritable_path.exists()


def test_default_mode_is_sasl_ssl_oauthbearer_oidc():
    """No security env vars set at all — must still authenticate rather
    than silently downgrading (see build_kafka_client_config's own
    docstring/comment for why this default exists)."""
    config = kafka_security.build_kafka_client_config("broker:9092")

    assert config["security.protocol"] == "sasl_ssl"
    assert config["sasl.mechanism"] == "OAUTHBEARER"
    assert config["sasl.oauthbearer.method"] == "oidc"
    assert "ssl.ca.location" not in config


def test_oauthbearer_optional_fields_all_set(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv("KAFKA_OAUTH_CLIENT_ID", "dpn-service-client")
    monkeypatch.setenv("KAFKA_OAUTH_CLIENT_SECRET", "s3cr3t")
    monkeypatch.setenv("KAFKA_OAUTH_TOKEN_ENDPOINT_URL", "https://keycloak/token")
    monkeypatch.setenv("KAFKA_OAUTH_SCOPE", "openid email profile")

    config = kafka_security.build_kafka_client_config("broker:9092")

    assert config["sasl.oauthbearer.client.id"] == "dpn-service-client"
    assert config["sasl.oauthbearer.client.secret"] == "s3cr3t"
    assert config["sasl.oauthbearer.token.endpoint.url"] == "https://keycloak/token"
    assert config["sasl.oauthbearer.scope"] == "openid email profile"


def test_non_oauthbearer_sasl_uses_username_password(monkeypatch: pytest.MonkeyPatch):
    """security.protocol still starts with 'sasl' but the mechanism isn't
    OAUTHBEARER (e.g. SASL_SSL + PLAIN) — the else branch's username/password
    path, never exercised by the OAUTHBEARER-focused tests above."""
    monkeypatch.setenv("KAFKA_SASL_MECHANISM", "PLAIN")
    monkeypatch.setenv("KAFKA_SASL_USERNAME", "alice")
    monkeypatch.setenv("KAFKA_SASL_PASSWORD", "hunter2")

    config = kafka_security.build_kafka_client_config("broker:9092")

    assert config["sasl.mechanism"] == "PLAIN"
    assert config["sasl.username"] == "alice"
    assert config["sasl.password"] == "hunter2"
    assert "sasl.oauthbearer.method" not in config


def test_ssl_certificate_key_and_endpoint_identification_fields(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv("KAFKA_SSL_CERTIFICATE_LOCATION", "/etc/kafka/secrets/tls.crt")
    monkeypatch.setenv("KAFKA_SSL_KEY_LOCATION", "/etc/kafka/secrets/tls.key")
    monkeypatch.setenv("KAFKA_SSL_KEY_PASSWORD", "keypass")
    monkeypatch.setenv("KAFKA_SSL_ENDPOINT_IDENTIFICATION", "none")

    config = kafka_security.build_kafka_client_config("broker:9092")

    assert config["ssl.certificate.location"] == "/etc/kafka/secrets/tls.crt"
    assert config["ssl.key.location"] == "/etc/kafka/secrets/tls.key"
    assert config["ssl.key.password"] == "keypass"
    assert config["ssl.endpoint.identification.algorithm"] == "none"


def test_ssl_keystore_from_explicit_location_env(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv("KAFKA_SSL_KEYSTORE_LOCATION", "/custom/keystore.jks")
    monkeypatch.setenv("KAFKA_SSL_KEYSTORE_PASSWORD", "ks-pass")

    config = kafka_security.build_kafka_client_config("broker:9092")

    assert config["ssl.keystore.location"] == "/custom/keystore.jks"
    assert config["ssl.keystore.password"] == "ks-pass"


def test_ssl_keystore_auto_discovered_at_default_path_prefers_keystore_password(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    """KEYSTORE_PASSWORD (the standalone var every chart injects) must win
    over KAFKA_SSL_KEYSTORE_PASSWORD when both happen to be set."""
    keystore_file = tmp_path / "keystore.jks"
    keystore_file.write_bytes(b"fake-pkcs12")
    monkeypatch.setattr(kafka_security, "DEFAULT_KEYSTORE_PATH", str(keystore_file))
    monkeypatch.setenv("KEYSTORE_PASSWORD", "chart-password")
    monkeypatch.setenv("KAFKA_SSL_KEYSTORE_PASSWORD", "manual-override-password")

    config = kafka_security.build_kafka_client_config("broker:9092")

    assert config["ssl.keystore.location"] == str(keystore_file)
    assert config["ssl.keystore.password"] == "chart-password"


def test_no_keystore_block_when_nothing_mounted_or_configured():
    config = kafka_security.build_kafka_client_config("broker:9092")

    assert "ssl.keystore.location" not in config
    assert "ssl.keystore.password" not in config
