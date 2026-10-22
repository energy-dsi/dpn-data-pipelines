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
# | 1.0.0   | Initial version                                          | DSI Team      | 2026-05-01  |
# | 1.1.0   | Mirror resolved CA to OIDC client's hardcoded CA path    | DSI Team      | 2026-10-01  |
# +---------+----------------------------------------------------------+---------------+-------------+
from __future__ import annotations

import os
from typing import Any

from utils import kafka_truststore

# Same dpn-tls Secret, same mount convention as kafka_truststore.py's
# DEFAULT_TRUSTSTORE_PATH — this is that Secret's other key, keystore.jks,
# mounted alongside it for client-identity mTLS.
DEFAULT_KEYSTORE_PATH = "/etc/secrets/keystore.jks"


def _resolve_ca_trust_location(fallback_ca_location: str) -> str:
    """Prefer the common password-protected PKCS12 truststore over a plain CA PEM.

    utils/kafka_truststore.py already knows how to open the dpn-tls Secret's
    truststore.jks (genuinely PKCS12 despite the extension) with
    TRUSTSTORE_PASSWORD, which every producer/consumer chart already injects
    from the dpn-keycloak-secrets Secret (originally for OTLP). Reusing it here means
    the Kafka client trusts the same CA via that password-protected,
    tamper-evident artefact instead of a loose ca.crt PEM.

    Falls back to *fallback_ca_location* (KAFKA_SSL_CA_LOCATION) only when no
    truststore file is mounted at all. Once a truststore IS found, a wrong
    password or a tampered file is fatal (raised by
    kafka_truststore.resolve_ca_pem_path()) rather than silently falling back
    to the PEM path: an intentionally provisioned truststore that can't be
    opened is a misconfiguration to fail loudly on, not paper over.
    """
    pem_path = kafka_truststore.resolve_ca_pem_path()
    return pem_path if pem_path is not None else fallback_ca_location


# confluent-kafka's PyPI manylinux wheel bundles a statically-linked
# librdkafka/OpenSSL/libcurl whose OAUTHBEARER/OIDC token-endpoint HTTP
# client hardcodes this RHEL-style path for CA trust, ignoring both
# ssl.ca.location and SSL_CERT_FILE entirely — confirmed directly against a
# deployed self-service pod: OIDC token fetches failed with "error setting
# certificate file: /etc/pki/tls/certs/ca-bundle.crt (-1)" even though
# SSL_CERT_FILE was already correctly set, because that path simply doesn't
# exist on this image's Debian base. A blueprint adaptor/extractor image
# never hits this — those compile confluent-kafka from source against
# Alpine's own librdkafka-dev (see e.g.
# blueprints/producer/topic/adaptor/Dockerfile), a different OpenSSL/libcurl
# build with no such hardcoded path. service/Dockerfile pre-creates this
# directory, owned by the runtime user, so the write below can succeed.
_BUNDLED_LIBCURL_OIDC_CA_PATH = "/etc/pki/tls/certs/ca-bundle.crt"


def _mirror_ca_to_bundled_libcurl_default_path(ca_location: str) -> None:
    """Best-effort: copy the resolved CA PEM to the fixed path described
    above, in addition to setting SSL_CERT_FILE. Never raises — a failure
    here (e.g. this image's confluent-kafka build doesn't have the bug, or
    the directory isn't writable for some other reason) must not block
    Kafka connectivity that would otherwise work fine via ssl.ca.location
    alone."""
    try:
        with open(ca_location, "rb") as src:
            content = src.read()
        with open(_BUNDLED_LIBCURL_OIDC_CA_PATH, "wb") as dst:
            dst.write(content)
    except OSError:
        pass


def build_kafka_client_config(bootstrap_server: str) -> dict[str, Any]:
    """Build a Confluent Kafka client config from environment variables.

    Security mode is controlled by `KAFKA_SECURITY_PROTOCOL` (SASL_SSL |
    SSL | PLAINTEXT) and, for SASL_SSL, `KAFKA_SASL_MECHANISM`
    (OAUTHBEARER). Both default to SASL_SSL + OAUTHBEARER when unset. See
    docs/kafka-auth-modes.md for example env blocks per mode.

    TLS trust comes from a PEM CA bundle (`KAFKA_SSL_CA_LOCATION`), unless the
    common password-protected truststore.jks is mounted (see
    `_resolve_ca_trust_location()`), in which case that takes precedence. A
    client identity for mTLS can be supplied either as a certificate/key PEM
    pair (`KAFKA_SSL_CERTIFICATE_LOCATION`/`KAFKA_SSL_KEY_LOCATION`) or as a
    PKCS#12 keystore, auto-discovered the same way the truststore is (see
    `DEFAULT_KEYSTORE_PATH`) with its password from `KEYSTORE_PASSWORD`
    (`KAFKA_SSL_KEYSTORE_LOCATION`/`KAFKA_SSL_KEYSTORE_PASSWORD` remain as
    manual overrides).
    """
    config: dict[str, Any] = {"bootstrap.servers": bootstrap_server}

    # Default to SASL_SSL + OAUTHBEARER when the mode isn't specified, so a
    # client with no security env vars set still authenticates rather than
    # silently connecting unauthenticated/unencrypted. Set both
    # KAFKA_SECURITY_PROTOCOL and KAFKA_SASL_MECHANISM explicitly to opt into
    # SSL (mTLS, no SASL) or PLAINTEXT (dev/test only). An explicitly empty
    # value falls back to SASL_SSL too: omitting security.protocol from the
    # config makes librdkafka default to plaintext, which is exactly the
    # silent downgrade this default exists to prevent.
    security_protocol = os.getenv("KAFKA_SECURITY_PROTOCOL", "").strip().lower() or "sasl_ssl"
    config["security.protocol"] = security_protocol

    sasl_mechanism = os.getenv("KAFKA_SASL_MECHANISM", "OAUTHBEARER").strip().upper()
    if security_protocol.startswith("sasl") and sasl_mechanism:
        config["sasl.mechanism"] = sasl_mechanism

    if security_protocol.startswith("sasl") and sasl_mechanism == "OAUTHBEARER":
        # librdkafka's built-in OIDC client-credentials flow: it fetches and
        # refreshes tokens itself, no oauth_cb needed. Its HTTP client does
        # NOT use this config's ssl.ca.location to verify the token endpoint —
        # it goes through OpenSSL's SSL_CERT_FILE instead (set below) — but
        # that bundle still has to cover Keycloak's CA as well as Kafka's.
        config["sasl.oauthbearer.method"] = "oidc"
        client_id = os.getenv("KAFKA_OAUTH_CLIENT_ID", "").strip()
        if client_id:
            config["sasl.oauthbearer.client.id"] = client_id
        client_secret = os.getenv("KAFKA_OAUTH_CLIENT_SECRET", "").strip()
        if client_secret:
            config["sasl.oauthbearer.client.secret"] = client_secret
        token_endpoint_url = os.getenv("KAFKA_OAUTH_TOKEN_ENDPOINT_URL", "").strip()
        if token_endpoint_url:
            config["sasl.oauthbearer.token.endpoint.url"] = token_endpoint_url
        scope = os.getenv("KAFKA_OAUTH_SCOPE", "").strip()
        if scope:
            config["sasl.oauthbearer.scope"] = scope
    else:
        sasl_username = os.getenv("KAFKA_SASL_USERNAME", "").strip()
        if sasl_username:
            config["sasl.username"] = sasl_username

        sasl_password = os.getenv("KAFKA_SASL_PASSWORD", "").strip()
        if sasl_password:
            config["sasl.password"] = sasl_password

    # Set by the charts (values.yaml -> deployment env), and overridable from
    # AKV at deploy time. Either source arrives here as an env var, so no code
    # change is needed to switch between them.
    #
    # Left unset, librdkafka falls back to the image's system CA store, which
    # is correct only if the broker's certificate chains to a public root. The
    # DPN brokers use a private CA, so SASL_SSL needs this pointing at the
    # mounted bundle — an unset value fails the TLS handshake, it does not
    # degrade to an unverified connection.
    # Resolved even when KAFKA_SSL_CA_LOCATION is unset/empty: the common
    # truststore.jks (see _resolve_ca_trust_location()) is looked for on its
    # own default path regardless, so a chart that mounts only the truststore
    # and never sets KAFKA_SSL_CA_LOCATION at all still gets TLS trust. The env
    # var survives purely as the last-resort fallback for images/charts with
    ssl_ca_location = os.getenv("KAFKA_SSL_CA_LOCATION", "").strip()
    ca_location = _resolve_ca_trust_location(ssl_ca_location)
    if ca_location:
        config["ssl.ca.location"] = ca_location
        # librdkafka's OIDC token-endpoint HTTP client ignores ssl.ca.location
        # and reads OpenSSL's SSL_CERT_FILE instead, so without this the token
        # fetch fails closed with "self-signed certificate in certificate
        # chain" even when the CA bundle above is correct. Deriving it here
        # keeps the OIDC path working when AKV supplies only the CA path; an
        # explicitly provided SSL_CERT_FILE always wins.
        os.environ.setdefault("SSL_CERT_FILE", ca_location)
        _mirror_ca_to_bundled_libcurl_default_path(ca_location)

    ssl_certificate_location = os.getenv("KAFKA_SSL_CERTIFICATE_LOCATION", "").strip()
    if ssl_certificate_location:
        config["ssl.certificate.location"] = ssl_certificate_location

    ssl_key_location = os.getenv("KAFKA_SSL_KEY_LOCATION", "").strip()
    if ssl_key_location:
        config["ssl.key.location"] = ssl_key_location

    ssl_key_password = os.getenv("KAFKA_SSL_KEY_PASSWORD", "").strip()
    if ssl_key_password:
        config["ssl.key.password"] = ssl_key_password

    # PKCS#12 client identity, as an alternative to the separate
    # certificate/key PEM pair above. This is the client-side counterpart of
    # the broker's tls.keystoreLocation/keystoreType=PKCS12 (config-sample.yaml)
    # and is only needed when the broker enforces mTLS (tls.clientAuth=required)
    # — every chart today authenticates via SASL_SSL/OAUTHBEARER alone, so this
    # stays soft: applied only when a keystore is actually mounted, exactly
    # like _resolve_ca_trust_location() above's truststore auto-discovery.
    # Unlike the CA/trust side, librdkafka reads a PKCS#12 keystore natively
    # via ssl.keystore.location/ssl.keystore.password — no PEM extraction
    # needed here, so there is no keystore equivalent of kafka_truststore.py.
    ssl_keystore_location = os.getenv("KAFKA_SSL_KEYSTORE_LOCATION", "").strip() or (
        DEFAULT_KEYSTORE_PATH if os.path.isfile(DEFAULT_KEYSTORE_PATH) else ""
    )
    if ssl_keystore_location:
        config["ssl.keystore.location"] = ssl_keystore_location
        # KEYSTORE_PASSWORD is the standalone var every chart injects (mirrors
        # TRUSTSTORE_PASSWORD's naming); KAFKA_SSL_KEYSTORE_PASSWORD survives as
        # a manual override for an image/environment publishing it under the
        # Kafka-prefixed name instead.
        ssl_keystore_password = (
            os.getenv("KEYSTORE_PASSWORD", "").strip()
            or os.getenv("KAFKA_SSL_KEYSTORE_PASSWORD", "").strip()
        )
        if ssl_keystore_password:
            config["ssl.keystore.password"] = ssl_keystore_password

    # Only set when supplied. librdkafka already defaults this to "https", so
    # leaving it out of values.yaml does not disable hostname verification.
    ssl_endpoint_identification = os.getenv("KAFKA_SSL_ENDPOINT_IDENTIFICATION", "").strip()
    if ssl_endpoint_identification:
        config["ssl.endpoint.identification.algorithm"] = ssl_endpoint_identification

    return config
