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
kafka_truststore - give the Kafka client its CA trust from the common
password-protected PKCS12 truststore instead of a loose ca.crt PEM.

Why this exists
---------------
The `dpn-tls` Secret, already mounted at `/etc/secrets/truststore.jks` in every
producer/consumer chart for `KAFKA_SSL_CA_LOCATION`, carries a second key:
`truststore.jks` (genuinely PKCS12 despite the extension). Every chart also
already injects that store's password as `TRUSTSTORE_PASSWORD` (from the
`tls-auth-secret` Secret, key `TRUSTSTORE_PASSWORD` - the universal DPN
platform password, created once by dpn-tls-cd) for OTLP
(`dpn_observability_sdk.otlp_truststore`). This module is the Kafka-side
counterpart, kept in `utils/` (not `dpn_observability_sdk/`) so
`utils/kafka_security.py` doesn't have to depend on an unrelated package to
get its CA trust.

What it actually does
----------------------
librdkafka (via confluent-kafka) has no native concept of a password-protected
CA store - `ssl.ca.location` only ever accepts a plain PEM file. So this
module:

    1. opens the .p12/.jks with the password (`cryptography` parses PKCS12,
       the standard library cannot),
    2. writes the CA certificates inside it to a private (0600) temporary PEM
       file,
    3. hands back that PEM's path for `ssl.ca.location`/`SSL_CERT_FILE`,
    4. removes it when the process ends.

BE CLEAR ABOUT WHAT THIS DOES AND DOES NOT GIVE YOU. The password is genuinely
required and genuinely verified: a wrong password, or a truststore altered by
so much as one byte, fails loudly. That integrity guarantee is the real reason
to prefer this over the plain PEM - not confidentiality, since a CA cert is
public by design.

Where this differs from dpn_observability_sdk.otlp_truststore
---------------------------------------------------------------
otlp_truststore.configure() is only ever called when OTLP TLS is actually in
play, so it treats a missing truststore at the default path as fatal too.
`build_kafka_client_config()` is called unconditionally - including in
PLAINTEXT/dev mode, where nothing is mounted at all - so
`resolve_ca_pem_path()` here is soft about that one case: no truststore at the
default, unoverridden path simply returns None so the caller can fall back to
`KAFKA_SSL_CA_LOCATION` (or nothing, in PLAINTEXT mode). Everything else - an
explicit override pointing at nothing, a wrong password, a tampered file - is
still fatal, for the same reason it is in otlp_truststore: a truststore that
was clearly provisioned but can't be opened is a misconfiguration to fail
loudly on, not paper over.

Where the password comes from
------------------------------
`TRUSTSTORE_PASSWORD` by default - the exact variable every producer/consumer
chart already injects for OTLP - overridable via `KAFKA_SSL_TRUSTSTORE_PASSWORD_ENV`
for a cluster that publishes it under another name. There is no built-in
default password: an unset variable resolves to None, and `resolve_ca_pem_path()`
raises rather than guessing one.

Where the truststore is read from
----------------------------------
`KAFKA_SSL_TRUSTSTORE_LOCATION` when set, otherwise `DEFAULT_TRUSTSTORE_PATH`
(`/etc/secrets/truststore.jks`, the same `dpn-tls` Secret already
mounted in every chart). Read by content, never by extension - the file is
genuinely PKCS12 regardless of its `.jks` name.
"""

from __future__ import annotations

import atexit
import logging
import os
import signal
import tempfile
import threading
from typing import Optional

_LOG = logging.getLogger(__name__)

# One explicit default, no search - see otlp_truststore.py for why trying
# several candidate paths is worse than one wrong path failing loudly.
ENV_TRUSTSTORE_PATH = "KAFKA_SSL_TRUSTSTORE_LOCATION"
DEFAULT_TRUSTSTORE_PATH = "/etc/secrets/truststore.jks"

# Which env var holds the password is itself overridable, for a cluster that
# already publishes it under another name. Unset, it resolves to
# TRUSTSTORE_PASSWORD, which is what every chart already injects for OTLP -
# so nothing has to be set for the normal case.
ENV_TRUSTSTORE_PASSWORD_NAME = "KAFKA_SSL_TRUSTSTORE_PASSWORD_ENV"
DEFAULT_TRUSTSTORE_PASSWORD_ENV = "TRUSTSTORE_PASSWORD"

# Set once resolve_ca_pem_path() has materialised a PEM, so repeated calls
# (e.g. multiple producers/consumers built in one process) reuse one file
# rather than decrypting repeatedly and leaking copies. Guarded because
# nothing promises callers are on the same thread.
_extracted_pem: Optional[str] = None
_lock = threading.Lock()


class TruststoreError(RuntimeError):
    """Raised when a truststore is found but cannot be opened or is unusable.

    Deliberately fatal rather than a warning that falls back to the plain PEM:
    once a trust anchor has been located, a client should stop rather than
    quietly switch to a different one.
    """


def truststore_password_env() -> str:
    """Return the name of the env var holding the truststore password."""
    return (os.getenv(ENV_TRUSTSTORE_PASSWORD_NAME) or "").strip() or DEFAULT_TRUSTSTORE_PASSWORD_ENV


def truststore_password() -> Optional[str]:
    """Return the truststore password, or None when the environment has none.

    None rather than a built-in default, so a store which cannot be opened
    fails loudly instead of being opened with a password that happened to be
    compiled in.
    """
    return (os.getenv(truststore_password_env()) or "").strip() or None


def _resolve_truststore_path() -> Optional[str]:
    """Return the truststore path, or None if nothing is mounted there at all.

    An explicit KAFKA_SSL_TRUSTSTORE_LOCATION override that points at nothing
    is always an error - a typo silently trusting a different (or no) anchor
    is exactly the failure that variable exists to remove. The default path
    missing is not an error: most local/PLAINTEXT setups have nothing mounted
    there, and that is expected.
    """
    override = os.getenv(ENV_TRUSTSTORE_PATH, "").strip()
    if override:
        if not os.path.isfile(override):
            raise TruststoreError(
                f"{ENV_TRUSTSTORE_PATH} is set to {override!r} but no file exists there."
            )
        return override

    return DEFAULT_TRUSTSTORE_PATH if os.path.isfile(DEFAULT_TRUSTSTORE_PATH) else None


def _load_ca_pem(path: str, password: Optional[str]) -> bytes:
    """Return every certificate in *path* concatenated as PEM."""
    try:
        from cryptography.hazmat.primitives.serialization import Encoding, pkcs12
    except ImportError as exc:  # pragma: no cover - depends on the environment
        raise TruststoreError(
            f"found truststore {path} but the 'cryptography' package is not "
            "installed; it is required to read PKCS12 from Python"
        ) from exc

    try:
        with open(path, "rb") as handle:
            raw = handle.read()
    except OSError as exc:
        raise TruststoreError(f"cannot read truststore {path}: {exc}") from exc

    # An empty password is NOT the same as no password: load_pkcs12 wants None
    # for a store built without one, and b"" fails against it.
    secret = password.encode() if password else None

    try:
        bundle = pkcs12.load_pkcs12(raw, secret)
    except ValueError as exc:
        # cryptography reports a wrong password and a tampered file through the
        # same ValueError - from the MAC failure alone they are genuinely
        # indistinguishable, so name both possibilities.
        raise TruststoreError(
            f"could not open truststore {path}: {exc}. Either "
            f"{truststore_password_env()} does not match the password the store "
            "was built with, or the file has been altered since it was created "
            "(a PKCS12 MAC covers the whole file)."
        ) from exc

    # A truststore holds trustedCertEntry items only, which land in
    # additional_certs. bundle.cert is the entry paired with a private key and
    # is None here - unless someone passed a keystore by mistake, so take it
    # too rather than silently producing an empty PEM.
    certs = [entry.certificate for entry in bundle.additional_certs]
    if bundle.cert is not None:
        certs.append(bundle.cert.certificate)

    if not certs:
        raise TruststoreError(
            f"truststore {path} opened successfully but contains no certificates"
        )

    _LOG.debug("loaded %d certificate(s) from %s", len(certs), path)
    return b"".join(cert.public_bytes(Encoding.PEM) for cert in certs)


def resolve_ca_pem_path() -> Optional[str]:
    """Return a PEM path extracted from the common truststore, or None.

    None means no truststore is mounted at all at the default,
    non-overridden path - the caller should fall back to whatever plain CA
    PEM path it already has (or nothing, in PLAINTEXT mode). Once a
    truststore IS found (default or override), a missing/wrong password or a
    tampered file raises TruststoreError rather than returning None - see the
    module docstring for why.

    Safe to call repeatedly and from any thread; the extraction only happens
    once per process.
    """
    global _extracted_pem

    with _lock:
        if _extracted_pem is not None:
            return _extracted_pem

        truststore = _resolve_truststore_path()
        if truststore is None:
            return None

        password = truststore_password()
        if password is None:
            raise TruststoreError(
                f"found truststore {truststore} but {truststore_password_env()} "
                "is not set. The tls-auth-secret Secret supplies it as key "
                "'TRUSTSTORE_PASSWORD'; check the secretKeyRef is "
                "present on the deployment."
            )

        pem = _load_ca_pem(truststore, password)

        # mkstemp gives 0600 and an O_EXCL create, so no other user on the
        # host can read or pre-create the file. The contents are a public CA,
        # but the PATH is what the TLS stack trusts - a world-writable one
        # would be swappable.
        fd, pem_path = tempfile.mkstemp(prefix="kafka-ca-", suffix=".pem")
        try:
            with os.fdopen(fd, "wb") as handle:
                handle.write(pem)
        except Exception:
            os.unlink(pem_path)
            raise

        _install_cleanup(pem_path)

        _extracted_pem = pem_path
        _LOG.info("Kafka CA trust taken from truststore %s", truststore)
        return pem_path


def _install_cleanup(path: str) -> None:
    """Arrange for *path* to be removed however this process ends.

    atexit ALONE IS NOT ENOUGH - it does not run when the process is
    signalled, and SIGTERM is precisely how a container is stopped. Chain a
    handler onto SIGTERM/SIGINT that cleans up and then lets the previous
    behaviour run, so we tidy up without swallowing the shutdown.
    """
    atexit.register(_cleanup, path)

    for sig in (signal.SIGTERM, signal.SIGINT):
        try:
            previous = signal.getsignal(sig)
        except (ValueError, OSError, AttributeError):  # pragma: no cover
            continue

        def handler(signum, frame, _previous=previous, _path=path):
            _cleanup(_path)
            if callable(_previous):
                _previous(signum, frame)
            else:
                signal.signal(signum, signal.SIG_DFL)
                os.kill(os.getpid(), signum)

        try:
            signal.signal(sig, handler)
        except (ValueError, OSError, AttributeError):
            # signal.signal only works on the main thread, and Windows
            # delivers a narrower set. Not fatal - atexit still covers the
            # normal path, and the file holds a public certificate, so a
            # leak on a hard kill is untidy rather than a disclosure.
            _LOG.debug("could not install %s cleanup handler", sig, exc_info=True)


def _cleanup(path: str) -> None:
    try:
        os.unlink(path)
    except OSError:  # already gone, or the temp dir was cleared under us
        pass
