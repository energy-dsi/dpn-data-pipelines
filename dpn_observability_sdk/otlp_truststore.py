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
otlp_truststore - take OTLP CA trust from the common password-protected PKCS12
truststore, the same file and the same password the JVM services consume.

Why this exists
---------------
The DPN PKI ships one trust anchor in two formats:

    truststore.p12   PKCS12 + password   (Keycloak, Kafka UI, and now this)
    rootCA.crt       PEM                 (Go and Python)

Both hold the identical NESO-DSI-Root-CA certificate. This module makes the
``.p12`` the single distributed artefact for the Python producers too: one file
to ship, one password to rotate, and the tamper-evidence a PKCS12 MAC provides.

What it actually does
---------------------
Python has no truststore support at any layer - not ``ssl``, not ``requests``,
not the OTLP exporters - because PKCS12 and JKS are Java container formats.
There is no ``trustStorePassword`` equivalent to set. So this module:

    1. opens the .p12 with the password (``cryptography`` parses PKCS12, which
       the standard library cannot),
    2. writes the CA certificates inside it to a private temporary PEM file,
    3. points ``OTEL_EXPORTER_OTLP_CERTIFICATE`` at that file,
    4. removes it when the process ends.

BE CLEAR ABOUT WHAT THIS DOES AND DOES NOT GIVE YOU. The password is genuinely
required and genuinely verified: a wrong password, or a truststore altered by so
much as one byte, fails at step 1 and the producer never starts. That integrity
guarantee is the real reason to prefer this over the PEM.

What it is NOT is Python "using a truststore" the way a JVM does. The JVM holds
the store and consults it inside its TLS stack; here it is decrypted up front
and Python's TLS stack sees only PEM. A short-lived 0600 copy of a PUBLIC CA
certificate exists on disk while the process runs. That is not a secret - it is
broadcast in every TLS handshake - but it does mean the .p12 is a distribution
and integrity mechanism here, not a runtime one.

Where the password comes from
-----------------------------
``TRUSTSTORE_PASSWORD``, injected from ``tls-auth-secret`` - the universal DPN
platform password Secret, created once (into every DPN namespace) by
dpn-tls-cd (dpn-federator-certificate-manager), from the same
``TRUSTSTORE-PASSWORD`` Key Vault object every other DPN component's
keystore/truststore password ultimately comes from. No separate Secret is
needed: every producer/consumer chart already injects it under this exact
key name::

    - name: TRUSTSTORE_PASSWORD
      valueFrom:
        secretKeyRef:
          name: tls-auth-secret
          key: TRUSTSTORE_PASSWORD

There is no built-in default. If the variable is absent and a truststore was
found, ``configure()`` raises rather than trying a password compiled into this
file - a store that cannot be opened should say so, not be opened with a guess.
Locally that means exporting the password the PKI was built with (``P12_PASS``
in config/certs/generate-certs.sh) alongside the usual OTLP variables.

Which variable is read is itself overridable via
``OTEL_TRUSTSTORE_PASSWORD_ENV``, defaulting to ``TRUSTSTORE_PASSWORD``. That
exists for the same reason ``OTEL_TRUSTSTORE_PATH`` does: a cluster already
publishing the password under another name is a deployment decision, not a code
change. The charts inject ``TRUSTSTORE_PASSWORD``, so nothing needs setting for
the normal case.

Where the truststore is read from
--------------------------------
``OTEL_TRUSTSTORE_PATH`` when set, otherwise ``DEFAULT_TRUSTSTORE_PATH``
(``/etc/secrets/truststore.jks`` - the existing ``dpn-tls`` Secret,
already mounted in every producer/consumer chart for ``KAFKA_SSL_CA_LOCATION``,
confirmed via ``kubectl describe pod`` on a running broker to carry a
``truststore.jks`` key that is genuinely PKCS12 despite the extension
(``KAFKA_SSL_TRUSTSTORE_TYPE`` on that pod says so, and this module reads by
content, never by extension)). No separate truststore Secret or volume is
needed.

There is exactly one default and no search. An earlier version tried several
candidate paths in order, which meant behaviour depended on which files
happened to exist on disk rather than on declared configuration - the same
class of problem as compiling in a password, just for a path instead of a
value. Two candidate directories were present on one machine holding CAs with
different serial numbers, so the wrong one was silently picked, and every
export failed a handshake with nothing pointing at the CA being the wrong
file. A single explicit default plus an environment override cannot guess
wrong.

The resolved path is validated to exist; if it is not, ``configure()`` raises
``TruststoreError`` immediately - there is no silent no-op. ``otlp_transport``
only calls ``configure()`` when TLS is actually in play, so a plaintext/gRPC
producer with no truststore mounted is unaffected.
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

# ---------------------------------------------------------------------------
# Where the truststore is looked for.
#
# One explicit default, no search. OTEL_TRUSTSTORE_PATH overrides it outright
# when set; there is no list of candidate locations to try, because trying
# several meant behaviour depended on which files happened to exist on disk -
# the same silent-wrong-anchor risk as a compiled-in password, just for a path
# instead of a value. Two candidate directories existed on one machine with
# different-serial CAs; the search picked whichever was nearer, and every
# export failed a handshake with nothing pointing at the CA being the wrong
# file.
# ---------------------------------------------------------------------------
ENV_TRUSTSTORE_PATH = "OTEL_TRUSTSTORE_PATH"

# The existing dpn-tls Secret, already mounted at /etc/secrets/truststore.jks in every
# producer/consumer chart for KAFKA_SSL_CA_LOCATION. Confirmed via
# `kubectl describe pod` on a running broker: this Secret carries a
# truststore.jks key that KAFKA_SSL_TRUSTSTORE_TYPE on that same pod reports as
# PKCS12 - the ".jks" is a filename habit, not the format, and this module
# reads by content, never by extension. No separate Secret or volume is
# required to reach it.
DEFAULT_TRUSTSTORE_PATH = "/etc/secrets/truststore.jks"

# ---------------------------------------------------------------------------
# Where the password comes from.
#
# The password itself is NEVER in this file. It is read from the environment,
# where the common truststore Secret injects it - the same variable, Secret and
# key the Keycloak realm-import job uses, so this is not a new mechanism.
#
# Which variable holds it is itself overridable, for the same reason
# OTEL_TRUSTSTORE_PATH exists: a cluster that already publishes the password
# under a different name should be a deployment decision, not a code change and
# an image rebuild. Unset, it resolves to TRUSTSTORE_PASSWORD, which is what the
# charts inject - so nothing has to be set for the normal case.
# ---------------------------------------------------------------------------
ENV_TRUSTSTORE_PASSWORD_NAME = "OTEL_TRUSTSTORE_PASSWORD_ENV"
DEFAULT_TRUSTSTORE_PASSWORD_ENV = "TRUSTSTORE_PASSWORD"

# The OpenTelemetry spec variable the exporters read the CA path out of. This is
# an OUTPUT - what this module sets - not a knob, so it is spelled out here
# rather than being configurable.
ENV_CERTIFICATE = "OTEL_EXPORTER_OTLP_CERTIFICATE"

# Set once configure() has materialised a PEM, so the three exporters (logs,
# traces, metrics) reuse one file rather than decrypting three times and
# leaking two of them. Guarded because the three signals are initialised
# independently and nothing promises they are set up on the same thread.
_extracted_pem: Optional[str] = None
_lock = threading.Lock()


class TruststoreError(RuntimeError):
    """Raised when a truststore is found but cannot be opened or is unusable.

    Deliberately fatal rather than a warning that falls back to the PEM: once a
    trust anchor has been located, a producer should stop rather than quietly
    switch to a different one.
    """


def truststore_password_env() -> str:
    """Return the name of the env var holding the truststore password."""
    return (os.getenv(ENV_TRUSTSTORE_PASSWORD_NAME) or "").strip() or DEFAULT_TRUSTSTORE_PASSWORD_ENV


def truststore_password() -> Optional[str]:
    """Return the truststore password, or None when the environment has none.

    None is returned rather than a built-in default so that a store which cannot
    be opened fails loudly instead of being opened with a password that happened
    to be compiled in. configure() turns None into a TruststoreError naming the
    variable it looked for.
    """
    return (os.getenv(truststore_password_env()) or "").strip() or None


def _resolve_truststore_path() -> str:
    """Return the truststore path, or raise if the resolved path does not exist.

    OTEL_TRUSTSTORE_PATH overrides DEFAULT_TRUSTSTORE_PATH outright when set, so
    a Secret whose key is not "truststore.jks" is a deployment decision rather
    than a code change and an image rebuild. There is no fallback list: unlike
    the password, a missing or wrong path has nothing safe to guess at, so this
    always resolves to exactly one location and validates it immediately.
    """
    path = (os.getenv(ENV_TRUSTSTORE_PATH) or DEFAULT_TRUSTSTORE_PATH).strip()
    if not os.path.isfile(path):
        raise TruststoreError(
            f"truststore not found at {path!r}. Set {ENV_TRUSTSTORE_PATH} to "
            "override, or confirm the dpn-tls Secret is mounted at "
            "/etc/secrets/truststore.jks and carries a truststore.jks key."
        )
    return path


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
            "was built with (P12_PASS in config/certs/generate-certs.sh), or "
            "the file has been altered since it was created (a PKCS12 MAC "
            "covers the whole file)."
        ) from exc

    # A truststore holds trustedCertEntry items only, which land in
    # additional_certs. bundle.cert is the entry paired with a private key and
    # is None here - unless someone passed a KEYSTORE by mistake, so take it too
    # rather than silently producing an empty PEM.
    certs = [entry.certificate for entry in bundle.additional_certs]
    if bundle.cert is not None:
        certs.append(bundle.cert.certificate)

    if not certs:
        raise TruststoreError(
            f"truststore {path} opened successfully but contains no certificates"
        )

    _LOG.debug("loaded %d certificate(s) from %s", len(certs), path)
    return b"".join(cert.public_bytes(Encoding.PEM) for cert in certs)


def configure() -> str:
    """Point OTLP CA trust at the common PKCS12 truststore.

    Returns the path of the extracted PEM. Raises TruststoreError if the
    resolved truststore path does not exist, the password is missing, or the
    file cannot be opened - there is no silent no-op any more. Call this only
    when a truststore is actually required; otlp_transport does so only when
    TLS is in play. Safe to call repeatedly and from any thread.
    """
    global _extracted_pem

    with _lock:
        if _extracted_pem is not None:
            return _extracted_pem

        truststore = _resolve_truststore_path()

        existing = (os.getenv(ENV_CERTIFICATE) or "").strip()
        if existing:
            # Both present is ambiguous about which anchor wins, and the
            # exporters read ENV_CERTIFICATE themselves - so say which one is
            # being used rather than letting it depend on import order.
            _LOG.warning(
                "truststore %s found and %s is also set (%s); the truststore "
                "takes precedence and %s will be overwritten",
                truststore, ENV_CERTIFICATE, existing, ENV_CERTIFICATE,
            )

        password = truststore_password()
        if password is None:
            # Without this the store would be opened with no password at all and
            # fail on the MAC, which reads as a corrupt file rather than an
            # unset variable. Name what is missing instead.
            raise TruststoreError(
                f"found truststore {truststore} but {truststore_password_env()} "
                "is not set. The tls-auth-secret Secret supplies it as key "
                "'TRUSTSTORE_PASSWORD'; check the secretKeyRef is "
                "present on the deployment."
            )

        pem = _load_ca_pem(truststore, password)

        # mkstemp gives 0600 and an O_EXCL create, so no other user on the host
        # can read or pre-create the file. The contents are a public CA, but the
        # PATH is what the TLS stack trusts - a world-writable one would be
        # swappable.
        fd, pem_path = tempfile.mkstemp(prefix="otlp-ca-", suffix=".pem")
        try:
            with os.fdopen(fd, "wb") as handle:
                handle.write(pem)
        except Exception:
            os.unlink(pem_path)
            raise

        _install_cleanup(pem_path)

        os.environ[ENV_CERTIFICATE] = pem_path
        _extracted_pem = pem_path

        _LOG.info("OTLP CA trust taken from truststore %s", truststore)
        return pem_path


def _install_cleanup(path: str) -> None:
    """Arrange for *path* to be removed however this process ends.

    atexit ALONE IS NOT ENOUGH. It runs on a normal interpreter exit, but not
    when the process is signalled - and SIGTERM is precisely how a container is
    stopped, so a pod that restarts nightly would leave one stale PEM behind per
    restart. Chain a handler onto SIGTERM/SIGINT that cleans up and then lets the
    previous behaviour run, so we tidy up without swallowing the shutdown.
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
                # SIG_DFL / SIG_IGN: restore it and re-raise so the process dies
                # with the right status instead of silently continuing.
                signal.signal(signum, signal.SIG_DFL)
                os.kill(os.getpid(), signum)

        try:
            signal.signal(sig, handler)
        except (ValueError, OSError, AttributeError):
            # signal.signal only works on the main thread, and Windows delivers
            # a narrower set. Not fatal - atexit still covers the normal path,
            # and the file holds a public certificate, so a leak on a hard kill
            # is untidy rather than a disclosure.
            _LOG.debug("could not install %s cleanup handler", sig, exc_info=True)


def _cleanup(path: str) -> None:
    try:
        os.unlink(path)
    except OSError:  # already gone, or the temp dir was cleared under us
        pass
