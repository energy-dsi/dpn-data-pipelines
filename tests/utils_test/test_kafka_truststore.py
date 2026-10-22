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
Full coverage for utils/kafka_truststore.py: opening the dpn-tls Secret's
password-protected PKCS12 truststore.jks/keystore.jks and handing back a
plain PEM, the way librdkafka/ssl.ca.location actually needs it.

Builds REAL PKCS12 blobs with the `cryptography` package (the same library
the module itself uses to parse them) rather than mocking the PKCS12 parser
— this exercises the real decode path, including the genuine MAC-failure
behavior on a wrong password or a tampered file.
"""
from __future__ import annotations

import datetime
import os
import signal
from pathlib import Path
from unittest.mock import MagicMock, call

import pytest
from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.hazmat.primitives.serialization import pkcs12
from cryptography.x509.oid import NameOID

from utils import kafka_truststore


# ---------------------------------------------------------------------------
# PKCS12 fixture builders
# ---------------------------------------------------------------------------
def _make_cert(common_name: str, key, issuer_key, issuer_cert=None, is_ca: bool = False):
    subject = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, common_name)])
    issuer = issuer_cert.subject if issuer_cert is not None else subject
    now = datetime.datetime.now(datetime.timezone.utc)
    builder = (
        x509.CertificateBuilder()
        .subject_name(subject)
        .issuer_name(issuer)
        .public_key(key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now - datetime.timedelta(days=1))
        .not_valid_after(now + datetime.timedelta(days=1))
        .add_extension(x509.BasicConstraints(ca=is_ca, path_length=None), critical=True)
    )
    return builder.sign(issuer_key, hashes.SHA256())


def _rsa_key():
    return rsa.generate_private_key(public_exponent=65537, key_size=2048)


def _ca():
    key = _rsa_key()
    cert = _make_cert("Test CA", key, key, is_ca=True)
    return key, cert


def _build_truststore_bytes(ca_certs, password: bytes | None) -> bytes:
    """A PKCS12 blob holding only trustedCertEntry items — the real shape of
    dpn-tls's truststore.jks (no private key)."""
    encryption = (
        serialization.BestAvailableEncryption(password) if password else serialization.NoEncryption()
    )
    return pkcs12.serialize_key_and_certificates(None, None, None, ca_certs, encryption)


def _build_keystore_bytes(key, cert, ca_certs, password: bytes | None) -> bytes:
    """A PKCS12 blob pairing a certificate with its private key — the real
    shape of dpn-tls's keystore.jks."""
    encryption = (
        serialization.BestAvailableEncryption(password) if password else serialization.NoEncryption()
    )
    return pkcs12.serialize_key_and_certificates(None, key, cert, ca_certs, encryption)


@pytest.fixture(autouse=True)
def _reset_module_state(monkeypatch: pytest.MonkeyPatch):
    """_extracted_pem is a process-global cache — never let one test's
    extraction leak into another's assertions about whether resolution ran."""
    monkeypatch.setattr(kafka_truststore, "_extracted_pem", None)
    monkeypatch.delenv(kafka_truststore.ENV_TRUSTSTORE_PATH, raising=False)
    monkeypatch.delenv(kafka_truststore.ENV_TRUSTSTORE_PASSWORD_NAME, raising=False)
    monkeypatch.delenv(kafka_truststore.DEFAULT_TRUSTSTORE_PASSWORD_ENV, raising=False)
    # Never let a real /etc/secrets/truststore.jks on the test host (there
    # shouldn't be one, but be explicit) leak into "no truststore" tests.
    monkeypatch.setattr(kafka_truststore, "DEFAULT_TRUSTSTORE_PATH", "/does/not/exist/truststore.jks")


# ---------------------------------------------------------------------------
# truststore_password_env() / truststore_password()
# ---------------------------------------------------------------------------
def test_truststore_password_env_defaults_when_unset():
    assert kafka_truststore.truststore_password_env() == "TRUSTSTORE_PASSWORD"


def test_truststore_password_env_override(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv(kafka_truststore.ENV_TRUSTSTORE_PASSWORD_NAME, "MY_PASSWORD_VAR")
    assert kafka_truststore.truststore_password_env() == "MY_PASSWORD_VAR"


def test_truststore_password_none_when_unset():
    assert kafka_truststore.truststore_password() is None


def test_truststore_password_none_when_blank(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv("TRUSTSTORE_PASSWORD", "   ")
    assert kafka_truststore.truststore_password() is None


def test_truststore_password_present(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv("TRUSTSTORE_PASSWORD", "s3cr3t")
    assert kafka_truststore.truststore_password() == "s3cr3t"


# ---------------------------------------------------------------------------
# _resolve_truststore_path()
# ---------------------------------------------------------------------------
def test_resolve_truststore_path_none_when_nothing_mounted():
    assert kafka_truststore._resolve_truststore_path() is None


def test_resolve_truststore_path_override_missing_raises(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv(kafka_truststore.ENV_TRUSTSTORE_PATH, "/no/such/file.jks")

    with pytest.raises(kafka_truststore.TruststoreError, match="no file exists"):
        kafka_truststore._resolve_truststore_path()


def test_resolve_truststore_path_override_existing(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    store = tmp_path / "custom.jks"
    store.write_bytes(b"irrelevant-bytes")
    monkeypatch.setenv(kafka_truststore.ENV_TRUSTSTORE_PATH, str(store))

    assert kafka_truststore._resolve_truststore_path() == str(store)


def test_resolve_truststore_path_default_when_present(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    store = tmp_path / "truststore.jks"
    store.write_bytes(b"irrelevant-bytes")
    monkeypatch.setattr(kafka_truststore, "DEFAULT_TRUSTSTORE_PATH", str(store))

    assert kafka_truststore._resolve_truststore_path() == str(store)


# ---------------------------------------------------------------------------
# _load_ca_pem() / load_ca_pem_from_path()
# ---------------------------------------------------------------------------
def test_load_ca_pem_success(tmp_path: Path):
    _, ca_cert = _ca()
    store = tmp_path / "truststore.jks"
    store.write_bytes(_build_truststore_bytes([ca_cert], b"pw"))

    pem = kafka_truststore._load_ca_pem(str(store), "pw")

    assert pem.count(b"BEGIN CERTIFICATE") == 1
    assert x509.load_pem_x509_certificate(pem).subject == ca_cert.subject


def test_load_ca_pem_includes_paired_cert_and_additional_certs(tmp_path: Path):
    """A bundle can carry both a cert-paired-with-key entry (bundle.cert)
    AND trustedCertEntry items (bundle.additional_certs) — both must land
    in the returned PEM."""
    ca_key, ca_cert = _ca()
    leaf_key = _rsa_key()
    leaf_cert = _make_cert("leaf", leaf_key, ca_key, issuer_cert=ca_cert)
    store = tmp_path / "mixed.jks"
    store.write_bytes(_build_keystore_bytes(leaf_key, leaf_cert, [ca_cert], b"pw"))

    pem = kafka_truststore._load_ca_pem(str(store), "pw")

    assert pem.count(b"BEGIN CERTIFICATE") == 2


def test_load_ca_pem_wrong_password_raises(tmp_path: Path):
    _, ca_cert = _ca()
    store = tmp_path / "truststore.jks"
    store.write_bytes(_build_truststore_bytes([ca_cert], b"correct-password"))

    with pytest.raises(kafka_truststore.TruststoreError, match="could not open truststore"):
        kafka_truststore._load_ca_pem(str(store), "wrong-password")


def test_load_ca_pem_tampered_file_raises(tmp_path: Path):
    _, ca_cert = _ca()
    raw = bytearray(_build_truststore_bytes([ca_cert], b"pw"))
    raw[-1] ^= 0xFF  # flip the last byte — PKCS12's MAC covers the whole file
    store = tmp_path / "tampered.jks"
    store.write_bytes(bytes(raw))

    with pytest.raises(kafka_truststore.TruststoreError, match="could not open truststore"):
        kafka_truststore._load_ca_pem(str(store), "pw")


def test_load_ca_pem_unreadable_file_raises(tmp_path: Path):
    missing = tmp_path / "does-not-exist.jks"

    with pytest.raises(kafka_truststore.TruststoreError, match="cannot read truststore"):
        kafka_truststore._load_ca_pem(str(missing), "pw")


def test_load_ca_pem_no_certificates_raises(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    store = tmp_path / "empty.jks"
    store.write_bytes(b"placeholder")

    empty_bundle = MagicMock(additional_certs=[], cert=None)
    monkeypatch.setattr(
        "cryptography.hazmat.primitives.serialization.pkcs12.load_pkcs12",
        lambda raw, secret: empty_bundle,
    )

    with pytest.raises(kafka_truststore.TruststoreError, match="contains no certificates"):
        kafka_truststore._load_ca_pem(str(store), "pw")


def test_load_ca_pem_from_path_public_wrapper(tmp_path: Path):
    _, ca_cert = _ca()
    store = tmp_path / "truststore.jks"
    store.write_bytes(_build_truststore_bytes([ca_cert], b"pw"))

    pem = kafka_truststore.load_ca_pem_from_path(str(store), "pw")

    assert pem.count(b"BEGIN CERTIFICATE") == 1


# ---------------------------------------------------------------------------
# resolve_ca_pem_path()
# ---------------------------------------------------------------------------
def test_resolve_ca_pem_path_none_when_nothing_mounted():
    assert kafka_truststore.resolve_ca_pem_path() is None


def test_resolve_ca_pem_path_raises_when_password_missing(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    store = tmp_path / "truststore.jks"
    store.write_bytes(b"irrelevant")
    monkeypatch.setattr(kafka_truststore, "DEFAULT_TRUSTSTORE_PATH", str(store))

    with pytest.raises(kafka_truststore.TruststoreError, match="is not set"):
        kafka_truststore.resolve_ca_pem_path()


def test_resolve_ca_pem_path_success_and_cache(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    _, ca_cert = _ca()
    store = tmp_path / "truststore.jks"
    store.write_bytes(_build_truststore_bytes([ca_cert], b"pw"))
    monkeypatch.setattr(kafka_truststore, "DEFAULT_TRUSTSTORE_PATH", str(store))
    monkeypatch.setenv("TRUSTSTORE_PASSWORD", "pw")

    pem_path = kafka_truststore.resolve_ca_pem_path()
    try:
        assert pem_path is not None
        assert Path(pem_path).read_bytes().count(b"BEGIN CERTIFICATE") == 1

        # Second call must hit the cache, not re-resolve — prove it by making
        # a repeat resolution explode if it were ever attempted.
        monkeypatch.setattr(
            kafka_truststore,
            "_resolve_truststore_path",
            lambda: (_ for _ in ()).throw(AssertionError("should not re-resolve")),
        )
        assert kafka_truststore.resolve_ca_pem_path() == pem_path
    finally:
        os.unlink(pem_path)


# ---------------------------------------------------------------------------
# _cleanup() / _install_cleanup()
# ---------------------------------------------------------------------------
def test_cleanup_removes_existing_file(tmp_path: Path):
    target = tmp_path / "gone.pem"
    target.write_bytes(b"x")

    kafka_truststore._cleanup(str(target))

    assert not target.exists()


def test_cleanup_swallows_missing_file():
    kafka_truststore._cleanup("/does/not/exist/anywhere.pem")  # must not raise


def test_install_cleanup_registers_atexit(monkeypatch: pytest.MonkeyPatch):
    registered = []
    monkeypatch.setattr(kafka_truststore.atexit, "register", lambda fn, path: registered.append((fn, path)))
    monkeypatch.setattr(kafka_truststore.signal, "signal", lambda sig, handler: None)

    kafka_truststore._install_cleanup("/tmp/whatever.pem")

    assert registered == [(kafka_truststore._cleanup, "/tmp/whatever.pem")]


def test_install_cleanup_handler_chains_to_callable_previous(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    target = tmp_path / "chain.pem"
    target.write_bytes(b"x")
    previous = MagicMock()
    monkeypatch.setattr(kafka_truststore.signal, "getsignal", lambda sig: previous)
    monkeypatch.setattr(kafka_truststore.atexit, "register", lambda fn, path: None)

    captured = {}

    def fake_signal(sig, handler):
        captured[sig] = handler

    monkeypatch.setattr(kafka_truststore.signal, "signal", fake_signal)

    kafka_truststore._install_cleanup(str(target))

    assert signal.SIGTERM in captured
    handler = captured[signal.SIGTERM]
    handler(signal.SIGTERM, None)

    assert not target.exists()
    previous.assert_called_once_with(signal.SIGTERM, None)


def test_install_cleanup_logs_when_signal_registration_fails(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, caplog
):
    """signal.signal() only works on the main thread, and Windows supports a
    narrower set of signals — a failure there must be swallowed (atexit
    still covers normal process exit), not raised."""
    target = tmp_path / "chain3.pem"
    target.write_bytes(b"x")
    monkeypatch.setattr(kafka_truststore.signal, "getsignal", lambda sig: None)
    monkeypatch.setattr(kafka_truststore.atexit, "register", lambda fn, path: None)

    def failing_signal(sig, handler):
        raise ValueError("signal only works in main thread")

    monkeypatch.setattr(kafka_truststore.signal, "signal", failing_signal)

    with caplog.at_level("DEBUG", logger=kafka_truststore._LOG.name):
        kafka_truststore._install_cleanup(str(target))  # must not raise

    assert "could not install" in caplog.text


def test_resolve_ca_pem_path_cleans_up_temp_file_on_write_failure(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    _, ca_cert = _ca()
    store = tmp_path / "truststore.jks"
    store.write_bytes(_build_truststore_bytes([ca_cert], b"pw"))
    monkeypatch.setattr(kafka_truststore, "DEFAULT_TRUSTSTORE_PATH", str(store))
    monkeypatch.setenv("TRUSTSTORE_PASSWORD", "pw")

    created_paths = []
    real_mkstemp = kafka_truststore.tempfile.mkstemp

    def spying_mkstemp(*args, **kwargs):
        fd, path = real_mkstemp(*args, **kwargs)
        created_paths.append(path)
        return fd, path

    monkeypatch.setattr(kafka_truststore.tempfile, "mkstemp", spying_mkstemp)

    def failing_fdopen(fd, mode):
        # Simulate the write itself failing after the fd was already open —
        # close it first, same as a real context manager would on the way
        # out, so the production code's subsequent os.unlink() isn't left
        # fighting Windows over a still-open handle (POSIX allows deleting
        # an open file; Windows does not).
        os.close(fd)
        raise OSError("disk full")

    monkeypatch.setattr(kafka_truststore.os, "fdopen", failing_fdopen)

    with pytest.raises(OSError, match="disk full"):
        kafka_truststore.resolve_ca_pem_path()

    assert created_paths, "mkstemp should still have been called"
    assert not os.path.exists(created_paths[0]), "the half-written temp file must be cleaned up"


def test_install_cleanup_handler_falls_back_to_sig_dfl_and_kill(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    target = tmp_path / "chain2.pem"
    target.write_bytes(b"x")
    # Not callable — e.g. the previous handler was SIG_DFL/SIG_IGN/None.
    monkeypatch.setattr(kafka_truststore.signal, "getsignal", lambda sig: signal.SIG_DFL)
    monkeypatch.setattr(kafka_truststore.atexit, "register", lambda fn, path: None)

    captured = {}

    def fake_signal(sig, handler):
        captured[sig] = handler

    monkeypatch.setattr(kafka_truststore.signal, "signal", fake_signal)
    kill_calls = []
    monkeypatch.setattr(kafka_truststore.os, "kill", lambda pid, sig: kill_calls.append((pid, sig)))

    kafka_truststore._install_cleanup(str(target))

    handler = captured[signal.SIGTERM]
    # fake_signal already overwrote signal.signal for the registration call
    # above; re-point it so we can observe the handler's own SIG_DFL restore.
    restore_calls = []
    monkeypatch.setattr(
        kafka_truststore.signal, "signal", lambda sig, h: restore_calls.append((sig, h))
    )

    handler(signal.SIGTERM, None)

    assert not target.exists()
    assert restore_calls == [(signal.SIGTERM, signal.SIG_DFL)]
    assert kill_calls == [(os.getpid(), signal.SIGTERM)]


# ---------------------------------------------------------------------------
# load_server_cert_and_key_from_path()
# ---------------------------------------------------------------------------
def test_load_server_cert_and_key_success(tmp_path: Path):
    ca_key, ca_cert = _ca()
    leaf_key = _rsa_key()
    leaf_cert = _make_cert("server", leaf_key, ca_key, issuer_cert=ca_cert)
    store = tmp_path / "keystore.jks"
    store.write_bytes(_build_keystore_bytes(leaf_key, leaf_cert, [ca_cert], b"pw"))

    cert_pem, key_pem = kafka_truststore.load_server_cert_and_key_from_path(str(store), "pw")

    assert cert_pem.count(b"BEGIN CERTIFICATE") == 2  # leaf + CA chain
    loaded_key = serialization.load_pem_private_key(key_pem, password=None)
    assert loaded_key.private_numbers().public_numbers == leaf_key.private_numbers().public_numbers


def test_load_server_cert_and_key_wrong_password_raises(tmp_path: Path):
    ca_key, ca_cert = _ca()
    leaf_key = _rsa_key()
    leaf_cert = _make_cert("server", leaf_key, ca_key, issuer_cert=ca_cert)
    store = tmp_path / "keystore.jks"
    store.write_bytes(_build_keystore_bytes(leaf_key, leaf_cert, [], b"correct"))

    with pytest.raises(kafka_truststore.TruststoreError, match="could not open keystore"):
        kafka_truststore.load_server_cert_and_key_from_path(str(store), "wrong")


def test_load_server_cert_and_key_unreadable_file_raises(tmp_path: Path):
    missing = tmp_path / "does-not-exist.jks"

    with pytest.raises(kafka_truststore.TruststoreError, match="cannot read keystore"):
        kafka_truststore.load_server_cert_and_key_from_path(str(missing), "pw")


def test_load_server_cert_and_key_truststore_passed_by_mistake_raises(tmp_path: Path):
    """A CA-only truststore (no cert/key entry) passed to this keystore
    loader — bundle.cert/bundle.key both end up None."""
    _, ca_cert = _ca()
    store = tmp_path / "truststore.jks"
    store.write_bytes(_build_truststore_bytes([ca_cert], b"pw"))

    with pytest.raises(kafka_truststore.TruststoreError, match="no certificate/private key entry"):
        kafka_truststore.load_server_cert_and_key_from_path(str(store), "pw")
