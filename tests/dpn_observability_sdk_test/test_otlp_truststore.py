# tests/dpn_observability_sdk_test/test_otlp_truststore.py

import datetime
import os

import pytest

from dpn_observability_sdk import otlp_truststore

cryptography = pytest.importorskip("cryptography")

from cryptography import x509  # noqa: E402
from cryptography.hazmat.primitives import hashes, serialization  # noqa: E402
from cryptography.hazmat.primitives.asymmetric import rsa  # noqa: E402
from cryptography.hazmat.primitives.serialization import pkcs12  # noqa: E402
from cryptography.x509.oid import NameOID  # noqa: E402


def _make_ca():
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "Test-Root-CA")])
    now = datetime.datetime(2020, 1, 1, tzinfo=datetime.timezone.utc)
    cert = (
        x509.CertificateBuilder()
        .subject_name(name)
        .issuer_name(name)
        .public_key(key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now)
        .not_valid_after(now + datetime.timedelta(days=3650))
        .add_extension(x509.BasicConstraints(ca=True, path_length=None), critical=True)
        .add_extension(
            x509.KeyUsage(
                digital_signature=True, key_cert_sign=True, crl_sign=True,
                content_commitment=False, key_encipherment=False,
                data_encipherment=False, key_agreement=False,
                encipher_only=False, decipher_only=False,
            ),
            critical=True,
        )
        .sign(key, hashes.SHA256())
    )
    return cert


def _write_truststore(tmp_path, password, name="truststore.p12"):
    """Build a trust-only PKCS12 (cert, no key) the way keytool -importcert does."""
    blob = pkcs12.serialize_key_and_certificates(
        name=b"test-root-ca",
        key=None,
        cert=None,
        cas=[_make_ca()],
        encryption_algorithm=(
            serialization.BestAvailableEncryption(password.encode())
            if password else serialization.NoEncryption()
        ),
    )
    path = tmp_path / name
    path.write_bytes(blob)
    return str(path)


@pytest.fixture(autouse=True)
def reset_module_state(monkeypatch):
    monkeypatch.setattr(otlp_truststore, "_extracted_pem", None)
    monkeypatch.delenv(otlp_truststore.ENV_CERTIFICATE, raising=False)
    monkeypatch.delenv(otlp_truststore.DEFAULT_TRUSTSTORE_PASSWORD_ENV, raising=False)
    monkeypatch.delenv(otlp_truststore.ENV_TRUSTSTORE_PASSWORD_NAME, raising=False)
    # Cleared too, or a developer who exports it in their shell gets failures
    # here that look nothing like the cause.
    monkeypatch.delenv(otlp_truststore.ENV_TRUSTSTORE_PATH, raising=False)
    yield
    monkeypatch.setattr(otlp_truststore, "_extracted_pem", None)


def test_password_comes_from_secret_injected_env(monkeypatch):
    monkeypatch.setenv(otlp_truststore.DEFAULT_TRUSTSTORE_PASSWORD_ENV, "from-secret")
    assert otlp_truststore.truststore_password() == "from-secret"


def test_password_has_no_builtin_default():
    # Nothing compiled in: an unset environment must yield None, not a guess.
    assert otlp_truststore.truststore_password() is None


def test_configure_extracts_pem_and_sets_certificate_env(tmp_path, monkeypatch):
    monkeypatch.setenv(otlp_truststore.DEFAULT_TRUSTSTORE_PASSWORD_ENV, "changeit")
    store = _write_truststore(tmp_path, "changeit")
    monkeypatch.setenv(otlp_truststore.ENV_TRUSTSTORE_PATH, store)

    pem_path = otlp_truststore.configure()

    assert pem_path is not None
    assert os.environ[otlp_truststore.ENV_CERTIFICATE] == pem_path
    body = open(pem_path, "rb").read()
    assert b"BEGIN CERTIFICATE" in body
    # Parses back as the anchor we put in.
    assert "Test-Root-CA" in x509.load_pem_x509_certificate(body).subject.rfc4514_string()


def test_configure_uses_secret_password(tmp_path, monkeypatch):
    store = _write_truststore(tmp_path, "s3cret")
    monkeypatch.setenv(otlp_truststore.ENV_TRUSTSTORE_PATH, store)
    monkeypatch.setenv(otlp_truststore.DEFAULT_TRUSTSTORE_PASSWORD_ENV, "s3cret")

    assert otlp_truststore.configure() is not None


def test_wrong_password_is_fatal(tmp_path, monkeypatch):
    store = _write_truststore(tmp_path, "s3cret")
    monkeypatch.setenv(otlp_truststore.ENV_TRUSTSTORE_PATH, store)
    monkeypatch.setenv(otlp_truststore.DEFAULT_TRUSTSTORE_PASSWORD_ENV, "wrong")

    with pytest.raises(otlp_truststore.TruststoreError) as excinfo:
        otlp_truststore.configure()
    assert "could not open truststore" in str(excinfo.value)


def test_tampered_truststore_is_fatal(tmp_path, monkeypatch):
    monkeypatch.setenv(otlp_truststore.DEFAULT_TRUSTSTORE_PASSWORD_ENV, "changeit")
    store = _write_truststore(tmp_path, "changeit")
    raw = bytearray(open(store, "rb").read())
    raw[len(raw) // 2] ^= 0xFF          # flip one byte; the PKCS12 MAC covers it
    open(store, "wb").write(bytes(raw))
    monkeypatch.setenv(otlp_truststore.ENV_TRUSTSTORE_PATH, store)

    with pytest.raises(otlp_truststore.TruststoreError):
        otlp_truststore.configure()


def test_missing_truststore_at_the_override_path_is_fatal(tmp_path, monkeypatch):
    # No more silent no-op: resolving to a path that does not exist is always
    # an error now, not a quiet "nothing to do".
    monkeypatch.setenv(
        otlp_truststore.ENV_TRUSTSTORE_PATH, str(tmp_path / "absent.p12")
    )
    with pytest.raises(otlp_truststore.TruststoreError) as excinfo:
        otlp_truststore.configure()
    assert "truststore not found" in str(excinfo.value)
    assert otlp_truststore.ENV_CERTIFICATE not in os.environ


def test_missing_truststore_at_the_default_path_is_fatal():
    # No override, and DEFAULT_TRUSTSTORE_PATH does not exist on a test
    # machine - this is the exact backward-compatible default-path behaviour,
    # exercised without monkeypatching anything.
    with pytest.raises(otlp_truststore.TruststoreError) as excinfo:
        otlp_truststore.configure()
    assert otlp_truststore.DEFAULT_TRUSTSTORE_PATH in str(excinfo.value)


def test_configure_is_idempotent_across_the_three_signals(tmp_path, monkeypatch):
    monkeypatch.setenv(otlp_truststore.DEFAULT_TRUSTSTORE_PASSWORD_ENV, "changeit")
    store = _write_truststore(tmp_path, "changeit")
    monkeypatch.setenv(otlp_truststore.ENV_TRUSTSTORE_PATH, store)

    first = otlp_truststore.configure()
    assert otlp_truststore.configure() == first
    assert otlp_truststore.configure() == first


def test_truststore_overrides_preset_certificate_env(tmp_path, monkeypatch):
    monkeypatch.setenv(otlp_truststore.DEFAULT_TRUSTSTORE_PASSWORD_ENV, "changeit")
    store = _write_truststore(tmp_path, "changeit")
    monkeypatch.setenv(otlp_truststore.ENV_TRUSTSTORE_PATH, store)
    monkeypatch.setenv(otlp_truststore.ENV_CERTIFICATE, "/etc/secrets/truststore.jks/ca.crt")

    pem_path = otlp_truststore.configure()

    assert os.environ[otlp_truststore.ENV_CERTIFICATE] == pem_path
    assert pem_path != "/etc/secrets/truststore.jks/ca.crt"


def test_default_truststore_path_is_the_dpn_tls_key():
    # dpn-tls is already mounted at /etc/secrets/truststore.jks in every chart; this is
    # the exact key confirmed via kubectl describe on a running broker pod.
    assert otlp_truststore.DEFAULT_TRUSTSTORE_PATH == "/etc/secrets/truststore.jks"


def test_jks_named_file_with_pkcs12_content_is_read_by_content_not_extension(
    tmp_path, monkeypatch
):
    # dpn-tls's truststore is named truststore.jks but is genuinely PKCS12
    # inside (KAFKA_SSL_TRUSTSTORE_TYPE on the broker confirms it). The parser
    # must not care about the extension.
    store = _write_truststore(tmp_path, "changeit", name="truststore.jks")
    monkeypatch.setenv(otlp_truststore.ENV_TRUSTSTORE_PATH, store)
    monkeypatch.setenv(otlp_truststore.DEFAULT_TRUSTSTORE_PASSWORD_ENV, "changeit")

    assert otlp_truststore.configure() is not None


def test_env_path_overrides_the_default(tmp_path, monkeypatch):
    monkeypatch.setenv(otlp_truststore.DEFAULT_TRUSTSTORE_PASSWORD_ENV, "changeit")
    store = _write_truststore(tmp_path, "changeit", name="some-other-name.p12")
    monkeypatch.setenv(otlp_truststore.ENV_TRUSTSTORE_PATH, store)
    # DEFAULT_TRUSTSTORE_PATH is left alone and does not exist here, so a pass
    # proves the override was used rather than the default.
    assert otlp_truststore.configure() is not None
    assert os.path.isfile(os.environ[otlp_truststore.ENV_CERTIFICATE])


def test_env_path_pointing_at_nothing_is_fatal(tmp_path, monkeypatch):
    # Must NOT fall through to the defaults - a typo silently trusting a
    # different anchor is the failure this variable exists to remove.
    monkeypatch.setenv(otlp_truststore.ENV_TRUSTSTORE_PATH, str(tmp_path / "absent.p12"))
    with pytest.raises(otlp_truststore.TruststoreError) as excinfo:
        otlp_truststore.configure()
    assert otlp_truststore.ENV_TRUSTSTORE_PATH in str(excinfo.value)


def test_password_env_var_name_is_overridable(monkeypatch):
    # A cluster publishing the password under another name is a deployment
    # decision, not a code change.
    monkeypatch.setenv(otlp_truststore.ENV_TRUSTSTORE_PASSWORD_NAME, "MY_OWN_PW_VAR")
    monkeypatch.setenv("MY_OWN_PW_VAR", "from-elsewhere")
    assert otlp_truststore.truststore_password_env() == "MY_OWN_PW_VAR"
    assert otlp_truststore.truststore_password() == "from-elsewhere"


def test_password_env_name_defaults_to_truststore_password():
    assert otlp_truststore.truststore_password_env() == "TRUSTSTORE_PASSWORD"


def test_missing_password_is_fatal(tmp_path, monkeypatch):
    # Found a store but no password: must name the variable, not fail on the MAC.
    store = _write_truststore(tmp_path, "changeit")
    monkeypatch.setenv(otlp_truststore.ENV_TRUSTSTORE_PATH, store)
    with pytest.raises(otlp_truststore.TruststoreError) as excinfo:
        otlp_truststore.configure()
    assert "TRUSTSTORE_PASSWORD" in str(excinfo.value)
