# Kafka client authentication modes

Every Python producer/consumer built on
[utils/kafka_security.py](../utils/kafka_security.py) shares the same two
switches, read from the environment:

| Variable                   | Values                          | Default        |
|----------------------------|----------------------------------|----------------|
| `KAFKA_SECURITY_PROTOCOL`  | `SASL_SSL` \| `SSL` \| `PLAINTEXT` | `SASL_SSL`     |
| `KAFKA_SASL_MECHANISM`     | `OAUTHBEARER` (only used with `SASL_SSL`) | `OAUTHBEARER`  |

Leaving both unset is equivalent to setting `KAFKA_SECURITY_PROTOCOL=SASL_SSL`
and `KAFKA_SASL_MECHANISM=OAUTHBEARER` — this is the recommended mode and the
one CI/production should run. `SSL` and `PLAINTEXT` exist as explicit opt-ins
for local development and interop testing, not as equally-supported
production modes.

No secret, password, token, or client ID is hardcoded in the Helm
`values.yaml` or Python source — everything sensitive is read from
`os.getenv(...)` (Python) or injected via a Kubernetes `Secret`/`secretKeyRef`
(Helm — see each chart's `templates/secret.yaml`).

## How the switch works

`KAFKA_SECURITY_PROTOCOL` / `KAFKA_SASL_MECHANISM` pass straight through to
`utils/kafka_security.py:build_kafka_client_config()`, which builds the
matching `confluent-kafka` config dict (PLAINTEXT / SSL / SASL_SSL, with
librdkafka's built-in OIDC client-credentials flow for OAUTHBEARER — no
custom token-fetch callback needed). Every producer/consumer/adaptor/
schema_mapper in this repo builds its Kafka client config through this one
function, so switching modes is a single, consistent change across the
whole pipeline.

## Example configs

Set the relevant block as environment variables (Helm `values.yaml` +
Secret, or however your deployment method injects env vars). Only one mode
should be active at a time — don't mix blocks.

### 1. SASL_SSL + OAUTHBEARER (default, recommended)

```dotenv
KAFKA_SECURITY_PROTOCOL=SASL_SSL
KAFKA_SASL_MECHANISM=OAUTHBEARER

# Keycloak client-credentials grant
KAFKA_OAUTH_CLIENT_ID=dpn-kafka-client
KAFKA_OAUTH_CLIENT_SECRET=<generate a strong secret; rotate regularly>
KAFKA_OAUTH_SCOPE=

# TLS trust/identity used underneath the SASL layer
KAFKA_SSL_CA_LOCATION=/certs/ca.crt
KAFKA_SSL_ENDPOINT_IDENTIFICATION=https

# Required for the OIDC token fetch itself (see note below) — librdkafka
# ignores ssl.ca.location for its OIDC HTTP client and instead reads this
# standard OpenSSL env var.
SSL_CERT_FILE=/certs/ca.crt
```

In Helm, the CA trust described here is provisioned via the `dpn-tls` Secret
(default name, override via `kafkaTlsSecretName`), mounted at
`/etc/secrets/truststore.jks` in every producer/consumer chart. `KAFKA_SSL_CA_LOCATION`
is not set by any chart any more (see below) — set it yourself only as a
manual fallback for an image/environment with no truststore mounted at all.

### CA trust: the common password-protected truststore, not a plain PEM

Every producer/consumer chart, and the Airflow worker chart, mounts the
`dpn-tls` Secret at `/etc/secrets/truststore.jks`, which carries a `truststore.jks` key
(genuinely PKCS12 despite the extension), and injects that store's password
as `TRUSTSTORE_PASSWORD` (from the `tls-auth-secret` Secret, key
`TRUSTSTORE_PASSWORD` — the universal DPN platform password, created once by
dpn-tls-cd) — originally for OTLP
(`dpn_observability_sdk.otlp_truststore`), now for the Kafka client too via
`utils/kafka_truststore.py`:

- [`utils/kafka_security.py`](../utils/kafka_security.py) (used by every
  producer/consumer stage) and
  [`charts/airflow/dags/utils/kafka_security.py`](../charts/airflow/dags/utils/kafka_security.py)
  (the standalone copy baked into the Airflow worker image, alongside its own
  copy of `kafka_truststore.py`) both call
  `kafka_truststore.resolve_ca_pem_path()` from `build_kafka_client_config()`
  unconditionally — independent of whether `KAFKA_SSL_CA_LOCATION` is set at
  all. If `/etc/secrets/truststore.jks` (or
  `KAFKA_SSL_TRUSTSTORE_LOCATION`, if set) exists, it is opened with
  `TRUSTSTORE_PASSWORD` and the CA cert(s) inside are extracted to a private
  (`0600`) temp PEM, which is what `ssl.ca.location` and `SSL_CERT_FILE`
  actually get pointed at.
- If no truststore file exists at all (nothing mounted, and no explicit
  `KAFKA_SSL_TRUSTSTORE_LOCATION` override), `resolve_ca_pem_path()` returns
  `None` and `KAFKA_SSL_CA_LOCATION`'s plain PEM (if set) is used instead —
  this is the fallback for PLAINTEXT/local dev, and for the Airflow chart
  until it's redeployed with the `TRUSTSTORE_PASSWORD` env var wired in
  (`airflow-worker.yaml`).
- If the truststore file exists (default path or override) but the password
  is missing or wrong, or the file has been tampered with, this is **fatal**
  — it does not silently fall back to the plain PEM. A truststore that was
  clearly provisioned but can't be opened is a misconfiguration to fail
  loudly on.

The Airflow copy of `kafka_truststore.py` is kept in sync manually with
[`utils/kafka_truststore.py`](../utils/kafka_truststore.py) (it's baked into
the image separately, see `docker/airflow/Dockerfile.airflow`) — if you change
one, change the other.

librdkafka itself still only ever reads a plain PEM via `ssl.ca.location` —
it has no native concept of a password-protected CA store — so the
truststore's password buys integrity/tamper-evidence and a single
password-protected artefact to rotate, not confidentiality of the CA cert
itself (a CA cert is public by design).

librdkafka's OIDC token-endpoint HTTP client ignores `ssl.ca.location` and
instead relies on OpenSSL's default trust-store resolution — which differs
per image:

- **Alpine-based images** (the real per-product producer/consumer
  Dockerfiles, e.g. `producer/topic/eqbd-pg-gas/adaptor/Dockerfile`): musl
  OpenSSL's default trust store is `/etc/ssl/certs/ca-certificates.crt`,
  not the RHEL-style `/etc/pki/tls/certs` path. Set `SSL_CERT_FILE` to
  override it directly — this is the mechanism verified working
  end-to-end against a real Keycloak-issued certificate chain.
  Without it, the OIDC HTTP client has no trusted roots at all and every
  token fetch fails with `self-signed certificate in certificate chain`,
  even though `ssl.ca.location`/`KAFKA_SSL_CA_LOCATION` is set correctly.
- **Debian/glibc-based images** (e.g. an `apache/airflow`-based image): also
  do not use `/etc/pki/tls/certs` by convention; the same `SSL_CERT_FILE`
  override applies.

### 2. SSL (mutual TLS, no SASL)

```dotenv
KAFKA_SECURITY_PROTOCOL=SSL
KAFKA_SASL_MECHANISM=

# Python client identity (client cert + key) for mTLS
KAFKA_SSL_CA_LOCATION=/certs/ca.crt
KAFKA_SSL_CERTIFICATE_LOCATION=/certs/client.crt
KAFKA_SSL_KEY_LOCATION=/certs/client.key
KAFKA_SSL_KEY_PASSWORD=
KAFKA_SSL_ENDPOINT_IDENTIFICATION=https
```

The broker side must independently be configured to require client
certificates (mTLS) for this mode to provide real authentication — a broker
accepting plain server-side TLS with no client-cert requirement means any
client can connect without presenting one.

### 3. PLAINTEXT (local/dev/test only — never production)

```dotenv
KAFKA_SECURITY_PROTOCOL=PLAINTEXT
KAFKA_SASL_MECHANISM=
```

No TLS material or OAuth client is needed in this mode; all `KAFKA_SSL_*`
and `KAFKA_OAUTH_*` variables are simply unused. Restrict this mode to
throwaway local clusters — see security notes below.

## Migration notes

- **From a previous hardcoded-OAUTHBEARER setup**: no action required.
  Adding `KAFKA_SECURITY_PROTOCOL=SASL_SSL` / `KAFKA_SASL_MECHANISM=OAUTHBEARER`
  (or leaving them unset) reproduces the same behavior, since these are the
  defaults `build_kafka_client_config()` falls back to.
- Existing consumer groups/offsets are unaffected by an auth-mode switch —
  only the client's security config changes, not topic data.
- **After changing the security mode, redeploy every Kafka-connected
  service** — brokers, producers, consumers, and any Airflow
  scheduler/worker/triggerer pods all need to pick up the new env vars;
  switching `KAFKA_SECURITY_PROTOCOL` on a subset of services while others
  still run the old mode will cause connection failures.

## Validation steps

1. **Python clients**: confirm `utils/kafka_security.py:build_kafka_client_config()`
   produces the expected `security.protocol`/`sasl.mechanism` for your
   configured mode — e.g.
   ```bash
   python -c "from utils.kafka_security import build_kafka_client_config as b; print(b('<bootstrap-server>:9094'))"
   ```
2. **Broker reachability + auth**: run a pipeline stage (adaptor or
   schema_mapper) against the real bootstrap servers and confirm the logs
   show a successful Kafka produce/consume (`Kafka message delivered` /
   `Kafka message received`) with no `_TRANSPORT` or OIDC/TLS errors.
3. **Keycloak token acquisition**: if OIDC token fetch fails, check for
   `self-signed certificate in certificate chain` in the logs — see the
   `SSL_CERT_FILE` note above.

## Security best practices

- **Never hardcode or commit secrets.** `KAFKA_OAUTH_CLIENT_SECRET` must
  come from a Kubernetes `Secret` (via `secretKeyRef`, see each chart's
  `templates/secret.yaml`) or an equivalent secrets manager — never a
  literal value in `values.yaml` or source code.
- **Rotate `KAFKA_OAUTH_CLIENT_SECRET` regularly** and treat it like any
  other service-account credential.
- **Restrict `PLAINTEXT` to isolated local/dev networks.** It sends
  credentials-free traffic in the clear with no authentication — anyone who
  can reach the broker port can produce/consume any topic. Never expose a
  PLAINTEXT listener outside a developer's own machine or an isolated CI
  network.
- **Prefer mutual TLS (client certificates) for pure SSL mode** so the
  broker enforces client identity instead of relying on network-level
  access control alone.
- **Scope the OAuth client** (`KAFKA_OAUTH_CLIENT_ID`) to the minimum
  Keycloak realm roles/audience it needs (`kafka` audience only) rather than
  reusing a broader service-account client.
- **Keep CA bundles current**: the CA bundle referenced by
  `KAFKA_SSL_CA_LOCATION`/`SSL_CERT_FILE` must contain every CA in the trust
  chain (Kafka's and Keycloak's, if they differ), or TLS verification will
  fail closed (the safe failure mode — don't work around it by disabling
  verification).
- **Prefer a Kubernetes `Secret`/CSI driver or a dedicated secrets manager**
  over plain environment files for any shared or non-local deployment.
