# README

**Repository:** `dpn-data-pipelines`

**Description:** `Responsible for producing and consuming data products of an organisation. This repo contains a collection of data pipelines that perform different operations over the data.`

<!-- SPDX-License-Identifier: Apache-2.0 AND OGL-UK-3.0 -->

---

## Overview

This repository contributes to the development of **secure, scalable, and interoperable data-sharing infrastructure**. It supports DSI's mission to enable **trusted, federated, and decentralised** data-sharing across organisations.

This repository is one of several open-source components that underpin DSI's **Data Preparation Node (DPN)**—a framework designed to allow organisations to manage and exchange data securely while maintaining control over their own information. The DPN is actively deployed and tested across multiple sectors, ensuring its adaptability and alignment with real-world needs.

The DPN Data Pipeline ensures secure and governed data exchange by validating and transforming datasets before and after transmission. It applies schema assurance, security labelling, and controlled processing across producer and consumer stages. This ensures all shared data conforms to required schemas, security classifications, and governance standards, enabling reliable and compliant data sharing.

**DSI DPN Data Pipeline component is aimed to do the following:**

- Transfer the Data Product specific files/stream messages produced by an internal Data Source Stage location to a Data Store target location
- Handoff the produced files/stream messages to the Federator Secure Gateway server to read and pass them to other DPN consumers from different Organisations subscribed to it
- Ingest data product files/stream messages placed by the Federator Secure Gateway client in a data store stage location
- Prepare the Data Product files/stream messages received in a data store location and hand off to the data destination process in the Organisation

## Prerequisites

* Python 3.14
* [Docker](https://www.docker.com/)
* [Git](https://git-scm.com/)
* Kubernetes Cluster
* Blob Storage/S3 Bucket
* cp-kafka:7.5.3
* Apache Airflow:2.9.3 (Optional)

## Configuration & Installation

Detailed configuration and installation instructions for this repository are present in **[dpn-integration-playbook](https://github.com/energy-dsi/dpn-integration-playbook)**

This includes producer/consumer setup, CI/CD pipeline configuration and execution, and deployment validation. Refer to the guide matching your deployment target:

### AWS Deployment

Refer [aws-manual-beta](https://github.com/energy-dsi/dpn-integration-playbook/tree/main/Docs/03-dpn-application-deployment/aws-manual-beta) for AWS specific deployment 

**Note** AWS Manual deployment is an interim solution and GitHub Actions based deployment to replace the manual deployment in future release

### Azure Deployment

Refer [azure-ado-beta](https://github.com/energy-dsi/dpn-integration-playbook/tree/main/Docs/03-dpn-application-deployment/azure-ado-beta) for Azure specific deployment

## Kafka Authentication (SASL_SSL + Keycloak OAuth)

Every Kafka-connected producer/consumer stage in this repo (adaptor,
schema_mapper, the kafka-trigger control/status backend, and the Airflow
DAGs) authenticates through a single shared helper,
[`utils/kafka_security.py`](utils/kafka_security.py), which builds a
`confluent-kafka` client config from environment variables. Full details,
example `.env` blocks per mode, migration notes, and validation steps live
in [`.docs/kafka-auth-modes.md`](.docs/kafka-auth-modes.md) — this section
is a summary.

### Architecture overview

- **Bootstrap servers** are supplied by DevOps (the actual broker addresses
  are environment-specific and out of scope for this repo — set them via
  `bootstrapServer` / `targetBootstrapServer` per DAG or the Helm
  `bootstrapServer` value).
- Client authentication mode is controlled by two environment variables,
  read the same way by every producer/consumer/DAG:

  | Variable | Values | Default |
  |---|---|---|
  | `KAFKA_SECURITY_PROTOCOL` | `SASL_SSL` \| `SSL` \| `PLAINTEXT` | `SASL_SSL` |
  | `KAFKA_SASL_MECHANISM` | `OAUTHBEARER` (used only with `SASL_SSL`) | `OAUTHBEARER` |

- `SASL_SSL` + `OAUTHBEARER` (the default) is the only mode intended for
  real deployments. `SSL` (mTLS, no SASL) and `PLAINTEXT` (no auth, no
  encryption) exist purely as opt-in fallbacks for local development and
  interop testing against brokers that aren't yet OAuth-enabled.

### Keycloak OAuth flow

1. A confidential, service-account-enabled client (`dpn-kafka-client`) must
   be provisioned in the Keycloak realm before deployment — a confidential
   client with `serviceAccountsEnabled: true` and an audience mapper
   injecting `aud: kafka` into issued tokens. This is a one-time Keycloak
   realm setup step, independent of this repo's SSL/PLAINTEXT modes (which
   never need Keycloak reachable at all).
2. Kafka brokers validate incoming tokens against Keycloak's JWKS endpoint
   (`OAuthBearerValidatorCallbackHandler`), checking the `kafka` audience and
   the expected issuer.
3. Producers/consumers/Airflow authenticate via librdkafka's built-in OIDC
   client-credentials flow (`sasl.oauthbearer.method=oidc`) — `KAFKA_OAUTH_CLIENT_ID`
   + `KAFKA_OAUTH_CLIENT_SECRET` are exchanged directly for a token against
   `KAFKA_OAUTH_TOKEN_ENDPOINT_URL`; librdkafka fetches and refreshes tokens
   itself, no application-level token handling is required.
4. The CA trust used for both the Kafka broker TLS handshake and the OIDC
   token endpoint comes from the common password-protected truststore (see
   "Helm deployment" below) and must cover both the Kafka brokers' CA and
   Keycloak's CA, since the same trust store is reused for both.

### Producer / consumer setup

- Every stage builds its client config via
  `build_kafka_client_config(bootstrap_server)` — no security config is
  hardcoded in `main.py` files; see
  [`producer/topic/eqbd-pg-gas/adaptor/main.py`](producer/topic/eqbd-pg-gas/adaptor/main.py)
  and [`producer/topic/eqbd-pg-gas/schema_mapper/main.py`](producer/topic/eqbd-pg-gas/schema_mapper/main.py).
- The kafka-trigger control/status backend
  ([`utils/kafka_trigger.py`](utils/kafka_trigger.py)) and topic utilities
  ([`utils/topic_forwarder.py`](utils/topic_forwarder.py),
  [`utils/topic_utils.py`](utils/topic_utils.py)) use the same helper for
  their internal producers/consumers/admin client.
- Consumer-side Airflow DAGs
  ([`charts/airflow/dags/dpn_consumer_file.py`](charts/airflow/dags/dpn_consumer_file.py),
  [`dpn_consumer_topic.py`](charts/airflow/dags/dpn_consumer_topic.py)) read
  `targetBootstrapServer` and default to the SASL_SSL listener port (9094),
  not the plaintext port — update this if your DevOps-provided bootstrap
  string uses a different port for the secured listener.

### Helm deployment

Each pipeline stage chart (`producer/topic/*/adaptor/charts`,
`.../schema_mapper/charts`) exposes the same Kafka security values:

- `KAFKA_SECURITY_PROTOCOL`, `KAFKA_SASL_MECHANISM`, `KAFKA_OAUTH_CLIENT_ID`,
  `KAFKA_OAUTH_TOKEN_ENDPOINT_URL` are plain values in `values.yaml`.
- `KAFKA_OAUTH_CLIENT_SECRET` is **never** stored in `values.yaml`. It's
  injected via `secretKeyRef` from a pre-existing Kubernetes `Secret` (default
  name `kafka-secrets`, override via `kafkaOauthSecretName`) with key
  `KAFKA_OAUTH_CLIENT_SECRET` — provision that `Secret` alongside the chart
  install; the chart does not create it.
- CA trust does **not** come from a plain `KAFKA_SSL_CA_LOCATION` PEM value
  any more. The `dpn-tls` Secret (default name, override via
  `kafkaTlsSecretName`) is mounted at `/etc/secrets/truststore.jks` and carries a
  password-protected `truststore.jks` (genuinely PKCS12). The chart also
  injects that store's password as `TRUSTSTORE_PASSWORD` from the
  `tls-auth-secret` Secret (key `TRUSTSTORE_PASSWORD` — the universal DPN
  platform password, created once by dpn-tls-cd). At runtime,
  `utils/kafka_security.py` (via `utils/kafka_truststore.py`)
  opens the truststore with that password, extracts the CA cert(s), and
  points librdkafka's `ssl.ca.location`/`SSL_CERT_FILE` at the extracted PEM
  automatically — no `KAFKA_SSL_CA_LOCATION` value is set or needed in
  `values.yaml`.

### Required environment variables

| Variable | Required for | Notes |
|---|---|---|
| `KAFKA_SECURITY_PROTOCOL` | all modes | `SASL_SSL` (default), `SSL`, or `PLAINTEXT` |
| `KAFKA_SASL_MECHANISM` | SASL_SSL | `OAUTHBEARER` (default) or unset for PLAIN/SCRAM via `KAFKA_SASL_USERNAME`/`KAFKA_SASL_PASSWORD` |
| `KAFKA_OAUTH_CLIENT_ID` / `KAFKA_OAUTH_CLIENT_SECRET` | SASL_SSL + OAUTHBEARER | Keycloak service-account client credentials |
| `KAFKA_OAUTH_TOKEN_ENDPOINT_URL` | SASL_SSL + OAUTHBEARER | Keycloak realm's token endpoint |
| `KAFKA_OAUTH_SCOPE` | optional | OAuth scope, if the realm requires one |
| `TRUSTSTORE_PASSWORD` | SSL and SASL_SSL | Password for the common `truststore.jks` (Kafka CA + Keycloak CA for SASL_SSL), mounted from the `dpn-tls` Secret. `utils/kafka_security.py` extracts the CA to a PEM at runtime and sets `ssl.ca.location`/`SSL_CERT_FILE` itself — no `KAFKA_SSL_CA_LOCATION` value is needed. `KAFKA_SSL_CA_LOCATION` still works as a manual fallback (a plain PEM path) only when no truststore is mounted at all. |
| `SSL_CERT_FILE` | SASL_SSL + OAUTHBEARER | Set automatically by `utils/kafka_security.py` to the same extracted CA PEM. librdkafka's OIDC token-endpoint HTTP client ignores `ssl.ca.location` and reads this standard OpenSSL env var instead. Without it the OIDC token fetch fails closed with "self-signed certificate in certificate chain" even though CA trust is otherwise correct. |
| `KAFKA_SSL_CERTIFICATE_LOCATION` / `KAFKA_SSL_KEY_LOCATION` / `KAFKA_SSL_KEY_PASSWORD` | SSL (mTLS) only | Client certificate/key for mutual TLS |
| `KAFKA_SSL_ENDPOINT_IDENTIFICATION` | optional | Hostname verification algorithm (`https` recommended) |

### Troubleshooting

- **`PKIX path building failed`**: the CA cert(s) inside `truststore.jks`
  don't cover the certificate presented by the broker (or Keycloak, in
  OAUTHBEARER mode) — regenerate/merge the CA bundle that gets imported into
  the truststore.
- **`TruststoreError: could not open truststore ...`**: `TRUSTSTORE_PASSWORD`
  doesn't match the password the store was built with, or the file has been
  tampered with (a PKCS12 MAC covers the whole file) — this is fatal by
  design, not something to silently fall back from.
- **OIDC token fetch fails with `self-signed certificate in certificate
  chain`**: this is set automatically now (`SSL_CERT_FILE` is derived from
  the same extracted truststore CA), so this normally means the truststore
  itself is missing the Keycloak CA, not a missing env var.
- **Token endpoint unreachable / OIDC errors**: confirm
  `KAFKA_OAUTH_TOKEN_ENDPOINT_URL` is reachable from the pod/container network
  and that the `dpn-kafka-client` service-account client exists in the
  target Keycloak realm.
- **Consumer never receives triggers after a fresh/auto-creating broker**:
  confirm the control/status topics exist — `KafkaTriggerBackend` now calls
  `KafkaTopicManager.ensure_exists(...)` on startup specifically to avoid a
  race where a brand-new broker throws `UNKNOWN_TOPIC_OR_PARTITION` before
  auto-creation catches up.
- **Switching security modes has no effect**: broker listener protocol maps
  and client configs are read once at process start — restart/redeploy
  every Kafka-connected service after changing `KAFKA_SECURITY_PROTOCOL` or
  `KAFKA_SASL_MECHANISM`, not just the brokers.

### Security considerations

- No secret, password, or client credential is hardcoded anywhere in this
  repo — everything is read from environment variables (Python) or a
  Kubernetes `Secret` (Helm deployments).
- `PLAINTEXT` mode sends unauthenticated, unencrypted traffic — restrict it
  to isolated local/dev networks and never expose it beyond a developer's
  own machine or an isolated CI network.
- Rotate `KAFKA_OAUTH_CLIENT_SECRET` regularly and scope the Keycloak client
  to the minimum realm roles/audience it needs (`kafka` audience only).

## Features

The Data Pipeline enables secure, governed data exchange between producing and consuming organisations, supporting both file-based and Kafka topic-based data products. Key features include:

### Producer Pipeline
- **Adaptor**: Reads Data Product files/stream messages from a given object storage location or from a Kafka source topic per data product and keeps it to another object storage or kafka topic read by Schema Mapper process.
- **Schema Mapper**: For file-based products, stores the file to a data store target location and publishes metadata (filename and location) to a Kafka topic. For Kafka topic-based products, moves the validated data to a target topic with metadata carried as a header.
- Handoff of validated data products to the **Federator** Secure Gateway server, which reads and passes them to DPN consumers subscribed from other organisations.

### Consumer Pipeline
- **Extractor**: Moves data product files received by the Federator client from the source data store location to the destination data store container location for the Consumer Mapper, publishing metadata to a Kafka topic. For topic-based products, moves messages from the source Kafka topic to a destination Kafka topic with metadata carried as a header.
- **Schema Mapper**: For file-based products, stores the file to a data store target location and publishes metadata to a Kafka topic. For Kafka topic-based products, moves the validated data to a target topic with metadata as a header.The target location is used for the Organisation data destination to consume the files/messages

### Orchestration
- **Airflow** (optional): Used for orchestration of DPN Data Pipelines, providing a visual representation of pipeline status (Running, Failed, Success). The pipelines can also be configured to run stand-alone, without Airflow.

## Public Funding Acknowledgment

This repository has been developed with public funding as part of the Data Sharing Infrastructure (DSI), a UK Government initiative.

## License

This repository contains both source code and documentation, covered by different licenses:
- **Code:** Licensed under the terms in [LICENSE.md](./LICENSE.md).
- **Documentation:** Licensed under the Open Government Licence v3.0 — see [OGL_LICENSE.md](./OGL_LICENSE.md).

By contributing to this repository, you agree that your contributions will be licensed under these terms.

## Security and Responsible Disclosure

We take security seriously. If you believe you have found a security vulnerability in this repository, please follow our responsible disclosure process outlined in [SECURITY.md](./SECURITY.md).

## Contributing

We welcome contributions that align with the Programme's objectives.

## Acknowledgements

This repository has benefited from collaboration with various organisations.

## Support and Contact

For questions, feedback, or support requests:

- Contact DSI team via email to [dsi@neso.energy](mailto:dsi@neso.energy)

## Maintained by the National Energy System Operator (NESO)

Copyright 2026 NESO.  This work is licensed under the Open Government Licence 3.0 (OGL). This work has been developed by NESO using content licensed by the Department for Business and Trade (UK) under the OGL.   
 
Licensed under the Open Government Licence v3.0.

For full licensing terms, [OGL_LICENSE.md](./OGL_LICENSE.md)