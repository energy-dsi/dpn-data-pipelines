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
# | 1.0.0   | Initial version                                           | DSI Team      | 2026-09-14  |
# +---------+----------------------------------------------------------+---------------+-------------+
"""
Environment-variable construction for one data product's adaptor/schema_mapper
containers — shared by both runtime backends (podman_ops.py locally,
k8s_ops.py on AKS/EKS/GKE). Kept pure/backend-agnostic on purpose: this is
the one place that encodes "what env vars does the generic image need",
independent of *how* the container gets launched.

Cloud/Kafka connection details default to this repo's local Podman demo
(see service/compose.yaml) but are overridable via environment
variables of the same name, so the same functions produce correct AKS values
when BOOTSTRAP_SERVER/AWS_ENDPOINT_URL/etc. are set to real cluster
endpoints — see charts/*/values-*.yaml.

Kafka SASL/OAuth fields (DPN_KAFKA_SASL_MECHANISM/DPN_KAFKA_OAUTH_*) default
to empty/PLAINTEXT for the same reason: they must be set explicitly (see
service/charts/values.yaml) for any deployment that isn't the local
demo, mirroring the static blueprint charts under blueprints/producer/*/charts
and blueprints/consumer/*/charts, which spell out KAFKA_SECURITY_PROTOCOL and
KAFKA_SASL_MECHANISM together rather than relying on
utils/kafka_security.py's own OAUTHBEARER default.
"""
from __future__ import annotations

import base64
import os

from service.models import DataProductDefinition

BOOTSTRAP_SERVER = os.getenv("DPN_BOOTSTRAP_SERVER", "dpn-kafka:9092")
KAFKA_SECURITY_PROTOCOL = os.getenv("DPN_KAFKA_SECURITY_PROTOCOL", "PLAINTEXT")
# Left empty by default (matching the PLAINTEXT local-demo default above) —
# utils/kafka_security.py itself defaults an unset mechanism to OAUTHBEARER,
# so a real SASL_SSL deployment must set DPN_KAFKA_SASL_MECHANISM (and the
# OAuth fields below) explicitly rather than relying on that fallback, the
# same way the static Helm charts (e.g. producer/*/charts/values.yaml) spell
# both KAFKA_SECURITY_PROTOCOL and KAFKA_SASL_MECHANISM out rather than
# leaving the mechanism to default.
KAFKA_SASL_MECHANISM = os.getenv("DPN_KAFKA_SASL_MECHANISM", "")
KAFKA_OAUTH_CLIENT_ID = os.getenv("DPN_KAFKA_OAUTH_CLIENT_ID", "")
KAFKA_OAUTH_CLIENT_SECRET = os.getenv("DPN_KAFKA_OAUTH_CLIENT_SECRET", "")
KAFKA_OAUTH_TOKEN_ENDPOINT_URL = os.getenv("DPN_KAFKA_OAUTH_TOKEN_ENDPOINT_URL", "")
# Name of the k8s Secret holding the Kafka/Keycloak CA bundle (same secret
# shape as the static charts' `dpn-tls`, see e.g.
# blueprints/producer/file/adaptor/charts/values.yaml's kafkaTlsSecretName).
# Only meaningful on the kubernetes backend — k8s_ops.py mounts it at
# /etc/kafka/secrets when this is set; podman_ops.py has no equivalent
# mount, so leave this unset for the local Podman demo (compose.yaml runs
# PLAINTEXT there anyway, which needs no CA).
KAFKA_TLS_SECRET_NAME = os.getenv("DPN_KAFKA_TLS_SECRET_NAME", "")
KAFKA_SSL_CA_LOCATION = "/etc/kafka/secrets/ca.crt" if KAFKA_TLS_SECRET_NAME else ""
# Passwords for the same dpn-tls Secret's truststore.jks/keystore.jks, mounted
# by k8s_ops.py at the exact default paths utils/kafka_security.py and
# utils/kafka_truststore.py already auto-discover — same tls-auth-secret
# Secret the static blueprint charts read these from directly via
# secretKeyRef, just resolved here once and forwarded like
# KAFKA_OAUTH_CLIENT_SECRET already is. Empty when unset — the truststore/
# keystore auto-discovery is soft, so a missing password only matters if the
# corresponding file is actually mounted.
TRUSTSTORE_PASSWORD = os.getenv("DPN_TRUSTSTORE_PASSWORD", "")
KEYSTORE_PASSWORD = os.getenv("DPN_KEYSTORE_PASSWORD", "")
OTEL_EXPORTER_OTLP_ENDPOINT = os.getenv(
    "DPN_OTEL_EXPORTER_OTLP_ENDPOINT",
    "dpn-otel-collector.ns-dpn-health-01.svc.cluster.local:4317",
)
OTEL_EXPORTER_OTLP_INSECURE = os.getenv("DPN_OTEL_EXPORTER_OTLP_INSECURE", "true")
# Name of the k8s Secret holding the OTel collector's CA bundle (key
# ca.crt) — same shape/rationale as KAFKA_TLS_SECRET_NAME above, but for
# the health-observability collector's TLS listener rather than
# Kafka/Keycloak. See charts/tls-ca-sync/health-manifests.yaml, which
# mirrors the dpn-tls sync for this secret (dpn-health-ca). Only
# meaningful when OTEL_EXPORTER_OTLP_INSECURE is "false" — k8s_ops.py
# mounts it at /etc/otel/certs when set, and dpn_observability_sdk's
# exporters read OTEL_EXPORTER_OTLP_CERTIFICATE from that mount to verify
# the collector's cert.
OTEL_TLS_SECRET_NAME = os.getenv("DPN_OTEL_TLS_SECRET_NAME", "")
OTEL_EXPORTER_OTLP_CERTIFICATE = "/etc/otel/certs/ca.crt" if OTEL_TLS_SECRET_NAME else ""
# dpn_observability_sdk.otlp_auth falls back to the KAFKA_OAUTH_CLIENT_ID/
# KAFKA_OAUTH_CLIENT_SECRET/KAFKA_OAUTH_TOKEN_ENDPOINT_URL env vars already
# set below by common_env() when the OTEL_OAUTH_* equivalents are unset, so
# no separate credential wiring is needed here — only the opt-out switch for
# collectors with no auth extension (dev/test). Defaults to disabled for the
# same reason KAFKA_SASL_MECHANISM above defaults empty: KAFKA_OAUTH_CLIENT_ID/
# _CLIENT_SECRET/_TOKEN_ENDPOINT_URL default empty too, and otel_tracer.py/
# otel_metrics.py/otel_logger.py call build_*_exporter() unguarded at
# startup — enabling OAuth with no credentials configured would crash every
# new data product's container the instant it starts, not just degrade
# telemetry. A real deployment must opt in explicitly (see
# service/charts/values.yaml's DPN_OTEL_OAUTH_DISABLED: "false").
OTEL_OAUTH_DISABLED = os.getenv("DPN_OTEL_OAUTH_DISABLED", "true")
AWS_ENDPOINT_URL = os.getenv("DPN_AWS_ENDPOINT_URL", "http://dpn-minio:9000")
AWS_ACCESS_KEY_ID = os.getenv("DPN_AWS_ACCESS_KEY_ID", "minioadmin")
AWS_SECRET_ACCESS_KEY = os.getenv("DPN_AWS_SECRET_ACCESS_KEY", "minioadmin")
AWS_REGION = os.getenv("DPN_AWS_REGION", "us-east-1")

# ---------------------------------------------------------------------------
# Platform-level config: organisation + cloud storage account.
#
# One organisation and one cloud storage account per self-service
# deployment — set up ONCE when this app is deployed (Helm values ->
# secretKeyRef, sourced from Key Vault on Azure, Secrets Manager on AWS, or
# Secret Manager on GCP — the exact same "inject a Secret into this pod"
# pattern this repo already uses for DPN_KAFKA_OAUTH_CLIENT_SECRET), never
# typed per data product. Every dynamically-created adaptor/schema_mapper
# container inherits these automatically via common_env()/_platform_cloud_env()
# below — a business user never sees or supplies a connection string.
# ---------------------------------------------------------------------------
PLATFORM_ORG_NAME = os.getenv("DPN_ORG_NAME", "dpn")
PLATFORM_CLOUD_PROVIDER = os.getenv("DPN_CLOUD_PROVIDER", "aws")  # aws | azure | gcp

# dsm-management-node (the downstream product catalog this org's products are
# ultimately registered into) stores this same organisation name in
# mn.producer.name, VARCHAR(255) as of its
# V20260617120000__alter_column_lengths.sql migration. Failing fast here at
# startup, once, is cheaper than failing later at registration time with
# every product this deployment ever creates.
if len(PLATFORM_ORG_NAME) > 255:
    raise ValueError(
        f"DPN_ORG_NAME is {len(PLATFORM_ORG_NAME)} characters — dsm-management-node's "
        "mn.producer.name column is VARCHAR(255), so a longer name would fail there "
        "for every product this deployment ever creates."
    )

# Azure: source and target are two distinct storage accounts in the real
# deployment (source = this org's own internal storage, target = the
# shared location the Federator Secure Gateway reads from) — mirroring
# producer-file-dp-secret's own SRC_CONNECTION_STRING/TARGET_CONNECTION_STRING
# keys (see service/charts/templates/deployment.yaml's
# platformStorageSecretName). No mapper connection string: the schema-mapper
# stage was absorbed into the adaptor, so nothing reads one any more.
PLATFORM_AZURE_SRC_CONNECTION_STRING = os.getenv("DPN_AZURE_SRC_CONNECTION_STRING", "")
PLATFORM_AZURE_TARGET_CONNECTION_STRING = os.getenv("DPN_AZURE_TARGET_CONNECTION_STRING", "")

# GCP: a service-account key JSON, base64-encoded the same way the other
# cloud credentials already are so main.py's decode step is uniform across
# all three providers.
PLATFORM_GCP_SERVICE_ACCOUNT_KEY = os.getenv("DPN_GCP_SERVICE_ACCOUNT_KEY", "")
PLATFORM_GCP_PROJECT_ID = os.getenv("DPN_GCP_PROJECT_ID", "")


def _b64(value: str) -> str:
    return base64.b64encode(value.encode("utf-8")).decode("ascii")


def common_env(dp: DataProductDefinition) -> dict[str, str]:
    return {
        "bootstrapServer": BOOTSTRAP_SERVER,
        "KAFKA_SECURITY_PROTOCOL": KAFKA_SECURITY_PROTOCOL,
        "KAFKA_SASL_MECHANISM": KAFKA_SASL_MECHANISM,
        "KAFKA_OAUTH_CLIENT_ID": KAFKA_OAUTH_CLIENT_ID,
        "KAFKA_OAUTH_CLIENT_SECRET": KAFKA_OAUTH_CLIENT_SECRET,
        "KAFKA_OAUTH_TOKEN_ENDPOINT_URL": KAFKA_OAUTH_TOKEN_ENDPOINT_URL,
        "KAFKA_SSL_CA_LOCATION": KAFKA_SSL_CA_LOCATION,
        "TRUSTSTORE_PASSWORD": TRUSTSTORE_PASSWORD,
        "KEYSTORE_PASSWORD": KEYSTORE_PASSWORD,
        "OTEL_EXPORTER_OTLP_ENDPOINT": OTEL_EXPORTER_OTLP_ENDPOINT,
        "OTEL_EXPORTER_OTLP_INSECURE": OTEL_EXPORTER_OTLP_INSECURE,
        "OTEL_EXPORTER_OTLP_CERTIFICATE": OTEL_EXPORTER_OTLP_CERTIFICATE,
        "OTEL_OAUTH_DISABLED": OTEL_OAUTH_DISABLED,
        "PRODUCT_NAME": dp.product_type,
        "orgName": dp.org_slug,
        "SCHEDULER_BACKEND": "interval",
        "EXECUTION_MODE": dp.execution_mode,
        # FR-D-016m-23: a heartbeat every 1 minute — overrides
        # dpn_observability_sdk.heartbeat.HeartbeatLogger's own 15-minute
        # default (HEARTBEAT_INTERVAL_SECONDS).
        "HEARTBEAT_INTERVAL_SECONDS": "60",
    }


def _platform_cloud_env() -> dict[str, str]:
    """
    cloudProviderType plus whichever credential set it needs — always the
    ONE platform-level cloud account (PLATFORM_CLOUD_PROVIDER), identical
    for every data product. main.py's validate_cloud_config() requires
    these non-empty and base64 for the selected provider, so a missing
    platform Secret surfaces there as a clear config error at container
    start rather than a silent fallback.
    """
    if PLATFORM_CLOUD_PROVIDER == "azure":
        # No _b64() here, unlike the aws/gcp branches below: producer-file-dp-
        # secret's SRC_CONNECTION_STRING/TARGET_CONNECTION_STRING are already
        # base64 at rest (the same convention blueprints/producer/file/adaptor/
        # main.py's own base64.b64decode() on these exact env var names has
        # always assumed for the static producer chart's identical secretKeyRef)
        # — re-encoding here would double-encode it for every dynamically
        # created producer pod.
        return {
            "cloudProviderType": "azure",
            "srcConnectionString": PLATFORM_AZURE_SRC_CONNECTION_STRING,
            "targetConnectionString": PLATFORM_AZURE_TARGET_CONNECTION_STRING,
        }
    if PLATFORM_CLOUD_PROVIDER == "gcp":
        return {
            "cloudProviderType": "gcp",
            "GCP_SERVICE_ACCOUNT_KEY": _b64(PLATFORM_GCP_SERVICE_ACCOUNT_KEY),
            "GCP_PROJECT_ID": PLATFORM_GCP_PROJECT_ID,
        }
    return {
        "cloudProviderType": "aws",
        "AWS_ACCESS_KEY_ID": _b64(AWS_ACCESS_KEY_ID),
        "AWS_SECRET_ACCESS_KEY": _b64(AWS_SECRET_ACCESS_KEY),
        "AWS_REGION": AWS_REGION,
        "AWS_ENDPOINT_URL": AWS_ENDPOINT_URL,
    }


def file_adaptor_env(dp: DataProductDefinition) -> dict[str, str]:
    """
    The file pathway's only container: pulls from srcContainerName, applies
    the (now-absorbed) schema mapper's rename, and writes straight to
    targetContainerName itself — see blueprints/producer/file/adaptor/main.py.
    No mapperContainerName / mapperTopicName: there is no intermediate
    staging hop any more, so nothing here needs them.
    """
    return {
        **common_env(dp),
        **_platform_cloud_env(),
        "srcContainerName": dp.source_container_name,
        "targetContainerName": dp.target_container_name,
        "targetTopicName": dp.target_topic_name,
        "scheduleInterval": str(dp.schedule_seconds),
    }


def topic_adaptor_env(dp: DataProductDefinition) -> dict[str, str]:
    """
    The topic pathway's only container: consumes srcTopicName, applies the
    (now-absorbed) schema mapper's header enrichment, and produces to
    targetTopicName — a real user-provided value from the review screen,
    not silently recomputed (see
    blueprints/producer/topic/adaptor/main.py's v3.1.0 changelog entry).
    No mapperTopicName / mapperGroupId: there is no intermediate RAW-topic
    hop any more. productType isn't in common_env() (only this pathway
    needs it, to stamp it onto every message and as the target topic's
    fallback-formula input if targetTopicName is ever left blank).
    """
    return {
        **common_env(dp),
        "srcTopicName": dp.src_topic_name,
        "srcGroupId": dp.src_group_id,
        "targetTopicName": dp.target_topic_name,
        "productType": dp.product_type,
        # The topic pathway's own "session window" knob (see
        # blueprints/producer/topic/adaptor/main.py) — the IntervalBackend's own
        # tick interval barely matters here since run_window() already blocks
        # for this long before returning, so the schedule the user picked is
        # applied here rather than to scheduleInterval.
        "TOPIC_TASK_TIMEOUT_SECS": str(dp.schedule_seconds),
    }


def adaptor_env(dp: DataProductDefinition) -> dict[str, str]:
    return file_adaptor_env(dp) if dp.pipeline_type == "file" else topic_adaptor_env(dp)
