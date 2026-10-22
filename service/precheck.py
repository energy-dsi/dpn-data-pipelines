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
Precheck a proposed Kafka topic / storage container name against the
platform's real Kafka broker / storage account before a data product is
ever created — see ui/app.py's preview_data_product()/confirm_data_product().

Uses the exact same platform-level credentials (service/env_builder.py)
every dynamically-launched adaptor container is given — there is one
platform Kafka cluster and one platform cloud account for the whole
self-service deployment, so there is nothing per-request to configure here.

Both checks fail soft on a connection/auth error: logged, and reported as
"doesn't exist" (never verified) rather than blocking product creation on
a transient Kafka/storage outage — a definitive "yes, it already exists"
is the only thing that ever blocks (see the two call sites in ui/app.py).
"""
from __future__ import annotations

import base64
import json
import logging
import os

from service import env_builder
from utils.data_transection import DataTransection
from utils.topic_utils import KafkaTopicManager

_logger = logging.getLogger(__name__)

_topic_manager: KafkaTopicManager | None = None
_data_trans: DataTransection | None = None


def _bridge_kafka_env() -> None:
    """
    utils.kafka_security.build_kafka_client_config() (used internally by
    KafkaTopicManager) reads plain KAFKA_*/TRUSTSTORE_PASSWORD env var
    names — the exact names env_builder.common_env() forwards into every
    dynamically-launched adaptor/extractor pod's own container. This
    self-service pod is not one of those pods: it only carries the
    DPN_-prefixed platform config env_builder.py already parsed into the
    module constants used below. Without this bridge,
    build_kafka_client_config() sees none of the real OAuth/truststore
    config, AdminClient construction fails exactly like a misconfigured
    adaptor would (`sasl.oauthbearer.client.secret is mandatory`), and
    kafka_topic_exists()'s fail-soft except swallows it — silently
    reporting every topic as "doesn't exist" no matter what's really on
    the broker, defeating the whole point of the precheck.
    """
    os.environ.setdefault("KAFKA_SECURITY_PROTOCOL", env_builder.KAFKA_SECURITY_PROTOCOL)
    os.environ.setdefault("KAFKA_SASL_MECHANISM", env_builder.KAFKA_SASL_MECHANISM)
    os.environ.setdefault("KAFKA_OAUTH_CLIENT_ID", env_builder.KAFKA_OAUTH_CLIENT_ID)
    os.environ.setdefault("KAFKA_OAUTH_CLIENT_SECRET", env_builder.KAFKA_OAUTH_CLIENT_SECRET)
    os.environ.setdefault("KAFKA_OAUTH_TOKEN_ENDPOINT_URL", env_builder.KAFKA_OAUTH_TOKEN_ENDPOINT_URL)
    os.environ.setdefault("KAFKA_SSL_CA_LOCATION", env_builder.KAFKA_SSL_CA_LOCATION)
    os.environ.setdefault("TRUSTSTORE_PASSWORD", env_builder.TRUSTSTORE_PASSWORD)


def _get_topic_manager() -> KafkaTopicManager:
    global _topic_manager
    if _topic_manager is None:
        _bridge_kafka_env()
        _topic_manager = KafkaTopicManager(
            bootstrap_server=env_builder.BOOTSTRAP_SERVER, logger=_logger
        )
    return _topic_manager


def _get_data_trans() -> DataTransection:
    global _data_trans
    if _data_trans is None:
        # PLATFORM_GCP_SERVICE_ACCOUNT_KEY is the raw JSON string (env_builder
        # only base64-encodes it when injecting into a child container's env
        # — see _platform_cloud_env()); DataTransection wants it pre-parsed.
        gcp_info = (
            json.loads(env_builder.PLATFORM_GCP_SERVICE_ACCOUNT_KEY)
            if env_builder.PLATFORM_GCP_SERVICE_ACCOUNT_KEY
            else None
        )
        # source_container_name/target_container_name/*_blob_name are unused
        # by container_exists() (the name to check is passed per-call) — left
        # blank rather than plumbing real values through for a check that
        # never reads them.
        # DataTransection passes these straight to
        # BlobServiceClient.from_connection_string() with no decoding of its
        # own, but producer-file-dp-secret stores them base64-encoded (see
        # env_builder._platform_cloud_env()'s comment) — decode here, same as
        # blueprints/producer/file/adaptor/main.py does for the adaptor pods.
        src_conn = env_builder.PLATFORM_AZURE_SRC_CONNECTION_STRING
        target_conn = env_builder.PLATFORM_AZURE_TARGET_CONNECTION_STRING
        _data_trans = DataTransection(
            source_azure_conn_str=base64.b64decode(src_conn).decode("utf-8") if src_conn else "",
            target_azure_conn_str=base64.b64decode(target_conn).decode("utf-8") if target_conn else "",
            source_container_name="",
            target_container_name="",
            source_blob_name=None,
            target_blob_name=None,
            aws_endpoint_url=env_builder.AWS_ENDPOINT_URL,
            aws_access_key_id=env_builder.AWS_ACCESS_KEY_ID,
            aws_secret_access_key=env_builder.AWS_SECRET_ACCESS_KEY,
            aws_region=env_builder.AWS_REGION,
            # A name-collision check is one request/response on the preview
            # panel, not a pipeline run — bound it so a misconfigured/down
            # storage endpoint fails soft in a few seconds (see module
            # docstring) instead of hanging the request until something
            # upstream (oauth2-proxy) times it out at 502, which is also way
            # too slow for a UI the user is actively waiting on.
            aws_connect_timeout=5,
            aws_read_timeout=10,
            gcp_service_account_info=gcp_info,
            gcp_project_id=env_builder.PLATFORM_GCP_PROJECT_ID,
            logger=_logger,
        )
    return _data_trans


def kafka_topic_exists(topic_name: str) -> bool:
    """True only when the broker confirms the topic is really there."""
    try:
        return _get_topic_manager().exists(topic_name)
    except Exception:  # noqa: BLE001 - connectivity/auth issue, not a collision
        _logger.warning("Could not check Kafka topic existence for %r", topic_name, exc_info=True)
        return False


def delete_kafka_topic(topic_name: str) -> None:
    """
    Permanently delete a Kafka topic on the platform broker — used by
    ui/app.py's delete_data_product so a deleted product's topics don't
    linger forever. Fail-soft like the exists()/ensure checks above: a
    connectivity/auth error is logged, not raised, since the product's
    registry entry and container are already gone by the time this runs
    (see delete_data_product's ordering) and there is no user-facing retry
    path for a teardown step — a stuck broker shouldn't block the rest of
    deletion or surface as a 500 after the fact.
    """
    try:
        _get_topic_manager().delete_topic(topic_name)
    except Exception:  # noqa: BLE001 - connectivity/auth issue, log and move on
        _logger.warning("Could not delete Kafka topic %r", topic_name, exc_info=True)


def delete_storage_container(container_name: str, side: str = "source") -> None:
    """Storage-side counterpart to delete_kafka_topic() — same fail-soft
    reasoning applies."""
    try:
        _get_data_trans().delete_container(
            env_builder.PLATFORM_CLOUD_PROVIDER, container_name, side
        )
    except Exception:  # noqa: BLE001 - connectivity/auth issue, log and move on
        _logger.warning(
            "Could not delete storage container %r", container_name, exc_info=True
        )


def storage_container_exists(container_name: str, side: str = "source") -> bool:
    """True only when the storage account confirms the container is really
    there. `side` picks which of the two platform Azure connection strings
    DataTransection.container_exists() checks against — source and target
    are different accounts in the real deployment, so checking the wrong
    side would silently look at the wrong container's account."""
    try:
        return _get_data_trans().container_exists(
            env_builder.PLATFORM_CLOUD_PROVIDER, container_name, side
        )
    except Exception:  # noqa: BLE001 - connectivity/auth issue, not a collision
        _logger.warning(
            "Could not check storage container existence for %r", container_name, exc_info=True
        )
        return False
