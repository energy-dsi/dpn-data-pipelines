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
# +---------+----------------------------------------------------------+---------------+-------------+

"""
topic_utils – shared helpers for the topic ingestion pathway.

Class-based implementation providing:
* TopicResolver.resolve        – derive a topic name
* KafkaTopicManager.ensure_exists – auto-create a Kafka topic if needed
"""

from __future__ import annotations

import logging
from typing import Optional

from confluent_kafka.admin import AdminClient, NewTopic


class TopicResolver:
    """
    Helper class for resolving Kafka topic names.
    """

    @staticmethod
    def resolve(provided: Optional[str], src_topic: str, suffix: str) -> str:
        """
        Return *provided* when non-empty, otherwise ``<src_topic>-<suffix>``.

        Parameters
        ----------
        provided:
            Value read from environment (may be empty string or None).
        src_topic:
            The source topic name used as the base for auto-naming.
        suffix:
            Appended to *src_topic* when *provided* is absent.

        Examples
        --------
        >>> TopicResolver.resolve("", "dpn-producer-eq-src", "trfm")
        'dpn-producer-eq-src-trfm'
        >>> TopicResolver.resolve("my-custom-topic", "dpn-producer-eq-src", "trfm")
        'my-custom-topic'
        """
        return (provided or "").strip() or f"{src_topic}-{suffix}"


class KafkaTopicManager:
    """
    Kafka administrative helper responsible for ensuring topic existence.
    """

    def __init__(self, bootstrap_server: str, logger: logging.Logger):
        self._admin = AdminClient(
            {"bootstrap.servers": bootstrap_server}
        )
        self._logger = logger

    def ensure_exists(
        self,
        topic_name: str,
        *,
        num_partitions: int = 1,
        replication_factor: int = 1,
    ) -> None:
        """
        Create *topic_name* on the Kafka cluster if it does not already exist.

        Safe to call on every startup.

        Parameters
        ----------
        topic_name:
            Topic to create.
        num_partitions:
            Number of partitions for the new topic.
        replication_factor:
            Replication factor for the new topic.
        """
        existing_topics = self._admin.list_topics(timeout=10).topics
        if topic_name in existing_topics:
            self._logger.info(
                "Kafka topic already exists – skipping creation",
                extra={"topic": topic_name},
            )
            return

        results = self._admin.create_topics(
            [
                NewTopic(
                    topic_name,
                    num_partitions=num_partitions,
                    replication_factor=replication_factor,
                )
            ]
        )

        for topic, future in results.items():
            try:
                future.result()
                self._logger.info(
                    "Kafka topic created",
                    extra={"topic": topic},
                )
            except Exception as exc:  # noqa: BLE001
                # TOPIC_ALREADY_EXISTS and similar cases are benign
                self._logger.warning(
                    "Kafka topic creation result",
                    extra={"topic": topic, "detail": str(exc)},
                )