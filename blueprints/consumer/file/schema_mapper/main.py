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
# | 1.0.0   | Initial version                                          | DSI Team      | 2026-05-01  |
# +---------+----------------------------------------------------------+---------------+-------------+
"""
Consumer Schema Mapper.

Reads file references from the mapper Kafka topic, validates each file,
renames it using a date-partitioned path convention, moves it to the target
storage tier, and publishes a downstream Kafka event.

File placement:

    Files are placed flat in the target container using the same object
    key they arrived with.  No folder structure is applied.

The original ``boostrap_server`` attribute spelling (one 't') is preserved.

New environment variables (AWS / MinIO)
---------------------------------------
``AWS_ENDPOINT_URL``      – MinIO endpoint (empty → real AWS S3)
``AWS_ACCESS_KEY_ID``     – access key
``AWS_SECRET_ACCESS_KEY`` – secret key
``AWS_REGION``            – AWS region (default ``us-east-1``)
"""

from __future__ import annotations

import base64
import os
import time
from typing import Any

from azure.core.exceptions import AzureError, HttpResponseError
from botocore.exceptions import BotoCoreError, ClientError
from dotenv import load_dotenv
from google.api_core.exceptions import GoogleAPIError

from utils.data_transection import DataTransection
from utils.exception_handler import HandleExceptions
from utils.kafka_transection import KafkaTransection
from utils.otel_logger import OtelLogger as Logging
from utils.config_validator import validate_cloud_config, validate_kafka_config

load_dotenv()

class SchemaMapper:
    """
    Consumer-side schema mapper.

    Validates and routes files from the mapper tier to the final target tier
    using date-partitioned storage paths.
    """

    def __init__(self) -> None:
        """Initialise from environment variables."""
        # ── Existing instance variables (names unchanged, incl. typo) ─────
        self.cloud_provider: str = os.getenv("cloudProviderType", "azure")
        self.target_kafka_topic: str = os.getenv("targetTopicName", "")
        self.source_kafka_topic: str = os.getenv("mapperTopicName", "")
        self.source_azure_conn_str: str = base64.b64decode(
            os.getenv("mapperConnectionString", "")
        ).decode("utf-8")
        self.source_container_name: str = os.getenv("mapperContainerName", "")
        self.target_container_name: str = os.getenv("targetContainerName", "")
        self.boostrap_server: str = os.getenv("bootstrapServer", "")  
        self.target_azure_conn_str: str = base64.b64decode(
            os.getenv("targetConnectionString", "")
        ).decode("utf-8")
        self.org_name: str | None = None
        self.schema_type: str | None = None
        self.file_name: str | None = None
        self.original_file_name: str | None = None

        # ── New AWS / MinIO variables ─────────────────────────────────────
        self.aws_endpoint_url: str | None = os.getenv("AWS_ENDPOINT_URL") or None
        self.aws_access_key_id: str | None = os.getenv("AWS_ACCESS_KEY_ID") or None
        self.aws_secret_access_key: str | None = (
            os.getenv("AWS_SECRET_ACCESS_KEY") or None
        )
        self.aws_region: str = os.getenv("AWS_REGION", "us-east-1")

        # ── Logger ────────────────────────────────────────────────────────
        self.logger = Logging().create_logger()

        # ── Validate Cloud Config ─────────────────────────────────────────
        validate_cloud_config(
            cloud_provider=self.cloud_provider,
            azure_fields=["srcConnectionString", "mapperConnectionString"],
            logger=self.logger,
        )

        # ── Validate Kafka Config ─────────────────────────────────────────
        validate_kafka_config(logger=self.logger)

        # ── Success Log ────────────────────────────────────
        self.logger.info(
            "Configuration validation successful",
            extra={
                "event.name": "config.validation.success",
                "cloud.provider": self.cloud_provider,
            },
        )   
        # ── DataTransection ───────────────────────────────────────────────
        self.data_trans = DataTransection(
            source_azure_conn_str=self.source_azure_conn_str,
            source_container_name=self.source_container_name,
            target_container_name=self.target_container_name,
            source_blob_name=None,
            target_blob_name=None,
            target_azure_conn_str=self.target_azure_conn_str,
            aws_endpoint_url=self.aws_endpoint_url,
            aws_access_key_id=self.aws_access_key_id,
            aws_secret_access_key=self.aws_secret_access_key,
            aws_region=self.aws_region,
            logger=self.logger,
        )

        # ── KafkaTransection – accepts the typo spelling ──────────────────
        self.kafka_trans = KafkaTransection(
            boostrap_server=self.boostrap_server,
            logger=self.logger,
        )

        self.logger.info(
            "Consumer SchemaMapper initialised",
            extra={
                "cloudProviderType": self.cloud_provider,
                "mapperTopicName": self.target_kafka_topic,
                "srcContainerName": self.source_container_name,
                "targetContainerName": self.target_container_name,
                "tgtBootstrapServer": self.boostrap_server,
            },
        )

    # -----------------------------------------------------------------------
    def read_from_kafka_topic(self, file_name: str) -> str:
        """
        Process a file reference received from Kafka.

        Parses the standardised filename convention
        ``<schema_type>-<org_name>-<original_name>`` to populate
        :attr:`schema_type`, :attr:`org_name`, and :attr:`original_file_name`.

        Parameters
        ----------
        file_name:
            Object key / blob name from the Kafka payload.

        Returns
        -------
        str
            The original *file_name* unchanged.
        """
        self.data_trans.source_blob_name = file_name
        self.data_trans.target_blob_name = file_name

        file_props = file_name.split("-")
        if len(file_props) >= 3:
            self.schema_type = file_props[0]
            self.org_name = file_props[1]
            self.original_file_name = file_props[2]
        else:
            self.logger.warning(
                "File name does not match expected convention "
                "<schema_type>-<org_name>-<filename>",
                extra={"file_name": file_name},
            )

        self.logger.info(
            "File name parsed",
            extra={
                "file_name": file_name,
                "schema_type": self.schema_type,
                "org_name": self.org_name,
                "original_file_name": self.original_file_name,
            },
        )
        return file_name

    def read_records(self) -> str:
        """
        Download the current ``source_blob_name`` from the mapper tier.

        Returns
        -------
        str
            UTF-8 file content.
        """
        data = self.data_trans.data_read(cloud_vendor=self.cloud_provider)
        return data

    def schema_validation(self, data: str) -> bool:
        """
        Validate *data* against its product schema.

        Parameters
        ----------
        data:
            Raw file content.

        Returns
        -------
        bool
            ``True`` if valid.

        .. note::
            Stub implementation – full logic scheduled for PI3.
        """
        # TODO (PI3): implement per-schema validation
        self.logger.info(
            "Schema validation invoked (stub – PI3)",
            extra={
                "schema_type": self.schema_type,
                "data_length": len(data) if data else 0,
            },
        )
        return True

    def move_files(self, file: str) -> bool:
        """
        Move *file* from the mapper tier to the target container / bucket.

        The file is placed flat in the target container using the same
        object key it arrived with – no folder structure or date partitioning
        is applied.  The source object is deleted after a successful copy.

        Parameters
        ----------
        file:
            Source object key / blob name.

        Returns
        -------
        bool
            ``True`` on success.
        """
        # Destination key is identical to source key – flat placement only.
        self.file_name = file

        is_file_moved: bool = self.data_trans.file_move(
            cloud_vendor=self.cloud_provider,
            file_name=file,
            dest_file_name=self.file_name,
        )
        self.logger.info(
            "File moved to target container",
            extra={
                "source": file,
                "destination": self.file_name,
                "target_container": self.target_container_name,
                "provider": self.cloud_provider,
            },
        )
        return is_file_moved

    def send_to_kafka(self) -> None:
        """
        Publish a file-ready event to the target Kafka topic.

        Uses the renamed ``file_name`` populated by :meth:`move_files`.
        """
        message = {
            "sourceType": "s3" if self.cloud_provider.lower() == "aws" else self.cloud_provider,
            "storageContainer": self.target_container_name,
            "path": self.file_name,
        }
        self.kafka_trans.send_message(
            target_topic=self.target_kafka_topic, message=message
        )
        self.logger.info(
            "Message pushed into Kafka topic",
            extra={"topic": self.target_kafka_topic, "path": self.file_name},
        )


# ---------------------------------------------------------------------------
# Per-message processing
# ---------------------------------------------------------------------------
def _process_message(
    schema_mapper: "SchemaMapper",
    except_handle: HandleExceptions,
    payload: dict[str, Any],
) -> None:
    """
    Process a single Kafka message payload through the full mapper pipeline.

    Called once per message by the consumer loop in :func:`start_consumer`.
    Keeping this as a standalone function (not a nested closure) makes it
    independently testable.

    Parameters
    ----------
    schema_mapper:
        Shared :class:`SchemaMapper` instance.
    except_handle:
        Shared :class:`HandleExceptions` instance.
    payload:
        Decoded JSON dict from the Kafka message.
    """
    file_name: str = payload.get("path", "")
    if not file_name:
        schema_mapper.logger.warning(
            "Payload missing 'path' – skipping",
            extra={"payload": payload},
        )
        return
    try:
        resolved = schema_mapper.read_from_kafka_topic(file_name)
        if resolved:
            data = schema_mapper.read_records()
            is_valid = schema_mapper.schema_validation(data)
            if is_valid:
                is_moved = schema_mapper.move_files(file=resolved)
                if is_moved:
                    schema_mapper.send_to_kafka()
                else:
                    schema_mapper.logger.warning(
                        "File move failed – Kafka event suppressed",
                        extra={"file": resolved},
                    )
    except (HttpResponseError, AzureError) as exc:
        except_handle.handle_storage_exception(exc, "Azure")
    except (ClientError, BotoCoreError) as exc:
        except_handle.handle_storage_exception(exc, "AWS S3")
    except GoogleAPIError as exc:
        except_handle.handle_storage_exception(exc, "GCP")
    except Exception as exc:  # noqa: BLE001
        except_handle.handle_storage_exception(exc, "")


def start_consumer(schema_mapper: "SchemaMapper") -> None:
    """
    Start the blocking Kafka consumer loop for the schema mapper.

    Invokes :func:`_process_message` for every message consumed from
    ``source_kafka_topic``.  Blocks until a fatal Kafka error occurs
    (the caller is responsible for retrying).

    Parameters
    ----------
    schema_mapper:
        Shared :class:`SchemaMapper` instance built once at startup.
    """
    _except_handle = HandleExceptions()

    def _handler(payload: dict[str, Any]) -> None:
        _process_message(schema_mapper, _except_handle, payload)

    schema_mapper.kafka_trans.consume_messages(
        source_topic=schema_mapper.source_kafka_topic,
        group_id="consumer_schema_mapper",
        handler=_handler,
    )


if __name__ == "__main__":  # pragma: no cover
    # Consumer mapper is event-driven (Kafka), not schedule-driven.
    # schedule / scheduleInterval are intentionally not used here.
    # Build SchemaMapper once – reused for every consumed message.
    _mapper = SchemaMapper()
    _mapper.logger.info(
        "Consumer mapper starting – listening for Kafka events",
        extra={
            "source_topic": _mapper.source_kafka_topic,
            "target_topic": _mapper.target_kafka_topic,
            "cloud_provider": _mapper.cloud_provider,
        },
    )

    # start_consumer() blocks indefinitely inside consume_messages().
    # Wrap in a retry loop so a broker restart does not kill the process.
    _retry_delay = int(os.getenv("consumerRetryDelaySecs", "5"))
    while True:
        try:
            start_consumer(schema_mapper=_mapper)
        except Exception as _exc:  # noqa: BLE001
            _mapper.logger.error(
                "Consumer loop exited unexpectedly – retrying",
                extra={"error": str(_exc), "retry_in_secs": _retry_delay},
                exc_info=True,
            )
            time.sleep(_retry_delay)
