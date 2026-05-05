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
Producer Schema Mapper.

Consumes file-ready events from the mapper Kafka topic, validates each file
against its schema, renames and moves the validated file to the target tier,
then publishes a downstream Kafka event.

Supports Azure Blob Storage and AWS S3 / MinIO.  All original instance
variable names are preserved.

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
    Producer-side schema mapper.

    Validates file content and routes it to the target storage tier.
    Consumes from ``mapperTopicName`` and produces to ``targetTopicName``.
    """

    def __init__(self) -> None:
        """Initialise from environment variables."""
        # ── Existing instance variables (names unchanged) ─────────────────
        self.cloud_provider: str = os.getenv("cloudProviderType", "azure")
        self.target_kafka_topic: str = os.getenv("targetTopicName", "")
        self.source_kafka_topic: str = os.getenv("mapperTopicName", "")
        self.source_azure_conn_str: str = base64.b64decode(
            os.getenv("mapperConnectionString", "")
        ).decode("utf-8")
        self.source_container_name: str = os.getenv("mapperContainerName", "")
        self.target_container_name: str = os.getenv("targetContainerName", "")
        self.bootstrap_server: str = os.getenv("bootstrapServer", "")
        self.target_azure_conn_str: str = base64.b64decode(
            os.getenv("targetConnectionString", "")
        ).decode("utf-8")
        self.org_name: str = os.getenv("orgName", "")
        self.schema_type: str = os.getenv("schemaType", "")
        self.file_name: str | None = None

        # ── New AWS / MinIO variables ─────────────────────────────────────
        self.aws_endpoint_url: str | None = os.getenv("AWS_ENDPOINT_URL") or None

        self.aws_access_key_id = base64.b64decode(
        os.getenv("AWS_ACCESS_KEY_ID", "")
        ).decode("utf-8")

        self.aws_secret_access_key = base64.b64decode(
            os.getenv("AWS_SECRET_ACCESS_KEY", "")
        ).decode("utf-8")

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

        # ── KafkaTransection ──────────────────────────────────────────────
        self.kafka_trans = KafkaTransection(
            bootstrap_server=self.bootstrap_server,
            logger=self.logger,
        )

        self._log_config_banner()
        self.logger.info(
            "Kafka consumer listening",
            extra={"topic": self.source_kafka_topic},
        )

    # -----------------------------------------------------------------------
    def _log_config_banner(self) -> None:
        """Emit a startup config summary (no credentials)."""
        log_lines = [
            "------------- Producer - Schema Mapper Config Information -------------",
            f"cloudProviderType    : {self.cloud_provider}",
            f"targetTopicName      : {self.target_kafka_topic}",
            f"srcContainerName     : {self.source_container_name}",
            f"targetContainerName  : {self.target_container_name}",
            f"bootstrapServer      : {self.bootstrap_server}",
        ]
        width = max(len(line) for line in log_lines) + 4
        border = "+" + "-" * (width - 2) + "+"
        self.logger.info(border)
        for line in log_lines:
            self.logger.info(f"| {line.ljust(width - 4)} |")
        self.logger.info(border)

    # -----------------------------------------------------------------------
    def read_records(self, file: str) -> str:
        """
        Download file content from the mapper storage tier.

        Parameters
        ----------
        file:
            Object key / blob name to read.

        Returns
        -------
        str
            UTF-8 file content.
        """
        self.data_trans.source_blob_name = file
        data = self.data_trans.data_read(cloud_vendor=self.cloud_provider)
        self.logger.info(
            "File content read",
            extra={
                "file": file,
                "provider": self.cloud_provider,
                "bytes": len(data),
            },
        )
        return data

    def schema_validation(self, data: str) -> bool:
        """
        Validate *data* against the product schema.

        Parameters
        ----------
        data:
            Raw file content.

        Returns
        -------
        bool
            ``True`` if valid.

        .. note::
            Full schema validation logic is scheduled for PI3.
            The current implementation is a pass-through stub.
        """
        # TODO (PI3): implement schema-specific validation
        self.logger.info(
            "Schema validation invoked (stub – PI3)",
            extra={"schema_type": self.schema_type, "data_length": len(data)},
        )
        return True

    def move_files(self, file: str) -> bool:
        """
        Rename and copy *file* to the target storage tier (source preserved).

        Destination name convention::

            <schema_type>-<org_name>-<normalised_filename>

        Before writing, the target is checked:
        - File absent             -> copied unconditionally.
        - File present, src newer -> overwritten.
        - File present, src old   -> skipped (returns True, no write).

        Parameters
        ----------
        file:
            Source object key / blob name.

        Returns
        -------
        bool
            True on success (copy performed OR intentionally skipped).
        """
        self.file_name = (
            self.schema_type.lower()
            + "-"
            + self.org_name.lower()
            + "-"
            + file.replace(" ", "_").replace("-", "_").lower()
        )
        result = self.data_trans.file_copy(
            cloud_vendor=self.cloud_provider,
            file_name=file,
            dest_file_name=self.file_name,
        )
        self.logger.info(
            "Producer mapper file_copy result",
            extra={
                "source_file": file,
                "dest_file": self.file_name,
                "copied": result.copied,
                "skipped": result.skipped,
                "reason": result.reason,
            },
        )
        # Only return True when a file was actually copied.
        # Skipped files (target already up-to-date) produce no Kafka event.
        return result.copied

    def send_to_kafka(self, is_file_move: bool) -> None:
        """
        Publish a file-ready event to the target Kafka topic.

        Parameters
        ----------
        is_file_move:
            When ``False`` the message is suppressed.
        """
        if is_file_move:
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
                extra={"topic": self.target_kafka_topic, "file": self.file_name},
            )
        else:
            self.logger.info(
                "Kafka message suppressed – file not copied (skipped or failed)",
                extra={"file": self.file_name},
            )


# ---------------------------------------------------------------------------
# Per-message processing
# ---------------------------------------------------------------------------
def main(schema_mapper: SchemaMapper, file: str) -> None:
    """
    Process a single file through the schema-mapper pipeline.

    Parameters
    ----------
    schema_mapper:
        Shared :class:`SchemaMapper` instance.
    file:
        Object key / blob name to process.
    """
    message = "Schema Mapper Started"
    border = "=" * (len(message) + 4)
    print(border)
    print(f"| {message} |")
    print(border)

    except_handle = HandleExceptions()
    try:
        data = schema_mapper.read_records(file=file)
        is_valid = schema_mapper.schema_validation(data)
        if is_valid:
            is_file_move = schema_mapper.move_files(file=file)
            schema_mapper.send_to_kafka(is_file_move=is_file_move)
    except (HttpResponseError, AzureError) as exc:
        except_handle.handle_storage_exception(exc, "Azure")
    except (ClientError, BotoCoreError) as exc:
        except_handle.handle_storage_exception(exc, "AWS S3")
    except GoogleAPIError as exc:
        except_handle.handle_storage_exception(exc, "GCP")
    except Exception as exc:  # noqa: BLE001
        except_handle.handle_storage_exception(exc, "")

    message = "Schema Mapper Completed"
    border = "=" * (len(message) + 4)
    print(border)
    print(f"| {message} |")
    print(border)


def start_consumer(schema_mapper: SchemaMapper) -> None:
    """
    Start the Kafka consumer loop for the schema mapper.

    Blocks indefinitely, invoking :func:`main` for each message consumed
    from the mapper topic.

    Parameters
    ----------
    schema_mapper:
        Shared :class:`SchemaMapper` instance.
    """
    def _handler(payload: dict[str, Any]) -> None:
        file_name: str = payload.get("path", "")
        if file_name:
            main(schema_mapper=schema_mapper, file=file_name)
        else:
            schema_mapper.logger.warning(
                "Kafka payload missing 'path' – skipping",
                extra={"payload": payload},
            )

    schema_mapper.kafka_trans.consume_messages(
        source_topic=schema_mapper.source_kafka_topic,
        group_id="producer_schema_mapper",
        handler=_handler,
    )


if __name__ == "__main__":  # pragma: no cover
    # Build SchemaMapper once – reused across all consumed messages.
    _mapper = SchemaMapper()
    _mapper.logger.info(
        "Producer mapper starting – listening for Kafka events",
        extra={
            "source_topic": _mapper.source_kafka_topic,
            "target_topic": _mapper.target_kafka_topic,
            "cloud_provider": _mapper.cloud_provider,
        },
    )
    # start_consumer blocks indefinitely via consume_messages().
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
