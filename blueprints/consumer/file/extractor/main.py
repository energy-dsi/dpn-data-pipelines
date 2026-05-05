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
Extractor – consumer-side file ingestion.

Polls the source container / bucket, moves files to the mapper staging tier,
and publishes a Kafka event per file.

The original ``boostrap_server`` attribute spelling (one 't') is preserved
for backward compatibility with existing configuration.

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

import schedule
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


class ExtractorFileProcess:
    """
    Consumer-side file extractor.

    Mirrors the producer-side adaptor but is deployed in the consumer
    blueprint.  All original instance variable names (including the
    ``boostrap_server`` spelling) are preserved.
    """

    def __init__(self) -> None:
        """Initialise from environment variables."""
        # ── Existing instance variables (names unchanged, incl. typo) ─────
        self.cloud_provider: str = os.getenv("cloudProviderType", "azure")
        self.target_kafka_topic: str = os.getenv("mapperTopicName", "")
        self.source_azure_conn_str: str = base64.b64decode(
            os.getenv("srcConnectionString", "")
        ).decode("utf-8")
        self.source_container_name: str = os.getenv("srcContainerName", "")
        self.target_container_name: str = os.getenv("mapperContainerName", "")
        self.boostrap_server: str = os.getenv("bootstrapServer", "")  # typo preserved
        self.target_azure_conn_str: str = base64.b64decode(
            os.getenv("mapperConnectionString", "")
        ).decode("utf-8")

        # ── New AWS / MinIO variables ─────────────────────────────────────
        self.aws_endpoint_url: str | None = os.getenv("AWS_ENDPOINT_URL") or None
        self.aws_access_key_id: str | None = base64.b64decode(os.getenv("AWS_ACCESS_KEY_ID")).decode("utf-8") or None
        self.aws_secret_access_key: str | None = (
            base64.b64decode(os.getenv("AWS_SECRET_ACCESS_KEY")).decode("utf-8") or None
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
            "Extractor initialised",
            extra={
                "cloudProviderType": self.cloud_provider,
                "mapperTopicName": self.target_kafka_topic,
                "srcContainerName": self.source_container_name,
                "mapperContainerName": self.target_container_name,
                "bootstrapServer": self.boostrap_server,
            },
        )

    # -----------------------------------------------------------------------
    def read_source_file_info(self) -> list[str]:
        """
        List files in the source container / bucket.

        Returns
        -------
        list[str]
            Object keys / blob names available for extraction.
        """
        source_file_name = self.data_trans.source_file_info(
            cloud_provider=self.cloud_provider
        )
        self.logger.info(
            "Source files discovered",
            extra={"count": len(source_file_name)},
        )
        return source_file_name

    def move_files(self, file: str) -> bool:
        """
        Move *file* from source to mapper staging.

        Parameters
        ----------
        file:
            Object key / blob name to move.

        Returns
        -------
        bool
            ``True`` on success.
        """
        is_file_moved: bool = self.data_trans.file_move(
            cloud_vendor=self.cloud_provider, file_name=file
        )
        return is_file_moved

    def send_to_kafka(self, file_name: str, is_file_move: bool) -> None:
        """
        Publish a file-ready event to the mapper Kafka topic.

        Parameters
        ----------
        file_name:
            Object key / blob name that was moved.
        is_file_move:
            Suppresses the message when ``False``.
        """
        if is_file_move:
            message = {
                "sourceType": "s3" if self.cloud_provider.lower() == "aws" else self.cloud_provider,
                "storageContainer": self.target_container_name,
                "path": file_name,
            }
            self.kafka_trans.send_message(
                target_topic=self.target_kafka_topic, message=message
            )
            self.logger.info(
                "Kafka message published",
                extra={"topic": self.target_kafka_topic, "file": file_name},
            )
        else:
            self.logger.warning(
                "Kafka message suppressed – move failed",
                extra={"file": file_name},
            )


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------
def main(extractor: "ExtractorFileProcess") -> None:
    """
    Execute one extractor processing cycle.

    Lists source files, moves each to the mapper staging area, and
    publishes a Kafka event per successful move.

    Parameters
    ----------
    extractor:
        Shared :class:`ExtractorFileProcess` instance built once at startup.
        Passing it in avoids re-creating cloud clients on every tick.
    """
    except_handle = HandleExceptions()
    try:
        source_files = extractor.read_source_file_info()
        if not source_files:
            extractor.logger.info("No files found in source – nothing to do")
            return
        for file in source_files:
            is_file_moved = extractor.move_files(file=file)
            # send_to_kafka is a no-op when move failed
            extractor.send_to_kafka(file_name=file, is_file_move=is_file_moved)
    except (HttpResponseError, AzureError) as exc:
        except_handle.handle_storage_exception(exc, "Azure")
    except (ClientError, BotoCoreError) as exc:
        except_handle.handle_storage_exception(exc, "AWS S3")
    except GoogleAPIError as exc:
        except_handle.handle_storage_exception(exc, "GCP")
    except Exception as exc:  # noqa: BLE001
        except_handle.handle_storage_exception(exc, "")


if __name__ == "__main__":  # pragma: no cover
    interval = int(os.getenv("scheduleInterval", "60"))

    # Build the extractor once so cloud clients are not re-created every tick.
    _extractor = ExtractorFileProcess()
    _extractor.logger.info(
        "Extractor scheduler started",
        extra={"interval_seconds": interval},
    )

    # Run immediately on startup – do not wait one full interval.
    main(_extractor)

    # Then repeat every interval seconds.
    schedule.every(interval).seconds.do(main, _extractor)
    while True:
        schedule.run_pending()
        time.sleep(1)
