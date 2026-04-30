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
Adaptor – file ingestion entry point.

Polls the source container / bucket for new files, moves each file to the
mapper staging area, and publishes a Kafka event so downstream consumers
know a file is ready to process.

Supports
--------
* Azure Blob Storage (original behaviour)
* AWS S3 and MinIO (new – activated by ``CLOUD_PROVIDER_TYPE=aws``)

All **existing** environment variable and instance variable names are
unchanged.  New AWS/MinIO variables are purely additive.

New environment variables (AWS / MinIO only)
--------------------------------------------
``AWS_ENDPOINT_URL``      – MinIO or custom S3 endpoint (empty → real AWS)
``AWS_ACCESS_KEY_ID``     – Access key ID
``AWS_SECRET_ACCESS_KEY`` – Secret access key
``AWS_REGION``            – AWS region  (default: ``us-east-1``)
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


class AdaptorFileProcess:
    """
    File-ingestion adaptor.

    Reads files from the source storage tier, moves them to the mapper
    staging tier, and publishes a Kafka notification for each successfully
    moved file.

    All instance variable names match the original implementation exactly.
    """

    def __init__(self) -> None:
        """Initialise the adaptor from environment variables."""
        # ── Existing instance variables (names unchanged) ─────────────────
        self.cloud_provider: str = os.getenv("cloudProviderType", "azure")
        self.target_kafka_topic: str = os.getenv("mapperTopicName", "")
        self.source_azure_conn_str: str = base64.b64decode(
            os.getenv("srcConnectionString", "")
        ).decode("utf-8")
        self.source_container_name: str = os.getenv("srcContainerName", "")
        self.target_container_name: str = os.getenv("mapperContainerName", "")
        self.bootstrap_server: str = os.getenv("bootstrapServer", "")
        self.target_azure_conn_str: str = base64.b64decode(
            os.getenv("mapperConnectionString", "")
        ).decode("utf-8")

        # ── New AWS / MinIO variables ─────────────────────────────────────
        self.aws_endpoint_url: str | None = os.getenv("AWS_ENDPOINT_URL") or None
        self.aws_access_key_id: str | None = os.getenv("AWS_ACCESS_KEY_ID") or None
        self.aws_secret_access_key: str | None = (
            os.getenv("AWS_SECRET_ACCESS_KEY") or None
        )
        self.aws_region: str = os.getenv("AWS_REGION", "us-east-1")

        # ── Logger (OTel-compatible JSON) ─────────────────────────────────
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

        # ── DataTransection ────────────────────
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

    # -----------------------------------------------------------------------
    # Internal helpers
    # -----------------------------------------------------------------------
    def _log_config_banner(self) -> None:
        """Emit a structured config summary at startup (no credentials logged)."""
        lines = [
            "------------- Producer - Adaptor Config Information -------------",
            f"cloudProviderType   : {self.cloud_provider}",
            f"mapperTopicName     : {self.target_kafka_topic}",
            f"srcContainerName    : {self.source_container_name}",
            f"mapperContainerName : {self.target_container_name}",
            f"bootstrapServer     : {self.bootstrap_server}",
        ]
        if self.cloud_provider.lower() == "aws":
            lines.append(
                f"awsEndpoint         : {self.aws_endpoint_url or 'AWS default'}"
            )
            lines.append(f"awsRegion           : {self.aws_region}")

        box_width = max(len(line) for line in lines) + 4
        border = "+" + "-" * (box_width - 2) + "+"

        self.logger.info(border)
        for line in lines:
            self.logger.info(f"| {line.ljust(box_width - 4)} |")
        self.logger.info(border)

    # -----------------------------------------------------------------------
    # Public API
    # -----------------------------------------------------------------------
    def read_source_file_info(self) -> list[str]:
        """
        List files available in the source container or bucket.

        Returns
        -------
        list[str]
            Object keys / blob names found in the source location.
        """
        source_file_name = self.data_trans.source_file_info(
            cloud_provider=self.cloud_provider
        )
        self.logger.info(
            "Source files discovered",
            extra={"count": len(source_file_name), "files": source_file_name},
        )
        return source_file_name

    def move_files(self, file: str) -> bool:
        """
        Copy *file* from source to mapper staging (source is preserved).

        Before copying, the mapper container / bucket is checked:
        - File absent in target      -> copied unconditionally.
        - File present, src newer    -> target is overwritten.
        - File present, src same/old -> copy is skipped.

        Parameters
        ----------
        file:
            Object key / blob name in the source container.

        Returns
        -------
        bool
            True on success (copy performed OR intentionally skipped).
        """
        result = self.data_trans.file_copy(
            cloud_vendor=self.cloud_provider, file_name=file
        )
        self.logger.info(
            "Adaptor file_copy result",
            extra={
                "file": file,
                "copied": result.copied,
                "skipped": result.skipped,
                "reason": result.reason,
            },
        )
        # Only return True when the file was actually copied.
        # A skipped result (target already up-to-date) must NOT trigger
        # a Kafka event – there is nothing new for downstream to process.
        return result.copied

    def send_to_kafka(self, file_name: str, is_file_move: bool) -> None:
        """
        Publish a file-ready event to the mapper Kafka topic.

        Parameters
        ----------
        file_name:
            Object key / blob name that was moved.
        is_file_move:
            ``True`` only when a file was actually copied (new or updated).
            ``False`` when the copy was skipped (file unchanged) or failed.
            In both skip and failure cases the message is suppressed.
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
                "Message pushed into Kafka topic",
                extra={"topic": self.target_kafka_topic, "file": file_name},
            )
        else:
            self.logger.info(
                "Kafka message suppressed – file not copied (skipped or failed)",
                extra={"file": file_name},
            )


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------
def main() -> None:
    """
    Execute one adaptor processing cycle.

    Lists all source files, moves each to the mapper staging area, and
    publishes a Kafka event per successful move.  Exception handling is
    cloud-provider-aware for structured error reporting.
    """
    message = "Adaptor File Processor started"
    border = "=" * (len(message) + 4)
    print(border)
    print(f"| {message} |")
    print(border)

    except_handle = HandleExceptions()
    try:
        adaptor_file_process = AdaptorFileProcess()
        source_files = adaptor_file_process.read_source_file_info()
        for file in source_files:
            is_file_move = adaptor_file_process.move_files(file=file)
            adaptor_file_process.send_to_kafka(
                file_name=file, is_file_move=is_file_move
            )
    except (HttpResponseError, AzureError) as exc:
        except_handle.handle_storage_exception(exc, "Azure")
    except (ClientError, BotoCoreError) as exc:
        except_handle.handle_storage_exception(exc, "AWS S3")
    except GoogleAPIError as exc:
        except_handle.handle_storage_exception(exc, "GCP")
    except Exception as exc:  # noqa: BLE001
        except_handle.handle_storage_exception(exc, "")

    message = "Adaptor Process Completed"
    border = "=" * (len(message) + 4)
    print(border)
    print(f"| {message} |")
    print(border)


if __name__ == "__main__":  # pragma: no cover
    print("Adaptor Process Starting")
    interval = int(os.getenv("scheduleInterval", "60"))

    # Build the processor once so cloud clients are not re-created every tick.
    _adaptor = AdaptorFileProcess()

    def _run_cycle() -> None:
        """One polling cycle: list -> copy (if new/updated) -> kafka."""
        except_handle = HandleExceptions()
        try:
            source_files = _adaptor.read_source_file_info()
            if not source_files:
                _adaptor.logger.info("No files found in source – nothing to do")
                return
            for _file in source_files:
                _copied = _adaptor.move_files(file=_file)
                # send_to_kafka is a no-op when _copied is False (skip or fail)
                _adaptor.send_to_kafka(file_name=_file, is_file_move=_copied)
        except (HttpResponseError, AzureError) as exc:
            except_handle.handle_storage_exception(exc, "Azure")
        except (ClientError, BotoCoreError) as exc:
            except_handle.handle_storage_exception(exc, "AWS S3")
        except GoogleAPIError as exc:
            except_handle.handle_storage_exception(exc, "GCP")
        except Exception as exc:  # noqa: BLE001
            except_handle.handle_storage_exception(exc, "")

    # Run immediately on startup so we do not wait one full interval
    _adaptor.logger.info(
        "Adaptor scheduler started",
        extra={"interval_seconds": interval},
    )
    _run_cycle()

    # Then repeat every interval seconds
    schedule.every(interval).seconds.do(_run_cycle)
    while True:
        schedule.run_pending()
        time.sleep(1)
