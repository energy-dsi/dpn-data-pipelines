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
Centralised exception-handling utility.

Provides structured, OTel-compatible error logging for storage-layer exceptions
across all supported cloud providers (Azure, AWS S3/MinIO, GCP).

Usage::

    handler = HandleExceptions()
    try:
        ...
    except AzureError as exc:
        handler.handle_storage_exception(exc, "Azure")
"""

from __future__ import annotations

import logging
from typing import Optional

from utils.otel_logger import Logging


class HandleExceptions:
    """
    Translates raw cloud-provider exceptions into structured log entries.

    Each call records:
      - ``exception.provider``  – which cloud raised the error
      - ``exception.type``      – fully qualified exception class name
      - ``exception.message``   – human-readable error text
      - ``exception.args``      – raw exception arguments (for diagnostics)

    No sensitive credential or PII data is included in log output.
    """

    def __init__(self, logger: Optional[logging.Logger] = None) -> None:
        """
        Parameters
        ----------
        logger:
            Optional pre-configured logger.  A default OTel-compatible logger
            is created when *None*.
        """
        self._logger: logging.Logger = logger or Logging().create_logger()

    # ------------------------------------------------------------------
    def handle_storage_exception(
        self,
        exc: Exception,
        provider: str,
    ) -> None:
        """
        Log a storage-layer exception with structured context.

        Parameters
        ----------
        exc:
            The exception that was raised.
        provider:
            Human-readable cloud provider label, e.g. ``"Azure"``,
            ``"AWS S3"``, ``"GCP"``.  Pass an empty string for
            non-provider-specific exceptions.
        """
        provider_label: str = provider.strip() if provider else "Unknown"
        exc_type: str = type(exc).__name__
        exc_message: str = str(exc)

        self._logger.error(
            "Storage operation failed",
            extra={
                "exception.provider": provider_label,
                "exception.type": exc_type,
                "exception.message": exc_message,
            },
            exc_info=True,
        )

    def handle_kafka_exception(self, exc: Exception, topic: str) -> None:
        """
        Log a Kafka-layer exception with structured context.

        Parameters
        ----------
        exc:
            The exception that was raised.
        topic:
            Kafka topic name involved in the failed operation.
        """
        self._logger.error(
            "Kafka operation failed",
            extra={
                "exception.type": type(exc).__name__,
                "exception.message": str(exc),
                "kafka.topic": topic,
            },
            exc_info=True,
        )
