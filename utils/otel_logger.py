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
OTel-compatible structured JSON logger.

**Why this file is named ``otel_logger.py`` and not ``logging.py``**

Python resolves imports relative to ``sys.path``.  A file called
``utils/logging.py`` shadows the standard-library ``logging`` module for
any code that does ``import logging`` inside the ``utils`` package — or any
package that has ``utils/`` on its path.  This causes a
``AttributeError: module 'logging' has no attribute 'getLogger'`` at
startup, which is the error you saw when creating a virtual environment.

Renaming to ``otel_logger.py`` removes the collision entirely.

Usage::

    from utils.otel_logger import OtelLogger
    logger = OtelLogger().create_logger()
    logger.info("step complete", extra={"file": "EQBD.xml"})
"""

from __future__ import annotations

import json
import logging
import os
import sys
from datetime import datetime, timezone
from typing import Any

# Optional OTel integration – safe to import even when OTel SDK is absent
try:
    from opentelemetry import trace as _otel_trace  # type: ignore[import]

    _OTEL_AVAILABLE = True
except ImportError:  # pragma: no cover
    _OTEL_AVAILABLE = False


# ---------------------------------------------------------------------------
# OTel numeric severity mapping (Log Record spec §2.2)
# ---------------------------------------------------------------------------
_SEVERITY_MAP: dict[int, tuple[int, str]] = {
    logging.DEBUG:    (5,  "DEBUG"),
    logging.INFO:     (9,  "INFO"),
    logging.WARNING:  (13, "WARN"),
    logging.ERROR:    (17, "ERROR"),
    logging.CRITICAL: (21, "FATAL"),
}

# Standard LogRecord keys – never forwarded as custom attributes
_STDLIB_KEYS: frozenset[str] = frozenset(
    {
        "name", "msg", "args", "levelname", "levelno", "pathname", "filename",
        "module", "exc_info", "exc_text", "stack_info", "lineno", "funcName",
        "created", "msecs", "relativeCreated", "thread", "threadName",
        "processName", "process", "message", "taskName",
    }
)


class _OTelJsonFormatter(logging.Formatter):
    """
    Formats a :class:`logging.LogRecord` as a single-line JSON string that
    follows the OpenTelemetry Log Data Model.

    Every record carries ``trace_id`` / ``span_id`` / ``trace_flags`` when
    the call is made inside an active OTel span, enabling automatic
    log-to-trace correlation in Grafana, Jaeger, Datadog, etc.
    """

    def __init__(self, service_name: str, service_version: str) -> None:
        super().__init__()
        self._resource: dict[str, str] = {
            "service.name": service_name,
            "service.version": service_version,
        }

    def format(self, record: logging.LogRecord) -> str:  # noqa: A003
        record.message = record.getMessage()
        now: str = datetime.now(tz=timezone.utc).isoformat()
        sev_num, sev_text = _SEVERITY_MAP.get(record.levelno, (9, "INFO"))

        entry: dict[str, Any] = {
            "timestamp": now,
            "observed_timestamp": now,
            "severity_number": sev_num,
            "severity_text": sev_text,
            "body": record.message,
            "resource": self._resource,
            "attributes": {
                "code.filepath": record.pathname,
                "code.lineno": record.lineno,
                "code.function": record.funcName,
                "logger.name": record.name,
            },
        }

        # Inject active OTel span context for log/trace correlation
        if _OTEL_AVAILABLE:
            span = _otel_trace.get_current_span()
            ctx = span.get_span_context()
            if ctx.is_valid:
                entry["trace_id"] = format(ctx.trace_id, "032x")
                entry["span_id"] = format(ctx.span_id, "016x")
                entry["trace_flags"] = format(ctx.trace_flags, "02x")

        # Merge caller-supplied ``extra`` fields into attributes
        for key, value in record.__dict__.items():
            if key not in _STDLIB_KEYS:
                entry["attributes"][key] = value

        # Attach exception details when present
        if record.exc_info:
            entry["attributes"]["exception.stacktrace"] = self.formatException(
                record.exc_info
            )
            if record.exc_info[1]:
                entry["attributes"]["exception.type"] = type(
                    record.exc_info[1]
                ).__name__
                entry["attributes"]["exception.message"] = str(record.exc_info[1])

        return json.dumps(entry, default=str)


class OtelLogger:
    """
    Logger factory.

    Reads ``SERVICE_NAME`` and ``SERVICE_VERSION`` from the environment so
    each deployed service gets a correctly labelled logger without code changes.

    Example::

        from utils.otel_logger import OtelLogger
        logger = OtelLogger().create_logger()
        logger.info("pipeline step", extra={"step": "adaptor"})

    .. note::
        The class was previously called ``Logging`` in ``utils/logging.py``.
        It has been renamed to ``OtelLogger`` in ``utils/otel_logger.py`` to
        avoid shadowing Python's standard-library ``logging`` module.
    """

    _DEFAULT_SERVICE_NAME: str = "data-pipeline"
    _DEFAULT_SERVICE_VERSION: str = "1.0.0"

    def create_logger(
        self,
        name: str | None = None,
        level: int = logging.INFO,
    ) -> logging.Logger:
        """
        Create and return a configured :class:`logging.Logger`.

        Parameters
        ----------
        name:
            Logger name.  Defaults to the ``SERVICE_NAME`` env var.
        level:
            Minimum logging level.  Defaults to ``INFO``.

        Returns
        -------
        logging.Logger
            Emits OTel-compatible JSON to stdout.
        """
        service_name: str = os.getenv(
            "SERVICE_NAME", self._DEFAULT_SERVICE_NAME
        )
        service_version: str = os.getenv(
            "SERVICE_VERSION", self._DEFAULT_SERVICE_VERSION
        )
        logger_name: str = name or service_name

        logger = logging.getLogger(logger_name)

        # Guard against duplicate handlers when the logger already exists
        if logger.handlers:
            return logger

        logger.setLevel(level)
        logger.propagate = False

        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(_OTelJsonFormatter(service_name, service_version))
        logger.addHandler(handler)

        return logger


# ---------------------------------------------------------------------------
# Backward-compatibility alias
# Keep ``Logging`` importable in case any external code still references it.
# ---------------------------------------------------------------------------
Logging = OtelLogger
