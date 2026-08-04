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
# | 1.0.0   | Initial version                                          | DSI Team      | 2026-06-26  |
# +---------+----------------------------------------------------------+---------------+-------------+
import json
import logging
import os
import sys
from datetime import datetime, timezone

# Optional OTel integration – safe to import even when OTel SDK is absent
try:
    from opentelemetry import trace as _otel_trace  # type: ignore[import]
    from opentelemetry.sdk._logs import LoggerProvider  # type: ignore[import]
    from opentelemetry.sdk.resources import Resource, SERVICE_NAME, SERVICE_VERSION  # type: ignore[import]
    from opentelemetry.exporter.otlp.proto.grpc._log_exporter import OTLPLogExporter  # type: ignore[import]
    from opentelemetry._logs import set_logger_provider  # type: ignore[import]
    from opentelemetry.sdk._logs import LoggingHandler  # type: ignore[import]
    from opentelemetry.sdk._logs.export import SimpleLogRecordProcessor

    _OTEL_AVAILABLE = True
except ImportError:  # pragma: no cover
    _OTEL_AVAILABLE = False


# ✅ Severity mapping (global)
_STDLIB_KEYS = frozenset({
    "name", "msg", "args", "levelname", "levelno",
    "pathname", "filename", "module", "exc_info",
    "exc_text", "stack_info", "lineno", "funcName",
    "created", "msecs", "relativeCreated", "thread",
    "threadName", "processName", "process", "message",
    "taskName",
})

SEVERITY_MAP = {
    logging.DEBUG: (5, "DEBUG"),
    logging.INFO: (9, "INFO"),
    logging.WARNING: (13, "WARN"),
    logging.ERROR: (17, "ERROR"),
    logging.CRITICAL: (21, "FATAL"),
}


# ✅ Formatter (standalone class ✅ good standard)
class OTelJsonFormatter(logging.Formatter):

    def __init__(self, service_name: str, service_version: str):
        super().__init__()
        self.service_name = service_name
        self.service_version = service_version

    def format(self, record: logging.LogRecord) -> str:

        now = datetime.now(timezone.utc).isoformat()
        sev_num, sev_text = SEVERITY_MAP.get(record.levelno, (9, "INFO"))

        log_entry = {
            "timestamp": now,
            "observed_timestamp": now,
            "severity_number": sev_num,
            "severity_text": sev_text,
            "body": record.getMessage(),
            "resource": {
                "service.name": self.service_name,
                "service.version": self.service_version,
            },
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
                log_entry["trace_id"] = format(ctx.trace_id, "032x")
                log_entry["span_id"] = format(ctx.span_id, "016x")
                log_entry["trace_flags"] = format(ctx.trace_flags, "02x")


        # ✅ include extra fields
        for key, value in record.__dict__.items():
            if key not in (
                "name", "msg", "args", "levelname", "levelno",
                "pathname", "filename", "module", "exc_info",
                "exc_text", "stack_info", "lineno", "funcName",
                "created", "msecs", "relativeCreated", "thread",
                "threadName", "processName", "process", "message"
            ):
                log_entry["attributes"][key] = value


        # Merge caller-supplied ``extra`` fields into attributes
        for key, value in record.__dict__.items():
            if key not in _STDLIB_KEYS:
                log_entry["attributes"][key] = value

        # Attach exception details when present
        if record.exc_info:
            log_entry["attributes"]["exception.stacktrace"] = self.formatException(
                record.exc_info
            )
            if record.exc_info[1]:
                log_entry["attributes"]["exception.type"] = type(
                    record.exc_info[1]
                ).__name__
                log_entry["attributes"]["exception.message"] = str(record.exc_info[1])

        return json.dumps(log_entry, default=str)


# ✅ Main class (clean)
class OtelLogger:

    _initialized = False

    def create_logger(self, name: str | None = None) -> logging.Logger:

        service_name = os.getenv("SERVICE_NAME", "data-pipeline")
        service_version = os.getenv("SERVICE_VERSION", "1.0.0")
        endpoint = os.getenv("OTEL_EXPORTER_OTLP_ENDPOINT", "dpn-otel-collector.ns-dpn-health-01.svc.cluster.local:4317")

        # ✅ INIT ONCE
        if not OtelLogger._initialized:

            provider = LoggerProvider(
                resource=Resource.create({
                    "service.name": service_name,
                    "service.version": service_version,
                })
            )

            exporter = OTLPLogExporter(endpoint=endpoint, insecure=True)

            provider.add_log_record_processor(
                SimpleLogRecordProcessor(exporter)
            )

            set_logger_provider(provider)

            # Prevent the OTel SDK/exporter's own diagnostic logs (e.g. export
            # failures from opentelemetry.exporter.otlp.*, grpc) from being
            # captured by the root handler below. Without this, a failed
            # export logs an error, that error gets re-exported, fails again,
            # and so on -- tripping the log processor's recursive-loop guard
            # and silently dropping real application logs.
            logging.getLogger("opentelemetry").propagate = False
            logging.getLogger("grpc").propagate = False

            root_logger = logging.getLogger()
            root_logger.setLevel(logging.INFO)
            root_logger.addHandler(LoggingHandler(logger_provider=provider))

            # BatchLogRecordProcessor exports on its own background thread,
            # so without this the last batch of logs (e.g. the pipeline's
            # final "completed" status) can be lost when these short-lived
            # scripts exit right after run() returns.
            import atexit
            atexit.register(provider.shutdown)

            OtelLogger._initialized = True
            print("✅ OTEL initialized")

        # ✅ CREATE LOGGER
        logger = logging.getLogger(name or service_name)
        logger.setLevel(logging.INFO)

        # ✅ STDOUT handler
        if not any(type(h) is logging.StreamHandler for h in logger.handlers):
            handler = logging.StreamHandler(sys.stdout)
            handler.setFormatter(OTelJsonFormatter(service_name, service_version))
            logger.addHandler(handler)

        # ✅ critical for OTEL
        logger.propagate = True

        return logger


# ✅ alias
Logging = OtelLogger