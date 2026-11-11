# Copyright 2026 DSI Project – Apache 2.0 License
"""
Tests for utils/exception_handler.py (HandleExceptions).

The module's real, current API (as of the 1.0.0 version in the file's own
changelog table) is:

    HandleExceptions(logger: Optional[logging.Logger] = None)
        - logger=None  -> builds a default logger via
          ``utils.otel_logger.Logging().create_logger()`` (imported into
          this module's namespace as ``Logging``)
        - logger=<obj> -> uses the supplied logger as-is

    handle_storage_exception(exc, provider) -> None
        Logs "Storage operation failed" at ERROR with
        extra={"exception.provider", "exception.type", "exception.message"}
        and exc_info=True. ``provider`` is stripped; falsy -> "Unknown".

    handle_kafka_exception(exc, topic) -> None
        Logs "Kafka operation failed" at ERROR with
        extra={"exception.type", "exception.message", "kafka.topic"}
        and exc_info=True.

This replaces a stale, fully-commented-out draft that targeted a very
different (and no-longer-existing) API: per-provider/per-status-code
branching (409/403/404/429/503/418), ``"utils.logging.Logging.create_logger"``
as the patch target, and printf-style log calls with no ``extra``/
``exc_info`` kwargs. None of that matches the current module.
"""
from __future__ import annotations

from unittest.mock import MagicMock, patch

from utils.exception_handler import HandleExceptions


# ---------------------------------------------------------------------------
# Construction / logger wiring
# ---------------------------------------------------------------------------
class TestConstruction:
    def test_default_logger_created_when_none_supplied(self):
        mock_logger = MagicMock()
        with patch("utils.exception_handler.Logging") as mock_logging_cls:
            mock_logging_cls.return_value.create_logger.return_value = mock_logger
            handler = HandleExceptions()

        mock_logging_cls.return_value.create_logger.assert_called_once_with()
        assert handler._logger is mock_logger

    def test_supplied_logger_is_used_as_is(self):
        mock_logger = MagicMock()
        with patch("utils.exception_handler.Logging") as mock_logging_cls:
            handler = HandleExceptions(logger=mock_logger)

        mock_logging_cls.assert_not_called()
        assert handler._logger is mock_logger


# ---------------------------------------------------------------------------
# handle_storage_exception
# ---------------------------------------------------------------------------
class TestHandleStorageException:
    def setup_method(self):
        self.mock_logger = MagicMock()
        self.handler = HandleExceptions(logger=self.mock_logger)

    def test_logs_structured_fields(self):
        exc = ValueError("boom")

        self.handler.handle_storage_exception(exc, "Azure")

        self.mock_logger.error.assert_called_once_with(
            "Storage operation failed",
            extra={
                "exception.provider": "Azure",
                "exception.type": "ValueError",
                "exception.message": "boom",
            },
            exc_info=True,
        )

    def test_provider_label_is_stripped(self):
        self.handler.handle_storage_exception(RuntimeError("x"), "  AWS S3  ")

        extra = self.mock_logger.error.call_args.kwargs["extra"]
        assert extra["exception.provider"] == "AWS S3"

    def test_empty_provider_defaults_to_unknown(self):
        self.handler.handle_storage_exception(RuntimeError("x"), "")

        extra = self.mock_logger.error.call_args.kwargs["extra"]
        assert extra["exception.provider"] == "Unknown"

    def test_none_provider_defaults_to_unknown(self):
        self.handler.handle_storage_exception(RuntimeError("x"), None)  # type: ignore[arg-type]

        extra = self.mock_logger.error.call_args.kwargs["extra"]
        assert extra["exception.provider"] == "Unknown"

    def test_exc_info_is_always_true(self):
        self.handler.handle_storage_exception(KeyError("missing"), "GCP")

        assert self.mock_logger.error.call_args.kwargs["exc_info"] is True


# ---------------------------------------------------------------------------
# handle_kafka_exception
# ---------------------------------------------------------------------------
class TestHandleKafkaException:
    def setup_method(self):
        self.mock_logger = MagicMock()
        self.handler = HandleExceptions(logger=self.mock_logger)

    def test_logs_structured_fields(self):
        exc = RuntimeError("fail")

        self.handler.handle_kafka_exception(exc, "my-topic")

        self.mock_logger.error.assert_called_once_with(
            "Kafka operation failed",
            extra={
                "exception.type": "RuntimeError",
                "exception.message": "fail",
                "kafka.topic": "my-topic",
            },
            exc_info=True,
        )

    def test_topic_passed_through_unmodified(self):
        self.handler.handle_kafka_exception(RuntimeError("x"), "dpn-pipeline-control")

        extra = self.mock_logger.error.call_args.kwargs["extra"]
        assert extra["kafka.topic"] == "dpn-pipeline-control"
