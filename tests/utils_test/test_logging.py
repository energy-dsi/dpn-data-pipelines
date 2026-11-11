# Copyright 2026 DSI Project – Apache 2.0 License
"""
Tests for utils/logging.py (the plain-stdlib ``Logging`` helper).

Note: ``Logging.create_logger`` always resolves to the SAME module-level
logger object via ``logging.getLogger(__name__)`` (name == "utils.logging"),
because it does not accept a ``name`` argument the way
``utils.otel_logger.OtelLogger.create_logger`` does. Every test therefore
resets that logger's handlers first so tests don't leak state into each
other.
"""
from __future__ import annotations

import logging

import pytest

from utils.logging import Logging


LOGGER_NAME = "utils.logging"


@pytest.fixture(autouse=True)
def _reset_module_logger():
    """Ensure each test starts from a handler-less logger."""
    logger = logging.getLogger(LOGGER_NAME)
    logger.handlers.clear()
    logger.setLevel(logging.NOTSET)
    yield
    logger.handlers.clear()
    logger.setLevel(logging.NOTSET)


class TestCreateLogger:
    def test_returns_a_logger_instance(self):
        logger = Logging().create_logger()
        assert isinstance(logger, logging.Logger)

    def test_logger_name_matches_module(self):
        logger = Logging().create_logger()
        assert logger.name == LOGGER_NAME

    def test_sets_level_info_when_no_handlers_present(self):
        logger = Logging().create_logger()
        assert logger.level == logging.INFO

    def test_adds_exactly_one_stream_handler(self):
        logger = Logging().create_logger()
        assert len(logger.handlers) == 1
        assert isinstance(logger.handlers[0], logging.StreamHandler)

    def test_handler_formatter_matches_expected_pattern(self):
        logger = Logging().create_logger()
        formatter = logger.handlers[0].formatter
        assert formatter is not None
        assert formatter._fmt == "%(asctime)s | %(levelname)s | %(name)s | %(message)s"

    def test_does_not_duplicate_handlers_on_repeated_calls(self):
        Logging().create_logger()
        Logging().create_logger()
        logger = Logging().create_logger()

        assert len(logger.handlers) == 1

    def test_existing_handlers_and_level_are_left_untouched(self):
        logger = logging.getLogger(LOGGER_NAME)
        logger.setLevel(logging.DEBUG)
        sentinel_handler = logging.NullHandler()
        logger.addHandler(sentinel_handler)

        result = Logging().create_logger()

        # The "if not logger.handlers" guard means neither the level nor
        # the handler list is touched when a handler already exists.
        assert result.level == logging.DEBUG
        assert result.handlers == [sentinel_handler]

    def test_returned_logger_can_log_without_raising(self, capsys):
        logger = Logging().create_logger()
        logger.info("hello from test")
        captured = capsys.readouterr()
        assert "hello from test" in captured.err or "hello from test" in captured.out
