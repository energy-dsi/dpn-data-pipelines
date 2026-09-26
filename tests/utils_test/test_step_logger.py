import pytest
from unittest.mock import MagicMock

from utils.step_logger import StepLogger, _StatsD


# ----------------------------------------------------------------------
# Dummy PipelineContext
# ----------------------------------------------------------------------

class DummyCtx:
    def __init__(self):
        self.run_id = "1234567890"
        self.pipeline_stage = "stage"
        self.pipeline_type = "type"
        self.pipeline_role = "role"
        self.mode = "automatic"

    def as_log_extra(self):
        return {"ctx": "value"}


# ----------------------------------------------------------------------
# StatsD
# ----------------------------------------------------------------------

def test_statsd_no_host(monkeypatch):
    monkeypatch.delenv("STATSD_HOST", raising=False)

    statsd = _StatsD()
    assert statsd._sock is None

    # should not raise
    statsd.counter("test.metric")
    statsd.gauge("test.metric", 1.0)


def test_statsd_send_success(monkeypatch):
    monkeypatch.setenv("STATSD_HOST", "localhost")

    mock_socket = MagicMock()
    monkeypatch.setattr("socket.socket", lambda *a, **k: mock_socket)

    statsd = _StatsD()
    statsd.counter("metric", 1)

    assert mock_socket.sendto.called


def test_statsd_send_failure(monkeypatch):
    monkeypatch.setenv("STATSD_HOST", "localhost")

    mock_socket = MagicMock()
    mock_socket.sendto.side_effect = OSError

    monkeypatch.setattr("socket.socket", lambda *a, **k: mock_socket)

    statsd = _StatsD()

    # should not raise
    statsd.gauge("metric", 1.23)


def test_format_tags():
    tags = {"a": 1, "b": 2}
    result = _StatsD._format_tags(tags)
    assert "|#" in result
    assert "a:1" in result


def test_format_tags_none():
    assert _StatsD._format_tags(None) == ""


# ----------------------------------------------------------------------
# StepLogger
# ----------------------------------------------------------------------

@pytest.fixture
def logger():
    return MagicMock()


@pytest.fixture
def step_logger(logger):
    return StepLogger(logger)


@pytest.fixture
def ctx():
    return DummyCtx()


def test_step_start(step_logger, ctx):
    step_logger._statsd = MagicMock()

    step_logger.step_start(ctx, "op")

    assert len(step_logger._timers) == 1
    assert step_logger._statsd.counter.called


def test_step_start_with_extra(step_logger, ctx):
    step_logger._statsd = MagicMock()

    step_logger.step_start(ctx, "op", extra={"x": 1})

    assert step_logger._statsd.counter.called


def test_step_end(step_logger, ctx):
    step_logger._statsd = MagicMock()

    step_logger.step_start(ctx, "op")
    step_logger.step_end(ctx, "op")

    assert step_logger._statsd.counter.called
    assert step_logger._statsd.gauge.called


def test_step_end_without_start(step_logger, ctx):
    step_logger._statsd = MagicMock()

    step_logger.step_end(ctx, "op")

    # duration should be 0
    assert step_logger._statsd.gauge.called


def test_step_failed(step_logger, ctx):
    step_logger._statsd = MagicMock()

    step_logger.step_start(ctx, "op")

    step_logger.step_failed(ctx, "op", Exception("fail"))

    assert step_logger._statsd.counter.called
    assert step_logger._statsd.gauge.called


def test_step_failed_without_start(step_logger, ctx):
    step_logger._statsd = MagicMock()

    step_logger.step_failed(ctx, "op", Exception("fail"))

    assert step_logger._statsd.gauge.called


def test_step_skipped(step_logger, ctx):
    step_logger._statsd = MagicMock()

    step_logger.step_skipped(ctx)

    assert step_logger._statsd.counter.called


def test_step_skipped_with_extra(step_logger, ctx):
    step_logger._statsd = MagicMock()

    step_logger.step_skipped(ctx, extra={"a": 1})

    assert step_logger._statsd.counter.called


def test_pipeline_banner(step_logger, ctx):
    step_logger.pipeline_banner(ctx, "service", {"a": 1})

    assert step_logger._logger.info.called


# ----------------------------------------------------------------------
# Internal helpers
# ----------------------------------------------------------------------

def test_build_key(step_logger, ctx):
    key = step_logger._build_key(ctx, "op")
    assert ctx.run_id in key


def test_build_tags(step_logger, ctx):
    tags = step_logger._build_tags(ctx, "op")

    assert tags["stage"] == "stage"
    assert tags["operation"] == "op"


def test_compute_duration_with_timer(step_logger, ctx):
    step_logger.step_start(ctx, "op")
    duration = step_logger._compute_duration(ctx, "op")

    assert duration >= 0


def test_compute_duration_without_timer(step_logger, ctx):
    duration = step_logger._compute_duration(ctx, "op")

    assert duration == 0.0
